# Event Loop Optimization Design for Multi-Threaded Coroutine Executors

## Status: DESIGN PROPOSAL (v2)

## Core Constraint

Modern coroutine runtimes (Tokio, Asio, folly::coro, libunifex, Go runtime)
run M coroutines across N OS threads with work-stealing. A coroutine that
writes to a pipe on thread 3 may have its consumer running on thread 7 — or
the consumer may migrate to thread 3 mid-flight, or both may be on thread 3
now and different threads next tick.

**This means:**
- Producer and consumer are always potentially on different OS threads
- All atomic operations in the SPSC protocol must be retained
- "Wakeup" is not a same-thread callback — it is a cross-thread task
  submission to an executor, which itself costs ~50-200ns
- Under high load, redundant wakeups compound: if N flushes each submit
  a task, the executor processes N scheduling events for what should be
  one drain loop

The design must make the common case fast without assuming colocation, and
must degrade gracefully under contention rather than amplifying it.

---

## Problem Statement

libzmq's architecture imposes five categories of overhead that become
dominant under a high-throughput event loop with many coroutine executors:

### 1. Signaler syscall overhead (~1000ns per transition)

Every pipe flush where the reader is asleep triggers `write()` to an
eventfd/pipe (`signaler_t::send`, signaler.cpp:146-201). Every wakeup
triggers `read()` from it (`signaler_t::recv`, signaler.cpp:275-309).
These are full kernel transitions. Under load with 1000 pipes flushing
per millisecond, this is 2000 syscalls/ms — ~2 million/sec of pure
kernel overhead.

An executor-based notification can replace these with a thread-safe task
submission (~50-200ns cross-thread, ~5-10ns same-thread), but only if
redundant submissions are suppressed.

### 2. Mailbox mutex serialization

`mailbox_t::send()` (mailbox.cpp:32-40) acquires `_sync` mutex to
serialize writers into the SPSC ypipe. Under fan-in (many coroutines
sending commands to one I/O thread), this mutex becomes a convoy:

```
Coroutine A: lock → write → flush → unlock     (holds lock ~30-50ns)
Coroutine B: spin-wait for lock...              (wastes ~30-50ns)
Coroutine C: spin-wait for lock...              (wastes ~60-100ns)
```

With 100 coroutines funneling commands to one I/O thread, the tail
latency is 100x the single-operation cost. A lock-free MPSC queue
reduces the push cost to a single atomic exchange (~5-8ns) with zero
convoy effects.

### 3. False sharing in data structure layouts

`yqueue_t` (yqueue.hpp:172-177) lays out reader fields (`_begin_chunk`,
`_begin_pos`) contiguously with writer fields (`_back_chunk`, `_back_pos`,
`_end_chunk`, `_end_pos`). On a 64-byte cache line, these share a line.
Every writer push invalidates the reader's cached `_begin_chunk`, and
every reader pop invalidates the writer's cached `_end_chunk`.

With coroutines on different physical cores (the common case under
work-stealing), this causes cache line bouncing at the L3/interconnect
level: ~40-80ns per invalidation on modern multi-socket systems.

`ypipe_t` (ypipe.hpp:150-172) has the same problem: `_w`, `_r`, `_f`,
`_c` are 32 bytes contiguous with no alignment control.

### 4. Per-message modulo in backpressure

`pipe_t::read()` (pipe.cpp:201) checks `_msgs_read % _lwm == 0` on
every complete message. Integer modulo by a non-power-of-2 is a
division: ~20-40 cycles. At 10M msgs/sec this is 200-400M wasted cycles/sec.

### 5. Wakeup amplification under fan-out

A PUB socket flushing to 100 subscriber pipes triggers 100 independent
`send_activate_read` commands, each going through the mailbox + signaler
path. Under an executor model, this becomes 100 cross-thread task
submissions in rapid succession. The executor's work-stealing queue
becomes a bottleneck, and the consumer thread processes 100 scheduling
events to drain what could be handled in a single batch.

---

## Current Architecture: What Works and What Doesn't

### What works well (retain as-is)

**The ypipe_t SPSC CAS protocol** (ypipe.hpp:76-122) is near-optimal
for cross-thread single-producer single-consumer queues:
- One CAS per flush batch (amortized across N writes)
- Reader CAS sets `_c = NULL` to signal sleep — this is the dedup
  mechanism that prevents redundant wakeups
- Writer detects `_c == NULL` and knows to notify exactly once

**The yqueue_t chunk allocator** (yqueue.hpp:78-97) amortizes malloc
overhead by 1/N and reuses one spare chunk via atomic exchange. For
message pipes (N=256, chunk=16KB), this is excellent.

**The msg_t layout** (msg.hpp:148-156) at exactly 64 bytes (one cache
line) with VSM inline storage is optimal. Sequential access through
yqueue chunks gives excellent hardware prefetch behavior.

### What doesn't work

**The notification layer** (signaler + mailbox mutex) was designed for a
world where threads own pollers and block in `epoll_wait()`. An executor
doesn't block in epoll — it runs a task queue. The signaler's eventfd
was the right way to wake a blocked OS thread; it's the wrong way to
schedule a coroutine.

**The command path** (object.cpp:520-522 → ctx.cpp:642-644 →
mailbox.cpp:32-40) forces every inter-object notification through a
centralized mutex + signaler bottleneck, even for high-frequency
operations like `activate_read`/`activate_write` that happen on every
pipe flush and every LWM crossing.

**The timer implementation** (poller_base.cpp:54-92) uses `std::multimap`
with O(log N) insertion and requires `clock_t::now_ms()` — a syscall or
VDSO call — on every operation. Under an executor, timers should
integrate with the executor's own timer wheel.

---

## Proposed Design

### Design Principle

Replace the **notification and dispatch layer** while retaining the
**data structures and lock-free protocols**. The ypipe CAS protocol and
yqueue chunk allocator are good. The signaler, mailbox mutex, and rigid
thread-to-poller binding are what need to change.

Every optimization must be correct under the assumption that producer
and consumer are on different OS threads. Same-thread execution is a
welcome fast-path, not a design requirement.

### 1. Executor-Abstract Notifier: `i_notifier_t`

**Goal**: Replace the signaler with a pluggable, thread-safe, idempotent
notification mechanism that works with any executor.

```cpp
// src/i_notifier.hpp
//
// Thread-safe, idempotent notification interface.
// Implementations must guarantee:
//   1. notify() is safe to call from any thread concurrently
//   2. Multiple notify() calls before the consumer acts collapse
//      into a single wakeup (idempotency)
//   3. The consumer observes all data written before notify()
//      (acquire-release ordering)

class i_notifier_t
{
  public:
    virtual ~i_notifier_t() = default;

    // Called by producer thread(s) to signal that work is available.
    // Must be thread-safe. Must be idempotent: calling notify() 10
    // times before the consumer runs has the same effect as calling
    // it once.
    virtual void notify() = 0;

    // Called by consumer to arm for next notification. After this
    // call, the next notify() will trigger a wakeup. Between arm()
    // and the next notify(), additional notify() calls are no-ops.
    // Must be called from the consumer context only.
    virtual void arm() = 0;
};
```

**Why idempotent + arm/notify instead of signal/wait:**

The existing ypipe `_c` pointer protocol already implements exactly this
pattern at the data level:
- `_c = NULL` means "reader is sleeping" (armed)
- `flush()` CAS detects this and returns false exactly once (notify)
- Subsequent flushes before the reader wakes see `_c != NULL` (no-op)

The `i_notifier_t` generalizes this to the notification transport layer,
replacing the signaler's eventfd with an executor-appropriate mechanism.

**Implementations:**

```cpp
// For legacy poller integration (epoll/kqueue/select)
class signaler_notifier_t : public i_notifier_t
{
    signaler_t _signaler;
    std::atomic<bool> _armed{true};

    void notify() override {
        // Only send if transition from armed to notified.
        // This is the key: one eventfd write per batch, not per flush.
        if (_armed.exchange(false, std::memory_order_acq_rel)) {
            _signaler.send();
        }
    }

    void arm() override {
        _signaler.recv_failable();  // drain
        _armed.store(true, std::memory_order_release);
    }
};

// For executor-based event loops (Asio, Tokio, io_uring, etc.)
class executor_notifier_t : public i_notifier_t
{
    using post_fn_t = void (*)(void *executor_ctx);

    post_fn_t _post;          // executor's thread-safe post function
    void *_executor_ctx;
    std::atomic<bool> _armed{true};

    void notify() override {
        if (_armed.exchange(false, std::memory_order_acq_rel)) {
            _post(_executor_ctx);  // cross-thread task submission
        }
        // If already notified, this is a no-op — no redundant
        // task submissions, no wakeup amplification.
    }

    void arm() override {
        _armed.store(true, std::memory_order_release);
    }
};
```

**Critical property — wakeup suppression under load:**

Under steady-state high throughput, the pattern is:
```
Writer 1: write, write, write, flush → notify() → _armed was true → post task
Writer 2: write, flush → notify() → _armed is false → NO-OP
Writer 3: write, flush → notify() → _armed is false → NO-OP
...
Consumer runs (on some executor thread):
  arm()         → _armed = true, ready for next batch
  drain_all()   → reads everything from all writers
  [goes idle or processes other tasks]

Writer 4: write, flush → notify() → _armed was true → post task (new batch)
```

Under load: 1 task submission per drain cycle, regardless of how many
writers flush between drains. Under light load: 1 task submission per
message (no batching needed since messages are infrequent).

This **adapts automatically** to load without tuning constants.

### 2. Lock-Free MPSC Mailbox: `mailbox_mpsc_t`

**Goal**: Eliminate the mutex that serializes command producers into the
SPSC ypipe.

The current mailbox (mailbox.cpp:32-40) wraps a single-producer ypipe
with a mutex to support multiple producers. This converts a lock-free
data structure into a lock-based one. Under fan-in from many executor
threads, the mutex creates a convoy.

Replace with an intrusive MPSC queue (Vyukov design) that is
wait-free for producers:

```cpp
// src/mailbox_mpsc.hpp
//
// Lock-free MPSC mailbox. Producers (any thread) push commands
// with a single atomic exchange. Consumer (one thread/coroutine)
// pops in FIFO order.

class mailbox_mpsc_t : public i_mailbox
{
  public:
    mailbox_mpsc_t(i_notifier_t *notifier)
        : _notifier(notifier)
    {
        _stub.next.store(nullptr, std::memory_order_relaxed);
        _head.store(&_stub, std::memory_order_relaxed);
        _tail = &_stub;
    }

    // Producer: lock-free push. O(1). Any thread.
    // Cost: one atomic exchange (~5-8ns) + conditional notify.
    void send(const command_t &cmd_) override
    {
        node_t *n = _pool.allocate();  // thread-local pool
        n->cmd = cmd_;
        n->next.store(nullptr, std::memory_order_relaxed);

        // Swing head to new node. This is the linearization point.
        node_t *prev = _head.exchange(n, std::memory_order_acq_rel);
        // Link previous head to new node. Consumer spins briefly
        // if it reaches this node before the store is visible
        // (~1 cycle window on x86, wider on ARM).
        prev->next.store(n, std::memory_order_release);

        // Notify consumer. Idempotent: only the first notify()
        // after arm() actually posts a task.
        _notifier->notify();
    }

    // Consumer: drain all available commands. Single-threaded.
    int recv(command_t *cmd_, int timeout_) override
    {
        node_t *n = pop();
        if (n) {
            *cmd_ = n->cmd;
            _pool.deallocate(n);
            return 0;
        }

        // No commands available. Arm the notifier so the next
        // send() will wake us.
        _notifier->arm();

        // Double-check after arming to close the race where
        // send() happened between our pop() and arm().
        n = pop();
        if (n) {
            *cmd_ = n->cmd;
            _pool.deallocate(n);
            return 0;
        }

        if (timeout_ == 0) {
            errno = EAGAIN;
            return -1;
        }

        // Blocking wait is handled by the executor: the notifier
        // will post a task when commands arrive. Timeout is
        // handled by the executor's timer facility.
        errno = EAGAIN;
        return -1;
    }

  private:
    struct node_t {
        std::atomic<node_t *> next;
        command_t cmd;
    };

    // Vyukov MPSC: head is producer-side (atomic exchange),
    // tail is consumer-side (plain pointer).
    alignas(64) std::atomic<node_t *> _head;
    alignas(64) node_t *_tail;
    node_t _stub;

    i_notifier_t *_notifier;
    pool_t<node_t> _pool;   // Per-thread freelist, see §2.1

    node_t *pop(); // Standard Vyukov MPSC pop, see below
};
```

#### 2.1 Node Pool Design

The MPSC queue requires per-node allocation. Under high load this would
be a malloc/free per command — unacceptable. We use a thread-local
freelist with bounded overflow to the global allocator:

```cpp
template <typename T>
class pool_t
{
  public:
    // Each thread keeps up to 64 nodes in its local freelist.
    // Overflow goes to a shared lock-free stack (Treiber stack)
    // which itself overflows to malloc/free.
    static constexpr int local_capacity = 64;
    static constexpr int shared_capacity = 1024;

    T *allocate()
    {
        // Fast path: thread-local freelist (~2ns)
        if (_local_count > 0)
            return _local[--_local_count];

        // Medium path: shared Treiber stack (~10-15ns)
        T *n = _shared.pop();
        if (n) return n;

        // Slow path: malloc (~50-200ns, amortized with batching)
        return static_cast<T *>(
            aligned_alloc(64, sizeof(T)));
    }

    void deallocate(T *n)
    {
        // Fast path: return to local freelist
        if (_local_count < local_capacity) {
            _local[_local_count++] = n;
            return;
        }

        // Overflow: return to shared stack
        if (!_shared.push(n))
            free(n);  // Shared stack full, release to OS
    }

  private:
    static thread_local T *_local[local_capacity];
    static thread_local int _local_count;
    static treiber_stack_t<T> _shared;
};
```

**Cost model**: Under steady state, nodes cycle through the thread-local
freelist. Allocation cost is ~2ns (array index decrement + pointer load).
The Treiber stack handles thread-to-thread migration of nodes without
malloc. Only sustained imbalance hits malloc.

### 3. Cache-Line Partitioned Data Structures

**Goal**: Eliminate false sharing between reader and writer fields across
all data structures. This is critical when coroutines migrate between
cores via work-stealing.

#### 3.1 yqueue_t layout

Current (yqueue.hpp:172-182):
```
_begin_chunk    8B  ← reader
_begin_pos      4B  ← reader
_back_chunk     8B  ← writer    ← likely same cache line as _begin_*
_back_pos       4B  ← writer
_end_chunk      8B  ← writer
_end_pos        4B  ← writer
// padding (compiler-dependent)
_spare_chunk    8B  ← shared (atomic)
```

Proposed:
```cpp
template <typename T, int N>
class yqueue_t
{
    // Reader fields: only touched by pop()/front()
    alignas(64) chunk_t *_begin_chunk;
    int _begin_pos;
    // 52 bytes padding to fill cache line

    // Writer fields: only touched by push()/back()/unpush()
    alignas(64) chunk_t *_back_chunk;
    int _back_pos;
    chunk_t *_end_chunk;
    int _end_pos;
    // 32 bytes padding to fill cache line

    // Shared: atomic exchange between reader (pop) and writer (push)
    alignas(64) atomic_ptr_t<chunk_t> _spare_chunk;
};
```

**Cost**: 192 bytes per yqueue instead of ~52 bytes. One per pipe
(negligible vs. chunk allocations).

**Benefit**: Reader pop() and writer push() never invalidate each
other's cache lines. The spare_chunk atomic exchange is the only
cross-core traffic, and it happens once per N operations (N=256 for
message pipes).

#### 3.2 ypipe_t layout

Current (ypipe.hpp:150-172):
```
yqueue_t _queue  [variable]
T *_w            8B  ← writer
T *_r            8B  ← reader    ← same cache line as _w
T *_f            8B  ← writer
atomic_ptr_t _c  8B  ← shared   ← same cache line as _r
```

Proposed:
```cpp
template <typename T, int N>
class ypipe_t : public ypipe_base_t<T>
{
  protected:
    yqueue_t<T, N> _queue;

    // Writer-only: touched on every write() and flush()
    alignas(64) T *_w;
    T *_f;

    // Reader-only: touched on every check_read()
    alignas(64) T *_r;

    // Shared: the single point of contention. Own cache line
    // ensures CAS doesn't invalidate reader or writer state.
    alignas(64) atomic_ptr_t<T> _c;
};
```

**Benefit**: The CAS on `_c` in `flush()` and `check_read()` no longer
bounces the cache lines containing `_w`/`_f` or `_r`. Under cross-core
execution this eliminates ~40-80ns of coherency latency per flush/read
cycle.

#### 3.3 command_t alignment

`command_t` (command.hpp:186-193) is already aligned to cache line size
on POSIX systems via `__attribute__((aligned(ZMQ_CACHELINE_SIZE)))`.
This is correct and should be retained.

### 4. Threshold-Based Backpressure

**Goal**: Replace per-message integer division with a branch-predicted
comparison.

Current (pipe.cpp:201):
```cpp
if (_lwm > 0 && _msgs_read % _lwm == 0)
    send_activate_write(_peer, _msgs_read);
```

Proposed:
```cpp
// In pipe_t constructor, and after each HWM reconfiguration:
_next_activate_threshold = _lwm;

// In pipe_t::read():
if (unlikely(_msgs_read >= _next_activate_threshold)) {
    _next_activate_threshold += _lwm;
    send_activate_write(_peer, _msgs_read);
}
```

**Why this matters**: Integer division/modulo by a non-power-of-2 value
compiles to a `mul` + shift sequence on x86 (~20-40 cycles). A comparison
is 1 cycle. The branch predictor correctly predicts "not taken" with
>99% accuracy (taken once per LWM messages). At 10M msgs/sec, this
saves ~200-400 million cycles/sec.

**No behavioral change**: The activation fires at exactly the same
message counts. The only difference is the computation method.

### 5. Integrated Notification: Pipe Flush Without Mailbox

**Goal**: For the highest-frequency commands (`activate_read`,
`activate_write`), bypass the mailbox entirely and use the notifier
directly.

Currently, every pipe flush that finds the reader asleep triggers:
1. `pipe_t::flush()` calls `send_activate_read(_peer)` (pipe.cpp:256)
2. `object_t::send_command(cmd)` (object.cpp:520-522)
3. `ctx_t::send_command(tid, cmd)` (ctx.cpp:642-644)
4. `_slots[tid]->send(cmd)` → mailbox mutex + ypipe + signaler

This is 4 layers of indirection for a 1-bit signal ("you have data").
The ypipe's `_c` pointer already carries this information atomically.
What's missing is the notification to the consumer's executor.

Proposed: attach the notifier directly to the pipe, not to the mailbox:

```cpp
class pipe_t
{
    // ...existing members...

    // Notifier for the read side. When flush() returns false
    // (reader sleeping), notify directly instead of routing
    // through mailbox.
    i_notifier_t *_read_notifier;   // set by consumer side
    i_notifier_t *_write_notifier;  // set by producer side

    void flush()
    {
        if (_state == term_ack_sent)
            return;

        if (_out_pipe && !_out_pipe->flush()) {
            // Reader is sleeping. Notify directly.
            if (_read_notifier) {
                _read_notifier->notify();
            } else {
                // Fallback: legacy path through mailbox
                send_activate_read(_peer);
            }
        }
    }
};
```

**Benefit**: Eliminates the mailbox entirely for the hottest path.
The flush returns false (reader sleeping) → one atomic exchange on
the notifier's `_armed` flag → conditional task post to executor.
No mutex, no command serialization, no ypipe write for a command that
carries zero data.

**For activate_write** (backpressure signal, pipe.cpp:202): same
pattern, but through `_write_notifier`:

```cpp
bool pipe_t::read(msg_t *msg_)
{
    // ...existing read logic...

    if (unlikely(_msgs_read >= _next_activate_threshold)) {
        _next_activate_threshold += _lwm;
        if (_write_notifier) {
            _peers_msgs_read_cache = _msgs_read;
            _write_notifier->notify();
        } else {
            send_activate_write(_peer, _msgs_read);
        }
    }
    return true;
}
```

**The mailbox remains** for infrequent commands (bind, term, hiccup,
pipe_hwm, etc.) which don't need this optimization. Only
`activate_read` and `activate_write` — which account for >95% of
command traffic under load — bypass it.

### 6. Batch-Aware Consumer Drain Loop

**Goal**: When the consumer is notified, drain all available work across
all pipes before re-arming the notifier. This amortizes the notification
cost across many messages.

```cpp
// Consumer-side drain loop (replaces io_thread_t::in_event
// and socket_base_t::process_commands for executor mode)

class executor_consumer_t
{
    i_notifier_t *_notifier;
    std::vector<pipe_t *> _active_pipes;
    mailbox_mpsc_t *_mailbox;

    // Called by executor when notifier fires.
    void on_notify()
    {
        // Phase 1: Process all pending mailbox commands.
        // These are infrequent (bind, term, etc.).
        command_t cmd;
        while (_mailbox->recv(&cmd, 0) == 0) {
            cmd.destination->process_command(cmd);
        }

        // Phase 2: Drain all readable pipes.
        // Each pipe's ypipe may have many messages batched.
        for (auto *pipe : _active_pipes) {
            msg_t msg;
            while (pipe->read(&msg)) {
                process_message(pipe, &msg);
            }
        }

        // Phase 3: Re-arm for next notification.
        _notifier->arm();

        // Phase 4: Double-check after arming.
        // Handles the race where a writer notified between
        // our last read attempt and arm().
        bool more_work = false;
        if (_mailbox->recv(&cmd, 0) == 0) {
            cmd.destination->process_command(cmd);
            more_work = true;
        }
        for (auto *pipe : _active_pipes) {
            if (pipe->check_read()) {
                more_work = true;
                break;
            }
        }
        if (more_work) {
            // Re-enter drain loop without waiting for notification.
            // Post to executor to avoid stack growth.
            _notifier->notify();
        }
    }
};
```

**Key property — self-tuning batch size:**

Under low load: one message arrives → notify → drain 1 message → arm.
Latency-optimal: message is processed as soon as it arrives.

Under high load: thousands of messages arrive while consumer processes →
all are drained in one pass → arm → next batch. Throughput-optimal:
one notification per drain cycle, regardless of arrival rate.

This replaces the hardcoded constants `inbound_poll_rate = 100` and
`max_command_delay = 3,000,000` ticks (config.hpp:26,43) with adaptive
behavior that emerges from the arm/notify protocol.

### 7. Executor Integration Interface

**Goal**: Provide a minimal, stable interface that executor libraries
implement to integrate with libzmq's pipe and command infrastructure.

```cpp
// src/i_executor.hpp
//
// Minimal interface an executor must provide. Implementations
// exist for each supported runtime (Asio, Tokio FFI, raw epoll,
// io_uring, etc.).

class i_executor_t
{
  public:
    virtual ~i_executor_t() = default;

    // Post a callable to be executed on this executor.
    // Thread-safe. The callable will run on one of the executor's
    // worker threads. If the executor is single-threaded, it runs
    // on that thread.
    //
    // This is the ONLY cross-thread operation. Everything else
    // (arm, drain, process) happens within the executor's context.
    virtual void post(void (*fn)(void *), void *arg) = 0;

    // Create a notifier bound to this executor. When notify()
    // is called, the executor will eventually invoke the given
    // callback. The notifier handles dedup internally.
    virtual i_notifier_t *create_notifier(void (*fn)(void *),
                                          void *arg) = 0;
};
```

**Integration with existing poller:**

The existing `worker_poller_base_t` (poller_base.hpp:135-166) owns a
dedicated OS thread running `loop()`. This is itself an executor — a
single-threaded run loop. The `signaler_notifier_t` maps naturally
onto it: `notify()` writes to eventfd, `loop()` wakes from
`epoll_wait()`, drains, `arm()` reads from eventfd.

New executor backends plug in at the same level without changing the
pipe, ypipe, or yqueue code.

---

## Cost Model: Current vs Proposed

### Per-message costs (steady-state, cross-thread, inproc)

| Operation | Current | Proposed | Savings |
|---|---|---|---|
| ypipe::write + push | 5-10ns | 5-10ns | (unchanged) |
| ypipe::flush CAS | 15-25ns | 15-25ns | (unchanged, required for cross-thread) |
| Notification (reader sleeping) | ~1000ns (eventfd write) | ~8ns (atomic exchange, no-op if already notified) | 99.2% |
| Notification (reader awake) | 0ns (CAS succeeds) | 0ns (CAS succeeds) | (unchanged) |
| Mailbox for activate_read | ~1500ns (mutex+ypipe+signaler) | 0ns (direct notifier, bypassed) | 100% |
| ypipe::check_read CAS | 15-25ns | 15-25ns | (unchanged) |
| ypipe::read + pop | 5-10ns | 5-10ns | (unchanged) |
| Backpressure check | 20-40ns (modulo) | 1ns (comparison) | 97% |
| Mailbox for activate_write | ~1500ns (mutex+ypipe+signaler) | ~8ns (direct notifier) | 99.5% |
| False sharing penalty | ~40-80ns (per cross-core access) | 0ns (cache-line separated) | 100% |
| process_commands overhead | 100-500ns (RDTSC/counter) | ~2ns (pointer null check on mailbox) | 99% |

### Aggregate per-message (amortized over batch)

| Scenario | Current | Proposed |
|---|---|---|
| Inproc, cross-thread, reader awake | ~100-200ns | ~30-50ns |
| Inproc, cross-thread, reader sleeping | ~3000-5000ns | ~80-150ns |
| Inproc, same-thread, reader sleeping | ~3000-5000ns | ~30-60ns |
| Fan-out to 100 pipes, all sleeping | ~150,000ns | ~500ns (1 notify, batch drain) |

### Notification count under load

| Scenario | Current notifications/sec | Proposed notifications/sec |
|---|---|---|
| 1M msgs/sec, 1 pipe | ~1M signaler writes | ~10K-50K notifier posts (self-batching) |
| 1M msgs/sec, 100 pipes | ~100M signaler writes | ~10K-50K notifier posts (coalesced) |
| 10K msgs/sec, 1 pipe (light) | ~10K signaler writes | ~10K notifier posts (no batching) |

The key insight: under high load, the arm/notify protocol naturally
collapses O(messages) notifications into O(drain_cycles) notifications.
Under light load, it degrades to at most O(messages) — no worse than
current.

---

## Implementation Strategy

### Phase 1: Cache-Line Alignment (Low Risk, Immediate Benefit)

**Changes:**
- Add `alignas(64)` to reader/writer field groups in `yqueue_t`
- Add `alignas(64)` to `_w`/`_f`, `_r`, `_c` in `ypipe_t`
- Replace modulo with threshold comparison in `pipe_t::read()`
- Add `_next_activate_threshold` field to `pipe_t`

**Impact**: Zero API change. Zero behavioral change. Measurable
throughput improvement on multi-core systems (~10-30% for cross-thread
inproc benchmarks based on false-sharing elimination alone).

**Risk**: Increased struct sizes by ~128-192 bytes per pipe. Negligible
compared to per-chunk allocations (16KB each).

### Phase 2: Notifier Abstraction

**Changes:**
- Introduce `i_notifier_t` interface
- Implement `signaler_notifier_t` wrapping existing `signaler_t`
- Add `_read_notifier` / `_write_notifier` to `pipe_t`
- Modify `pipe_t::flush()` to use notifier when available
- Modify `pipe_t::read()` to use notifier for backpressure

**Impact**: Existing code paths unchanged when notifiers are NULL
(legacy mode). New code paths activated when an executor sets notifiers
on its pipes.

### Phase 3: MPSC Mailbox

**Changes:**
- Implement `mailbox_mpsc_t` with Vyukov MPSC queue
- Implement `pool_t<node_t>` with thread-local freelist
- Add as alternative `i_mailbox` implementation selectable at
  context creation time

**Impact**: Optional replacement for `mailbox_t`. Legacy path remains
default. Executor-mode contexts use `mailbox_mpsc_t`.

### Phase 4: Executor Integration

**Changes:**
- Introduce `i_executor_t` interface
- Implement `executor_notifier_t`
- Implement `executor_consumer_t` drain loop with arm/notify/drain cycle
- Create `executor_io_thread_t` as alternative to `io_thread_t`
  (no dedicated OS thread; uses executor's thread pool)

**Impact**: New context option `ZMQ_EXECUTOR` to provide an executor
implementation. When set, I/O "threads" are executor tasks rather than
OS threads.

### Phase 5: Benchmarking and Tuning

**Benchmarks:**
- Inproc latency: 1-to-1, 1-to-N, N-to-1 pipe configurations
- Inproc throughput: messages/sec at various batch sizes
- Command dispatch latency: mailbox_t vs mailbox_mpsc_t
- Notification overhead: signaler vs executor_notifier under load
- Cache miss rates: before/after alignment changes (perf stat)
- Work-stealing interaction: latency under thread migration

**Tuning parameters to evaluate:**
- `message_pipe_granularity`: 256 is good for throughput (16KB chunks,
  99.6% allocation amortization). Consider 128 for workloads with
  many pipes (8KB, better L1 residency). Expose as pipe option rather
  than compile-time constant.
- `pool_t::local_capacity`: 64 nodes per thread. May need tuning based
  on command fan-in degree.
- Drain batch limit: Should the consumer drain ALL available messages
  per notification, or cap at some limit for fairness? Default: drain
  all. Make configurable for latency-sensitive workloads.

---

## Correctness Arguments

### 1. ypipe CAS protocol is unchanged

The lock-free protocol (ypipe.hpp:76-122) is retained exactly. All
atomics, memory orderings, and the `_c` pointer lifecycle are preserved.
The only change is what happens AFTER `flush()` returns false — instead
of writing to an eventfd, we call `_notifier->notify()`.

This is a pure substitution of the notification transport. The data
consistency guarantee comes from the CAS (acquire-release ordering),
not from the signaler.

### 2. Notifier idempotency prevents lost wakeups

The arm/notify protocol has a potential race:

```
Consumer:                    Producer:
  drain (reads everything)
                             write + flush → _c was NULL → need notify
  arm()                      notify() → _armed was true → post task
```

This is correct: producer's notify() fires because _armed was true.

The dangerous race is:
```
Consumer:                    Producer:
  drain (reads everything)
                             write + flush → _c was NULL → need notify
                             notify() → _armed was true → post task
  arm()
  double-check → finds data → re-enters drain
```

Also correct: the double-check after arm() catches data that arrived
between the last drain and arm(). The re-entry via self-notify()
ensures the consumer processes it.

The truly subtle case:
```
Consumer:                    Producer:
  drain (reads everything)
  arm() → _armed = true
                             write + flush → notify() → _armed was true → post
  double-check → finds data → processes it
                             [task from notify arrives]
  on_notify() → drain → nothing to read → arm()
```

Correct but wasteful: one spurious wakeup. This is acceptable — the
cost is one empty drain cycle (~50ns), not a lost message.

### 3. MPSC queue linearizability

The Vyukov MPSC queue has a well-known proof of linearizability. The
linearization point for push is the `_head.exchange()`. The consumer's
pop observes all pushes that completed their `prev->next.store()` before
the consumer's `tail->next.load()`.

The brief window where `prev->next` is not yet set (between exchange and
store) is handled by the consumer returning nullptr — it will retry on
the next drain cycle. Under the arm/notify protocol, the producer's
notify() ensures the consumer will retry.

### 4. Thread safety of pool_t

Thread-local freelists are inherently thread-safe (no sharing). The
shared Treiber stack uses a standard lock-free push/pop with CAS. Nodes
that migrate between threads (allocated on thread A, freed on thread B)
go to thread B's local freelist, which is correct because the freelist
holds raw memory, not thread-affine state.

---

## What This Design Does NOT Do

1. **Does not assume same-thread execution.** Every optimization works
   correctly when producer and consumer are on different cores. Same-
   thread is faster (cache hits) but not required.

2. **Does not replace the ypipe protocol.** The CAS-based SPSC
   protocol is retained. It is already near-optimal for the cross-
   thread case.

3. **Does not require a specific executor.** The `i_executor_t` /
   `i_notifier_t` interfaces are minimal and map naturally onto Asio's
   `post()`, Tokio's `spawn()`, Go's goroutine scheduling, raw
   `io_uring`, or a custom event loop.

4. **Does not break existing users.** All changes are additive. The
   existing signaler/mailbox/poller path is the default. Executor
   integration is opt-in via new context options.

5. **Does not add per-message overhead.** The optimizations reduce or
   eliminate per-message costs. No new per-message work is introduced.
   The only new per-message cost is a comparison (`_msgs_read >=
   threshold`) which replaces a more expensive modulo.

---

## Appendix: Key Source Files and Line References

| Component | File | Key Lines | Role |
|---|---|---|---|
| yqueue_t fields | src/yqueue.hpp | 172-182 | False-sharing layout problem |
| yqueue_t push | src/yqueue.hpp | 78-97 | Writer hot path |
| yqueue_t pop | src/yqueue.hpp | 131-145 | Reader hot path |
| yqueue_t spare | src/yqueue.hpp | 86, 142 | Atomic exchange (only cross-thread op) |
| ypipe_t fields | src/ypipe.hpp | 150-172 | False-sharing layout problem |
| ypipe_t flush | src/ypipe.hpp | 76-98 | CAS protocol, writer side |
| ypipe_t check_read | src/ypipe.hpp | 101-122 | CAS protocol, reader side |
| pipe_t read | src/pipe.cpp | 170-205 | Modulo backpressure, activate_write |
| pipe_t flush | src/pipe.cpp | 249-257 | activate_read trigger |
| pipe_t write | src/pipe.cpp | 222-234 | HWM check |
| mailbox_t send | src/mailbox.cpp | 32-40 | Mutex + SPSC + signaler |
| mailbox_t recv | src/mailbox.cpp | 42-74 | Signaler wait + SPSC read |
| signaler_t send | src/signaler.cpp | 146-201 | eventfd write (~1000ns) |
| signaler_t recv | src/signaler.cpp | 275-309 | eventfd read (~1000ns) |
| signaler_t wait | src/signaler.cpp | 203-273 | poll() on signaler fd |
| io_thread_t in_event | src/io_thread.cpp | 54-69 | Command drain loop |
| object_t send_command | src/object.cpp | 520-522 | TID-based routing |
| ctx_t send_command | src/ctx.cpp | 642-644 | Mailbox dispatch |
| socket_base_t send | src/socket_base.cpp | 1205-1291 | RDTSC throttle + xsend |
| socket_base_t recv | src/socket_base.cpp | 1293-1387 | Tick counter + xrecv |
| socket_base_t process_commands | src/socket_base.cpp | 1452-1500 | Throttled command processing |
| config.hpp constants | src/config.hpp | 15-58 | Granularity, poll rate, cmd delay |
| command_t | src/command.hpp | 22-194 | Command types and layout |
| i_mailbox | src/i_mailbox.hpp | 13-28 | Mailbox interface |
| ypipe_base_t | src/ypipe_base.hpp | 15-26 | Pipe virtual interface |
| mailbox_safe_t | src/mailbox_safe.cpp | 50-67 | Multi-signaler broadcast pattern |
| epoll_t::loop | src/epoll.cpp | 140-193 | Poller event loop |
| worker_poller_base_t | src/poller_base.hpp | 135-166 | Thread-per-poller model |

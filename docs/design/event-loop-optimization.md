# Event Loop Optimization Design for Coroutine Executors

## Status: DESIGN PROPOSAL

## Problem Statement

libzmq's current architecture assumes a threading model where each I/O thread
owns a dedicated OS thread, blocking in `epoll_wait()`, and sockets are owned by
application threads that block in `poll()/select()` on signaler FDs. This
model imposes costs that become dominant when the execution model shifts to
many coroutine executors sharing a single-threaded event loop:

1. **Signaler syscall overhead**: Every pipe flush that finds the reader asleep
   triggers `write()` to an eventfd/pipe, and every wakeup triggers `read()`.
   In a coroutine model, the "reader" is a suspended coroutine on the same
   thread -- no kernel transition is needed.

2. **Mailbox mutex contention**: `mailbox_t::send()` acquires a mutex to
   serialize access to the SPSC ypipe. With many coroutines acting as
   producers, this mutex becomes a serialization bottleneck even on a single
   thread (where it's pure overhead since there's no actual concurrency).

3. **False sharing in yqueue chunk layout**: `chunk_t` places `values[N]`
   followed by `prev`/`next` pointers. When N is large (256 for messages),
   this isn't an issue. But for command pipes (N=16), the navigation pointers
   sit in the same cache line as data, and reader/writer threads bounce that
   line.

4. **Per-message modulo in backpressure**: `pipe_t::read()` checks
   `_msgs_read % _lwm == 0` on every complete message. Integer modulo is
   ~20-40 cycles on modern CPUs -- avoidable with threshold comparison.

5. **Rigid two-phase command/data separation**: Commands (activate_read,
   activate_write) travel through mailbox+signaler while data travels through
   ypipe. For inproc coroutine-to-coroutine messaging, both paths carry
   unnecessary overhead.

---

## Current Architecture Analysis

### Data Flow: Send Path

```
Application thread                    I/O thread
    |                                     |
zmq_send(msg)                             |
    |                                     |
socket_base::send()                       |
    +-- process_commands(0, throttle=true) |
    |   +-- RDTSC check (skip if <1ms)    |
    |   +-- mailbox->recv(&cmd, 0)        |
    |                                     |
    +-- xsend(msg)                        |
        +-- pipe_t::write(msg)            |
        |   +-- check_hwm()               |
        |   +-- ypipe::write(msg, more)   |
        |       +-- yqueue::back() = msg  |
        |       +-- yqueue::push()        |
        |       +-- _f = &back() if !more |
        |                                 |
        +-- pipe_t::flush()               |
            +-- ypipe::flush()            |
            |   +-- CAS(_w, _f) on _c     |
            |   +-- returns false if      |
            |       reader was asleep     |
            |                             |
            +-- send_activate_read(peer)  |
                +-- mailbox::send(cmd)    |
                    +-- mutex lock        |
                    +-- cpipe.write(cmd)  |
                    +-- cpipe.flush()     |
                    +-- mutex unlock      |
                    +-- signaler.send()   |
                        +-- write(eventfd)|  <-- SYSCALL
                                          |
                                     epoll_wait() returns
                                          |
                                     io_thread::in_event()
                                          +-- mailbox.recv()
                                          |   +-- signaler.recv()  <-- SYSCALL
                                          |   +-- cpipe.read(&cmd)
                                          +-- cmd.dest->process_command(cmd)
                                              +-- pipe_t::process_activate_read()
                                                  +-- sink->read_activated(pipe)
```

### Data Flow: Recv Path

```
Application thread
    |
zmq_recv(msg)
    |
socket_base::recv()
    +-- ++_ticks
    +-- if _ticks == 100:        <-- every 100 messages
    |       process_commands(0)
    |       _ticks = 0
    |
    +-- xrecv(msg)
        +-- pipe_t::read(msg)
            +-- ypipe::check_read()
            |   +-- if front != _r: return true (prefetched)
            |   +-- _r = CAS(&front, NULL) on _c
            |   +-- return _r != &front && _r != NULL
            |
            +-- ypipe::read(msg)
            |   +-- *msg = queue.front()
            |   +-- queue.pop()
            |
            +-- _msgs_read++
            +-- if _msgs_read % _lwm == 0:    <-- MODULO per message
                    send_activate_write(peer)  <-- command via mailbox
```

### Cost Breakdown Per Message (Steady State, Inproc)

| Operation | Cost | Notes |
|-----------|------|-------|
| ypipe::write + push | ~5-10ns | Store to array, increment index |
| ypipe::flush (CAS) | ~15-25ns | Atomic CAS on `_c`, amortized across batch |
| signaler::send | ~500-1000ns | `write()` syscall to eventfd |
| signaler::recv | ~500-1000ns | `read()` syscall from eventfd |
| mailbox mutex lock/unlock | ~20-50ns | Uncontended; ~500ns+ contended |
| ypipe::check_read (CAS) | ~15-25ns | Atomic CAS on `_c` |
| ypipe::read + pop | ~5-10ns | Load from array, increment index |
| pipe_t msgs_read % lwm | ~20-40ns | Integer division |
| mailbox::send for activate_write | ~1000-2000ns | mutex + ypipe + signaler |
| process_commands overhead | ~100-500ns | RDTSC + mailbox recv attempt |

**Total inproc message round-trip: ~3-5 microseconds**

The signaler syscalls dominate. In a coroutine event loop where producer and
consumer run on the same thread, this overhead is entirely eliminable.

---

## Proposed Design

### Design Principle

Rather than replacing the existing architecture, we introduce a parallel
set of data structures and integration points optimized for same-thread
coroutine execution, selectable at pipe creation time.

### 1. Coroutine-Aware Pipe: `ypipe_coro_t`

**Goal**: Eliminate signaler syscalls and mailbox mutex for same-event-loop pipes.

```cpp
// New file: src/ypipe_coro.hpp
//
// SPSC pipe optimized for coroutine executors where producer and consumer
// run on the same event loop thread. "Wakeup" is a callback/coroutine
// reschedule rather than a kernel signal.

template <typename T, int N>
class ypipe_coro_t : public ypipe_base_t<T>
{
public:
    using wakeup_fn_t = void (*)(void *ctx);

    ypipe_coro_t(wakeup_fn_t reader_wakeup, void *reader_ctx)
        : _reader_wakeup(reader_wakeup)
        , _reader_ctx(reader_ctx)
    {
        _queue.push();
        _r = _w = _f = &_queue.back();
        _c.set(&_queue.back());
    }

    // Same write() as ypipe_t -- no change needed.
    void write(const T &value_, bool incomplete_)
    {
        _queue.back() = value_;
        _queue.push();
        if (!incomplete_)
            _f = &_queue.back();
    }

    // flush() replaces signaler::send() with direct callback.
    // Returns true if reader was already awake.
    bool flush()
    {
        if (_w == _f)
            return true;

        // In single-threaded coroutine mode, we can use a simple
        // flag instead of CAS. The _c atomic is retained for
        // compatibility but the fast path avoids it entirely.
        if (_reader_sleeping) {
            _reader_sleeping = false;
            _w = _f;
            // Direct wakeup -- no syscall, no mailbox, no mutex.
            // This reschedules the consumer coroutine on the event loop.
            _reader_wakeup(_reader_ctx);
            return false;
        }

        _w = _f;
        return true;
    }

    bool check_read()
    {
        if (&_queue.front() != _r && _r)
            return true;

        // Single-threaded: simple load instead of CAS
        if (_w != _f) {
            // Writer has unflushed data -- shouldn't happen in
            // well-behaved code, but handle gracefully.
        }

        _r = _f;  // Directly read the flush pointer
        if (&_queue.front() == _r || !_r) {
            _reader_sleeping = true;  // Mark as sleeping
            return false;
        }
        return true;
    }

    bool read(T *value_)
    {
        if (!check_read())
            return false;
        *value_ = _queue.front();
        _queue.pop();
        return true;
    }

private:
    yqueue_t<T, N> _queue;
    T *_w;
    T *_r;
    T *_f;
    atomic_ptr_t<T> _c;  // Retained for cross-thread fallback

    bool _reader_sleeping{false};
    wakeup_fn_t _reader_wakeup;
    void *_reader_ctx;
};
```

**Key insight**: When producer and consumer are on the same event loop thread,
there is no actual concurrency. The `_c` atomic CAS can be replaced by plain
loads/stores. The "wakeup" is a function pointer call that enqueues the
consumer coroutine for the next event loop iteration, costing ~2ns instead of
~1000ns.

**Migration path**: `ypipe_base_t` already provides the virtual interface.
`ypipe_coro_t` derives from it, so `pipe_t` can use it transparently.

### 2. MPSC Lock-Free Command Queue: `mpsc_queue_t`

**Goal**: Eliminate mutex in mailbox for multi-producer command dispatch.

The current mailbox wraps an SPSC ypipe with a mutex for multi-producer
access. This is correct but suboptimal. We replace it with an intrusive
MPSC queue based on Dmitry Vyukov's design:

```cpp
// New file: src/mpsc_queue.hpp
//
// Lock-free multi-producer single-consumer queue.
// Producers: any thread/coroutine. Consumer: the owning event loop.
// Based on Vyukov's MPSC queue with acquire-release semantics.

template <typename T>
class mpsc_queue_t
{
public:
    struct node_t
    {
        std::atomic<node_t *> next{nullptr};
        T value;
    };

    mpsc_queue_t()
    {
        _stub.next.store(nullptr, std::memory_order_relaxed);
        _head.store(&_stub, std::memory_order_relaxed);
        _tail = &_stub;
    }

    // Producer: lock-free, wait-free push. O(1).
    // Multiple producers can call this concurrently with no mutex.
    void push(node_t *node)
    {
        node->next.store(nullptr, std::memory_order_relaxed);
        node_t *prev = _head.exchange(node, std::memory_order_acq_rel);
        // This store completes the link. The consumer will spin
        // briefly if it reaches this node before the store completes.
        prev->next.store(node, std::memory_order_release);
    }

    // Consumer: single-threaded pop. Returns nullptr if empty.
    // May return nullptr transiently even if push() is in progress
    // (the "incomplete" window is ~1 instruction wide).
    node_t *pop()
    {
        node_t *tail = _tail;
        node_t *next = tail->next.load(std::memory_order_acquire);
        if (tail == &_stub) {
            if (next == nullptr)
                return nullptr;
            _tail = next;
            tail = next;
            next = next->next.load(std::memory_order_acquire);
        }
        if (next != nullptr) {
            _tail = next;
            tail->value = std::move(tail->value);
            return tail;
        }
        node_t *head = _head.load(std::memory_order_acquire);
        if (tail != head)
            return nullptr;  // push in progress, retry later
        // Re-insert stub to allow future pushes
        _stub.next.store(nullptr, std::memory_order_relaxed);
        node_t *prev = _head.exchange(&_stub, std::memory_order_acq_rel);
        prev->next.store(&_stub, std::memory_order_release);
        next = tail->next.load(std::memory_order_acquire);
        if (next != nullptr) {
            _tail = next;
            return tail;
        }
        return nullptr;
    }

private:
    // Producers push onto head. Consumer pops from tail.
    // Separated by padding to avoid false sharing.
    alignas(64) std::atomic<node_t *> _head;
    alignas(64) node_t *_tail;
    node_t _stub;
};
```

**Benefit**: Eliminates the `mutex_t _sync` in `mailbox_t`. Producers simply
do an atomic exchange (one instruction on x86) instead of lock + ypipe write +
ypipe flush + unlock.

**Trade-off**: Each command needs its own `node_t` allocation. We mitigate
this with a per-thread node freelist (or arena).

### 3. Optimized yqueue Chunk Layout

**Goal**: Better cache behavior for command pipes and separation of reader/writer metadata.

Current `chunk_t` layout:
```
+---------------------------+
| values[0..N-1]            |  N * sizeof(T) bytes
| prev pointer              |  8 bytes
| next pointer              |  8 bytes
+---------------------------+
```

For message pipes (N=256, T=msg_t at 64 bytes): chunk is 16KB + 16 bytes.
`prev`/`next` sit at the end, separate from where reader/writer are active.
This is already fine.

For command pipes (N=16, T=command_t): chunk is much smaller, and `prev`/`next`
may share a cache line with the last few `values[]` entries. We propose:

```cpp
// Revised chunk_t with explicit padding for small N

template <typename T, int N>
struct chunk_t
{
    T values[N];

    // Pad to ensure navigation pointers are on their own cache line
    // when the values array doesn't fill one.
    static constexpr size_t values_end = sizeof(T) * N;
    static constexpr size_t pad_needed =
        (values_end % 64 == 0) ? 0 : (64 - (values_end % 64));
    char _pad[pad_needed > 0 ? pad_needed : 1];

    chunk_t *prev;
    chunk_t *next;
};
```

Additionally, separate reader-only and writer-only fields in `yqueue_t`
into distinct cache lines:

```cpp
template <typename T, int N>
class yqueue_t
{
    // Reader-only fields (reader thread / coroutine)
    alignas(64) struct {
        chunk_t<T,N> *begin_chunk;
        int begin_pos;
    } _reader;

    // Writer-only fields (writer thread / coroutine)
    alignas(64) struct {
        chunk_t<T,N> *back_chunk;
        int back_pos;
        chunk_t<T,N> *end_chunk;
        int end_pos;
    } _writer;

    // Shared (atomic exchange between reader and writer)
    alignas(64) atomic_ptr_t<chunk_t<T,N>> _spare_chunk;
};
```

**Benefit**: Eliminates false sharing between reader and writer fields.
Currently all six fields (`_begin_chunk`, `_begin_pos`, `_back_chunk`,
`_back_pos`, `_end_chunk`, `_end_pos`) are laid out contiguously with no
alignment guarantees. On a 64-byte cache line, reader and writer fields
will frequently share cache lines, causing coherency traffic.

### 4. Threshold-Based Backpressure

**Goal**: Replace per-message modulo with branch-predicted threshold check.

Current code in `pipe_t::read()`:
```cpp
if (_lwm > 0 && _msgs_read % _lwm == 0)    // ~20-40 cycle modulo
    send_activate_write(_peer, _msgs_read);
```

Proposed:
```cpp
if (unlikely(_msgs_read >= _next_activate_threshold)) {
    _next_activate_threshold = _msgs_read + _lwm;
    send_activate_write(_peer, _msgs_read);
}
```

**Benefit**: Replaces integer modulo (~20-40 cycles) with comparison (~1 cycle).
The threshold is computed once per activation, not per message. Branch
prediction will correctly predict "not taken" ~99.5% of the time (for LWM=50
with HWM=100).

**Additional consideration**: For same-thread coroutine pipes, the
`send_activate_write` can be a direct function call instead of a command
through the mailbox, eliminating another signaler round-trip.

### 5. Coroutine-Aware Mailbox: `mailbox_coro_t`

**Goal**: Provide a mailbox variant that integrates with the event loop
without kernel signaling.

```cpp
// New file: src/mailbox_coro.hpp
//
// Mailbox for coroutine event loop. Uses MPSC queue + event loop
// callback instead of signaler + mutex + ypipe.

class mailbox_coro_t : public i_mailbox
{
public:
    using notify_fn_t = void (*)(void *ctx);

    mailbox_coro_t(notify_fn_t notify, void *ctx)
        : _notify(notify), _ctx(ctx), _active(false) {}

    void send(const command_t &cmd_) override
    {
        auto *node = _node_pool.allocate();
        node->value = cmd_;
        _queue.push(node);

        // If consumer is sleeping, wake it via event loop callback.
        // No mutex, no syscall.
        if (!_active) {
            _notify(_ctx);
        }
    }

    int recv(command_t *cmd_, int timeout_) override
    {
        auto *node = _queue.pop();
        if (node) {
            *cmd_ = std::move(node->value);
            _node_pool.deallocate(node);
            _active = true;
            return 0;
        }

        _active = false;

        if (timeout_ == 0) {
            errno = EAGAIN;
            return -1;
        }

        // For blocking recv in coroutine context:
        // Suspend the coroutine and set up a timer if timeout > 0.
        // The event loop will resume us when send() calls _notify().
        errno = EAGAIN;
        return -1;
    }

private:
    mpsc_queue_t<command_t> _queue;
    node_pool_t<command_t> _node_pool;  // Per-thread freelist
    notify_fn_t _notify;
    void *_ctx;
    bool _active;
};
```

### 6. Batched Flush with Coalescing

**Goal**: Reduce the number of wakeup notifications per event loop tick.

In a coroutine event loop, multiple coroutines may write to different pipes
before yielding. Each flush currently triggers an independent wakeup. We
propose a **deferred flush** mechanism:

```cpp
class event_loop_integration_t
{
public:
    // Called by pipe_t::flush() instead of immediate signaler send.
    void schedule_activation(pipe_t *pipe)
    {
        _pending_activations.push_back(pipe);
        if (!_flush_scheduled) {
            _flush_scheduled = true;
            // Schedule a single callback for end of current event loop tick
            schedule_microtask([this]() { flush_all(); });
        }
    }

private:
    void flush_all()
    {
        for (auto *pipe : _pending_activations) {
            pipe->process_activate_read();
        }
        _pending_activations.clear();
        _flush_scheduled = false;
    }

    std::vector<pipe_t *> _pending_activations;
    bool _flush_scheduled = false;
};
```

**Benefit**: N pipe flushes in the same event loop tick result in exactly 1
event loop callback instead of N signaler writes. This matters when a single
coroutine fans out to many peers (e.g., PUB socket with many subscribers).

### 7. Adaptive Command Throttling

**Goal**: Replace fixed RDTSC/tick-counter throttling with event-loop-aware
batching.

Current design:
- **Send path**: RDTSC-based, skip `process_commands` if <1ms since last check
- **Recv path**: Counter-based, check every 100 messages

These heuristics assume OS-thread blocking semantics. In a coroutine event
loop, commands should be processed at natural yield points:

```cpp
// In coroutine-aware socket:
int send_coro(msg_t *msg)
{
    // Process all pending commands at start of each user operation.
    // This is cheap because mailbox_coro_t::recv is a simple
    // pointer check (~1ns) when empty, not a syscall.
    process_all_pending_commands();

    int rc = xsend(msg);
    if (rc == 0)
        return 0;

    if (errno == EAGAIN) {
        // Suspend coroutine, resume when pipe becomes writable
        co_await writable_event(this);
        return xsend(msg);
    }
    return rc;
}
```

**Benefit**: Eliminates RDTSC reads on the send path and tick-counter
bookkeeping on the recv path. Commands are processed at every yield point
with negligible cost because the check is a simple NULL pointer comparison.

---

## Cache Line and Memory Layout Analysis

### msg_t (64 bytes -- exactly 1 cache line)

```
Offset  Field                   Size    Access Pattern
------  -----                   ----    --------------
0       metadata *              8       Read on recv, rarely
8       data/content/unused     varies  Read/write on data access
56      type                    1       Read on every operation
57      flags                   1       Read on every recv (more check)
58-61   routing_id              4       Read on ROUTER/SERVER sockets
62+     group                   varies  Read on RADIO/DISH sockets
```

**Observation**: `msg_t` is exactly 64 bytes (1 cache line). This is
intentional and optimal. When stored in `yqueue_t` with N=256:
- Each chunk holds 256 * 64 = 16,384 bytes = 256 cache lines of data
- Sequential access pattern has excellent hardware prefetch behavior
- One chunk fits in ~25% of a typical 64KB L1d cache

**No change recommended** for `msg_t` layout.

### ypipe_t Member Layout

```
Current layout (no alignment control):
  yqueue_t _queue    [variable size, contains 6 fields + spare_chunk]
  T *_w              8 bytes  -- WRITER ONLY
  T *_r              8 bytes  -- READER ONLY
  T *_f              8 bytes  -- WRITER ONLY
  atomic_ptr_t _c    8 bytes  -- SHARED (atomic)
```

**Problem**: `_w`, `_r`, `_f`, `_c` are likely on the same cache line (32
bytes total). Writer touches `_w` and `_f` on every flush. Reader touches `_r`
on every check_read. `_c` is touched atomically by both. All four sharing a
cache line means every write to `_w`/`_f` invalidates the reader's cached
`_r`, and vice versa.

**Proposed layout**:
```cpp
alignas(64) struct { T *_w; T *_f; } _writer_state;
alignas(64) struct { T *_r; }        _reader_state;
alignas(64) atomic_ptr_t<T> _c;      // Shared, own cache line
```

**Cost**: 192 bytes instead of 32 bytes for the state fields. This is a
one-time allocation per pipe, negligible compared to the chunks.

**Benefit**: Eliminates false sharing between reader, writer, and the
shared atomic pointer. Each can be modified without invalidating the
other's cache line.

---

## Quantified Improvement Estimates

### Inproc, Same-Thread Coroutine Pipe

| Operation | Current | Proposed | Savings |
|-----------|---------|----------|---------|
| Pipe flush wakeup | ~1000ns (eventfd write) | ~2ns (callback) | 99.8% |
| Mailbox send for activate_write | ~1500ns (mutex+ypipe+signaler) | ~5ns (direct call) | 99.7% |
| Mailbox send for activate_read | ~1500ns | ~5ns | 99.7% |
| check_read CAS | ~20ns | ~1ns (plain load) | 95% |
| Backpressure modulo | ~30ns | ~1ns (comparison) | 97% |
| Command processing check | ~100ns (RDTSC or counter) | ~1ns (pointer check) | 99% |

**Estimated inproc round-trip**: ~3-5us (current) -> ~50-100ns (proposed)

### Cross-Thread Pipe (Fallback to Existing Path)

No regression. The existing ypipe_t/signaler path remains the default for
cross-thread pipes. The optimization is opt-in at pipe creation time based
on whether both endpoints are on the same event loop.

---

## Implementation Strategy

### Phase 1: Foundation (Non-Breaking)

1. **Cache-line alignment for ypipe_t and yqueue_t fields**
   - Add `alignas(64)` to reader/writer field groups
   - Add padding in `chunk_t` for small N
   - Zero API change, measurable throughput improvement

2. **Threshold-based backpressure in pipe_t**
   - Replace `_msgs_read % _lwm == 0` with threshold comparison
   - Add `_next_activate_threshold` member to `pipe_t`
   - Zero API change, ~20-40 cycle savings per message recv

### Phase 2: Coroutine Integration Points

3. **`ypipe_coro_t`**: Same-thread SPSC pipe with callback wakeup
   - Implement as new ypipe_base_t subclass
   - Wire into pipepair() with a new `coro_mode` flag
   - Test with synthetic event loop benchmark

4. **`mpsc_queue_t`**: Lock-free multi-producer command queue
   - Implement Vyukov MPSC queue with node pool
   - Create `mailbox_coro_t` using it
   - Benchmark against current mailbox_t

### Phase 3: Event Loop Integration

5. **`event_loop_integration_t`**: Batched flush coalescing
   - Deferred activation callback
   - Integrate with coroutine scheduler interface

6. **Adaptive command throttling**
   - Replace RDTSC/tick-counter with yield-point processing
   - Coroutine-aware blocking with `co_await`

### Phase 4: Benchmarking and Tuning

7. **Micro-benchmarks**
   - ypipe_t vs ypipe_coro_t throughput (messages/sec)
   - mailbox_t vs mailbox_coro_t command dispatch latency
   - End-to-end inproc latency comparison

8. **Tuning constants**
   - `message_pipe_granularity`: 256 is good for throughput but uses 16KB
     per chunk. Consider 128 (8KB) for better L1 residency with many pipes.
   - `command_pipe_granularity`: 16 is fine.
   - `inbound_poll_rate`: irrelevant for coroutine mode (always check).
   - `max_command_delay`: irrelevant for coroutine mode (always check).

---

## Risk Analysis

| Risk | Impact | Mitigation |
|------|--------|------------|
| Single-threaded assumption violated | Data corruption | Runtime check: assert producer/consumer on same thread in debug builds |
| Node allocation overhead in MPSC | Memory fragmentation | Per-thread freelist with bounded size |
| Callback wakeup ordering | Priority inversion | FIFO processing of pending activations |
| ABI compatibility | Existing users break | All new types, existing types unchanged |
| Increased binary size | Larger library | Conditional compilation with cmake flag |

---

## Appendix: Key Source Files and Line References

| Component | File | Key Lines | Role |
|-----------|------|-----------|------|
| yqueue_t | src/yqueue.hpp | 78-97 (push), 131-145 (pop) | Chunk-based storage |
| ypipe_t | src/ypipe.hpp | 47-56 (write), 76-98 (flush), 101-122 (check_read) | Lock-free SPSC protocol |
| pipe_t | src/pipe.cpp | 170-205 (read), 222-234 (write), 249-257 (flush) | Bidirectional pipe + HWM |
| mailbox_t | src/mailbox.cpp | 32-40 (send), 42-74 (recv) | Mutex + SPSC + signaler |
| signaler_t | src/signaler.cpp | 146-201 (send), 275-309 (recv) | Kernel wakeup (eventfd/pipe) |
| io_thread_t | src/io_thread.cpp | 54-69 (in_event command loop) | I/O thread event dispatch |
| socket_base_t | src/socket_base.cpp | 1205-1291 (send), 1293-1387 (recv), 1452-1500 (process_commands) | Application-thread hot path |
| config.hpp | src/config.hpp | 15 (granularity=256), 26 (poll_rate=100), 43 (cmd_delay=3M) | Tuning constants |
| atomic_ptr_t | src/atomic_ptr.hpp | 163-175 (xchg), 181-194 (cas) | CAS/exchange primitives |
| msg_t | src/msg.hpp | 148-156 (size=64, max_vsm_size) | 64-byte message container |

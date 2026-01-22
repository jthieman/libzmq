# libzmq Internal Mechanisms

This document details the internal notification and synchronization mechanisms used in libzmq.

## 1. Pipe-to-Session Notification Mechanism

### 1a. How does I/O thread get notified?

**Explicit commands through the mailbox.** Not polling, not direct function calls.

The mechanism:
1. Socket writes to pipe, calls `flush()`
2. ypipe's atomic CAS detects if reader is "sleeping"
3. If sleeping → `send_activate_read` command sent via mailbox
4. Mailbox writes to ypipe + signals via signaler FD
5. I/O thread's poller wakes on signaler FD

### 1b. Complete Code Path

```
Application thread:
─────────────────────────────────────────────────────────────────
socket_base_t::send(&msg)
  → lb_t::send() or dist_t::send_to_matching()
    → pipe_t::write(msg_)                          [pipe.cpp:222-234]
      → _out_pipe->write(*msg_, more)              [ypipe stores unflushed]
    → pipe_t::flush()                              [pipe.cpp:249-257]
      → _out_pipe->flush()                         [ypipe.hpp:76-98]
        → CAS: if (_c.cas(_w, _f) != _w)           [reader sleeping]
          → return false
      → if (!ok) send_activate_read(_peer)         [pipe.cpp:256]
        → object_t::send_command()                 [object.cpp:520]
          → ctx->send_command(tid, cmd)            [ctx.cpp:642-644]
            → _slots[tid]->send(cmd)               [mailbox for I/O thread]
              → mailbox_t::send()                  [mailbox.cpp:32-40]
                → _sync.lock()
                → _cpipe.write(cmd, false)
                → _cpipe.flush()
                → _sync.unlock()
                → if (!ok) _signaler.send()        [write 1 byte to FD]

I/O thread (wakes from poll on signaler FD):
─────────────────────────────────────────────────────────────────
io_thread_t::in_event()
  → mailbox.recv(&cmd)
  → cmd.destination->process_command(cmd)
    → pipe_t::process_activate_read()              [pipe.cpp:259-264]
      → _sink->read_activated(this)                [_sink = session]
        → session_base_t::read_activated()         [session_base.cpp:275-295]
          → _engine->restart_output()              [stream_engine_base.cpp:383-398]
            → set_pollout()                        [register for POLLOUT]
            → out_event()                          [SPECULATIVE WRITE!]
              → (this->*_next_msg)(&_tx_msg)       [pulls from pipe]
                → session->pull_msg()
                  → _pipe->read(&msg)
              → _encoder->load_msg(&_tx_msg)
              → _encoder->encode()
              → write(_outpos, _outsize)           [TCP send!]
```

**Key insight at line 397**: `out_event()` is called speculatively without waiting for POLLOUT - the assumption is the socket is probably writable when the app just sent data.

---

## 2. YPipe Write-Steal Pattern

### 2a. How it works

ypipe uses **three pointers** for two-phase commit:

| Pointer | Meaning |
|---------|---------|
| `_w` | Last flushed position (reader can see up to here) |
| `_f` | Flush-up-to position (will become _w on flush) |
| `_r` | Reader's prefetch position |
| `_c` | Atomic contention point (NULL = reader sleeping) |

**Write-steal = `unwrite()`** - removes items between `_f` and queue back.

### 2b. Step-by-step for multipart atomicity

```cpp
// ypipe.hpp:47-56 - write()
void write(const T &value_, bool incomplete_) {
    _queue.back() = value_;
    _queue.push();
    if (!incomplete_)        // Only advance _f on final frame
        _f = &_queue.back();
}

// ypipe.hpp:64-71 - unwrite()
bool unwrite(T *value_) {
    if (_f == &_queue.back())  // Nothing uncommitted
        return false;
    _queue.unpush();           // Remove from back
    *value_ = _queue.back();   // Return removed value
    return true;
}
```

**Usage in pipe_t::rollback()** (`pipe.cpp:236-247`):
```cpp
void pipe_t::rollback() const {
    msg_t msg;
    while (_out_pipe->unwrite(&msg)) {
        zmq_assert(msg.flags() & msg_t::more);  // Only incomplete frames
        msg.close();
    }
}
```

**Flow for multipart `[A, B, C]`:**
```
write(A, incomplete=true)   → _f unchanged, A uncommitted
write(B, incomplete=true)   → _f unchanged, B uncommitted
write(C, incomplete=false)  → _f advances, A,B,C committed atomically
flush()                     → reader can now see A,B,C
```

If error after B: `rollback()` calls `unwrite()` twice, removing B then A.

---

## 3. Mailbox Implementation

### 3a. Lock-free?

**NO.** The mailbox uses a **mutex for senders** (`mailbox.hpp:49`):

```cpp
//  There's only one thread receiving from the mailbox, but there
//  is arbitrary number of threads sending. Given that ypipe requires
//  synchronised access on both of its endpoints, we have to synchronise
//  the sending side.
mutex_t _sync;
```

The ypipe is SPSC (single-producer single-consumer) lock-free, but multiple threads can send commands, so a mutex serializes them.

### 3b. Implementation

```cpp
// mailbox.hpp:39-40
typedef ypipe_t<command_t, command_pipe_granularity> cpipe_t;
cpipe_t _cpipe;         // ypipe backed by yqueue (chunked linked list)
signaler_t _signaler;   // FD for waking receiver
mutex_t _sync;          // Serializes senders

// mailbox.cpp:32-40
void mailbox_t::send(const command_t &cmd_) {
    _sync.lock();                    // Mutex lock
    _cpipe.write(cmd_, false);       // Write to ypipe
    const bool ok = _cpipe.flush();  // Atomic flush
    _sync.unlock();                  // Mutex unlock
    if (!ok)
        _signaler.send();            // Wake receiver if sleeping
}

// mailbox.cpp:42-74
int mailbox_t::recv(command_t *cmd_, int timeout_) {
    if (_active) {
        if (_cpipe.read(cmd_))       // Try read without wait
            return 0;
        _active = false;             // No data, go passive
    }
    _signaler.wait(timeout_);        // Block on FD
    _signaler.recv_failable();       // Consume signal byte
    _active = true;
    _cpipe.read(cmd_);               // Now guaranteed to have data
    return 0;
}
```

**Data structure**: `yqueue_t` - chunked linked list with batch allocation (N items per chunk).

---

## 4. Notification Coalescing

**Three levels of coalescing:**

### Level 1: ypipe flush() CAS (`ypipe.hpp:83`)
```cpp
if (_c.cas(_w, _f) != _w) {
    // CAS failed → _c was NULL → reader sleeping
    _c.set(_f);
    return false;  // Must signal
}
return true;  // Reader awake, no signal needed
```
If reader is awake processing previous data, no signal sent.

### Level 2: Multipart batching (`pipe.cpp:229-230`)
```cpp
_out_pipe->write(*msg_, more);
if (!more && !is_routing_id)
    _msgs_written++;
```
Only the **final frame** of a multipart message triggers flush. Intermediate frames don't notify.

### Level 3: _in_active flag (`pipe.cpp:261-264`)
```cpp
void pipe_t::process_activate_read() {
    if (!_in_active && (...)) {
        _in_active = true;        // Set flag
        _sink->read_activated(this);
    }
}
```
Once activated, further activate_read commands are no-ops until reader goes inactive again.

---

## Summary

| Question | Answer |
|----------|--------|
| **1a. Notification mechanism** | Command mailbox + signaler FD |
| **1b. Code path** | socket→pipe→flush→CAS→mailbox→signaler→I/O thread→session→engine→TCP |
| **2a. Write-steal** | `unwrite()` removes uncommitted items between `_f` and queue back |
| **2b. Atomicity** | Multipart frames written with `incomplete=true`, only final advances `_f` |
| **3a. Lock-free?** | No - mutex for senders, ypipe is SPSC lock-free |
| **3b. Data structure** | `ypipe_t<command_t>` backed by `yqueue_t` (chunked linked list) |
| **4. Coalescing** | CAS check + multipart batching + _in_active flag |

---

## Key Files Reference

| Component | File | Lines |
|-----------|------|-------|
| Pipe write/flush | `src/pipe.cpp` | 222-257 |
| ypipe lock-free queue | `src/ypipe.hpp` | full |
| Mailbox | `src/mailbox.cpp` | full |
| Session activation | `src/session_base.cpp` | 275-307 |
| Engine restart | `src/stream_engine_base.cpp` | 383-398 |
| Command routing | `src/object.cpp` | 520-522 |
| Context slots | `src/ctx.cpp` | 642-644 |

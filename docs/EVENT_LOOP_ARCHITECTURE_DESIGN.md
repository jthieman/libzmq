# Event-Loop Based ZMQ Architecture Design

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Current libzmq Architecture Analysis](#current-libzmq-architecture-analysis)
3. [Performance Bottlenecks in Current Design](#performance-bottlenecks-in-current-design)
4. [Event-Loop Architecture Overview](#event-loop-architecture-overview)
5. [Unified Event Loop Design](#unified-event-loop-design)
6. [High-Performance Mailbox Design](#high-performance-mailbox-design)
7. [Lock-Free Queue Improvements](#lock-free-queue-improvements)
8. [io_uring Integration](#io_uring-integration)
9. [Socket and Pipe Architecture](#socket-and-pipe-architecture)
10. [Migration Strategy](#migration-strategy)

---

## Executive Summary

This document proposes a redesigned ZMQ architecture that replaces the multi-threaded I/O model with a unified event-loop approach using modern kernel interfaces (io_uring, kqueue, epoll). The design maintains semantic compatibility with existing ZMQ concepts (sockets, pipes, HWM, etc.) while achieving higher performance through:

- **Reduced context switching**: Single event loop per core instead of per-socket threads
- **Batched I/O operations**: io_uring submission queues enable batching
- **Simplified signaling**: Eventfd-based signaling with futex fallback
- **Improved cache locality**: Thread-local data structures eliminate false sharing
- **Zero-copy paths**: Direct kernel-to-userspace buffers where possible

---

## Current libzmq Architecture Analysis

### Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                      Application Thread                          │
│  ┌──────────────┐                                               │
│  │ socket_base_t│◄──────── User calls zmq_send/recv             │
│  │  - mailbox_t │                                               │
│  │  - pipes[]   │                                               │
│  └──────┬───────┘                                               │
│         │ pipe (ypipe_t)                                        │
└─────────┼───────────────────────────────────────────────────────┘
          │
          │ Lock-free queue (single atomic CAS point)
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      I/O Thread N                                │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │ io_thread_t  │    │ session_base │    │   engine     │      │
│  │  - mailbox_t │───►│  - pipe_t    │───►│  - tcp/ws/..│      │
│  │  - poller_t  │    │  - engine    │    │  - codec    │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│         │                                        │               │
│         │ epoll/kqueue/select                    │ Network I/O   │
│         ▼                                        ▼               │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Kernel                                 │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Key Mechanisms

#### 1. Signaler (signaler_t)
Current implementation uses:
- **Linux**: `eventfd` (optimal) or `socketpair`
- **macOS/BSD**: `pipe()` or `socketpair`
- **Windows**: Loopback TCP sockets

```cpp
// Current signaler - creates FD pair for wake-up
class signaler_t {
    fd_t _w;  // Write end
    fd_t _r;  // Read end (polled)

    void send() { write(_w, &dummy, 1); }   // Wake up reader
    void recv() { read(_r, &buf, 1); }      // Consume signal
};
```

#### 2. Mailbox (mailbox_t)
```cpp
class mailbox_t {
    ypipe_t<command_t> _cpipe;  // Lock-free command queue
    signaler_t _signaler;        // Wake-up mechanism
    mutex_t _sync;               // Synchronizes multiple writers
    bool _active;                // Reader state

    void send(command_t &cmd) {
        _sync.lock();
        _cpipe.write(cmd, false);
        bool ok = _cpipe.flush();
        _sync.unlock();
        if (!ok) _signaler.send();  // Reader was sleeping
    }

    int recv(command_t *cmd, int timeout) {
        if (_active && _cpipe.read(cmd)) return 0;
        _active = false;
        _signaler.wait(timeout);
        _signaler.recv();
        _active = true;
        return _cpipe.read(cmd) ? 0 : -1;
    }
};
```

#### 3. Lock-Free Queue (ypipe_t)
```cpp
template<typename T, int N>
class ypipe_t {
    yqueue_t<T, N> _queue;  // Chunk-allocated storage
    T *_w;                   // Writer: first unflushed
    T *_f;                   // Flush point
    T *_r;                   // Reader: first unprefetched
    atomic_ptr_t<T> _c;      // Single contention point

    bool flush() {
        if (_w == _f) return true;
        // CAS: try to publish from _w to _f
        if (_c.cas(_w, _f) != _w) {
            // Reader sleeping (c was NULL)
            _c.set(_f);
            _w = _f;
            return false;  // Signal needed
        }
        _w = _f;
        return true;  // No signal needed
    }

    bool check_read() {
        if (&_queue.front() != _r && _r) return true;
        // Prefetch: CAS c from front to NULL
        _r = _c.cas(&_queue.front(), NULL);
        return &_queue.front() != _r && _r;
    }
};
```

### Thread Model

```
Context
├── Term Mailbox (slot 0) - termination coordination
├── Reaper Thread (slot 1) - socket cleanup
│   └── mailbox_t
├── I/O Thread 0 (slot 2)
│   ├── mailbox_t
│   ├── poller_t (epoll/kqueue/select)
│   └── owns: sessions, engines, listeners
├── I/O Thread 1 (slot 3)
│   └── ...
└── Socket Threads (slots 4+)
    └── Each socket has own mailbox
```

---

## Performance Bottlenecks in Current Design

### 1. Per-Thread Overhead
- Each I/O thread runs its own event loop
- Context switches when work distributed across threads
- Cache misses from thread-local data

### 2. Signaling Overhead
```
Producer                      Consumer
────────                      ────────
lock mutex
write to queue
flush (CAS)
if reader sleeping:
  write 1 byte to FD    ───► poll() wakes up
unlock mutex                  read 1 byte from FD
                              read from queue
```
**Cost**: 1 syscall (write) + 1 syscall (poll return) + 1 syscall (read) per wake-up

### 3. Mailbox Contention
- Multiple senders contend on mutex
- Each command goes through: lock → write → CAS → unlock → potential signal
- High-frequency commands (activate_read/write) generate syscall storms

### 4. Poll Loop Inefficiency
```cpp
// Current epoll loop
while (true) {
    int n = epoll_wait(fd, events, max, timeout);
    for (int i = 0; i < n; i++) {
        // Process one event at a time
        pe->events->in_event();
    }
}
```
- Single-threaded processing of events
- No batching of related operations
- Timer handling interleaved with I/O

### 5. Memory Allocation
- `yqueue_t` allocates chunks dynamically
- Command structs copied into queue
- Message buffers may require allocation

---

## Event-Loop Architecture Overview

### Design Principles

1. **One loop per core, not per thread**: Scale with CPU cores, not connections
2. **Batch everything**: Combine syscalls, queue operations, notifications
3. **Minimize cross-thread communication**: Prefer thread-local processing
4. **Use kernel features**: io_uring for batching, SQPOLL for kernel-side polling
5. **Maintain ZMQ semantics**: Same socket types, HWM, patterns

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Application Layer                             │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    Socket Handle (Lightweight)                  │ │
│  │  - Reference to event loop                                      │ │
│  │  - Thread-safe submission queue                                 │ │
│  │  - Completion callbacks                                         │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   │ Submit operations (lock-free)
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Event Loop (per-core)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │  io_uring   │  │   Timer     │  │  Internal   │                 │
│  │  /kqueue    │  │   Wheel     │  │  Mailbox    │                 │
│  │  /epoll     │  │             │  │             │                 │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                 │
│         │                │                │                         │
│         └────────────────┼────────────────┘                         │
│                          ▼                                          │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    Unified Event Handler                        │ │
│  │  - Process I/O completions                                      │ │
│  │  - Execute timer callbacks                                      │ │
│  │  - Handle internal commands                                     │ │
│  │  - Batch message delivery                                       │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                          │                                          │
│                          ▼                                          │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    Socket State Machine                         │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │ │
│  │  │  PUSH    │  │  PULL    │  │  PUB     │  │  SUB     │  ...  │ │
│  │  │  State   │  │  State   │  │  State   │  │  State   │       │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘       │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Unified Event Loop Design

### Event Loop Core

```cpp
class event_loop_t {
public:
    // Configuration
    struct config_t {
        size_t max_events = 256;          // Events per iteration
        size_t submission_batch = 32;      // io_uring SQ batch
        size_t timer_wheel_slots = 4096;   // ~4 seconds at 1ms resolution
        bool use_sqpoll = false;           // io_uring kernel polling
    };

    // Core state
    io_backend_t* _backend;               // io_uring/kqueue/epoll
    timer_wheel_t _timers;                // Hierarchical timer wheel
    submission_queue_t _submissions;       // From external threads
    completion_queue_t _completions;       // To external threads

    // Socket management
    slot_map_t<socket_state_t> _sockets;  // Socket states
    connection_pool_t _connections;        // Active connections

    // Run the loop
    void run() {
        while (!_stopping) {
            // 1. Process external submissions
            process_submissions();

            // 2. Execute ready timers
            process_timers();

            // 3. Submit batched I/O
            _backend->submit();

            // 4. Wait for events
            auto events = _backend->wait(calculate_timeout());

            // 5. Process completions
            for (auto& event : events) {
                dispatch_event(event);
            }

            // 6. Deliver completions to external threads
            flush_completions();
        }
    }

private:
    void process_submissions() {
        submission_t sub;
        size_t batch = 0;
        while (batch < 64 && _submissions.try_pop(sub)) {
            handle_submission(sub);
            batch++;
        }
    }

    microseconds calculate_timeout() {
        auto next_timer = _timers.next_deadline();
        if (_submissions.size_approx() > 0) return 0us;
        return min(next_timer, 100ms);
    }
};
```

### Submission Queue (External → Loop)

This is the key interface for thread-safe operation submission:

```cpp
class submission_queue_t {
    // Lock-free MPSC queue (multiple producers, single consumer)
    struct node_t {
        submission_t data;
        atomic<node_t*> next;
    };

    alignas(64) atomic<node_t*> _head;  // Producers push here
    alignas(64) node_t* _tail;           // Consumer pops from here
    alignas(64) node_t _stub;            // Sentinel node

public:
    // Called from any thread - lock-free
    void push(submission_t&& sub) {
        auto node = new node_t{std::move(sub), nullptr};
        auto prev = _head.exchange(node, memory_order_acq_rel);
        prev->next.store(node, memory_order_release);
    }

    // Called only from event loop thread
    bool try_pop(submission_t& out) {
        auto tail = _tail;
        auto next = tail->next.load(memory_order_acquire);
        if (tail == &_stub) {
            if (!next) return false;
            _tail = next;
            tail = next;
            next = tail->next.load(memory_order_acquire);
        }
        if (next) {
            out = std::move(tail->data);
            _tail = next;
            delete tail;
            return true;
        }
        return false;
    }

    // Approximate size for adaptive waiting
    size_t size_approx() const {
        size_t count = 0;
        auto node = _tail->next.load(memory_order_relaxed);
        while (node && count < 100) { node = node->next.load(memory_order_relaxed); count++; }
        return count;
    }
};
```

### Submission Types

```cpp
struct submission_t {
    enum class type_t : uint8_t {
        // Socket operations
        SEND,           // Queue message for sending
        RECV,           // Request receive notification
        CONNECT,        // Initiate connection
        BIND,           // Bind to address
        CLOSE,          // Close socket

        // Configuration
        SET_OPTION,     // Set socket option

        // Control
        WAKE,           // Wake up loop
        SHUTDOWN,       // Graceful shutdown
    };

    type_t type;
    uint32_t socket_id;

    union {
        struct { msg_t* msg; int flags; } send;
        struct { msg_t* buffer; int flags; } recv;
        struct { const char* endpoint; } connect;
        struct { const char* endpoint; } bind;
        struct { int option; const void* value; size_t len; } set_option;
    } args;

    // Callback for completion notification
    completion_callback_t callback;
    void* user_data;
};
```

---

## High-Performance Mailbox Design

The mailbox system is critical for cross-thread communication. Here's a redesigned version optimized for the event loop model.

### Design Goals

1. **Minimal syscalls**: Use eventfd with batching
2. **Lock-free fast path**: No mutex on common path
3. **Coalesced wake-ups**: Multiple messages, one signal
4. **Cache-efficient**: Avoid false sharing

### Eventfd-Based Signaler

```cpp
class signaler_eventfd_t {
    int _efd;
    alignas(64) atomic<uint64_t> _pending{0};  // Pending signals (avoids syscall)

public:
    signaler_eventfd_t() {
        _efd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    }

    ~signaler_eventfd_t() {
        close(_efd);
    }

    int fd() const { return _efd; }

    // Called from producer - try to avoid syscall
    void signal() {
        // Fast path: if already pending, skip syscall
        uint64_t expected = 0;
        if (_pending.compare_exchange_strong(expected, 1,
                memory_order_release, memory_order_relaxed)) {
            // We transitioned 0→1, need syscall
            uint64_t val = 1;
            write(_efd, &val, sizeof(val));
        }
        // If already non-zero, someone else will wake consumer
    }

    // Called from consumer after being woken
    void consume() {
        uint64_t val;
        read(_efd, &val, sizeof(val));  // Clear eventfd
        _pending.store(0, memory_order_release);
    }

    // For polling
    bool check() {
        return _pending.load(memory_order_acquire) > 0;
    }
};
```

### Optimized Mailbox

```cpp
template<typename T, size_t CacheLineSize = 64>
class mailbox_t {
    // Producer side (multiple threads)
    struct alignas(CacheLineSize) producer_t {
        atomic<node_t*> head;
    } _producer;

    // Consumer side (single thread - event loop)
    struct alignas(CacheLineSize) consumer_t {
        node_t* tail;
        node_t stub;
    } _consumer;

    // Signaler
    alignas(CacheLineSize) signaler_eventfd_t _signaler;

    struct node_t {
        T data;
        atomic<node_t*> next{nullptr};
    };

public:
    mailbox_t() {
        _consumer.tail = &_consumer.stub;
        _producer.head.store(&_consumer.stub, memory_order_relaxed);
    }

    // Multi-producer push
    void send(T&& item) {
        auto node = new node_t{std::move(item)};
        auto prev = _producer.head.exchange(node, memory_order_acq_rel);
        prev->next.store(node, memory_order_release);
        _signaler.signal();
    }

    // Batch send - reduces signaling overhead
    template<typename Iterator>
    void send_batch(Iterator begin, Iterator end) {
        if (begin == end) return;

        // Build chain
        node_t* first = new node_t{std::move(*begin)};
        node_t* last = first;
        for (auto it = begin + 1; it != end; ++it) {
            auto node = new node_t{std::move(*it)};
            last->next.store(node, memory_order_relaxed);
            last = node;
        }

        // Atomic splice
        auto prev = _producer.head.exchange(last, memory_order_acq_rel);
        prev->next.store(first, memory_order_release);
        _signaler.signal();  // Single signal for batch
    }

    // Single-consumer receive
    bool recv(T& out) {
        auto tail = _consumer.tail;
        auto next = tail->next.load(memory_order_acquire);

        if (tail == &_consumer.stub) {
            if (!next) return false;
            _consumer.tail = next;
            tail = next;
            next = tail->next.load(memory_order_acquire);
        }

        if (next) {
            out = std::move(tail->data);
            _consumer.tail = next;
            delete tail;
            return true;
        }
        return false;
    }

    // Batch receive - more efficient
    template<typename OutputIterator>
    size_t recv_batch(OutputIterator out, size_t max_count) {
        size_t count = 0;
        T item;
        while (count < max_count && recv(item)) {
            *out++ = std::move(item);
            count++;
        }
        if (count > 0) {
            _signaler.consume();
        }
        return count;
    }

    int fd() const { return _signaler.fd(); }
};
```

### Cross-Platform Signaler

For non-Linux platforms, we need fallbacks:

```cpp
class signaler_t {
#if defined(__linux__)
    signaler_eventfd_t _impl;
#elif defined(__APPLE__) || defined(__FreeBSD__)
    signaler_kqueue_t _impl;  // Uses EVFILT_USER
#else
    signaler_pipe_t _impl;    // Traditional pipe fallback
#endif

public:
    int fd() const { return _impl.fd(); }
    void signal() { _impl.signal(); }
    void consume() { _impl.consume(); }
    bool check() { return _impl.check(); }
};

// macOS/BSD: Use kqueue EVFILT_USER for zero-copy signaling
class signaler_kqueue_t {
    int _kq;
    static constexpr uintptr_t IDENT = 0xDEADBEEF;
    alignas(64) atomic<bool> _pending{false};

public:
    signaler_kqueue_t() {
        _kq = kqueue();
        struct kevent ev;
        EV_SET(&ev, IDENT, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
        kevent(_kq, &ev, 1, nullptr, 0, nullptr);
    }

    int fd() const { return _kq; }

    void signal() {
        if (!_pending.exchange(true, memory_order_acq_rel)) {
            struct kevent ev;
            EV_SET(&ev, IDENT, EVFILT_USER, 0, NOTE_TRIGGER, 0, nullptr);
            kevent(_kq, &ev, 1, nullptr, 0, nullptr);
        }
    }

    void consume() {
        _pending.store(false, memory_order_release);
    }
};
```

---

## Lock-Free Queue Improvements

### Enhanced ypipe Design

The current `ypipe_t` is well-designed but can be improved:

```cpp
template<typename T, size_t ChunkSize = 256>
class ypipe_v2_t {
    // Chunk-based storage with cache-line alignment
    struct alignas(64) chunk_t {
        T items[ChunkSize];
        atomic<chunk_t*> next{nullptr};
        size_t start_idx;  // For debugging
    };

    // Writer state (owned by writer thread)
    struct alignas(64) writer_state_t {
        chunk_t* chunk;
        size_t pos;
        T* flush_point;  // First unflushed item
    } _writer;

    // Reader state (owned by reader thread)
    struct alignas(64) reader_state_t {
        chunk_t* chunk;
        size_t pos;
        T* prefetch_end;  // End of prefetched region
    } _reader;

    // Shared synchronization point
    alignas(64) atomic<T*> _cursor{nullptr};  // Last flushed item

    // Chunk pool for allocation efficiency
    chunk_pool_t<chunk_t> _pool;

public:
    // Write without flushing (batched)
    void write(T&& item, bool incomplete = false) {
        current_item() = std::move(item);
        advance_write();
        if (!incomplete) {
            _writer.flush_point = write_ptr();
        }
    }

    // Flush all complete items - returns true if reader was awake
    bool flush() {
        T* w = _writer.flush_point;
        T* f = write_ptr();

        if (w == f) return true;  // Nothing to flush

        T* expected = w;
        if (_cursor.compare_exchange_strong(expected, f,
                memory_order_release, memory_order_acquire)) {
            // CAS succeeded, reader was awake
            _writer.flush_point = f;
            return true;
        }

        // Reader was sleeping (cursor was null)
        _cursor.store(f, memory_order_release);
        _writer.flush_point = f;
        return false;  // Need to signal
    }

    // Check if data available (fast path)
    bool check_read() {
        if (has_prefetched()) return true;
        return prefetch();
    }

    // Read single item
    bool read(T& out) {
        if (!check_read()) return false;
        out = std::move(current_read());
        advance_read();
        return true;
    }

    // Batch read - more efficient
    template<typename OutputIt>
    size_t read_batch(OutputIt out, size_t max_count) {
        size_t count = 0;
        while (count < max_count && check_read()) {
            *out++ = std::move(current_read());
            advance_read();
            count++;
        }
        return count;
    }

private:
    bool has_prefetched() {
        return read_ptr() != _reader.prefetch_end && _reader.prefetch_end;
    }

    bool prefetch() {
        T* r = read_ptr();
        T* expected = r;

        // Try to get cursor, set to null to indicate sleeping
        T* c = _cursor.exchange(nullptr, memory_order_acq_rel);

        if (c == r || !c) {
            // No new data
            _reader.prefetch_end = r;
            return false;
        }

        _reader.prefetch_end = c;
        return true;
    }

    T* write_ptr() {
        return &_writer.chunk->items[_writer.pos];
    }

    T* read_ptr() {
        return &_reader.chunk->items[_reader.pos];
    }

    T& current_item() { return *write_ptr(); }
    T& current_read() { return *read_ptr(); }

    void advance_write() {
        if (++_writer.pos == ChunkSize) {
            auto new_chunk = _pool.acquire();
            _writer.chunk->next.store(new_chunk, memory_order_release);
            _writer.chunk = new_chunk;
            _writer.pos = 0;
        }
    }

    void advance_read() {
        if (++_reader.pos == ChunkSize) {
            auto old = _reader.chunk;
            _reader.chunk = old->next.load(memory_order_acquire);
            _reader.pos = 0;
            _pool.release(old);
        }
    }
};
```

### Message Ring Buffer

For fixed-size high-throughput scenarios:

```cpp
template<typename T, size_t Size>
class spsc_ring_t {
    static_assert((Size & (Size - 1)) == 0, "Size must be power of 2");
    static constexpr size_t Mask = Size - 1;

    alignas(64) atomic<size_t> _write_idx{0};
    alignas(64) atomic<size_t> _read_idx{0};
    alignas(64) T _buffer[Size];

public:
    bool try_push(T&& item) {
        size_t w = _write_idx.load(memory_order_relaxed);
        size_t next = (w + 1) & Mask;

        if (next == _read_idx.load(memory_order_acquire)) {
            return false;  // Full
        }

        _buffer[w] = std::move(item);
        _write_idx.store(next, memory_order_release);
        return true;
    }

    bool try_pop(T& out) {
        size_t r = _read_idx.load(memory_order_relaxed);

        if (r == _write_idx.load(memory_order_acquire)) {
            return false;  // Empty
        }

        out = std::move(_buffer[r]);
        _read_idx.store((r + 1) & Mask, memory_order_release);
        return true;
    }

    // Batch operations for efficiency
    size_t push_batch(T* items, size_t count) {
        size_t w = _write_idx.load(memory_order_relaxed);
        size_t r = _read_idx.load(memory_order_acquire);
        size_t available = (r - w - 1) & Mask;
        size_t to_write = min(count, available);

        for (size_t i = 0; i < to_write; i++) {
            _buffer[(w + i) & Mask] = std::move(items[i]);
        }

        _write_idx.store((w + to_write) & Mask, memory_order_release);
        return to_write;
    }
};
```

---

## io_uring Integration

### io_uring Backend

```cpp
class io_uring_backend_t : public io_backend_t {
    struct io_uring _ring;

    // Submission batching
    struct pending_op_t {
        enum class type { READ, WRITE, ACCEPT, CONNECT, TIMEOUT, CANCEL };
        type op_type;
        int fd;
        void* buffer;
        size_t len;
        socket_state_t* socket;
        uint64_t user_data;
    };

    vector<pending_op_t> _pending;

public:
    io_uring_backend_t(const config_t& config) {
        struct io_uring_params params = {};

        if (config.use_sqpoll) {
            params.flags |= IORING_SETUP_SQPOLL;
            params.sq_thread_idle = 2000;  // 2s idle before sleeping
        }

        params.flags |= IORING_SETUP_COOP_TASKRUN;  // Reduce interrupts
        params.flags |= IORING_SETUP_SINGLE_ISSUER; // Single thread optimization

        io_uring_queue_init_params(config.ring_size, &_ring, &params);

        // Register buffers for zero-copy
        if (config.use_registered_buffers) {
            register_buffers();
        }
    }

    ~io_uring_backend_t() {
        io_uring_queue_exit(&_ring);
    }

    // Queue a read operation
    void queue_read(int fd, void* buf, size_t len, uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        io_uring_prep_read(sqe, fd, buf, len, 0);
        io_uring_sqe_set_data64(sqe, user_data);
    }

    // Queue a write operation
    void queue_write(int fd, const void* buf, size_t len, uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        io_uring_prep_write(sqe, fd, buf, len, 0);
        io_uring_sqe_set_data64(sqe, user_data);
    }

    // Queue vectored write (for multi-part messages)
    void queue_writev(int fd, const iovec* iov, int iovcnt, uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        io_uring_prep_writev(sqe, fd, iov, iovcnt, 0);
        io_uring_sqe_set_data64(sqe, user_data);
    }

    // Queue accept
    void queue_accept(int listen_fd, sockaddr* addr, socklen_t* len,
                      uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        io_uring_prep_accept(sqe, listen_fd, addr, len, SOCK_NONBLOCK);
        io_uring_sqe_set_data64(sqe, user_data);
        sqe->flags |= IOSQE_FIXED_FILE;  // Use registered FD if available
    }

    // Queue connect
    void queue_connect(int fd, const sockaddr* addr, socklen_t len,
                       uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        io_uring_prep_connect(sqe, fd, addr, len);
        io_uring_sqe_set_data64(sqe, user_data);
    }

    // Queue timeout
    void queue_timeout(__kernel_timespec* ts, uint64_t user_data) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        io_uring_prep_timeout(sqe, ts, 0, 0);
        io_uring_sqe_set_data64(sqe, user_data);
    }

    // Submit all queued operations
    int submit() {
        return io_uring_submit(&_ring);
    }

    // Wait for completions
    vector<completion_t> wait(microseconds timeout) {
        vector<completion_t> results;
        struct io_uring_cqe* cqe;

        // Convert timeout
        __kernel_timespec ts;
        ts.tv_sec = timeout.count() / 1000000;
        ts.tv_nsec = (timeout.count() % 1000000) * 1000;

        // Wait for at least one completion or timeout
        int ret = io_uring_wait_cqe_timeout(&_ring, &cqe, &ts);
        if (ret == -ETIME) return results;

        // Collect all ready completions
        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(&_ring, head, cqe) {
            results.push_back({
                .user_data = io_uring_cqe_get_data64(cqe),
                .result = cqe->res,
                .flags = cqe->flags
            });
            count++;
        }

        io_uring_cq_advance(&_ring, count);
        return results;
    }

    // Register file descriptors for faster access
    void register_fd(int fd, int slot) {
        io_uring_register_files_update(&_ring, slot, &fd, 1);
    }

private:
    void register_buffers() {
        // Pre-register buffers for zero-copy
        // This is optional but can improve performance
    }
};
```

### Multishot Operations

io_uring supports "multishot" operations that stay active:

```cpp
class multishot_accept_t {
    io_uring_backend_t& _backend;
    int _listen_fd;

public:
    void start() {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_backend._ring);
        io_uring_prep_multishot_accept(sqe, _listen_fd, nullptr, nullptr, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
        // This single SQE will generate multiple CQEs for each accept
    }
};

class multishot_recv_t {
    io_uring_backend_t& _backend;
    int _fd;

public:
    void start() {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_backend._ring);
        io_uring_prep_recv_multishot(sqe, _fd, nullptr, 0, 0);
        // Kernel will provide buffers automatically (requires buffer ring)
    }
};
```

### Buffer Ring for Zero-Copy Receives

```cpp
class buffer_ring_t {
    struct io_uring_buf_ring* _br;
    void* _buffer_base;
    size_t _buffer_size;
    int _bgid;  // Buffer group ID

public:
    buffer_ring_t(io_uring_backend_t& backend, int num_buffers,
                  size_t buffer_size, int bgid)
        : _buffer_size(buffer_size), _bgid(bgid)
    {
        // Allocate buffer ring
        size_t ring_size = num_buffers * sizeof(struct io_uring_buf);
        _br = (struct io_uring_buf_ring*)mmap(nullptr, ring_size,
            PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

        // Allocate actual buffers
        _buffer_base = mmap(nullptr, num_buffers * buffer_size,
            PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);

        // Initialize ring
        io_uring_buf_ring_init(_br);

        // Add buffers to ring
        for (int i = 0; i < num_buffers; i++) {
            void* buf = (char*)_buffer_base + i * buffer_size;
            io_uring_buf_ring_add(_br, buf, buffer_size, i,
                io_uring_buf_ring_mask(num_buffers), i);
        }
        io_uring_buf_ring_advance(_br, num_buffers);

        // Register with kernel
        io_uring_register_buf_ring(&backend._ring, _br, num_buffers, bgid, 0);
    }

    // Return buffer to ring after processing
    void return_buffer(int buffer_id) {
        void* buf = (char*)_buffer_base + buffer_id * _buffer_size;
        io_uring_buf_ring_add(_br, buf, _buffer_size, buffer_id,
            io_uring_buf_ring_mask(/*num_buffers*/), 0);
        io_uring_buf_ring_advance(_br, 1);
    }
};
```

---

## Socket and Pipe Architecture

### Socket State Machine

```cpp
class socket_state_t {
public:
    uint32_t id;
    socket_type_t type;
    options_t options;

    // Connection state
    enum class state_t {
        CREATED,
        BINDING,
        BOUND,
        CONNECTING,
        CONNECTED,
        CLOSING,
        CLOSED
    };
    state_t state = state_t::CREATED;

    // Pipes to peer sockets
    vector<pipe_handle_t> pipes;

    // Pending operations
    struct pending_send_t {
        msg_t msg;
        completion_callback_t callback;
    };
    deque<pending_send_t> send_queue;

    struct pending_recv_t {
        msg_t* buffer;
        completion_callback_t callback;
    };
    deque<pending_recv_t> recv_queue;

    // HWM tracking
    uint64_t msgs_written = 0;
    uint64_t msgs_read = 0;

    // Type-specific state
    union {
        struct { /* PUSH state */ } push;
        struct { /* PULL state */ } pull;
        struct {
            subscription_trie_t subscriptions;
        } sub;
        struct {
            subscription_trie_t subscribers;
        } pub;
        struct {
            round_robin_t lb;
        } dealer;
        struct {
            routing_table_t routes;
        } router;
    };
};
```

### Pipe Design

```cpp
class pipe_t {
    // Bidirectional lock-free queues
    ypipe_v2_t<msg_t, 256> _inbound;   // Peer → This socket
    ypipe_v2_t<msg_t, 256> _outbound;  // This socket → Peer

    // State
    socket_state_t* _socket;
    socket_state_t* _peer;

    // Flow control
    uint64_t _hwm;
    uint64_t _lwm;
    atomic<uint64_t> _msgs_written{0};
    atomic<uint64_t> _peer_msgs_read{0};

    // Activation state
    atomic<bool> _write_active{true};
    atomic<bool> _read_active{true};

public:
    // Called from writer's event loop
    bool write(msg_t&& msg) {
        if (!check_hwm()) {
            _write_active.store(false, memory_order_release);
            return false;
        }

        _outbound.write(std::move(msg));
        _msgs_written.fetch_add(1, memory_order_relaxed);
        return true;
    }

    // Flush and potentially notify peer
    void flush(event_loop_t& loop) {
        if (!_outbound.flush()) {
            // Peer was sleeping, wake it up
            loop.wake_socket(_peer->id);
        }
    }

    // Called from reader's event loop
    bool read(msg_t& out) {
        if (!_inbound.read(out)) {
            _read_active.store(false, memory_order_release);
            return false;
        }

        _msgs_read++;

        // Low watermark - tell peer to resume writing
        if (_msgs_read % _lwm == 0) {
            _peer_msgs_read.store(_msgs_read, memory_order_release);
            // Potentially wake peer if it was blocked
        }

        return true;
    }

private:
    bool check_hwm() {
        if (_hwm == 0) return true;  // No limit
        uint64_t written = _msgs_written.load(memory_order_relaxed);
        uint64_t read = _peer_msgs_read.load(memory_order_acquire);
        return (written - read) < _hwm;
    }
};
```

### Cross-Loop Communication

When sockets are on different event loops:

```cpp
class cross_loop_pipe_t : public pipe_t {
    event_loop_t* _local_loop;
    event_loop_t* _remote_loop;

    // Per-loop submission queues
    mailbox_t<pipe_event_t> _local_to_remote;
    mailbox_t<pipe_event_t> _remote_to_local;

public:
    void flush() override {
        pipe_t::flush();

        // If peer is on different loop, use mailbox
        if (_remote_loop != _local_loop) {
            _local_to_remote.send(pipe_event_t::DATA_READY);
        }
    }

    void notify_write_ready() {
        _remote_to_local.send(pipe_event_t::WRITE_READY);
    }
};
```

---

## Migration Strategy

### Phase 1: Abstraction Layer
- Create `io_backend_t` interface
- Implement epoll/kqueue/select backends
- Existing code continues working

### Phase 2: Event Loop Core
- Implement `event_loop_t`
- Add submission/completion queues
- Test with simple socket operations

### Phase 3: io_uring Backend
- Implement io_uring backend
- Add buffer rings for zero-copy
- Benchmark against epoll

### Phase 4: New Mailbox
- Implement optimized mailbox
- Replace existing signaler
- Measure latency improvements

### Phase 5: Socket Migration
- Migrate socket types one by one
- Maintain backward compatibility
- Add new API for event-loop mode

### Compatibility Layer

```cpp
// Legacy API wrapper
class legacy_socket_wrapper_t {
    socket_handle_t _handle;
    event_loop_t& _loop;

public:
    // zmq_send compatible
    int send(void* buf, size_t len, int flags) {
        msg_t msg;
        msg.init_data(buf, len, nullptr, nullptr);

        if (flags & ZMQ_DONTWAIT) {
            return try_send(msg) ? len : -1;
        }

        // Blocking send
        sync_completion_t completion;
        _loop.submit_send(_handle, std::move(msg), &completion);
        completion.wait();
        return completion.result;
    }

    // zmq_recv compatible
    int recv(void* buf, size_t len, int flags) {
        msg_t msg;

        if (flags & ZMQ_DONTWAIT) {
            if (!try_recv(msg)) return -1;
        } else {
            sync_completion_t completion;
            _loop.submit_recv(_handle, &msg, &completion);
            completion.wait();
            if (completion.result < 0) return -1;
        }

        size_t copy_len = min(len, msg.size());
        memcpy(buf, msg.data(), copy_len);
        return copy_len;
    }
};
```

---

## Performance Expectations

### Latency
| Operation | Current libzmq | Event Loop Design |
|-----------|---------------|-------------------|
| In-process msg | ~1-2 us | ~200-500 ns |
| TCP loopback | ~10-15 us | ~5-8 us |
| Cross-thread signal | ~2-3 us | ~500 ns - 1 us |

### Throughput
| Scenario | Current libzmq | Event Loop Design |
|----------|---------------|-------------------|
| 1:1 TCP | ~2-4M msg/s | ~6-10M msg/s |
| Fan-out 1:N | ~1-2M msg/s | ~4-8M msg/s |
| In-process | ~10-15M msg/s | ~20-40M msg/s |

### Resource Usage
- **Threads**: From N I/O threads to 1 per core
- **FDs**: Reduced from 2 per signaler to shared eventfd
- **Memory**: Better cache locality, fewer allocations

---

## Conclusion

This event-loop architecture maintains ZMQ's semantic model while modernizing the implementation:

1. **Unified event loop** replaces per-thread pollers
2. **io_uring integration** enables batched syscalls and zero-copy
3. **Optimized mailboxes** reduce signaling overhead
4. **Improved lock-free queues** with better cache behavior
5. **Backward compatibility** through wrapper layer

The design prioritizes simplicity where possible while enabling high performance paths for demanding use cases.

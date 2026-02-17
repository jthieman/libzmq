# ZMQ Pipe Implementation on Zig + zio

## Status: DESIGN PROPOSAL (v8)

## Context

We are implementing a ZMQ-compatible message pipe layer in Zig, using
[zio](https://github.com/lalinsky/zio) as the event loop and coroutine
runtime. The goal is to match libzmq's wire semantics (messages, frames,
multi-part, HWM backpressure) while delivering equal or better hot-path
performance, natively integrated with zio's multi-threaded executor model.

This is not a port of libzmq. It is a ground-up design that takes the
proven data structure concepts (ypipe SPSC protocol, yqueue chunk
allocator, pipe backpressure) and adapts them to Zig's type system and
zio's completion-based, multi-executor runtime.

---

## zio Runtime Model (What We Build On)

### Executor Architecture

zio runs M coroutines across N executor threads (default: 1 per CPU core).
Each `Executor` owns:

- An `ev.Loop` (epoll/io_uring/kqueue/IOCP backend)
- A local `ready_queue` (FIFO of runnable tasks)
- A `next_ready_queue_remote` (lock-free Treiber stack for cross-thread wakeups)
- A `current_tick` counter and `tick_task_count` for fairness

Task scheduling (runtime.zig:477-517):
- Tasks are assigned to executors round-robin at spawn time
- A task woken by a thread other than its home executor uses `scheduleTaskRemote()`:
  atomic push to Treiber stack + `loop.wake()` (one syscall, coalesced via `fetchOr`)
- A task woken by the same executor thread uses `scheduleTaskLocal()`: plain queue push, no syscall
- Tasks may migrate to the current executor for cache locality (runtime.zig:505-511)

### Cross-Thread Notification

`loop.wake()` (loop.zig:295-301) uses `fetchOr` on an atomic `wake_requested`
flag. Multiple wakes between loop ticks collapse into a single backend
syscall (eventfd write, io_uring futex, etc.). This is exactly the
arm/notify idempotent pattern — zio already has it.

### Completion Model

All I/O goes through `ev.Completion` objects (completion.zig). Operations
are submitted to the loop, and the loop calls back when complete. The
coroutine layer wraps this: `waitForIo()` (common.zig) parks the current
task, submits the completion, and resumes the task when the callback fires.

### Channel Primitive

zio provides `sync/channel.zig` — an MPMC bounded channel with mutex-based
synchronization, sender/receiver wait queues, and graceful close semantics.
This is a useful reference but too heavyweight for the pipe hot path
(mutex per send/recv, memcpy of arbitrary-size elements).

---

## Design: What We Build

### Layer 1: Message (`Msg`)

The fundamental unit of data. Matches libzmq's `msg_t` semantics:
small messages inline, large messages reference-counted, zero-copy handoff.

```zig
pub const Msg = struct {
    pub const max_vsm_size = 48;  // inline data capacity
    pub const Flag = packed struct(u8) {
        more: bool = false,       // multi-part continuation
        command: bool = false,    // protocol command frame
        shared: bool = false,     // data is refcounted
        _pad: u5 = 0,
    };

    const Data = union(enum) {
        // Value Small Message: data stored inline, no allocation
        vsm: struct {
            bytes: [max_vsm_size]u8,
            len: u8,
        },
        // Large Message: heap-allocated, reference-counted
        lmsg: struct {
            content: *Content,
        },
        // Constant Message: pointer to external data, not owned
        cmsg: struct {
            ptr: [*]const u8,
            len: usize,
        },
        // Empty / delimiter / control
        empty: void,
    };

    data: Data = .{ .empty = {} },
    flags: Flag = .{},
    routing_id: u32 = 0,

    // Shared content block for large messages.
    // Content header + data buffer are allocated contiguously in a single
    // allocation for cache locality and to avoid double-alloc/double-free.
    // Layout: [Content header | data bytes...]
    const Content = struct {
        data: [*]u8,
        len: usize,
        refcount: std.atomic.Value(u32),
        allocator: Allocator,  // stored so release() can free without caller
        free_fn: ?*const fn ([*]u8, usize, ?*anyopaque) void,
        hint: ?*anyopaque,

        fn acquire(self: *Content) void {
            _ = self.refcount.fetchAdd(1, .monotonic);
        }

        fn release(self: *Content) void {
            if (self.refcount.fetchSub(1, .release) == 1) {
                @fence(.acquire);
                if (self.free_fn) |ffn| {
                    ffn(self.data, self.len, self.hint);
                }
                // Free the Content struct (and contiguous data buffer).
                // This works for both single-alloc and custom-alloc paths.
                const alloc = self.allocator;
                alloc.destroy(self);
            }
        }
    };

    pub fn initSize(allocator: Allocator, size: usize) !Msg {
        if (size <= max_vsm_size) {
            return .{ .data = .{ .vsm = .{
                .bytes = undefined, .len = @intCast(size),
            } } };
        }
        // Single contiguous allocation: Content header + data buffer.
        // This avoids a second allocation and improves cache locality.
        const header_size = std.mem.alignForward(usize, @sizeOf(Content), @alignOf(Content));
        const total = header_size + size;
        const raw = try allocator.alignedAlloc(u8, @alignOf(Content), total);
        errdefer allocator.free(raw);  // free on any subsequent error
        const content: *Content = @ptrCast(@alignCast(raw.ptr));
        content.* = .{
            .data = raw.ptr + header_size,
            .len = size,
            .refcount = .init(1),
            .allocator = allocator,
            .free_fn = null,
            .hint = null,
        };
        return .{ .data = .{ .lmsg = .{ .content = content } } };
    }

    pub fn dataSlice(self: *const Msg) []const u8 {
        return switch (self.data) {
            .vsm => |*v| v.bytes[0..v.len],
            .lmsg => |l| l.content.data[0..l.content.len],
            .cmsg => |c| c.ptr[0..c.len],
            .empty => &.{},
        };
    }

    // Move semantics: source becomes empty, no refcount change
    pub fn move(self: *Msg) Msg {
        const result = self.*;
        self.* = .{};
        return result;
    }

    // Copy: for lmsg, increments refcount (zero-copy sharing)
    pub fn copy(self: *const Msg) Msg {
        var result = self.*;
        switch (result.data) {
            .lmsg => |l| l.content.acquire(),
            else => {},
        }
        result.flags.shared = (result.data == .lmsg);
        return result;
    }

    pub fn deinit(self: *Msg, allocator: Allocator) void {
        switch (self.data) {
            .lmsg => |l| l.content.release(),
            else => {},
        }
        self.* = .{};
    }
};
```

**Size**: Target is 64 bytes (one cache line), matching libzmq's msg_t.
The `Data` union is 49 bytes (48 inline + 1 len for vsm), flags + routing_id
+ tag + padding fill the rest. Enforced at compile time:

```zig
comptime {
    // Msg must fit in one cache line for optimal queue throughput.
    // If this fails, adjust max_vsm_size or field layout.
    std.debug.assert(@sizeOf(Msg) <= 64);
}
```

**Why this matches libzmq semantics**:
- VSM (value small message): messages <= 48 bytes stored inline, zero allocation
- LMSG: large messages heap-allocated with refcount, zero-copy on pipe transfer
- CMSG: external constant data, zero-copy reference
- Multi-part: `flags.more` links frames into logical messages
- Move semantics: `msg.move()` transfers ownership without copy or refcount bump

### Layer 2: Queue (`YQueue`)

Chunk-based ring queue. Direct adaptation of libzmq's yqueue_t with
cache-line alignment for cross-thread operation.

```zig
pub fn YQueue(comptime T: type, comptime N: comptime_int) type {
    return struct {
        const Self = @This();

        const Chunk = struct {
            values: [N]T align(64),
            prev: ?*Chunk,
            next: ?*Chunk,
        };

        // Reader fields: only touched by front()/pop()
        // Own cache line to prevent false sharing with writer
        reader: align(64) struct {
            begin_chunk: *Chunk,
            begin_pos: u32,
        },

        // Writer fields: only touched by back()/push()/unpush()
        // Own cache line to prevent false sharing with reader
        writer: align(64) struct {
            back_chunk: ?*Chunk,
            back_pos: u32,
            end_chunk: *Chunk,
            end_pos: u32,
        },

        // Shared between reader and writer via atomic exchange
        // Own cache line to prevent bouncing reader/writer lines
        spare_chunk: align(64) std.atomic.Value(?*Chunk),

        allocator: Allocator,

        pub fn init(allocator: Allocator) !Self {
            const chunk = try allocateChunk(allocator);
            return .{
                .reader = .{ .begin_chunk = chunk, .begin_pos = 0 },
                .writer = .{
                    .back_chunk = null, .back_pos = 0,
                    .end_chunk = chunk, .end_pos = 0,
                },
                .spare_chunk = .init(null),
                .allocator = allocator,
            };
        }

        fn allocateChunk(allocator: Allocator) !*Chunk {
            const chunk = try allocator.create(Chunk);
            chunk.prev = null;
            chunk.next = null;
            return chunk;
        }

        pub fn front(self: *Self) *T {
            return &self.reader.begin_chunk.values[self.reader.begin_pos];
        }

        pub fn back(self: *Self) *T {
            return &self.writer.back_chunk.?.values[self.writer.back_pos];
        }

        pub fn push(self: *Self) !void {
            self.writer.back_chunk = self.writer.end_chunk;
            self.writer.back_pos = self.writer.end_pos;

            self.writer.end_pos += 1;
            if (self.writer.end_pos != N) return;

            // Chunk full — try spare, else allocate
            const spare = self.spare_chunk.swap(null, .acquire);
            const next = spare orelse try allocateChunk(self.allocator);
            next.prev = self.writer.end_chunk;
            self.writer.end_chunk.next = next;
            self.writer.end_chunk = next;
            self.writer.end_pos = 0;
        }

        pub fn pop(self: *Self) void {
            self.reader.begin_pos += 1;
            if (self.reader.begin_pos != N) return;

            const old = self.reader.begin_chunk;
            self.reader.begin_chunk = old.next.?;
            self.reader.begin_chunk.prev = null;
            self.reader.begin_pos = 0;

            // Recycle: old chunk becomes new spare (better cache locality)
            const prev_spare = self.spare_chunk.swap(old, .release);
            if (prev_spare) |s| self.allocator.destroy(s);
        }

        /// Free all chunks and any messages remaining in the queue.
        /// Called during pipe teardown. The caller must ensure no
        /// concurrent access (both reader and writer are done).
        pub fn deinit(self: *Self) void {
            // Walk the chunk linked list from begin_chunk to end_chunk,
            // deinit any Msg values still in the queue, then free chunks.
            var chunk: ?*Chunk = self.reader.begin_chunk;
            var pos: u32 = self.reader.begin_pos;
            while (chunk) |c| {
                // If this chunk contains unconsumed values, deinit them.
                // (Only matters if T has a deinit — for Msg, this releases
                // Content refcounts.)
                const end_pos = if (c == self.writer.end_chunk)
                    self.writer.end_pos
                else
                    N;
                while (pos < end_pos) : (pos += 1) {
                    if (comptime @hasDecl(T, "deinit")) {
                        c.values[pos].deinit(self.allocator);
                    }
                }
                const next = c.next;
                self.allocator.destroy(c);
                chunk = next;
                pos = 0;
            }
            // Free spare chunk if any
            if (self.spare_chunk.load(.acquire)) |s| {
                self.allocator.destroy(s);
            }
        }
    };
}
```

**Cache-line separation**: `reader`, `writer`, and `spare_chunk` each
get `align(64)`. This ensures that when producer and consumer coroutines
run on different executor threads (the common case under work-stealing),
push() and pop() never bounce each other's cache lines. The only
cross-core traffic is the spare_chunk atomic exchange, which happens once
per N items (N=256 for messages).

### Layer 3: Pipe (`YPipe`)

Lock-free SPSC pipe. Same protocol as libzmq's ypipe_t — the CAS on `c`
is the single synchronization point — but with the notification integrated
into zio's executor model instead of going through a signaler.

```zig
pub fn YPipe(comptime T: type, comptime N: comptime_int) type {
    return struct {
        const Self = @This();

        queue: YQueue(T, N),

        // Writer-only: own cache line
        writer: align(64) struct {
            w: *T,  // first un-flushed item
            f: *T,  // first item to flush in future
        },

        // Reader-only: own cache line
        reader_state: align(64) struct {
            r: ?*T,  // first un-prefetched item
        },

        // Shared: the single point of contention. Own cache line.
        c: align(64) std.atomic.Value(?*T),

        pub fn init(allocator: Allocator) !Self {
            var queue = try YQueue(T, N).init(allocator);
            try queue.push();  // terminator element
            const back = queue.back();
            return .{
                .queue = queue,
                .writer = .{ .w = back, .f = back },
                .reader_state = .{ .r = back },
                .c = .init(back),
            };
        }

        // --- Writer thread ---

        pub fn write(self: *Self, value: T, incomplete: bool) !void {
            self.queue.back().* = value;
            try self.queue.push();
            if (!incomplete) {
                self.writer.f = self.queue.back();
            }
        }

        /// Flush completed items. Returns true if reader was awake,
        /// false if reader was sleeping (caller must notify).
        pub fn flush(self: *Self) bool {
            if (self.writer.w == self.writer.f)
                return true;

            // CAS: try to update c from w to f
            const old = self.c.cmpxchgStrong(
                self.writer.w, self.writer.f, .acq_rel, .acquire,
            );

            if (old != null) {
                // CAS failed: c was NULL (reader sleeping).
                // Non-atomic store is safe — reader won't look at c
                // until we notify it, and notification provides the
                // acquire barrier.
                self.c.store(self.writer.f, .release);
                self.writer.w = self.writer.f;
                return false;  // caller must wake reader
            }

            self.writer.w = self.writer.f;
            return true;  // reader was awake
        }

        // --- Reader thread ---

        pub fn checkRead(self: *Self) bool {
            const front_ptr = self.queue.front();
            if (front_ptr != self.reader_state.r and self.reader_state.r != null)
                return true;  // prefetched data available

            // Try to prefetch: CAS c from &front to NULL.
            // Zig cmpxchgStrong returns null on success, else the actual old value.
            // We want r = the old value of c in both cases:
            //   Success → old c was front_ptr, so r = front_ptr
            //   Failure → old c was something else, r = that value
            const old_c = self.c.cmpxchgStrong(
                front_ptr, null, .acq_rel, .acquire,
            );
            if (old_c) |actual| {
                self.reader_state.r = actual;
            } else {
                self.reader_state.r = front_ptr;
            }

            // If front == r or r is null, nothing to read — reader sleeps.
            // During pipe lifetime r should never be null, but it can
            // happen during shutdown when items are being deallocated.
            if (self.queue.front() == self.reader_state.r or self.reader_state.r == null)
                return false;

            return true;
        }

        pub fn read(self: *Self) ?T {
            if (!self.checkRead()) return null;
            const value = self.queue.front().*;
            self.queue.pop();
            return value;
        }

        /// Free all resources. Drains remaining messages and frees all
        /// chunks. Must only be called after both writer and reader are
        /// done (pipe is fully terminated).
        pub fn deinit(self: *Self) void {
            self.queue.deinit();
        }
    };
}
```

**Identical protocol to libzmq**: The CAS on `c` provides the
acquire-release barrier that makes the queue contents visible to the
reader. The `c = NULL` state means "reader is sleeping" — this is the
one bit of information the notification layer needs.

**No signaler, no mailbox**: When `flush()` returns false, the caller
doesn't write to an eventfd or push a command through a mailbox. Instead,
it uses zio's native cross-thread wakeup (see Layer 4).

### Layer 4: Bidirectional Pipe with zio Integration (`Pipe`)

This is where the ZMQ pipe semantics (HWM, LWM, backpressure, multi-part
messages) integrate with zio's coroutine scheduling.

#### Why Locality Detection Is the Wrong Approach

It's tempting to check `getCurrentExecutor() == pipe.executor` and use a
fast local path. Don't. This is fragile:

1. **Task migration**: zio migrates tasks to the current executor for cache
   locality (runtime.zig:505-511). A task that was local last message may
   be remote this message.
2. **Work-stealing**: Future zio versions will implement work-stealing
   (runtime.zig:503 TODO). Tasks will move unpredictably.
3. **Two paths = two bugs**: A local fast path that bypasses atomics will
   produce data races the moment a task migrates. These bugs are
   intermittent and nearly impossible to reproduce.
4. **The CAS already adapts**: Same-core CAS costs ~5ns (L1 hit). Cross-core
   CAS costs ~20-25ns (coherency traffic). The hardware already gives you
   a 4-5x speedup when local — you don't need to detect it.

The right approach is: **always use the atomic protocol, but minimize the
number of atomic operations and notifications through batching and
adaptive spinning.**

#### The Three Throughput Killers (and How We Solve Them)

**Killer 1: Per-message flush**

Every `flush()` does a CAS on `c`. Cross-core, that's ~20-25ns of cache
line bouncing. If you flush after every send, your ceiling is ~40-50M
msg/s from CAS contention alone — before any notifications.

**Solution**: Separate `send()` (enqueue, no CAS) from `flush()` (one CAS
for the whole batch). The API makes this explicit: callers accumulate
writes, then flush once. `sendBlocking()` for the single-message case
still flushes, but `sendBatch()` amortizes one CAS over N messages.

**Killer 2: Reader parks immediately**

When the reader drains all data, it calls `recvBlocking()`, finds nothing,
and immediately parks the coroutine (Waiter.wait). The very next message
triggers a full notification cycle: CAS detects c=NULL → Treiber stack
push (~15ns) → loop.wake() syscall (~200-500ns) → context switch (~100ns).
That's **~300-600ns per sleep/wake cycle**.

Under moderate throughput, the reader constantly outruns the writer:
sleep → wake → drain → sleep → wake → drain. Every iteration pays the
full cross-executor notification cost.

**Solution**: Adaptive spin before parking. The reader spins briefly
(checking for new data via `checkRead()`) before committing to a park.
Under load, the spin absorbs the gap between writer flush and reader
drain — the writer's next flush lands while the reader is still spinning,
so no notification is needed. Under idle conditions, the spin burns a
few hundred nanoseconds then parks — negligible because the system is
idle anyway.

The spin count adapts: after a successful spin (data arrived during spin),
increase the count. After a spin timeout (had to park), decrease it.
This converges on the right spin duration for the workload.

**Killer 3: Reader does CAS per read**

`read()` calls `checkRead()`, which may CAS on `c` to prefetch more data.
If the reader processes messages one at a time and each one calls
`checkRead()`, you get a CAS per message on the reader side too — doubling
the cache line bouncing.

**Solution**: `checkRead()` only does a CAS when the prefetch is exhausted
(when `front == r`). Between CAS operations, `read()` is just a pointer
comparison + array read — no atomics. With N=256 chunk size, that's one
CAS per 256 reads in steady state. But the caller can also drain in a
batch: `recvBatch()` reads all available messages in one pass, doing
at most one CAS for the entire batch.

```zig
pub const Pipe = struct {
    // Underlying unidirectional SPSC pipes
    in_pipe: *YPipe(Msg, 256),   // incoming messages (we read)
    out_pipe: *YPipe(Msg, 256),  // outgoing messages (we write)

    // Backpressure state
    hwm: u32,
    lwm: u32,
    msgs_read: u64 = 0,
    msgs_written: u64 = 0,
    peers_msgs_read: std.atomic.Value(u64) = .init(0),  // atomic: cross-thread
    next_activate_threshold: u64 = 0,

    // Activity flags.
    // in_active: only touched by the reader — plain bool is safe.
    // out_active: written by the reader (activate after LWM) and read by
    //   the writer (send path). Cross-thread access requires atomic.
    in_active: bool = true,
    out_active: std.atomic.Value(bool) = .init(true),

    // Peer reference (the other end of the pipe pair)
    peer: *Pipe = undefined,

    // --- zio integration ---

    // The executor that owns this pipe endpoint. Set when the pipe
    // is attached to a socket/session on a specific executor.
    executor: *zio.Runtime.Executor = undefined,

    // ReadySignal for the read side: persistent, pipe-owned.
    // When flush() returns false, we notify this instead of going
    // through a mailbox. See "Concurrency Model" section for why
    // these must be pipe-owned (not stack-allocated Waiters).
    read_signal: ReadySignal = .{},

    // ReadySignal for the write side: parked task waiting for HWM space.
    write_signal: ReadySignal = .{},

    // Adaptive spin calibration
    spin_count: u32 = initial_spin_count,

    const initial_spin_count: u32 = 100;
    const min_spin_count: u32 = 0;
    const max_spin_count: u32 = 10_000;

    // -------------------------------------------------------
    // Write path (called from producer coroutine)
    // -------------------------------------------------------

    /// Enqueue a message to the pipe. Does NOT flush — no CAS, no
    /// notification. The message is not visible to the reader until
    /// flush() is called. This is a plain array write (~5ns).
    pub fn send(self: *Pipe, msg: *Msg) !bool {
        if (!self.out_active.load(.acquire)) return false;

        if (!self.checkHwm()) {
            self.out_active.store(false, .release);
            return false;
        }

        const more = msg.flags.more;

        // Write the value into the ypipe slot, THEN push.
        // We must NOT move() before push() succeeds, because push()
        // can fail (chunk allocation) and move() zeroes the source.
        // If we moved first and push fails, the message is lost.
        self.out_pipe.queue.back().* = msg.*;
        try self.out_pipe.queue.push();
        if (!more) {
            self.out_pipe.writer.f = self.out_pipe.queue.back();
        }
        msg.* = .{};  // zero source only after push succeeds (move semantics)

        if (!more) self.msgs_written += 1;
        return true;
    }

    /// Flush all enqueued messages to the reader. One CAS on `c`,
    /// regardless of how many messages were enqueued since last flush.
    /// Returns true if reader was awake, false if reader was sleeping
    /// (in which case we wake it).
    pub fn flush(self: *Pipe) void {
        if (!self.out_pipe.flush()) {
            // Reader is sleeping (c was NULL). Wake it directly.
            self.wakeReader();
        }
    }

    /// Convenience: send a single message and flush immediately.
    /// Use this for latency-sensitive single sends. For throughput,
    /// prefer calling send() N times then flush() once.
    pub fn sendAndFlush(self: *Pipe, msg: *Msg) !bool {
        const ok = try self.send(msg);
        if (ok) self.flush();
        return ok;
    }

    /// Send a batch of messages with a single flush at the end.
    /// One CAS amortized over the entire batch. Returns the number
    /// of messages successfully sent (may be < msgs.len if HWM hit).
    pub fn sendBatch(self: *Pipe, msgs: []Msg) !u32 {
        var sent: u32 = 0;
        for (msgs) |*msg| {
            if (!try self.send(msg)) break;
            sent += 1;
        }
        if (sent > 0) self.flush();
        return sent;
    }

    fn wakeReader(self: *Pipe) void {
        // Notify the peer's persistent ReadySignal.
        // Safe from any thread — ReadySignal is pipe-owned (heap lifetime),
        // and AnyTask is executor-managed (heap lifetime).
        // See "Concurrency Model" section for the full happens-before proof.
        self.peer.read_signal.notify();
    }

    fn wakeWriter(self: *Pipe) void {
        self.peer.write_signal.notify();
    }

    fn checkHwm(self: *const Pipe) bool {
        if (self.hwm == 0) return true;  // infinite
        return (self.msgs_written - self.peers_msgs_read.load(.acquire)) < self.hwm;
    }

    // -------------------------------------------------------
    // Read path (called from consumer coroutine)
    // -------------------------------------------------------

    /// Non-blocking read. Returns null if no data available.
    /// Between prefetch boundaries, this is just a pointer comparison +
    /// array read — no atomics. CAS only happens when the prefetch
    /// is exhausted, which is at most once per chunk (N=256 messages).
    pub fn recv(self: *Pipe) ?Msg {
        if (!self.in_active) return null;

        const msg = self.in_pipe.read() orelse {
            self.in_active = false;
            return null;
        };

        if (!msg.flags.more) {
            self.msgs_read += 1;

            // Threshold-based backpressure: comparison, not modulo (~1ns vs ~30ns)
            if (self.msgs_read >= self.next_activate_threshold) {
                self.next_activate_threshold += self.lwm;
                self.peer.peers_msgs_read.store(self.msgs_read, .release);
                if (!self.peer.out_active.load(.acquire)) {
                    self.peer.out_active.store(true, .release);
                    self.wakeWriter();
                }
            }
        }

        return msg;
    }

    /// Drain all available messages into a buffer. At most one CAS on
    /// `c` for the entire drain, then pure array reads until exhausted.
    /// Returns the number of messages drained.
    pub fn recvBatch(self: *Pipe, buf: []Msg) u32 {
        var count: u32 = 0;
        while (count < buf.len) {
            const msg = self.recv() orelse break;
            buf[count] = msg;
            count += 1;
        }
        return count;
    }

    // -------------------------------------------------------
    // Blocking variants (coroutine-aware)
    // -------------------------------------------------------

    /// Blocking send: parks coroutine if HWM reached, resumes when
    /// space available. Does NOT flush — caller must call flush()
    /// when ready (or use sendBlockingAndFlush for single-message case).
    pub fn sendBlocking(self: *Pipe, msg: *Msg) !void {
        var gen = self.write_signal.currentGen();
        while (true) {
            if (try self.send(msg)) return;
            // Park until writer is activated (see Concurrency Model)
            gen = try self.write_signal.park(gen);
        }
    }

    /// Blocking send + flush. Convenience for the single-message
    /// latency-sensitive case.
    pub fn sendBlockingAndFlush(self: *Pipe, msg: *Msg) !void {
        try self.sendBlocking(msg);
        self.flush();
    }

    /// Blocking recv with adaptive spin-before-park.
    ///
    /// The spin absorbs the latency gap between writer flush and reader
    /// drain. Under high throughput, data arrives during the spin window
    /// and we never park — zero notification overhead. Under idle
    /// conditions, we burn a few hundred nanoseconds spinning then park,
    /// which is negligible because the system is idle.
    ///
    /// The spin count adapts to the workload:
    /// - Spin succeeds → increase count (more spinning pays off)
    /// - Spin times out → decrease count (spinning is wasted)
    /// This converges on the right spin duration for any throughput level.
    pub fn recvBlocking(self: *Pipe) !Msg {
        // Fast path: data already available (common under high throughput)
        if (self.recv()) |msg| return msg;

        // Adaptive spin: check for data without parking
        var spun: u32 = 0;
        while (spun < self.spin_count) : (spun += 1) {
            std.atomic.spinLoopHint();  // PAUSE on x86, YIELD on ARM
            if (self.in_pipe.checkRead()) {
                // Data arrived during spin — avoid park entirely.
                // Increase spin count: spinning is paying off.
                self.spin_count = @min(self.spin_count + (self.spin_count / 4), max_spin_count);
                return self.recv().?;
            }
        }

        // Spin exhausted, no data — park via ReadySignal.
        // Decrease spin count: spinning wasn't worthwhile.
        if (self.spin_count > min_spin_count) {
            self.spin_count -= self.spin_count / 8;
        }

        // Snapshot generation BEFORE checking data (see Concurrency Model)
        var gen = self.read_signal.currentGen();
        while (true) {
            if (self.in_pipe.checkRead()) return self.recv().?;
            // Park with generation check (prevents missed wakeup)
            gen = try self.read_signal.park(gen);
            if (self.in_pipe.checkRead()) return self.recv().?;
        }
    }

    /// Free all pipe resources. Called after pipe reaches `terminated`
    /// state and both endpoints are done. Frees underlying ypipes
    /// (which drain and free remaining messages and chunks).
    pub fn deinit(self: *Pipe, allocator: Allocator) void {
        self.in_pipe.deinit();
        allocator.destroy(self.in_pipe);
        // out_pipe is the peer's in_pipe — freed by the peer's deinit.
        // Only free ourselves.
        allocator.destroy(self);
    }
};

/// Create a pipe pair for bidirectional communication.
/// Returns heap-allocated pointers. Pipes contain self-referential `peer`
/// pointers, so they MUST be heap-allocated — returning by value would
/// leave `peer` dangling after the move.
pub fn pipePair(allocator: Allocator, hwm0: u32, hwm1: u32) !struct { *Pipe, *Pipe } {
    const pipe_a = try allocator.create(YPipe(Msg, 256));
    errdefer allocator.destroy(pipe_a);
    pipe_a.* = try YPipe(Msg, 256).init(allocator);

    const pipe_b = try allocator.create(YPipe(Msg, 256));
    errdefer allocator.destroy(pipe_b);
    pipe_b.* = try YPipe(Msg, 256).init(allocator);

    const p0 = try allocator.create(Pipe);
    errdefer allocator.destroy(p0);
    const p1 = try allocator.create(Pipe);
    errdefer allocator.destroy(p1);

    p0.* = Pipe{
        .in_pipe = pipe_a, .out_pipe = pipe_b,
        .hwm = hwm1, .lwm = computeLwm(hwm0),
    };
    p1.* = Pipe{
        .in_pipe = pipe_b, .out_pipe = pipe_a,
        .hwm = hwm0, .lwm = computeLwm(hwm1),
    };
    p0.peer = p1;
    p1.peer = p0;
    p0.next_activate_threshold = p0.lwm;
    p1.next_activate_threshold = p1.lwm;
    return .{ p0, p1 };
}

fn computeLwm(hwm: u32) u32 {
    return (hwm + 1) / 2;
}
```

### How Notification Works (The Critical Path)

There are three distinct throughput regimes. The design handles all of
them efficiently — without ever needing to detect whether producer and
consumer are on the same executor.

#### Regime 1: Sustained Throughput (Batch Send → Batch Recv)

The highest-throughput path. Producer writes N messages, flushes once.
Reader drains all N messages in one pass. One CAS total per batch.

```
Executor Thread A (producer):              Executor Thread B (consumer):
                                                  |
for (msgs) |*msg|                                pipe.recvBlocking()
    pipe.send(msg)                                 recv() → null (drained)
    // no CAS, no flush — just array writes        checkRead() → false, c set to NULL
    // ~5ns per message                            adaptive spin begins...
end                                                spin iteration 1..N:
pipe.flush()                                         spinLoopHint()
    CAS(c, w, f) → fails, c was NULL                checkRead() → false, keep spinning
    c.store(f, .release)                             ...
    return false                                     ...
    wakeReader()                                   [spin exhausted, park]
      waiter.signal()                              waiter.wait()
        → Treiber push + loop.wake()                 → task parked
                                                      |
                                           [executor B loop tick]
                                           drain Treiber stack, resume task
                                                      |
                                           // reader wakes, drains everything:
                                           recv() → msg₁  // pure array read
                                           recv() → msg₂  // pure array read
                                           ...
                                           recv() → msgₙ  // pure array read
                                           recv() → null
```

**Cost**: One CAS (flush) + one notification (Treiber + wake) amortized
over N messages. At N=100: **~5ns per message** including notification.

#### Regime 2: Moderate Throughput (Spin Absorbs Latency)

Producer sends messages at moderate rate. Reader's adaptive spin catches
data before parking, avoiding the notification path entirely.

```
Executor Thread A (producer):              Executor Thread B (consumer):
                                                  |
pipe.sendAndFlush(&msg₁)                         pipe.recvBlocking()
    write + CAS → reader awake                     recv() → msg₁
    flush returns true, no notify                  return msg₁
                                                  |
                                                  pipe.recvBlocking()
                                                    recv() → null
                                                    adaptive spin begins...
pipe.sendAndFlush(&msg₂)                            spin iteration 47:
    write + CAS → reader awake (spinning)             checkRead() → true!
    flush returns true, no notify                       spin_count increases
                                                    recv() → msg₂
                                                    return msg₂
```

**Cost**: One CAS per flush. Zero notifications — the reader's spin
catches the data before it commits to parking. The adaptive spin count
converges so the spin window matches the inter-message interval.

**This is the regime that kills naive implementations**: without the spin,
every recv would park and every send would notify. With spin, zero
notifications and zero syscalls.

#### Regime 3: Ping-Pong (Request-Response)

Lowest throughput, latency-sensitive. Each side sends one message then
waits. This is the worst case — one notification per message in each
direction. But even here, the design is ~10x faster than libzmq because
there's no signaler/mailbox overhead.

```
Executor Thread A:                         Executor Thread B:
                                                  |
pipe.sendBlockingAndFlush(&request)              pipe.recvBlocking()
    write + CAS → reader sleeping                  spin... timeout, park
    flush returns false                            [parked]
    wakeReader() → Treiber + wake                  |
                                                  [woken] recv() → request
pipe.recvBlocking()                              process(request)
    recv() → null                                pipe.sendBlockingAndFlush(&reply)
    spin... timeout, park                            write + CAS → reader sleeping
    [parked]                                         flush returns false
    |                                                wakeReader() → Treiber + wake
    [woken] recv() → reply                        |
```

**Cost**: One CAS + one notification per message per direction.
~250-500ns per notification cross-executor. But adaptive spin count
will decrease toward zero after repeated timeouts, minimizing wasted
spin cycles in this regime.

#### Why the CAS Protocol Makes Locality Detection Unnecessary

The `c` pointer already encodes all the information:

| `c` value | Meaning | flush() does | Cost |
|-----------|---------|--------------|------|
| != NULL, == w | Nothing to flush | returns true | ~2ns (pointer compare) |
| != NULL, != w | Reader is awake/spinning | CAS(w,f), returns true | ~5-25ns (CAS) |
| == NULL | Reader is parked | CAS fails, store(f), returns false | ~25ns + notify |

- When producer and consumer are **same-core**: CAS is ~5ns (L1 hit, no
  coherency traffic). The hardware gives you the fast path automatically.
- When **cross-core**: CAS is ~20-25ns (cache line bounce on `c` only —
  queue data is on separate cache lines thanks to `align(64)`).
- **Notification only fires at the idle→busy transition**. Under sustained
  load, the reader is always awake/spinning, so flush() always returns
  true and no notification is needed regardless of locality.

The adaptive spin further reduces notifications: even in cross-executor
moderate-throughput scenarios, the reader catches data during the spin
window and never parks. The spin count converges to match the workload,
so it's not wasting cycles in ping-pong scenarios where spinning doesn't
help.

### Layer 5: Multi-Part Message Framing

ZMQ's multi-part message semantics are critical for protocol compatibility.
Frames within a logical message are linked by the `more` flag:

```zig
/// Send a complete multi-part message atomically.
/// Either all frames are written or none are (rollback on HWM).
pub fn sendMultipart(pipe: *Pipe, frames: []Msg) !void {
    for (frames, 0..) |*frame, i| {
        const is_last = (i == frames.len - 1);
        if (!is_last) frame.flags.more = true;

        if (!try pipe.send(frame)) {
            // HWM hit mid-message — rollback incomplete frames
            pipe.rollback();
            return error.HighWaterMarkReached;
        }
    }
    pipe.flush();
}

/// Receive a complete multi-part message.
/// Collects frames until one without the `more` flag.
pub fn recvMultipart(pipe: *Pipe, allocator: Allocator) ![]Msg {
    var frames = std.ArrayList(Msg).init(allocator);
    errdefer {
        for (frames.items) |*f| f.deinit(allocator);
        frames.deinit();
    }

    while (true) {
        const msg = try pipe.recvBlocking();
        const more = msg.flags.more;
        try frames.append(msg);
        if (!more) break;
    }
    return frames.toOwnedSlice();
}
```

**Rollback** uses ypipe's `unwrite()` to remove incomplete frames from the
pipe, matching libzmq's `pipe_t::rollback()` behavior.

### Memory Lifecycle Through the Pipe

This is where correctness matters most. A message traverses:

```
Producer coroutine                          Consumer coroutine
    |                                           |
msg = Msg.initSize(alloc, 1024)                 |
  → allocates Content + data (refcount=1)       |
msg.dataSlice()[0..] = payload                  |
    |                                           |
pipe.send(&msg)                                 |
  → msg.move() into ypipe slot                  |
  → producer's msg is now .empty                |
  → ypipe slot owns the Content (refcount=1)    |
    |                                           |
pipe.flushPipe()                                |
  → CAS makes data visible to consumer          |
    |                                           |
                                            msg = pipe.recv()
                                              → reads from ypipe slot
                                              → consumer owns Content (refcount=1)
                                              → ypipe slot is popped
                                                |
                                            process(msg.dataSlice())
                                                |
                                            msg.deinit(alloc)
                                              → Content.release()
                                              → refcount 1→0, free data+Content
```

**Zero-copy path**: The Content pointer moves through the pipe without
any memcpy or refcount bump. The `msg.move()` operation is a register-width
struct copy (the Msg is 64 bytes = 8 registers on x86_64) followed by
zeroing the source. The actual message data is never touched.

**Shared path** (pub/sub fan-out): `msg.copy()` bumps the refcount
atomically. Multiple pipe slots hold the same Content. Last consumer
to `deinit()` frees the data.

**VSM path** (small messages <= 48 bytes): No allocation at all. The
data is inline in the Msg struct, which is copied by value into the
ypipe queue slot. This is a 64-byte memcpy, which is one cache line
write — optimal on all architectures.

---

## Layer 6: Transport Layer (TCP, IPC, inproc)

### Architecture

The Pipe is always inproc — it connects two coroutines within the same
process. What differs per transport is what sits on the other end:

```
┌──────────────────────────────────────────────────────────────────────────┐
│ inproc                                                                   │
│                                                                          │
│   App coroutine A ↔ Pipe ↔ App coroutine B                              │
│                                                                          │
│   No session, no codec, no I/O. Pure pipe throughput.                   │
│   Pipe pair connects two sockets directly.                              │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ tcp / ipc                                                                │
│                                                                          │
│   App coroutine ↔ Pipe ↔ Session coroutine ↔ ZMTP codec ↔ zio Stream   │
│                                                                          │
│   Session bridges pipe to network. ZMTP encodes/decodes on the wire.    │
│   Stream is either TCP (IpAddress) or Unix domain socket (UnixAddress). │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ Full picture: two peers connected over TCP                               │
│                                                                          │
│   App ↔ Pipe ↔ Session ↔ ZMTP ↔ [TCP] ↔ ZMTP ↔ Session ↔ Pipe ↔ App   │
│             ▲                                              ▲             │
│             │         These are the same Pipe              │             │
│             │         as Layers 1-4 above.                 │             │
│             │         All batch/spin/CAS                   │             │
│             │         optimizations apply.                 │             │
└──────────────────────────────────────────────────────────────────────────┘
```

The Pipe's batch flush, adaptive spin, and CAS protocol don't care what's
on the other end. For TCP, the "reader coroutine" on one side of the pipe
is the Session, which drains the pipe and writes to the network. The
"writer coroutine" on the other side is also the Session, which reads from
the network and fills the pipe. The Session is just another coroutine from
the Pipe's perspective.

### Session Coroutine

Each TCP/IPC connection gets a Session — a coroutine that runs two
concurrent loops: one for sending (pipe → network) and one for receiving
(network → pipe).

```zig
pub const Session = struct {
    // Pipe endpoint (our side of the pipe pair)
    pipe: *Pipe,

    // Network stream (TCP or Unix domain socket)
    stream: zio.net.Stream,

    // ZMTP codec state
    codec: ZmtpCodec,

    // Scratch buffers for vectored I/O
    iov_storage: [16]std.os.iovec_const = undefined,

    allocator: Allocator,

    /// Run the session. Spawns send and receive loops as concurrent
    /// coroutines. Returns when the connection closes or errors.
    pub fn run(self: *Session) !void {
        // ZMTP greeting + handshake
        try self.codec.handshake(&self.stream);

        // Run send and receive concurrently.
        // Both are coroutines on the same executor (or wherever
        // zio schedules them — doesn't matter, pipes handle it).
        var group = try zio.TaskGroup.init(self.allocator);
        defer group.deinit();

        try group.spawn(sendLoop, .{self});
        try group.spawn(recvLoop, .{self});
        try group.wait();
    }

    // -------------------------------------------------------
    // Send loop: Pipe → Network
    // -------------------------------------------------------

    fn sendLoop(self: *Session) !void {
        while (true) {
            // Drain pipe into ZMTP frames, write to network.
            // Batch: accumulate multiple messages, write as one writev.
            const msg = self.pipe.recvBlocking() catch |err| switch (err) {
                error.PipeClosed => return,
                else => return err,
            };

            // Encode ZMTP frame header + collect data pointers for writev
            var batch_buf: [8]Msg = undefined;
            var batch_count: u32 = 1;
            batch_buf[0] = msg;

            // Opportunistically drain more messages without blocking
            while (batch_count < batch_buf.len) {
                const next = self.pipe.recv() orelse break;
                batch_buf[batch_count] = next;
                batch_count += 1;
            }

            // Encode and write the batch
            try self.writeBatch(batch_buf[0..batch_count]);

            // Clean up sent messages
            for (batch_buf[0..batch_count]) |*m| m.deinit(self.allocator);
        }
    }

    /// Encode multiple messages as ZMTP frames and write with writev.
    /// Up to 16 iovecs: pairs of (header, body) for up to 8 messages.
    fn writeBatch(self: *Session, msgs: []const Msg) !void {
        var iovecs: [16]std.os.iovec_const = undefined;
        var headers: [8]ZmtpCodec.FrameHeader = undefined;
        var iov_count: usize = 0;

        for (msgs, 0..) |*msg, i| {
            const data = msg.dataSlice();

            // Encode ZMTP frame header (1-9 bytes)
            headers[i] = self.codec.encodeFrameHeader(
                data.len,
                msg.flags.more,
                msg.flags.command,
            );

            // iovec 1: frame header
            iovecs[iov_count] = .{
                .iov_base = &headers[i].bytes,
                .iov_len = headers[i].len,
            };
            iov_count += 1;

            // iovec 2: frame body (zero-copy from Msg data)
            if (data.len > 0) {
                iovecs[iov_count] = .{
                    .iov_base = data.ptr,
                    .iov_len = data.len,
                };
                iov_count += 1;
            }
        }

        // Single writev syscall for the entire batch
        try self.stream.writeVecAll(iovecs[0..iov_count], .none);
    }

    // -------------------------------------------------------
    // Receive loop: Network → Pipe
    // -------------------------------------------------------

    fn recvLoop(self: *Session) !void {
        // Read buffer: one contiguous allocation for network reads.
        // ZMTP frames are decoded in-place, then copied into Msg structs
        // (VSM for small) or zero-copy referenced (LMSG for large).
        var read_buf: [16384]u8 = undefined;
        var buf_pos: usize = 0;
        var buf_len: usize = 0;

        while (true) {
            // Read more data from the network
            if (buf_pos >= buf_len) {
                buf_len = self.stream.read(&read_buf, .none) catch |err| switch (err) {
                    error.EndOfStream => return,
                    else => return err,
                };
                if (buf_len == 0) return;  // connection closed
                buf_pos = 0;
            }

            // Decode ZMTP frames from the buffer
            var batch_count: u32 = 0;
            while (buf_pos < buf_len) {
                const frame = self.codec.decodeFrame(
                    read_buf[buf_pos..buf_len],
                ) orelse break;  // incomplete frame, need more data

                // ZMQ_MAXMSGSIZE enforcement: reject oversized frames
                // at the transport layer before allocating memory.
                if (self.options.max_msg_size >= 0 and
                    frame.body_len > @as(usize, @intCast(self.options.max_msg_size)))
                {
                    return error.MessageTooLarge;
                }

                // Create Msg from decoded frame.
                // VSM path (<=48 bytes): inline copy, zero alloc.
                // LMSG path (>48 bytes): single contiguous allocation
                // (Content header + data). We must copy from the network
                // read buffer because the buffer is reused for the next
                // read() call. True zero-copy of large frames would
                // require refcounted read buffers with per-frame
                // sub-slicing (a future optimization — see note below).
                var msg = try Msg.initSize(self.allocator, frame.body_len);
                @memcpy(msg.dataMut()[0..frame.body_len], frame.body);
                msg.flags.more = frame.more;
                msg.flags.command = frame.command;

                // Write to pipe (no flush yet — batch it)
                if (!try self.pipe.send(&msg)) {
                    // HWM hit — block until space available
                    try self.pipe.sendBlocking(&msg);
                }
                batch_count += 1;
                buf_pos += frame.total_len;
            }

            // Flush the batch: one CAS for all decoded messages
            if (batch_count > 0) {
                self.pipe.flush();
            }

            // Shift remaining bytes to front of buffer
            if (buf_pos > 0 and buf_pos < buf_len) {
                std.mem.copyForwards(u8, &read_buf, read_buf[buf_pos..buf_len]);
                buf_len -= buf_pos;
                buf_pos = 0;
            } else if (buf_pos >= buf_len) {
                buf_pos = 0;
                buf_len = 0;
            }
        }
    }
};
```

**Future optimization — zero-copy large frame receive**: The current
recvLoop copies every frame body into a new Msg allocation. For large
messages, this could be avoided by using a refcounted read buffer:
allocate a large buffer for `stream.read()`, wrap it in a refcounted
object, and create CMSG-style Msgs that point into sub-slices of the
buffer. The buffer is freed when all Msgs referencing it are deinit'd.
This trades simplicity for throughput on large-message workloads.
The current copy path is correct and simple; the optimization can be
added later without API changes.

### ZMTP Codec

ZMTP 3.1 frame format on the wire:

```
Short frame:  Flags(1) + Size(1) + Body(0..255)
Long frame:   Flags(1) + Size(8) + Body(0..2^63)

Flags byte:
  bit 0: MORE    - more frames in this logical message
  bit 1: LONG    - size field is 8 bytes (not 1)
  bit 2: COMMAND - this is a command frame (not data)
```

```zig
pub const ZmtpCodec = struct {
    pub const FrameHeader = struct {
        bytes: [9]u8,  // max header size: 1 flags + 8 size
        len: u8,       // actual header length (2 or 9)
    };

    pub const DecodedFrame = struct {
        body: []const u8,
        body_len: usize,
        total_len: usize,  // header + body
        more: bool,
        command: bool,
    };

    /// Encode a ZMTP frame header. Does not copy body data —
    /// the caller uses writev to send header + body together.
    pub fn encodeFrameHeader(
        self: *ZmtpCodec,
        body_len: usize,
        more: bool,
        command: bool,
    ) FrameHeader {
        var header: FrameHeader = .{ .bytes = undefined, .len = undefined };
        var flags: u8 = 0;
        if (more) flags |= 0x01;
        if (command) flags |= 0x04;

        if (body_len <= 255) {
            // Short frame: 1 byte flags + 1 byte size
            header.bytes[0] = flags;
            header.bytes[1] = @intCast(body_len);
            header.len = 2;
        } else {
            // Long frame: 1 byte flags + 8 byte size
            flags |= 0x02;  // LONG bit
            header.bytes[0] = flags;
            std.mem.writeInt(u64, header.bytes[1..9], @intCast(body_len), .big);
            header.len = 9;
        }
        return header;
    }

    /// Decode a ZMTP frame from the buffer. Returns null if the buffer
    /// doesn't contain a complete frame (need more data from the network).
    pub fn decodeFrame(self: *ZmtpCodec, buf: []const u8) ?DecodedFrame {
        if (buf.len < 2) return null;  // need at least flags + 1 byte size

        const flags = buf[0];
        const more = (flags & 0x01) != 0;
        const long = (flags & 0x02) != 0;
        const command = (flags & 0x04) != 0;

        var body_len: usize = undefined;
        var header_len: usize = undefined;

        if (long) {
            if (buf.len < 9) return null;  // need 8-byte size
            header_len = 9;
            body_len = @intCast(std.mem.readInt(u64, buf[1..9], .big));
        } else {
            header_len = 2;
            body_len = buf[1];
        }

        const total_len = header_len + body_len;
        if (buf.len < total_len) return null;  // incomplete body

        return .{
            .body = buf[header_len..total_len],
            .body_len = body_len,
            .total_len = total_len,
            .more = more,
            .command = command,
        };
    }

    /// Perform ZMTP 3.1 greeting and handshake on a new connection.
    pub fn handshake(self: *ZmtpCodec, stream: *zio.net.Stream) !void {
        // Greeting: 64-byte exchange
        //   Bytes 0-9:   Signature (0xFF + 8 padding + 0x7F)
        //   Byte 10:     Major version (3)
        //   Byte 11:     Minor version (1)
        //   Bytes 12-31: Mechanism ("NULL" + padding)
        //   Byte 32:     as-server flag
        //   Bytes 33-63: Padding (zeros)
        var greeting: [64]u8 = .{0} ** 64;
        greeting[0] = 0xFF;
        greeting[9] = 0x7F;
        greeting[10] = 3;  // major
        greeting[11] = 1;  // minor
        @memcpy(greeting[12..16], "NULL");

        // Send greeting and read peer's greeting concurrently.
        // (In practice, send first then read — simpler and works fine
        // because TCP buffers absorb the 64 bytes.)
        try stream.writeAll(&greeting, .none);

        var peer_greeting: [64]u8 = undefined;
        try stream.readAll(&peer_greeting, .none);

        // Validate peer greeting
        if (peer_greeting[0] != 0xFF or peer_greeting[9] != 0x7F)
            return error.InvalidGreeting;
        if (peer_greeting[10] < 3)
            return error.UnsupportedVersion;

        // NULL mechanism: send READY command, receive READY
        try self.sendReady(stream);
        try self.recvReady(stream);
    }

    fn sendReady(self: *ZmtpCodec, stream: *zio.net.Stream) !void {
        // READY command frame with socket type property
        const ready_body = "\x05READY" ++ // command name
            "\x0bSocket-Type" ++ // property name (11 bytes)
            "\x00\x00\x00\x04" ++ // property value length
            "PAIR"; // socket type (varies)
        const header = self.encodeFrameHeader(ready_body.len, false, true);
        var iovecs: [2]std.os.iovec_const = .{
            .{ .iov_base = &header.bytes, .iov_len = header.len },
            .{ .iov_base = ready_body.ptr, .iov_len = ready_body.len },
        };
        try stream.writeVecAll(&iovecs, .none);
    }

    fn recvReady(self: *ZmtpCodec, stream: *zio.net.Stream) !void {
        // Read and validate READY command from peer
        var header_buf: [9]u8 = undefined;
        try stream.readAll(header_buf[0..2], .none);
        // ... decode and validate READY command
    }
};
```

### Transport Bindings

All transports produce the same thing: a `Session` with a `Pipe` and
a `zio.net.Stream`. The only difference is how the `Stream` is obtained.

```zig
pub const Transport = struct {
    /// TCP transport: connect to remote host
    pub fn tcpConnect(
        allocator: Allocator,
        addr: zio.net.IpAddress,
        pipe: *Pipe,
    ) !*Session {
        var stream = try addr.connect(.{});
        errdefer stream.close();

        // TCP_NODELAY: critical for latency. Without it, Nagle's algorithm
        // delays small writes by up to 40ms waiting to coalesce.
        try stream.socket.setNoDelay(true);

        // Apply socket options to the TCP connection
        applyTcpOptions(stream.socket, options);

        const session = try allocator.create(Session);
        session.* = .{
            .pipe = pipe,
            .stream = stream,
            .codec = .{},
            .allocator = allocator,
            .options = options,
        };
        return session;
    }

    /// TCP transport: accept incoming connection
    pub fn tcpAccept(
        allocator: Allocator,
        server: zio.net.Server,
        pipe: *Pipe,
    ) !*Session {
        var stream = try server.accept();
        errdefer stream.close();

        try stream.socket.setNoDelay(true);
        applyTcpOptions(stream.socket, options);

        const session = try allocator.create(Session);
        session.* = .{
            .pipe = pipe,
            .stream = stream,
            .codec = .{},
            .allocator = allocator,
            .options = options,
        };
        return session;
    }

    /// Apply socket-level TCP options from Socket.Options.
    fn applyTcpOptions(sock: anytype, opts: *const Socket.Options) void {
        if (opts.sndbuf >= 0) sock.setSndBuf(@intCast(opts.sndbuf)) catch {};
        if (opts.rcvbuf >= 0) sock.setRcvBuf(@intCast(opts.rcvbuf)) catch {};
        if (opts.tos > 0) sock.setTos(opts.tos) catch {};
        if (opts.tcp_keepalive >= 0) {
            sock.setKeepAlive(opts.tcp_keepalive == 1) catch {};
            if (opts.tcp_keepalive == 1) {
                if (opts.tcp_keepalive_idle >= 0)
                    sock.setKeepIdle(@intCast(opts.tcp_keepalive_idle)) catch {};
                if (opts.tcp_keepalive_cnt >= 0)
                    sock.setKeepCnt(@intCast(opts.tcp_keepalive_cnt)) catch {};
                if (opts.tcp_keepalive_intvl >= 0)
                    sock.setKeepIntvl(@intCast(opts.tcp_keepalive_intvl)) catch {};
            }
        }
    }

    /// IPC transport: connect via Unix domain socket
    pub fn ipcConnect(
        allocator: Allocator,
        path: []const u8,
        pipe: *Pipe,
    ) !*Session {
        const addr = try zio.net.UnixAddress.init(path);
        var stream = try addr.connect(.{});
        errdefer stream.close();

        const session = try allocator.create(Session);
        session.* = .{
            .pipe = pipe,
            .stream = stream,
            .codec = .{},
            .allocator = allocator,
        };
        return session;
    }

    /// IPC transport: accept via Unix domain socket
    pub fn ipcAccept(
        allocator: Allocator,
        server: zio.net.Server,
        pipe: *Pipe,
    ) !*Session {
        var stream = try server.accept();
        errdefer stream.close();

        const session = try allocator.create(Session);
        session.* = .{
            .pipe = pipe,
            .stream = stream,
            .codec = .{},
            .allocator = allocator,
        };
        return session;
    }
};
```

### Transport Hot Path Analysis

For TCP/IPC, the hot path has two segments: the Pipe (inproc) and the
network I/O. The Pipe segment is identical to pure inproc and benefits
from all the same optimizations. The network segment is dominated by
syscall and kernel overhead.

```
App coroutine                 Session coroutine                Network
     |                              |                              |
  send(&msg)  ← ~5ns              |                              |
  flush()     ← ~20ns CAS         |                              |
  [wakeReader if needed]           |                              |
                                recvBlocking()                    |
                                  spin → catch data               |
                                  recv() × N  ← ~5ns/msg          |
                                  writeBatch() →→→→→→→→→→→→→ writev()
                                    encode headers ← ~3ns/msg     |  ← ~500-2000ns
                                    writev (batch)                |     (syscall +
                                    ← one syscall for N msgs      |      kernel copy)
                                                                  |
                              recvLoop():                     read()
                                stream.read() ←←←←←←←←←←← ← ~500-2000ns
                                decode frames ← ~5ns/frame        |
                                pipe.send() × N ← ~5ns/msg        |
                                pipe.flush() ← ~20ns CAS          |
                                [wakeReader if needed]             |
```

**Key insight**: The network syscall (~500-2000ns) dominates over the pipe
operations (~5-25ns). This means:

1. **Batching matters even more for TCP**: Each writev/read is a syscall.
   The session's send loop batches multiple messages into one writev
   (up to 8 messages = 16 iovecs of header+body pairs).

2. **The pipe's adaptive spin helps TCP too**: The session's recv loop
   decodes frames and writes to the pipe in batches, flushing once per
   read() syscall return. The app coroutine's adaptive spin catches
   these batches without parking.

3. **TCP_NODELAY is mandatory**: Without it, Nagle's algorithm adds up
   to 40ms latency on small writes. With it, each writev goes out
   immediately. The batching in writeBatch() replaces Nagle's role of
   coalescing small writes.

4. **Zero-copy where possible**: For LMSG (large messages), the session's
   writev uses the Msg's Content data pointer directly in the iovec —
   no memcpy. For VSM (small messages ≤48 bytes), the data is inline
   in the Msg struct and gets copied into the ZMTP frame, which is
   unavoidable but fast (one cache line).

5. **Session placement doesn't matter**: The session coroutine can be on
   any executor. If it ends up on the same executor as the app, pipe
   operations are ~5ns (L1 CAS). If different, ~20-25ns (cross-core
   CAS). Either way, the network syscall dominates.

### Reconnection

When a TCP connection drops, the session detects it (read returns 0 or
error) and exits. The socket layer handles reconnection:

```zig
// Reconnection is a socket-layer concern, not a session concern.
// The socket spawns a new session coroutine on reconnect.
fn reconnectLoop(socket: *Socket, addr: zio.net.IpAddress) !void {
    const opts = socket.options;
    // ZMQ_RECONNECT_IVL: -1 disables reconnect entirely.
    if (opts.reconnect_ivl_ms < 0) return;

    const base_ivl: u64 = @intCast(opts.reconnect_ivl_ms);
    // ZMQ_RECONNECT_IVL_MAX: 0 = no backoff (always use base_ivl).
    const max_ivl: u64 = if (opts.reconnect_ivl_max_ms > 0)
        @intCast(opts.reconnect_ivl_max_ms)
    else
        base_ivl;
    var backoff: u64 = base_ivl;

    while (socket.active) {
        // Create pipe pair for the new connection (heap-allocated)
        const pipes = try pipePair(
            socket.allocator, opts.hwm_send, opts.hwm_recv,
        );

        // Connect
        const session = Transport.tcpConnect(
            socket.allocator, addr, pipes[1],
        ) catch {
            // Connection failed — exponential backoff
            zio.time.sleep(backoff * std.time.ns_per_ms);
            backoff = @min(backoff * 2, max_ivl);
            continue;
        };

        backoff = base_ivl;  // reset on success

        // Attach pipe to socket
        socket.attachPipe(pipes[0]);

        // Run session (blocks until disconnect)
        session.run() catch {};

        // Detach pipe, clean up
        socket.detachPipe(pipes[0]);
        session.stream.close();
    }
}
```

---


## Layer 7: Socket Semantics (Pipe Lifecycle, Routing, Readiness)

This is the layer that gives ZMQ its programming model: socket types with
different routing patterns, connection/disconnection handling, readiness
polling, and multi-part message atomicity. In libzmq, these semantics are
spread across `socket_base_t`, `pipe_t`, `fq_t`, `lb_t`, and the
command/mailbox system. We consolidate them into a single coherent layer
that runs entirely on coroutines and our existing Pipe infrastructure.

### What libzmq Does (and What We Replace)

libzmq's socket layer has six interacting subsystems:

1. **Command system** (`mailbox_t` + `object_t::send_command`): Delivers
   pipe lifecycle events (`activate_read`, `activate_write`, `pipe_term`,
   `pipe_term_ack`, `hiccup`) as serialized commands through a mutex-guarded
   ypipe + signaler. **We eliminate this entirely** — our Pipe already
   handles data-flow signals through Waiters, and lifecycle events use
   atomic state + sentinel messages (see below).

2. **Pipe state machine** (6 states in `pipe_t`): Manages two-phase
   termination, flow control activation, and hiccup recovery. **We replace
   this** with a 4-state atomic state machine + delimiter messages through
   the existing ypipe.

3. **Fair queuing / load balancing** (`fq_t` / `lb_t`): Round-robin
   routing across active pipes with swap-to-back deactivation. **We keep
   the same algorithm**, adapted to Zig with direct method calls instead
   of command dispatch.

4. **Socket type dispatch** (virtual `xsend`/`xrecv`/`xhas_in`/`xhas_out`):
   Each socket type overrides these to implement its routing pattern.
   **We replace C++ virtual dispatch** with a Zig tagged union for
   zero-indirection dispatch.

5. **Readiness + poll** (`ZMQ_FD` + `ZMQ_EVENTS` + `zmq_poll`): FD-based
   readiness that integrates with OS poll/epoll. **We replace this** with
   coroutine-native multi-wait using a shared Waiter.

6. **Monitor events** (`zmq_socket_monitor`): Sends socket lifecycle events
   to a monitor socket via inproc. **We replace this** with a typed event
   channel.

### Pipe Lifecycle State Machine

In libzmq, pipe lifecycle transitions flow through the command system:
`terminate()` sends `PIPE_TERM` through the mailbox, the peer processes it
in `process_commands()`, and replies with `PIPE_TERM_ACK`. This requires
the full mailbox + signaler machinery.

We replace this with **in-band sentinel messages** (delimiters written into
the ypipe itself) plus an **atomic state field** on each pipe endpoint.
The ypipe is already a reliable ordered channel — we just send lifecycle
signals through it as special messages instead of through a separate
command channel.

```
State machine per pipe endpoint:

    ┌──────────┐
    │  active   │─── terminate() called ──→ write delimiter, flush
    └──────────┘                            set state = term_sent
         │                                       │
    read delimiter                               │
    from peer                                    ▼
         │                                ┌─────────────┐
         ▼                                │  term_sent   │
    ┌──────────────────┐                  └─────────────┘
    │ term_received     │                       │
    │ (drain remaining, │                  read delimiter
    │  write delimiter, │                  from peer (ack)
    │  flush)           │                       │
    └──────────────────┘                       ▼
         │                                ┌─────────────┐
         ▼                                │ terminated   │
    ┌─────────────┐                       └─────────────┘
    │ terminated   │
    └─────────────┘
```

The four states:

| State | Meaning | Entered when |
|-------|---------|--------------|
| `active` | Normal operation | Pipe created |
| `term_sent` | We initiated shutdown, waiting for peer ack | Local `terminate()` called |
| `term_received` | Peer initiated shutdown, we're draining | Read delimiter from inpipe |
| `terminated` | Both sides agreed, pipe is dead | Read delimiter after `term_sent`, or wrote delimiter after `term_received` |

```zig
pub const PipeState = enum(u8) {
    active,
    term_sent,
    term_received,
    terminated,
};

// Added to Pipe struct from Layer 4:
state: std.atomic.Value(PipeState) = .init(.active),
delay: bool = true,  // if true, drain pending messages before terminating

// Routing identity (set by ROUTER during identification)
routing_id: u32 = 0,

// Callback to socket layer on termination
on_terminated: ?*const fn (*Pipe) void = null,

// Sentinel message detection — uses a reserved bit in Msg.flags
// that never appears in user messages (ZMTP never sets bit 7)
const delimiter_flag: u8 = 0x80;

fn isDelimiter(msg: *const Msg) bool {
    return @as(u8, @bitCast(msg.flags)) & delimiter_flag != 0;
}

fn makeDelimiter() Msg {
    var msg = Msg{};
    msg.flags = @bitCast(delimiter_flag);
    return msg;
}
```

#### `terminate()` — Initiate Pipe Shutdown

Called by the socket layer when closing a connection or the entire socket.
The `delay` parameter controls whether the peer should drain pending
messages before completing termination (matches libzmq's `pipe_t::terminate`
behavior).

```zig
pub fn terminate(self: *Pipe, delay: bool) void {
    self.delay = delay;
    const current = self.state.load(.acquire);

    switch (current) {
        .active, .term_received => {
            // Stop writing — no more user messages after this
            self.out_active.store(false, .release);

            // Rollback any incomplete multi-part message
            self.rollback();

            // Write delimiter sentinel to outpipe and flush.
            // The peer will read this and know we're terminating.
            self.out_pipe.write(makeDelimiter(), false) catch {};
            _ = self.out_pipe.flush();

            if (current == .active) {
                self.state.store(.term_sent, .release);
            } else {
                // Was term_received → now both sides done
                self.state.store(.terminated, .release);
                self.notifyTerminated();
            }
        },
        .term_sent, .terminated => {},  // already terminating/terminated
    }
}

fn notifyTerminated(self: *Pipe) void {
    if (self.on_terminated) |cb| cb(self);
}
```

#### Reading Delimiters — Peer Termination Detection

The read path in `recv()` checks for delimiter sentinels. When one is
found, it means the peer called `terminate()`:

```zig
pub fn recv(self: *Pipe) ?Msg {
    if (!self.in_active) return null;

    const msg = self.in_pipe.read() orelse {
        self.in_active = false;
        return null;
    };

    // Check for delimiter sentinel
    if (isDelimiter(&msg)) {
        self.processDelimiter();
        return null;  // No user-visible message
    }

    // Normal message — update backpressure counters.
    // NOTE: peers_msgs_read is atomic (release/acquire) because it is
    // read by the writer on a different thread. See "Concurrency Model"
    // Path 2 for the happens-before proof.
    if (!msg.flags.more) {
        self.msgs_read += 1;
        if (self.msgs_read >= self.next_activate_threshold) {
            self.next_activate_threshold += self.lwm;
            self.peer.peers_msgs_read.store(self.msgs_read, .release);
            if (!self.peer.out_active.load(.acquire)) {
                self.peer.out_active.store(true, .release);
                self.wakeWriter();
            }
        }
    }
    return msg;
}

fn processDelimiter(self: *Pipe) void {
    const current = self.state.load(.acquire);
    switch (current) {
        .active => {
            // Peer terminated first. If delay=true, keep reading
            // remaining messages until inpipe is empty, THEN ack.
            // If delay=false, ack immediately.
            self.state.store(.term_received, .release);
            if (!self.delay) {
                self.terminate(false);  // Immediate ack
            }
            // If delay=true, socket layer continues draining.
            // When it calls terminate(), we'll transition to terminated.
        },
        .term_sent => {
            // We sent term, now peer acked — fully terminated
            self.state.store(.terminated, .release);
            self.notifyTerminated();
        },
        else => {},  // Already terminated
    }
}
```

#### Why Delimiters Through the YPipe (Not a Separate Channel)

It's tempting to use a separate atomic flag or channel for termination.
But sending the delimiter through the ypipe has critical ordering properties:

1. **Ordering guarantee**: The delimiter appears after all data messages
   the peer wrote before calling `terminate()`. A separate channel could
   race — the termination signal could arrive before the last data messages.

2. **Wake integration**: The delimiter flows through the same flush/CAS/Waiter
   path as data. If the reader is parked waiting for data, the delimiter's
   flush wakes it. No separate wake mechanism needed.

3. **No extra allocation**: The delimiter is a regular `Msg` (64 bytes)
   written into the existing ypipe slot. No channel, no extra memory.

This exactly mirrors libzmq's approach where the delimiter is written into
the pipe's ypipe (pipe.cpp:430-439), not sent as a command.

#### Hiccup (Reconnection)

In libzmq, `hiccup()` replaces the inpipe on the peer side to handle
reconnection without losing the pipe object. This is complex because it
must splice a new ypipe into an existing pipe.

We handle reconnection differently: **the session terminates the old pipe
pair entirely, and the reconnect loop creates a new pipe pair** (as shown
in Layer 6's `reconnectLoop`). The socket sees `pipeTerminated()` followed
by `attachPipe()`. This is simpler because:

1. The reconnect loop already creates fresh pipe pairs
2. Each Session is a separate coroutine with its own pipe endpoint
3. No need to splice ypipes — just attach/detach at the socket level
4. ROUTER tracks identity → pipe mappings, so the new pipe inherits
   the old identity if the same peer reconnects

### Socket Base

The socket base manages a set of pipes and delegates routing to the
socket type implementation. In libzmq, this is `socket_base_t` with
C++ virtual dispatch. In Zig, we use a tagged union.

```zig
pub const Socket = struct {
    // All attached pipes
    pipes: std.ArrayList(*Pipe),

    // Socket type implementation (tagged union, zero-indirection dispatch)
    pattern: Pattern,

    // Socket options
    options: Options,

    // Readiness signal: persistent, socket-owned. Pointer so Poller can
    // temporarily redirect it to a shared signal (see Updated Poller
    // Integration in Concurrency Model section).
    readiness_signal: *ReadySignal = undefined,  // set in init()
    own_readiness_signal: ReadySignal = .{},

    // Monitor event channel (null if no monitor attached)
    monitor: ?*MonitorChannel = null,

    // Lifecycle
    active: bool = true,
    allocator: Allocator,

    pub const Options = struct {
        // ---- Flow Control / HWM (ZMQ_SNDHWM, ZMQ_RCVHWM) ----
        hwm_send: u32 = 1000,          // 0 = no limit
        hwm_recv: u32 = 1000,          // 0 = no limit

        // ---- Message Limits (ZMQ_MAXMSGSIZE) ----
        // Maximum inbound message size. -1 = no limit.
        // Frames larger than this are rejected during ZMTP decode,
        // protecting against OOM from malicious/buggy peers.
        // Enforced in Session.recvLoop at the codec layer.
        max_msg_size: i64 = -1,

        // ---- Send/Recv Timeouts (ZMQ_SNDTIMEO, ZMQ_RCVTIMEO) ----
        // Timeout for blocking send/recv. -1 = infinite, 0 = non-blocking.
        // Applied in sendBlocking/recvBlocking via ReadySignal.parkTimeout.
        send_timeout_ms: i64 = -1,
        recv_timeout_ms: i64 = -1,

        // ---- Linger (ZMQ_LINGER) ----
        linger_ms: i64 = -1,           // -1 = infinite, 0 = discard, >0 = timeout

        // ---- Reconnection (ZMQ_RECONNECT_IVL, ZMQ_RECONNECT_IVL_MAX) ----
        // -1 = disable reconnect entirely.
        reconnect_ivl_ms: i32 = 100,
        reconnect_ivl_max_ms: i32 = 0, // 0 = no exponential backoff

        // ---- Connection (ZMQ_CONNECT_TIMEOUT, ZMQ_HANDSHAKE_IVL) ----
        connect_timeout_ms: u32 = 0,   // 0 = OS default
        handshake_timeout_ms: u32 = 30_000,

        // ---- TCP Transport (ZMQ_TCP_KEEPALIVE, ZMQ_SNDBUF, ZMQ_RCVBUF, ZMQ_TOS) ----
        tcp_keepalive: i32 = -1,       // -1 = OS default, 0 = off, 1 = on
        tcp_keepalive_idle: i32 = -1,  // seconds, -1 = OS default
        tcp_keepalive_cnt: i32 = -1,   // probe count, -1 = OS default
        tcp_keepalive_intvl: i32 = -1, // seconds between probes, -1 = OS default
        sndbuf: i32 = -1,              // SO_SNDBUF, -1 = OS default
        rcvbuf: i32 = -1,              // SO_RCVBUF, -1 = OS default
        tos: u8 = 0,                   // IP_TOS / DSCP

        // ---- ZMTP Heartbeat (ZMQ_HEARTBEAT_IVL, _TTL, _TIMEOUT) ----
        // Heartbeats detect dead connections faster than TCP keepalive.
        // 0 = disabled. When enabled, PING/PONG are exchanged at the
        // ZMTP level (session coroutine handles the timer).
        heartbeat_ivl_ms: u32 = 0,
        heartbeat_ttl_ms: u32 = 0,     // remote timeout, rounded to deciseconds
        heartbeat_timeout_ms: i32 = -1, // local timeout, -1 = use handshake_timeout

        // ---- Network (ZMQ_IPV6, ZMQ_BACKLOG, ZMQ_IMMEDIATE) ----
        ipv6: bool = false,            // enable dual-stack IPv4+6
        backlog: u32 = 100,            // listen(2) backlog
        // If true, only queue messages to completed connections
        // (connections that have finished the ZMTP handshake).
        // Prevents message loss during connect when using DEALER/PUSH.
        immediate: bool = false,

        // ---- Identity / Routing (ZMQ_ROUTING_ID) ----
        routing_id: ?[]const u8 = null, // ROUTER identity (1-255 bytes)

        // ---- ROUTER Options ----
        // ZMQ_ROUTER_MANDATORY: return error.HostUnreachable instead
        // of silently dropping when sending to an unknown routing ID.
        router_mandatory: bool = false,
        // ZMQ_ROUTER_HANDOVER: when a new connection arrives with an
        // existing routing ID, take over the old pipe (disconnect old peer).
        router_handover: bool = false,
        // ZMQ_PROBE_ROUTER: on connect, send an empty message to trigger
        // routing ID exchange immediately.
        probe_router: bool = false,

        // ---- REQ Options ----
        // ZMQ_REQ_RELAXED: allow sending a new request before receiving
        // the reply to the previous one (auto-reset REQ state machine).
        req_relaxed: bool = false,
        // ZMQ_REQ_CORRELATE: match replies to requests by correlation ID.
        // Enables safe pipelining by discarding mis-matched replies.
        req_correlate: bool = false,

        // ---- PUB/SUB Options ----
        // ZMQ_CONFLATE: keep only the last message in the queue.
        // Useful for telemetry/state where only latest value matters.
        // Disables multi-part messages.
        conflate: bool = false,
        // ZMQ_INVERT_MATCHING: deliver to non-matching subscriptions.
        invert_matching: bool = false,

        // ---- XPUB Options ----
        // ZMQ_XPUB_VERBOSE: pass all subscription messages upstream,
        // including duplicates (not just unique new ones).
        xpub_verbose: bool = false,
        // ZMQ_XPUB_VERBOSER: like verbose, also for unsubscribes.
        xpub_verboser: bool = false,

        // ---- Security Mechanism ----
        // Security is handled at the session/codec layer during ZMTP
        // handshake. The socket stores the configuration; the session
        // uses it when negotiating the connection.
        mechanism: SecurityMechanism = .null_mechanism,
        // PLAIN credentials (client side)
        plain_username: ?[]const u8 = null,
        plain_password: ?[]const u8 = null,
        // CURVE keys (32 bytes each). Zeroed = not set.
        curve_public_key: [32]u8 = .{0} ** 32,
        curve_secret_key: [32]u8 = .{0} ** 32,
        curve_server_key: [32]u8 = .{0} ** 32,
        // ZAP domain for authentication
        zap_domain: ?[]const u8 = null,

        // ---- Application Messages (DRAFT) ----
        // ZMQ_HELLO_MSG: auto-sent to each new peer on connect.
        hello_msg: ?[]const u8 = null,
        // ZMQ_DISCONNECT_MSG: received locally when a peer disconnects.
        disconnect_msg: ?[]const u8 = null,
    };

    pub const SecurityMechanism = enum {
        null_mechanism,  // ZMQ_NULL — no auth
        plain,           // ZMQ_PLAIN — username/password
        curve,           // ZMQ_CURVE — CurveZMQ (libsodium)
    };

    pub fn init(self: *Socket, allocator: Allocator, pattern: Pattern) void {
        self.* = .{
            .pipes = std.ArrayList(*Pipe).init(allocator),
            .pattern = pattern,
            .options = .{},
            .allocator = allocator,
        };
        self.readiness_signal = &self.own_readiness_signal;
    }

    // ----------------------------------------------------------
    // Pipe management (called by transport layer)
    // ----------------------------------------------------------

    /// Attach a new pipe from a connection or bind.
    /// Called by transport layer when a connection is established.
    pub fn attachPipe(self: *Socket, pipe: *Pipe, locally_initiated: bool) void {
        pipe.on_terminated = Socket.pipeTerminatedCb;
        self.pipes.append(pipe) catch return;

        // Delegate to socket type for pattern-specific setup
        self.pattern.attachPipe(pipe, locally_initiated);

        // Monitor event
        if (self.monitor) |m| m.send(.{ .pipe_attached = .{} });

        // If socket is terminating, immediately terminate the new pipe
        if (!self.active) {
            pipe.terminate(false);
        }
    }

    /// Called when a pipe reaches the `terminated` state.
    fn pipeTerminatedCb(pipe: *Pipe) void {
        // The socket reference is recovered from the pipe's parent
        const self = pipe.socket orelse return;
        self.pipeTerminated(pipe);
    }

    fn pipeTerminated(self: *Socket, pipe: *Pipe) void {
        // Delegate to socket type first (removes from FQ/LB)
        self.pattern.pipeTerminated(pipe);

        // Remove from master pipe list
        for (self.pipes.items, 0..) |p, i| {
            if (p == pipe) {
                _ = self.pipes.swapRemove(i);
                break;
            }
        }

        // Monitor event
        if (self.monitor) |m| m.send(.{ .disconnected = .{} });

        // Signal readiness change (pipe removal may affect has_in/has_out)
        self.signalReadiness();
    }

    // ----------------------------------------------------------
    // Readiness callbacks (called by Pipe on state changes)
    // ----------------------------------------------------------

    /// Called when a pipe that was empty now has data to read.
    pub fn readActivated(self: *Socket, pipe: *Pipe) void {
        self.pattern.readActivated(pipe);
        self.signalReadiness();
    }

    /// Called when a pipe that was at HWM now has space to write.
    pub fn writeActivated(self: *Socket, pipe: *Pipe) void {
        self.pattern.writeActivated(pipe);
        self.signalReadiness();
    }

    fn signalReadiness(self: *Socket) void {
        self.readiness_signal.notify();
    }

    // ----------------------------------------------------------
    // User-facing send/recv
    // ----------------------------------------------------------

    pub fn send(self: *Socket, msg: *Msg) !void {
        return self.pattern.send(msg);
    }

    pub fn recv(self: *Socket) !Msg {
        return self.pattern.recv();
    }

    /// Non-blocking readiness checks (equivalent to ZMQ_EVENTS).
    /// Unlike libzmq, these are simple field reads — no side effects,
    /// no process_commands() drain. State is always current because
    /// pipe events fire callbacks directly.
    pub fn hasIn(self: *Socket) bool {
        return self.pattern.hasIn();
    }

    pub fn hasOut(self: *Socket) bool {
        return self.pattern.hasOut();
    }

    /// Read-only getters (ZMQ_TYPE, ZMQ_RCVMORE, ZMQ_LAST_ENDPOINT)
    pub fn socketType(self: *Socket) SocketType {
        return @as(SocketType, self.pattern);
    }

    pub fn lastEndpoint(self: *Socket) ?[]const u8 {
        return self.last_endpoint;
    }

    /// Blocking send: parks coroutine until the socket can accept a message.
    /// Uses the persistent ReadySignal (not stack-allocated Waiters) to
    /// avoid the lifetime/UAF issues described in the Concurrency Model.
    /// Respects options.send_timeout_ms: -1 = block forever, 0 = try once.
    pub fn sendBlocking(self: *Socket, msg: *Msg) !void {
        if (self.hasOut()) return try self.send(msg);
        if (self.options.send_timeout_ms == 0) return error.Eagain;

        var gen = self.readiness_signal.currentGen();
        if (self.options.send_timeout_ms > 0) {
            const timeout = Timeout.fromMilliseconds(
                @intCast(self.options.send_timeout_ms),
            );
            while (true) {
                gen = self.readiness_signal.parkTimeout(gen, timeout) catch
                    return error.Eagain;
                if (self.hasOut()) return try self.send(msg);
            }
        } else {
            while (true) {
                gen = try self.readiness_signal.park(gen);
                if (self.hasOut()) return try self.send(msg);
            }
        }
    }

    /// Blocking recv: parks coroutine until a message is available.
    /// Respects options.recv_timeout_ms: -1 = block forever, 0 = try once.
    pub fn recvBlocking(self: *Socket) !Msg {
        if (self.hasIn()) return try self.recv();
        if (self.options.recv_timeout_ms == 0) return error.Eagain;

        var gen = self.readiness_signal.currentGen();
        if (self.options.recv_timeout_ms > 0) {
            const timeout = Timeout.fromMilliseconds(
                @intCast(self.options.recv_timeout_ms),
            );
            while (true) {
                gen = self.readiness_signal.parkTimeout(gen, timeout) catch
                    return error.Eagain;
                if (self.hasIn()) return try self.recv();
            }
        } else {
            while (true) {
                gen = try self.readiness_signal.park(gen);
                if (self.hasIn()) return try self.recv();
            }
        }
    }

    // ----------------------------------------------------------
    // Graceful shutdown
    // ----------------------------------------------------------

    /// Close the socket. Terminates all pipes, optionally waiting
    /// for outbound messages to drain (linger).
    ///
    /// Linger semantics (matches libzmq):
    ///   linger = 0:  Discard unsent messages immediately.
    ///   linger > 0:  Wait up to linger_ms for pipes to drain, then discard.
    ///   linger = -1: Wait indefinitely for all pipes to finish.
    pub fn close(self: *Socket) !void {
        self.active = false;

        // Terminate all pipes. delay=true gives them time to flush.
        const delay = self.options.linger_ms != 0;
        for (self.pipes.items) |pipe| {
            pipe.terminate(delay);
        }

        if (self.options.linger_ms == 0) {
            // Immediate: pipes already terminated with delay=false.
            // No waiting — unsent messages are discarded.
            return;
        }

        // Wait for all pipes to reach terminated state.
        // Use the socket's ReadySignal — pipeTerminated() calls
        // signalReadiness(), which bumps the generation.
        var gen = self.readiness_signal.currentGen();
        if (self.options.linger_ms > 0) {
            // Timed linger: wait up to linger_ms, then force-terminate
            // any remaining pipes without delay.
            const timeout = Timeout.fromMilliseconds(
                @intCast(self.options.linger_ms),
            );
            while (self.pipes.items.len > 0) {
                gen = self.readiness_signal.parkTimeout(gen, timeout) catch break;
            }
            // Force-terminate any pipes that didn't finish in time
            for (self.pipes.items) |pipe| {
                pipe.terminate(false);
            }
        } else {
            // Infinite linger (linger_ms == -1): wait until all done
            while (self.pipes.items.len > 0) {
                gen = try self.readiness_signal.park(gen);
            }
        }
    }
};
```

**Key difference from libzmq**: There is no `process_commands()` loop.
In libzmq, `socket_base_t::process_commands()` drains the mailbox on
every `getsockopt(ZMQ_EVENTS)`, every `send()`, and every `recv()` — it's
the mechanism that turns async pipe events into socket state changes. We
don't need this because pipe events (readActivated, writeActivated,
pipeTerminated) are delivered directly via callbacks or ReadySignal
notifications. The socket's state is always current.

#### libzmq Socket Option Coverage

The table below maps every libzmq socket option to our equivalent. Options
are grouped into: **Supported** (in Options struct or API), **Not Applicable**
(our architecture eliminates the need), and **Future** (deferred but designed
for). GSSAPI and niche transport options (VMCI, NORM, PGM/EPGM, WSS) are
omitted — they can be added as separate transport modules without core changes.

| libzmq Option | Ours | Notes |
|---|---|---|
| **Flow Control** | | |
| `ZMQ_SNDHWM` | `options.hwm_send` | Default 1000 |
| `ZMQ_RCVHWM` | `options.hwm_recv` | Default 1000 |
| `ZMQ_MAXMSGSIZE` | `options.max_msg_size` | Enforced in Session.recvLoop |
| `ZMQ_CONFLATE` | `options.conflate` | Pattern-level: keep only last msg |
| `ZMQ_SNDBUF` | `options.sndbuf` | Applied via `applyTcpOptions` |
| `ZMQ_RCVBUF` | `options.rcvbuf` | Applied via `applyTcpOptions` |
| **Timeouts** | | |
| `ZMQ_SNDTIMEO` | `options.send_timeout_ms` | Used in `sendBlocking` |
| `ZMQ_RCVTIMEO` | `options.recv_timeout_ms` | Used in `recvBlocking` |
| `ZMQ_LINGER` | `options.linger_ms` | Implemented in `close()` |
| `ZMQ_CONNECT_TIMEOUT` | `options.connect_timeout_ms` | Session connect |
| `ZMQ_HANDSHAKE_IVL` | `options.handshake_timeout_ms` | ZMTP handshake |
| **Reconnection** | | |
| `ZMQ_RECONNECT_IVL` | `options.reconnect_ivl_ms` | -1 = disable |
| `ZMQ_RECONNECT_IVL_MAX` | `options.reconnect_ivl_max_ms` | 0 = no backoff |
| **TCP Transport** | | |
| `ZMQ_TCP_KEEPALIVE` | `options.tcp_keepalive` | Applied via `applyTcpOptions` |
| `ZMQ_TCP_KEEPALIVE_IDLE` | `options.tcp_keepalive_idle` | |
| `ZMQ_TCP_KEEPALIVE_CNT` | `options.tcp_keepalive_cnt` | |
| `ZMQ_TCP_KEEPALIVE_INTVL` | `options.tcp_keepalive_intvl` | |
| `ZMQ_TOS` | `options.tos` | IP_TOS / DSCP |
| **Heartbeat (ZMTP 3.1)** | | |
| `ZMQ_HEARTBEAT_IVL` | `options.heartbeat_ivl_ms` | Session timer |
| `ZMQ_HEARTBEAT_TTL` | `options.heartbeat_ttl_ms` | Sent to peer |
| `ZMQ_HEARTBEAT_TIMEOUT` | `options.heartbeat_timeout_ms` | Local timeout |
| **Network** | | |
| `ZMQ_IPV6` | `options.ipv6` | Dual-stack |
| `ZMQ_BACKLOG` | `options.backlog` | `listen(2)` |
| `ZMQ_IMMEDIATE` | `options.immediate` | Queue only to completed connections |
| **Identity / Routing** | | |
| `ZMQ_ROUTING_ID` | `options.routing_id` | 1-255 bytes |
| `ZMQ_ROUTER_MANDATORY` | `options.router_mandatory` | Error vs silent drop |
| `ZMQ_ROUTER_HANDOVER` | `options.router_handover` | Takeover on ID collision |
| `ZMQ_PROBE_ROUTER` | `options.probe_router` | Empty msg on connect |
| **REQ Options** | | |
| `ZMQ_REQ_RELAXED` | `options.req_relaxed` | Skip strict alternation |
| `ZMQ_REQ_CORRELATE` | `options.req_correlate` | Match replies to requests |
| **PUB/SUB Options** | | |
| `ZMQ_SUBSCRIBE` | `SubPattern.subscribe()` | Method, not option |
| `ZMQ_UNSUBSCRIBE` | `SubPattern.unsubscribe()` | Method, not option |
| `ZMQ_INVERT_MATCHING` | `options.invert_matching` | |
| `ZMQ_XPUB_VERBOSE` | `options.xpub_verbose` | |
| `ZMQ_XPUB_VERBOSER` | `options.xpub_verboser` | |
| **Security** | | |
| `ZMQ_MECHANISM` | `options.mechanism` | NULL/PLAIN/CURVE |
| `ZMQ_PLAIN_USERNAME` | `options.plain_username` | |
| `ZMQ_PLAIN_PASSWORD` | `options.plain_password` | |
| `ZMQ_CURVE_PUBLICKEY` | `options.curve_public_key` | |
| `ZMQ_CURVE_SECRETKEY` | `options.curve_secret_key` | |
| `ZMQ_CURVE_SERVERKEY` | `options.curve_server_key` | |
| `ZMQ_ZAP_DOMAIN` | `options.zap_domain` | |
| **Application Messages** | | |
| `ZMQ_HELLO_MSG` | `options.hello_msg` | Auto-sent on connect |
| `ZMQ_DISCONNECT_MSG` | `options.disconnect_msg` | Local notification |
| **Read-Only Getters** | | |
| `ZMQ_TYPE` | `socket.socketType()` | From pattern tag |
| `ZMQ_RCVMORE` | `msg.flags.more` | Direct field access |
| `ZMQ_EVENTS` | `hasIn()` / `hasOut()` | No side effects (see note below) |
| `ZMQ_LAST_ENDPOINT` | `socket.lastEndpoint()` | |
| **Not Applicable** | | |
| `ZMQ_FD` | — | No FDs; coroutine-native poll replaces OS poll |
| `ZMQ_AFFINITY` | — | zio executors replace I/O thread affinity |
| `ZMQ_THREAD_SAFE` | — | All sockets are coroutine-safe by design |
| `ZMQ_BLOCKY` | — | No context-level global; linger is per-socket |
| `ZMQ_IN_BATCH_SIZE` | — | Batching controlled by Session read buffer |
| `ZMQ_OUT_BATCH_SIZE` | — | Batching controlled by writeBatch iovec |
| `ZMQ_ROUTER_RAW` | — | Raw TCP is a separate transport, not a mode |
| `ZMQ_TCP_ACCEPT_FILTER` | — | Deprecated in libzmq |
| `ZMQ_IPC_FILTER_*` | — | Deprecated in libzmq |
| `ZMQ_IPV4ONLY` | — | Deprecated alias for `!ZMQ_IPV6` |
| **Future** | | |
| `ZMQ_SOCKS_PROXY` | — | Requires SOCKS5 transport layer |
| `ZMQ_CONNECT_ROUTING_ID` | — | Pre-assign routing ID on connect |
| `ZMQ_XPUB_MANUAL` | — | Manual subscription management |
| `ZMQ_XPUB_WELCOME_MSG` | — | Welcome message on subscribe |
| `ZMQ_HICCUP_MSG` | — | Temporary disconnect notification |
| `ZMQ_METADATA` | — | ZMTP handshake metadata |
| `ZMQ_USE_FD` | — | Pre-allocated FD passthrough |
| `ZMQ_STREAM_NOTIFY` | — | STREAM socket type not yet implemented |
| `ZMQ_ROUTER_NOTIFY` | — | Connect/disconnect notifications |
| `ZMQ_RECONNECT_STOP` | — | Conditional reconnect disable |
| `ZMQ_TCP_MAXRT` | — | Max TCP retransmit timeout |
| `ZMQ_BINDTODEVICE` | — | SO_BINDTODEVICE for VRF |

### Fair Queuing (`FairQueue`)

Round-robin receive across multiple pipes. Same algorithm as libzmq's
`fq_t`: active pipes are kept at the front of the array, the `current`
index advances after each complete message, and pipes that become empty
are swapped to the back (deactivated).

```zig
pub const FairQueue = struct {
    pipes: std.ArrayList(*Pipe),
    active: usize = 0,       // pipes[0..active] are readable
    current: usize = 0,      // next pipe to read from (round-robin)
    more: bool = false,       // in the middle of a multi-part message?

    pub fn init(allocator: Allocator) FairQueue {
        return .{ .pipes = std.ArrayList(*Pipe).init(allocator) };
    }

    /// Add a pipe to the active set.
    pub fn attach(self: *FairQueue, pipe: *Pipe) void {
        self.pipes.append(pipe) catch return;
        // Move new pipe into active region by swapping with first inactive
        self.swap(self.active, self.pipes.items.len - 1);
        self.active += 1;
    }

    /// Remove a terminated pipe.
    pub fn pipeTerminated(self: *FairQueue, pipe: *Pipe) void {
        const index = self.indexOf(pipe) orelse return;

        if (index == self.current and self.more) {
            self.more = false;  // Protocol error: mid-multipart on dead pipe
        }

        if (index < self.active) {
            self.active -= 1;
            self.swap(index, self.active);
            if (self.current == self.active) self.current = 0;
        }
        self.removePipe(pipe);
    }

    /// Re-activate a pipe that has new data.
    pub fn activated(self: *FairQueue, pipe: *Pipe) void {
        const index = self.indexOf(pipe) orelse return;
        self.swap(index, self.active);
        self.active += 1;
    }

    /// Round-robin receive. Returns the message and the source pipe.
    pub fn recv(self: *FairQueue) ?struct { msg: Msg, pipe: *Pipe } {
        while (self.active > 0) {
            const pipe = self.pipes.items[self.current];
            if (pipe.recv()) |msg| {
                self.more = msg.flags.more;
                if (!self.more) {
                    // Complete message — advance round-robin
                    self.current = (self.current + 1) % self.active;
                }
                return .{ .msg = msg, .pipe = pipe };
            }

            // Pipe empty — deactivate (must not happen mid-multipart)
            self.active -= 1;
            self.swap(self.current, self.active);
            if (self.current == self.active) self.current = 0;
        }
        return null;
    }

    /// Check if any active pipe has data.
    pub fn hasIn(self: *FairQueue) bool {
        if (self.more) return true;  // Continuation guaranteed

        while (self.active > 0) {
            if (self.pipes.items[self.current].in_pipe.checkRead())
                return true;

            self.active -= 1;
            self.swap(self.current, self.active);
            if (self.current == self.active) self.current = 0;
        }
        return false;
    }

    fn swap(self: *FairQueue, a: usize, b: usize) void {
        const tmp = self.pipes.items[a];
        self.pipes.items[a] = self.pipes.items[b];
        self.pipes.items[b] = tmp;
    }

    fn indexOf(self: *FairQueue, pipe: *Pipe) ?usize {
        for (self.pipes.items, 0..) |p, i| {
            if (p == pipe) return i;
        }
        return null;
    }

    fn removePipe(self: *FairQueue, pipe: *Pipe) void {
        if (self.indexOf(pipe)) |i| _ = self.pipes.swapRemove(i);
    }
};
```

**Fairness guarantee**: The round-robin pointer advances after each
complete logical message (all frames including multi-part). A peer
sending large multi-part messages doesn't starve others.

### Load Balancer (`LoadBalancer`)

Round-robin send across multiple pipes. Same algorithm as libzmq's `lb_t`:
active pipes at front, current index advances after each complete message,
pipes at HWM are swapped to back.

```zig
pub const LoadBalancer = struct {
    pipes: std.ArrayList(*Pipe),
    active: usize = 0,
    current: usize = 0,
    more: bool = false,       // in the middle of a multi-part send?
    dropping: bool = false,   // dropping remainder of failed multipart?

    pub fn init(allocator: Allocator) LoadBalancer {
        return .{ .pipes = std.ArrayList(*Pipe).init(allocator) };
    }

    pub fn attach(self: *LoadBalancer, pipe: *Pipe) void {
        self.pipes.append(pipe) catch return;
        self.swap(self.active, self.pipes.items.len - 1);
        self.active += 1;
    }

    pub fn pipeTerminated(self: *LoadBalancer, pipe: *Pipe) void {
        const index = self.indexOf(pipe) orelse return;

        // If mid-multipart to this pipe, drop the rest
        if (index == self.current and self.more) {
            self.dropping = true;
        }

        if (index < self.active) {
            self.active -= 1;
            self.swap(index, self.active);
            if (self.current == self.active) self.current = 0;
        }
        self.removePipe(pipe);
    }

    pub fn activated(self: *LoadBalancer, pipe: *Pipe) void {
        const index = self.indexOf(pipe) orelse return;
        self.swap(index, self.active);
        self.active += 1;
    }

    /// Load-balanced send. Returns the pipe used, or null if dropping.
    ///
    /// Multi-part atomicity: all frames go to the same pipe. If the
    /// pipe hits HWM mid-message, rollback and drop the remainder.
    pub fn send(self: *LoadBalancer, msg: *Msg) !?*Pipe {
        // Phase 1: Drop remainder of failed multipart
        if (self.dropping) {
            self.more = msg.flags.more;
            self.dropping = self.more;
            _ = msg.move();  // Discard
            return null;
        }

        // Phase 2: Find writable pipe and write
        while (self.active > 0) {
            const pipe = self.pipes.items[self.current];

            if (pipe.send(msg) catch false) {
                self.more = msg.flags.more;
                if (!self.more) {
                    // Complete message — flush and advance
                    pipe.flush();
                    self.current = (self.current + 1) % self.active;
                }
                return pipe;
            }

            // Write failed (HWM). If mid-multipart, rollback.
            if (self.more) {
                pipe.rollback();
                self.dropping = msg.flags.more;
                self.more = false;
                return error.Eagain;
            }

            // Try next pipe
            self.active -= 1;
            if (self.current < self.active)
                self.swap(self.current, self.active)
            else
                self.current = 0;
        }

        return error.Eagain;
    }

    pub fn hasOut(self: *LoadBalancer) bool {
        if (self.more) return true;

        while (self.active > 0) {
            if (self.pipes.items[self.current].checkHwm()) return true;

            self.active -= 1;
            self.swap(self.current, self.active);
            if (self.current == self.active) self.current = 0;
        }
        return false;
    }

    fn swap(self: *LoadBalancer, a: usize, b: usize) void {
        const tmp = self.pipes.items[a];
        self.pipes.items[a] = self.pipes.items[b];
        self.pipes.items[b] = tmp;
    }

    fn indexOf(self: *LoadBalancer, pipe: *Pipe) ?usize {
        for (self.pipes.items, 0..) |p, i| {
            if (p == pipe) return i;
        }
        return null;
    }

    fn removePipe(self: *LoadBalancer, pipe: *Pipe) void {
        if (self.indexOf(pipe)) |i| _ = self.pipes.swapRemove(i);
    }
};
```

### Distribution (`Dist`) — Fan-Out for PUB

PUB sockets need to send to ALL matching pipes, not round-robin to one:

```zig
pub const Dist = struct {
    pipes: std.ArrayList(*Pipe),
    active: usize = 0,       // writable pipes at front
    more: bool = false,
    eligible: usize = 0,     // pipes eligible for current message

    pub fn init(allocator: Allocator) Dist {
        return .{ .pipes = std.ArrayList(*Pipe).init(allocator) };
    }

    pub fn attach(self: *Dist, pipe: *Pipe) void {
        self.pipes.append(pipe) catch return;
        self.swap(self.active, self.pipes.items.len - 1);
        self.active += 1;
    }

    pub fn activated(self: *Dist, pipe: *Pipe) void {
        const index = self.indexOf(pipe) orelse return;
        self.swap(index, self.active);
        self.active += 1;
    }

    pub fn pipeTerminated(self: *Dist, pipe: *Pipe) void {
        const index = self.indexOf(pipe) orelse return;
        if (index < self.active) {
            self.active -= 1;
            self.swap(index, self.active);
        }
        if (index < self.eligible) self.eligible -= 1;
        self.removePipe(pipe);
    }

    /// Send to all eligible pipes. For the first frame, all active
    /// pipes are eligible. Each pipe gets a copy (refcount bump for LMSG).
    pub fn send(self: *Dist, msg: *Msg) void {
        if (!self.more) {
            self.eligible = self.active;
        }

        var i: usize = 0;
        while (i < self.eligible) {
            const pipe = self.pipes.items[i];
            var copy = msg.copy();
            if (pipe.send(&copy) catch false) {
                i += 1;
            } else {
                // HWM — drop this pipe for this message
                copy.deinit(pipe.allocator);
                self.eligible -= 1;
                self.swap(i, self.eligible);
            }
        }

        self.more = msg.flags.more;
        if (!self.more) {
            for (self.pipes.items[0..self.eligible]) |pipe| {
                pipe.flush();
            }
        }
    }

    pub fn hasOut(_: *Dist) bool {
        return true;  // PUB always accepts (drops if all at HWM)
    }

    fn swap(self: *Dist, a: usize, b: usize) void {
        const tmp = self.pipes.items[a];
        self.pipes.items[a] = self.pipes.items[b];
        self.pipes.items[b] = tmp;
    }
    fn indexOf(self: *Dist, pipe: *Pipe) ?usize {
        for (self.pipes.items, 0..) |p, i| {
            if (p == pipe) return i;
        }
        return null;
    }
    fn removePipe(self: *Dist, pipe: *Pipe) void {
        if (self.indexOf(pipe)) |i| _ = self.pipes.swapRemove(i);
    }
};
```

### Socket Type Implementations (`Pattern`)

Each socket type is a variant in a tagged union. The `Pattern` provides
routing logic; the `Socket` provides pipe management and blocking.

```zig
pub const Pattern = union(enum) {
    pair: PairPattern,
    push: PushPattern,
    pull: PullPattern,
    pub_: PubPattern,
    sub: SubPattern,
    dealer: DealerPattern,
    router: RouterPattern,
    req: ReqPattern,
    rep: RepPattern,

    // Dispatch methods — inline switch, zero pointer indirection,
    // branch-predicted after first call (always same variant).
    pub fn attachPipe(self: *Pattern, pipe: *Pipe, locally_initiated: bool) void {
        switch (self.*) {
            inline else => |*p| p.attachPipe(pipe, locally_initiated),
        }
    }
    pub fn pipeTerminated(self: *Pattern, pipe: *Pipe) void {
        switch (self.*) {
            inline else => |*p| p.pipeTerminated(pipe),
        }
    }
    pub fn readActivated(self: *Pattern, pipe: *Pipe) void {
        switch (self.*) {
            inline else => |*p| p.readActivated(pipe),
        }
    }
    pub fn writeActivated(self: *Pattern, pipe: *Pipe) void {
        switch (self.*) {
            inline else => |*p| p.writeActivated(pipe),
        }
    }
    pub fn send(self: *Pattern, msg: *Msg) !void {
        switch (self.*) {
            inline else => |*p| try p.send(msg),
        }
    }
    pub fn recv(self: *Pattern) !Msg {
        switch (self.*) {
            inline else => |*p| return try p.recv(),
        }
    }
    pub fn hasIn(self: *Pattern) bool {
        switch (self.*) {
            inline else => |*p| return p.hasIn(),
        }
    }
    pub fn hasOut(self: *Pattern) bool {
        switch (self.*) {
            inline else => |*p| return p.hasOut(),
        }
    }
};
```

#### PAIR — One-to-One Bidirectional

Exactly one pipe. Additional connections rejected.

```zig
pub const PairPattern = struct {
    pipe: ?*Pipe = null,

    pub fn attachPipe(self: *PairPattern, pipe: *Pipe, _: bool) void {
        if (self.pipe != null) {
            pipe.terminate(false);  // PAIR allows only one peer
            return;
        }
        self.pipe = pipe;
    }

    pub fn pipeTerminated(self: *PairPattern, pipe: *Pipe) void {
        if (self.pipe == pipe) self.pipe = null;
    }

    pub fn readActivated(_: *PairPattern, _: *Pipe) void {}
    pub fn writeActivated(_: *PairPattern, _: *Pipe) void {}

    pub fn send(self: *PairPattern, msg: *Msg) !void {
        const pipe = self.pipe orelse return error.Eagain;
        if (!(pipe.send(msg) catch false)) return error.Eagain;
        if (!msg.flags.more) pipe.flush();
    }

    pub fn recv(self: *PairPattern) !Msg {
        const pipe = self.pipe orelse return error.Eagain;
        return pipe.recv() orelse error.Eagain;
    }

    pub fn hasIn(self: *PairPattern) bool {
        const pipe = self.pipe orelse return false;
        return pipe.in_pipe.checkRead();
    }

    pub fn hasOut(self: *PairPattern) bool {
        const pipe = self.pipe orelse return false;
        return pipe.checkHwm();
    }
};
```

#### PUSH / PULL — Load-Balanced Send / Fair-Queued Receive

```zig
pub const PushPattern = struct {
    lb: LoadBalancer,

    pub fn init(allocator: Allocator) PushPattern {
        return .{ .lb = LoadBalancer.init(allocator) };
    }

    pub fn attachPipe(self: *PushPattern, pipe: *Pipe, _: bool) void {
        self.lb.attach(pipe);
    }
    pub fn pipeTerminated(self: *PushPattern, pipe: *Pipe) void {
        self.lb.pipeTerminated(pipe);
    }
    pub fn readActivated(_: *PushPattern, _: *Pipe) void {}
    pub fn writeActivated(self: *PushPattern, pipe: *Pipe) void {
        self.lb.activated(pipe);
    }
    pub fn send(self: *PushPattern, msg: *Msg) !void {
        _ = try self.lb.send(msg);
    }
    pub fn recv(_: *PushPattern) !Msg { return error.NotSupported; }
    pub fn hasIn(_: *PushPattern) bool { return false; }
    pub fn hasOut(self: *PushPattern) bool { return self.lb.hasOut(); }
};

pub const PullPattern = struct {
    fq: FairQueue,

    pub fn init(allocator: Allocator) PullPattern {
        return .{ .fq = FairQueue.init(allocator) };
    }

    pub fn attachPipe(self: *PullPattern, pipe: *Pipe, _: bool) void {
        self.fq.attach(pipe);
    }
    pub fn pipeTerminated(self: *PullPattern, pipe: *Pipe) void {
        self.fq.pipeTerminated(pipe);
    }
    pub fn readActivated(self: *PullPattern, pipe: *Pipe) void {
        self.fq.activated(pipe);
    }
    pub fn writeActivated(_: *PullPattern, _: *Pipe) void {}
    pub fn send(_: *PullPattern, _: *Msg) !void { return error.NotSupported; }
    pub fn recv(self: *PullPattern) !Msg {
        const result = self.fq.recv() orelse return error.Eagain;
        return result.msg;
    }
    pub fn hasIn(self: *PullPattern) bool { return self.fq.hasIn(); }
    pub fn hasOut(_: *PullPattern) bool { return false; }
};
```

#### PUB / SUB — Fan-Out with Subscription Filtering

```zig
pub const PubPattern = struct {
    dist: Dist,

    pub fn init(allocator: Allocator) PubPattern {
        return .{ .dist = Dist.init(allocator) };
    }

    pub fn attachPipe(self: *PubPattern, pipe: *Pipe, _: bool) void {
        self.dist.attach(pipe);
    }
    pub fn pipeTerminated(self: *PubPattern, pipe: *Pipe) void {
        self.dist.pipeTerminated(pipe);
    }
    pub fn readActivated(_: *PubPattern, _: *Pipe) void {}
    pub fn writeActivated(self: *PubPattern, pipe: *Pipe) void {
        self.dist.activated(pipe);
    }
    pub fn send(self: *PubPattern, msg: *Msg) !void {
        self.dist.send(msg);
    }
    pub fn recv(_: *PubPattern) !Msg { return error.NotSupported; }
    pub fn hasIn(_: *PubPattern) bool { return false; }
    pub fn hasOut(self: *PubPattern) bool { return self.dist.hasOut(); }
};

pub const SubPattern = struct {
    fq: FairQueue,
    // Trie for O(prefix_len) subscription matching (matches libzmq's
    // mtrie_t). Linear scan is O(subscriptions * prefix_len) per
    // message, which degrades badly with many subscriptions.
    trie: PrefixTrie,
    // Keep a list for re-sending subscriptions on new pipe attach.
    subscriptions: std.ArrayList([]const u8),

    pub fn init(allocator: Allocator) SubPattern {
        return .{
            .fq = FairQueue.init(allocator),
            .trie = PrefixTrie.init(allocator),
            .subscriptions = std.ArrayList([]const u8).init(allocator),
        };
    }

    pub fn attachPipe(self: *SubPattern, pipe: *Pipe, _: bool) void {
        self.fq.attach(pipe);
        // Send existing subscriptions to the new publisher
        for (self.subscriptions.items) |prefix| {
            sendSubscriptionCmd(pipe, prefix, true);
        }
    }
    pub fn pipeTerminated(self: *SubPattern, pipe: *Pipe) void {
        self.fq.pipeTerminated(pipe);
    }
    pub fn readActivated(self: *SubPattern, pipe: *Pipe) void {
        self.fq.activated(pipe);
    }
    pub fn writeActivated(_: *SubPattern, _: *Pipe) void {}

    pub fn subscribe(self: *SubPattern, prefix: []const u8) !void {
        try self.subscriptions.append(prefix);
        try self.trie.insert(prefix);
        for (self.fq.pipes.items) |pipe| {
            sendSubscriptionCmd(pipe, prefix, true);
        }
    }

    pub fn unsubscribe(self: *SubPattern, prefix: []const u8) void {
        for (self.subscriptions.items, 0..) |sub, i| {
            if (std.mem.eql(u8, sub, prefix)) {
                _ = self.subscriptions.swapRemove(i);
                break;
            }
        }
        self.trie.remove(prefix);
        for (self.fq.pipes.items) |pipe| {
            sendSubscriptionCmd(pipe, prefix, false);
        }
    }

    pub fn send(_: *SubPattern, _: *Msg) !void { return error.NotSupported; }

    /// Recv with subscription filtering — discard non-matching messages
    pub fn recv(self: *SubPattern) !Msg {
        while (true) {
            const result = self.fq.recv() orelse return error.Eagain;
            if (self.trie.matches(result.msg.dataSlice())) return result.msg;
            // Doesn't match — discard (drain remaining multipart frames)
            var m = result.msg;
            while (m.flags.more) {
                const next = self.fq.recv() orelse break;
                m = next.msg;
            }
        }
    }

    pub fn hasIn(self: *SubPattern) bool { return self.fq.hasIn(); }
    pub fn hasOut(_: *SubPattern) bool { return false; }

    fn sendSubscriptionCmd(pipe: *Pipe, prefix: []const u8, is_sub: bool) void {
        // ZMTP subscription: byte 0 = 0x01 (sub) or 0x00 (unsub), then prefix
        var cmd = Msg.initSize(pipe.allocator, 1 + prefix.len) catch return;
        const data = cmd.dataMut();
        data[0] = if (is_sub) 0x01 else 0x00;
        @memcpy(data[1..], prefix);
        cmd.flags.command = true;
        _ = pipe.send(&cmd) catch {};
        pipe.flush();
    }
};

/// Prefix trie for subscription matching (replaces linear scan).
/// Matches libzmq's mtrie_t: each node has 256 children (one per byte).
/// A node is "terminal" if a subscription ends there. Matching walks
/// the trie byte-by-byte; any terminal node on the path means "match".
///
/// Complexity:
///   insert/remove: O(prefix_len)
///   match: O(min(data_len, max_prefix_len))
///
/// This is critical for PUB/SUB workloads with many subscriptions.
/// Linear scan is O(N * prefix_len) per message; the trie is O(prefix_len).
pub const PrefixTrie = struct {
    const Node = struct {
        children: [256]?*Node = .{null} ** 256,
        terminal_count: u32 = 0,  // >0 means a subscription ends here
    };

    root: Node = .{},
    empty_sub: bool = false,  // empty-string subscription (matches all)
    allocator: Allocator,

    pub fn init(allocator: Allocator) PrefixTrie {
        return .{ .allocator = allocator };
    }

    pub fn insert(self: *PrefixTrie, prefix: []const u8) !void {
        if (prefix.len == 0) {
            self.empty_sub = true;
            return;
        }
        var node = &self.root;
        for (prefix) |byte| {
            if (node.children[byte] == null) {
                node.children[byte] = try self.allocator.create(Node);
                node.children[byte].?.* = .{};
            }
            node = node.children[byte].?;
        }
        node.terminal_count += 1;
    }

    pub fn remove(self: *PrefixTrie, prefix: []const u8) void {
        if (prefix.len == 0) {
            self.empty_sub = false;
            return;
        }
        var node = &self.root;
        for (prefix) |byte| {
            node = node.children[byte] orelse return;
        }
        if (node.terminal_count > 0) node.terminal_count -= 1;
    }

    /// Returns true if any subscription prefix matches the data.
    /// Walks the trie byte-by-byte; returns true at the first
    /// terminal node encountered (shortest matching prefix).
    pub fn matches(self: *PrefixTrie, data: []const u8) bool {
        if (self.empty_sub) return true;
        var node = &self.root;
        for (data) |byte| {
            node = node.children[byte] orelse return false;
            if (node.terminal_count > 0) return true;
        }
        return false;
    }
};
```

#### REQ / REP — Strict Request-Reply

REQ enforces send→recv→send→recv alternation and prepends an empty
delimiter frame. REP enforces recv→send→recv→send and strips/restores it.

```zig
pub const ReqPattern = struct {
    lb: LoadBalancer,
    fq: FairQueue,
    receiving_reply: bool = false,
    message_begins: bool = true,
    reply_pipe: ?*Pipe = null,

    pub fn init(allocator: Allocator) ReqPattern {
        return .{
            .lb = LoadBalancer.init(allocator),
            .fq = FairQueue.init(allocator),
        };
    }

    pub fn attachPipe(self: *ReqPattern, pipe: *Pipe, _: bool) void {
        self.lb.attach(pipe);
        self.fq.attach(pipe);
    }
    pub fn pipeTerminated(self: *ReqPattern, pipe: *Pipe) void {
        if (self.reply_pipe == pipe) {
            self.reply_pipe = null;
            self.receiving_reply = false;
            self.message_begins = true;
        }
        self.lb.pipeTerminated(pipe);
        self.fq.pipeTerminated(pipe);
    }
    pub fn readActivated(self: *ReqPattern, pipe: *Pipe) void {
        self.fq.activated(pipe);
    }
    pub fn writeActivated(self: *ReqPattern, pipe: *Pipe) void {
        self.lb.activated(pipe);
    }

    pub fn send(self: *ReqPattern, msg: *Msg) !void {
        if (self.receiving_reply) return error.WrongState;  // EFSM

        if (self.message_begins) {
            // Prepend empty delimiter frame (REQ envelope)
            var delim = Msg{};
            delim.flags.more = true;
            self.reply_pipe = try self.lb.send(&delim);
            self.message_begins = false;
        }

        const more = msg.flags.more;
        _ = try self.lb.send(msg);
        if (!more) {
            self.receiving_reply = true;
            self.message_begins = true;
        }
    }

    pub fn recv(self: *ReqPattern) !Msg {
        if (!self.receiving_reply) return error.WrongState;

        while (true) {
            const result = self.fq.recv() orelse return error.Eagain;

            // Discard stale replies from previous requests
            if (self.reply_pipe != null and result.pipe != self.reply_pipe.?) {
                var m = result.msg;
                while (m.flags.more) {
                    const next = self.fq.recv() orelse break;
                    m = next.msg;
                }
                continue;
            }

            // First frame is empty delimiter — skip it
            if (result.msg.dataSlice().len == 0 and result.msg.flags.more) {
                const reply = self.fq.recv() orelse return error.Eagain;
                if (!reply.msg.flags.more) self.receiving_reply = false;
                return reply.msg;
            }
        }
    }

    pub fn hasIn(self: *ReqPattern) bool {
        return self.receiving_reply and self.fq.hasIn();
    }
    pub fn hasOut(self: *ReqPattern) bool {
        return !self.receiving_reply and self.lb.hasOut();
    }
};

pub const RepPattern = struct {
    fq: FairQueue,
    sending_reply: bool = false,
    request_begins: bool = true,
    reply_pipe: ?*Pipe = null,

    pub fn init(allocator: Allocator) RepPattern {
        return .{ .fq = FairQueue.init(allocator) };
    }

    pub fn attachPipe(self: *RepPattern, pipe: *Pipe, _: bool) void {
        self.fq.attach(pipe);
    }
    pub fn pipeTerminated(self: *RepPattern, pipe: *Pipe) void {
        if (self.reply_pipe == pipe) {
            self.reply_pipe = null;
            self.sending_reply = false;
            self.request_begins = true;
        }
        self.fq.pipeTerminated(pipe);
    }
    pub fn readActivated(self: *RepPattern, pipe: *Pipe) void {
        self.fq.activated(pipe);
    }
    pub fn writeActivated(_: *RepPattern, _: *Pipe) void {}

    pub fn recv(self: *RepPattern) !Msg {
        if (self.sending_reply) return error.WrongState;

        if (self.request_begins) {
            // Strip routing envelope up to empty delimiter
            while (true) {
                const result = self.fq.recv() orelse return error.Eagain;
                if (result.msg.dataSlice().len == 0 and result.msg.flags.more) {
                    self.reply_pipe = result.pipe;
                    break;  // Found delimiter
                }
                if (!result.msg.flags.more) return error.Eagain;  // Malformed
            }
            self.request_begins = false;
        }

        const result = self.fq.recv() orelse return error.Eagain;
        if (!result.msg.flags.more) self.sending_reply = true;
        return result.msg;
    }

    pub fn send(self: *RepPattern, msg: *Msg) !void {
        if (!self.sending_reply) return error.WrongState;
        const pipe = self.reply_pipe orelse return error.Eagain;

        // Prepend empty delimiter for reply routing
        if (self.request_begins) {
            var delim = Msg{};
            delim.flags.more = true;
            _ = pipe.send(&delim) catch return error.Eagain;
        }

        const more = msg.flags.more;
        if (!(pipe.send(msg) catch false)) return error.Eagain;
        if (!more) {
            pipe.flush();
            self.sending_reply = false;
            self.request_begins = true;
        }
    }

    pub fn hasIn(self: *RepPattern) bool {
        return !self.sending_reply and self.fq.hasIn();
    }
    pub fn hasOut(self: *RepPattern) bool {
        return self.sending_reply and self.reply_pipe != null;
    }
};
```

#### DEALER / ROUTER — Async Routing

DEALER: load-balanced send + fair-queued recv, no enforced alternation.

```zig
pub const DealerPattern = struct {
    fq: FairQueue,
    lb: LoadBalancer,

    pub fn init(allocator: Allocator) DealerPattern {
        return .{
            .fq = FairQueue.init(allocator),
            .lb = LoadBalancer.init(allocator),
        };
    }

    pub fn attachPipe(self: *DealerPattern, pipe: *Pipe, _: bool) void {
        self.fq.attach(pipe);
        self.lb.attach(pipe);
    }
    pub fn pipeTerminated(self: *DealerPattern, pipe: *Pipe) void {
        self.fq.pipeTerminated(pipe);
        self.lb.pipeTerminated(pipe);
    }
    pub fn readActivated(self: *DealerPattern, pipe: *Pipe) void {
        self.fq.activated(pipe);
    }
    pub fn writeActivated(self: *DealerPattern, pipe: *Pipe) void {
        self.lb.activated(pipe);
    }
    pub fn send(self: *DealerPattern, msg: *Msg) !void {
        _ = try self.lb.send(msg);
    }
    pub fn recv(self: *DealerPattern) !Msg {
        const result = self.fq.recv() orelse return error.Eagain;
        return result.msg;
    }
    pub fn hasIn(self: *DealerPattern) bool { return self.fq.hasIn(); }
    pub fn hasOut(self: *DealerPattern) bool { return self.lb.hasOut(); }
};
```

ROUTER: identity-aware routing. Prepends routing ID frame on recv,
uses routing ID to select outbound pipe on send.

```zig
pub const RouterPattern = struct {
    fq: FairQueue,
    // Identity → outbound pipe map
    out_pipes: std.AutoHashMap(u32, *Pipe),
    // Pipes waiting for identification
    anonymous_pipes: std.AutoHashMap(*Pipe, void),
    // Currently selected pipe for multi-part sends
    current_out: ?*Pipe = null,
    more_out: bool = false,
    // Auto-generated routing ID counter
    next_routing_id: u32 = 1,
    // Prefetch buffer for identity frame prepending
    prefetch: ?Msg = null,
    prefetch_pipe: ?*Pipe = null,

    // ZMQ_ROUTER_MANDATORY: if true, return error on unknown routing ID
    // instead of silently dropping.
    mandatory: bool = false,

    allocator: Allocator,

    pub fn init(allocator: Allocator, options: *const Socket.Options) RouterPattern {
        return .{
            .fq = FairQueue.init(allocator),
            .out_pipes = std.AutoHashMap(u32, *Pipe).init(allocator),
            .anonymous_pipes = std.AutoHashMap(*Pipe, void).init(allocator),
            .mandatory = options.router_mandatory,
            .allocator = allocator,
        };
    }

    pub fn attachPipe(self: *RouterPattern, pipe: *Pipe, locally_initiated: bool) void {
        if (self.identifyPeer(pipe, locally_initiated)) {
            self.fq.attach(pipe);
        } else {
            self.anonymous_pipes.put(pipe, {}) catch {};
        }
    }

    pub fn pipeTerminated(self: *RouterPattern, pipe: *Pipe) void {
        if (self.anonymous_pipes.remove(pipe) != null) return;

        _ = self.out_pipes.remove(pipe.routing_id);
        self.fq.pipeTerminated(pipe);

        if (self.current_out == pipe) {
            self.current_out = null;
            self.more_out = false;
        }
    }

    pub fn readActivated(self: *RouterPattern, pipe: *Pipe) void {
        if (self.anonymous_pipes.contains(pipe)) {
            if (self.identifyPeer(pipe, false)) {
                _ = self.anonymous_pipes.remove(pipe);
                self.fq.attach(pipe);
            }
        } else {
            self.fq.activated(pipe);
        }
    }

    pub fn writeActivated(_: *RouterPattern, _: *Pipe) void {}

    /// ROUTER recv: prepends routing ID frame to every message.
    /// Returns [routing_id_frame][...payload frames...]
    pub fn recv(self: *RouterPattern) !Msg {
        // If we have a prefetched payload, return it
        if (self.prefetch) |msg| {
            self.prefetch = null;
            return msg;
        }

        // Fair-queue a message, then prepend the source pipe's routing ID
        const result = self.fq.recv() orelse return error.Eagain;

        // Save the payload for the next recv() call
        self.prefetch = result.msg;
        self.prefetch_pipe = result.pipe;

        // Return the routing ID frame first
        var id_msg = Msg.initSize(self.allocator, 4) catch return error.Eagain;
        std.mem.writeInt(u32, id_msg.dataMut()[0..4], result.pipe.routing_id, .little);
        id_msg.flags.more = true;  // More frames follow (the actual payload)
        return id_msg;
    }

    /// ROUTER send: first frame is routing ID (selects pipe),
    /// subsequent frames are the payload.
    pub fn send(self: *RouterPattern, msg: *Msg) !void {
        if (!self.more_out) {
            // First frame — extract routing ID
            const data = msg.dataSlice();
            if (data.len != 4) return error.InvalidRoutingId;
            const routing_id = std.mem.readInt(u32, data[0..4], .little);

            self.current_out = self.out_pipes.get(routing_id) orelse {
                // ZMQ_ROUTER_MANDATORY: error vs silent drop
                if (self.mandatory) return error.HostUnreachable;
                return;  // silently drop (libzmq default)
            };
            self.more_out = true;
            return;  // Consumed routing ID frame
        }

        const pipe = self.current_out orelse return error.HostUnreachable;
        const more = msg.flags.more;
        if (!(pipe.send(msg) catch false)) return error.Eagain;
        if (!more) {
            pipe.flush();
            self.more_out = false;
            self.current_out = null;
        }
    }

    pub fn hasIn(self: *RouterPattern) bool {
        return self.prefetch != null or self.fq.hasIn();
    }

    pub fn hasOut(_: *RouterPattern) bool {
        return true;  // Can always attempt (fails at send time if ID unknown)
    }

    fn identifyPeer(self: *RouterPattern, pipe: *Pipe, _: bool) bool {
        const id = self.next_routing_id;
        self.next_routing_id += 1;
        pipe.routing_id = id;
        self.out_pipes.put(id, pipe) catch return false;
        return true;
    }
};
```

### Coroutine-Native Poll/Select

libzmq's `zmq_poll()` works by getting each socket's FD (`ZMQ_FD`),
calling OS `poll()`, then checking `ZMQ_EVENTS`. This requires the entire
signaler/mailbox infrastructure.

We replace it with **coroutine-native multi-wait**. No FDs, no OS poll,
no signaler.

**Original design (shown here to illustrate the UAF problem — see fix
below in "Updated Poller Integration")**. This version uses a stack-
allocated Waiter, which is vulnerable to use-after-free if the signal
arrives after the poller returns:

```zig
pub const Poller = struct {
    pub const Item = struct {
        socket: *Socket,
        events: Events,      // What to watch for
        revents: Events = .{}, // What fired
    };

    pub const Events = packed struct(u8) {
        pollin: bool = false,
        pollout: bool = false,
        _pad: u6 = 0,
    };

    /// Wait for any socket to become ready. Returns count of ready sockets.
    ///
    /// 1. Non-blocking check all sockets
    /// 2. If nothing ready, register shared Waiter on all sockets
    /// 3. Park coroutine (with optional timeout)
    /// 4. Any socket readiness change signals Waiter → wakes us
    /// 5. Re-check all sockets, return results
    pub fn poll(items: []Item, timeout_ms: i64) !u32 {
        // Phase 1: non-blocking
        var ready = checkAll(items);
        if (ready > 0 or timeout_ms == 0) return ready;

        // Phase 2: register shared waiter
        var waiter = Waiter.init();
        for (items) |*item| {
            item.socket.readiness_waiter = &waiter;
        }
        defer {
            for (items) |*item| {
                item.socket.readiness_waiter = null;
            }
        }

        // Phase 3: park
        if (timeout_ms > 0) {
            const ns: u64 = @intCast(timeout_ms * std.time.ns_per_ms);
            waiter.waitTimeout(1, ns, .allow_cancel) catch {};
        } else {
            try waiter.wait(1, .allow_cancel);
        }

        // Phase 4: re-check
        return checkAll(items);
    }

    fn checkAll(items: []Item) u32 {
        var ready: u32 = 0;
        for (items) |*item| {
            item.revents = .{};
            if (item.events.pollin and item.socket.hasIn())
                item.revents.pollin = true;
            if (item.events.pollout and item.socket.hasOut())
                item.revents.pollout = true;
            if (@as(u8, @bitCast(item.revents)) != 0) ready += 1;
        }
        return ready;
    }
};
```

**Why this beats FD-based poll:**

| | libzmq `zmq_poll()` | Coroutine `Poller.poll()` |
|---|---|---|
| Readiness check | `getsockopt(ZMQ_EVENTS)` → `process_commands()` → drain mailbox → `xhas_in/xhas_out` | Direct `hasIn()`/`hasOut()` — no mailbox |
| Blocking | OS `poll()` on signaler FDs (~1000ns wake each way) | Waiter park/signal (~8ns atomic) |
| Multi-socket | One `poll()` syscall for N FDs | One Waiter shared across N sockets |

### Monitor Events

libzmq creates an entire inproc PAIR/PUB socket for monitoring. We use a
typed event channel:

```zig
pub const MonitorEvent = union(enum) {
    connected: struct { endpoint: []const u8 },
    connect_delayed: struct { endpoint: []const u8 },
    connect_retried: struct { endpoint: []const u8, interval_ms: u32 },
    disconnected: struct { endpoint: []const u8 },
    listening: struct { endpoint: []const u8 },
    bind_failed: struct { endpoint: []const u8, err: anyerror },
    accepted: struct { endpoint: []const u8 },
    accept_failed: struct { endpoint: []const u8, err: anyerror },
    pipe_attached: struct {},
    pipe_terminated: struct {},
    handshake_succeeded: struct { endpoint: []const u8 },
    handshake_failed: struct { endpoint: []const u8, err: anyerror },
};

pub const MonitorChannel = struct {
    // Bounded ring buffer. Non-blocking send — drops on full
    // to never block the socket hot path.
    events: BoundedRing(MonitorEvent, 256),
    waiter: ?*Waiter = null,

    pub fn send(self: *MonitorChannel, event: MonitorEvent) void {
        _ = self.events.tryPush(event);
        if (self.waiter) |w| w.signal();
    }

    pub fn recv(self: *MonitorChannel) !MonitorEvent {
        while (true) {
            if (self.events.tryPop()) |event| return event;
            var waiter = Waiter.init();
            self.waiter = &waiter;
            defer self.waiter = null;
            try waiter.wait(1, .allow_cancel);
        }
    }

    pub fn tryRecv(self: *MonitorChannel) ?MonitorEvent {
        return self.events.tryPop();
    }
};
```

### Connection Lifecycle: Full Picture

Here is the complete flow of a TCP connection through all layers:

```
1. CONNECT
   ─────────
   socket.connect("tcp://host:port")
     → spawn connector coroutine

   connector:
     TCP connect → success
     ZMTP handshake → success
     pipes = pipePair(alloc, hwm, hwm)
     socket.attachPipe(&pipes[0], locally_initiated=true)
       → pattern.attachPipe(pipe)    // PUSH → lb.attach(pipe)
       → monitor.send(.connected)
       → signalReadiness()           // wake blocked send/recv
     session = Session{ pipe: &pipes[1], stream: tcp_stream }
     session.run()                   // blocks until disconnect

2. STEADY STATE
   ─────────────
   app:                              session:
     socket.sendBlocking(&msg)
       → pattern.send(&msg)          sendLoop:
         → lb.send(&msg)               pipe.recvBlocking()
           → pipe.send + flush            → spin/wait → msg
                                         writeBatch → writev

                                      recvLoop:
                                        stream.read → decode
                                        pipe.send + flush
                                          → socket.readActivated
   socket.recvBlocking()
     → pattern.recv() → msg

3. DISCONNECT
   ──────────
   session recvLoop: stream.read → EndOfStream
   session.run() returns

   connector:
     pipe.terminate(true)            // delimiter → outpipe
       → peer reads delimiter → processDelimiter → term_received
       → peer writes delimiter back → ack
     pipe reads ack → terminated
       → socket.pipeTerminated(pipe)
         → pattern.pipeTerminated    // PUSH → lb.pipeTerminated
         → monitor.send(.disconnected)

4. RECONNECT
   ─────────
   connector: backoff sleep → TCP connect → new pipePair
     socket.attachPipe(new_pipe)
       → PUSH: lb gets new pipe
       → ROUTER: new pipe inherits identity
     new session.run()
```

### Layer Summary

| Component | libzmq | This design | Change |
|-----------|--------|-------------|--------|
| Pipe lifecycle | 6-state FSM + PIPE_TERM/PIPE_TERM_ACK commands through mailbox | 4-state FSM + delimiter sentinels through ypipe | Eliminates command system |
| Flow control | activate_read/activate_write commands through mailbox | Direct Waiter.signal() (already in Layer 4) | ~1000ns → ~8ns |
| Readiness | ZMQ_FD + ZMQ_EVENTS + process_commands() | Direct hasIn()/hasOut() on pattern | No mailbox drain |
| Poll | OS poll() on signaler FDs | Coroutine Waiter multi-wait | No FDs, no syscall |
| Socket dispatch | C++ virtual methods (vtable indirection) | Zig tagged union (inline switch) | Zero indirection |
| Fair queue | fq_t with command-driven activation | FairQueue with direct activation | Same algorithm, no commands |
| Load balance | lb_t with command-driven activation | LoadBalancer with direct activation | Same algorithm, no commands |
| Fan-out | dist_t | Dist | Same algorithm |
| Monitor | inproc PAIR socket + binary event encoding | Typed MonitorChannel + Waiter | No socket overhead |
| Reconnection | hiccup() splices new ypipe into existing pipe | New pipe pair, attach/detach at socket level | Simpler |

---


## Concurrency Model: Happens-Before, Waiter Safety, Cancellation

This section defines the formal concurrency rules for the entire design.
Every cross-thread interaction in the system must be covered here; if it
isn't, it's a bug.

### The Waiter Lifetime Problem

The Layer 4 Pipe design uses stack-allocated `Waiter` structs published
to other threads via raw pointers:

```zig
// Reader (Layer 4 recvBlocking, as originally proposed):
var waiter = Waiter.init();       // stack-allocated
self.read_waiter = &waiter;       // publish raw pointer to shared field
defer self.read_waiter = null;    // unpublish
try waiter.wait(1, .allow_cancel);
```

```zig
// Writer (Layer 4 wakeReader):
fn wakeReader(self: *Pipe) void {
    if (self.peer.read_waiter) |waiter| {
        waiter.signal();  // dereferences raw pointer to reader's stack
    }
}
```

**This has a use-after-free race.**

zio's `Waiter.signal()` accesses `self.mode.direct.task` and
`self.mode.direct.notify.state` — fields stored inside the Waiter struct
itself. If the Waiter is on the reader's stack, the writer's `signal()`
dereferences a pointer into the reader's stack frame. The race:

```
Writer thread                    Reader thread
─────────────                    ─────────────
                                 var waiter = Waiter.init()
                                 read_waiter = &waiter
                                 waiter.wait(1, ...)
                                 // ... woken by timeout/cancel ...
                                 read_waiter = null
                                 // function returns
load read_waiter → &waiter       // stack frame freed
waiter.signal()                  //
  ↑ accesses freed memory        //
  UAF → undefined behavior       //
```

The window between the writer loading the pointer and calling `signal()`
is small (a few instructions), but **any race on memory lifetime is
undefined behavior** — it doesn't matter how small the window is.

The same race exists in the Layer 7 Socket for `readiness_waiter` in
`Poller.poll()` and in `Socket.sendBlocking()`/`recvBlocking()`.

### Solution: Persistent ReadySignal (Pipe-Owned, Heap-Lifetime)

We replace stack-allocated `Waiter` pointers with a persistent
`ReadySignal` struct owned by each Pipe endpoint. The ReadySignal
has heap lifetime (same as the Pipe) and separates the signal state
from the wake mechanism:

```zig
pub const ReadySignal = struct {
    /// Monotonic generation counter. Incremented on every signal.
    /// Persistent — lives as long as the Pipe. No lifetime issue.
    generation: std.atomic.Value(u64) = .init(0),

    /// Pointer to the parked coroutine's task struct.
    /// The task struct is managed by zio's executor (heap-allocated
    /// when the coroutine is spawned, freed when it completes).
    /// Safe to access from any thread as long as the coroutine exists.
    ///
    /// null = nobody is parked. Non-null = a coroutine is waiting.
    parked_task: std.atomic.Value(?*AnyTask) = .init(null),

    // -------------------------------------------------------
    // Writer side (called from producer coroutine/thread)
    // -------------------------------------------------------

    /// Notify the reader that new data is available.
    /// Safe to call from any thread at any time.
    pub fn notify(self: *ReadySignal) void {
        // 1. Increment generation (release: makes prior writes visible)
        _ = self.generation.fetchAdd(1, .release);

        // 2. Load task pointer (acquire: sees reader's publication)
        //    task.wake() is safe because:
        //    - AnyTask is heap-allocated by zio runtime
        //    - AnyTask lives until coroutine completes
        //    - wake() does scheduleTask() which is thread-safe
        if (self.parked_task.load(.acquire)) |task| {
            task.wake();
        }
        // If null: reader isn't parked (either actively processing
        // or hasn't parked yet). Either way, the generation bump
        // ensures the reader sees data on its next check.
    }

    // -------------------------------------------------------
    // Reader side (called from consumer coroutine)
    // -------------------------------------------------------

    /// Park the current coroutine until notify() is called.
    /// Returns the new generation (caller uses it for next park).
    ///
    /// Protocol — mirrors zio's preparing_to_wait → waiting handshake:
    ///
    /// 1. Caller checks for data and snapshots generation BEFORE calling park
    /// 2. Publish task pointer (announce "I'm about to sleep")
    /// 3. Set task state to .preparing_to_wait (release: the handshake entry)
    /// 4. Re-check generation (catch races — see happens-before)
    /// 5. If generation changed: abort park, set state back to .ready, return
    /// 6. Else: yield(.park). The executor's processCleanup() will CAS
    ///    .preparing_to_wait → .waiting. If notify()'s task.wake() already
    ///    swapped state to .ready, the CAS fails and the executor
    ///    reschedules the task immediately (never truly parked — no lost wake).
    /// 7. On resume: unpublish task pointer, return new generation.
    ///
    /// This is the same double-check pattern used by zio's common.zig
    /// waitTask(): the .preparing_to_wait state is a reservation that
    /// allows scheduleTask() to detect and prevent lost wakes.
    pub fn park(self: *ReadySignal, last_seen_gen: u64) !u64 {
        const task = zio.runtime.getCurrentTask();

        // Step 2: Publish task pointer (release: makes our
        // "last_seen_gen" observation visible to the notifier)
        self.parked_task.store(task, .release);

        // Step 3: Enter the preparing_to_wait state.
        // This is the KEY integration with zio's scheduler:
        // - scheduleTask() (called by notify → task.wake()) does
        //   state.swap(.ready, .acq_rel).
        // - If it sees .preparing_to_wait, it returns immediately —
        //   the yield below will see .ready and skip parking.
        // - If it sees .waiting, it schedules the task to run.
        // - processCleanup() does CAS(.preparing_to_wait → .waiting).
        //   If CAS fails (state already .ready), it reschedules locally.
        task.state.store(.preparing_to_wait, .release);

        // Step 4: Re-check generation AFTER publishing task pointer
        // AND entering preparing_to_wait. This is the critical double-
        // check that prevents missed wakeups.
        // (See happens-before Case 2 below.)
        const current_gen = self.generation.load(.acquire);
        if (current_gen != last_seen_gen) {
            // Data arrived between caller's check and our publication.
            // Abort park: restore state and unpublish.
            task.state.store(.ready, .release);
            self.parked_task.store(null, .release);
            return current_gen;
        }

        // Step 6: Yield to the executor. The executor's processCleanup
        // will CAS our state from .preparing_to_wait → .waiting.
        // If notify()'s task.wake() already swapped us to .ready
        // (between step 3 and now), the CAS fails and processCleanup
        // reschedules us immediately — we never actually park.
        //
        // .allow_cancel: if the task is cancelled while parked,
        // yield returns error.Canceled.
        task.yield(.park, .allow_cancel) catch |err| {
            // Cancellation: unpublish before propagating
            self.parked_task.store(null, .release);
            return err;
        };

        // Step 7: Woken (or immediately rescheduled). Unpublish.
        self.parked_task.store(null, .release);
        return self.generation.load(.acquire);
    }

    /// Park with a timeout. Same handshake as park(), but registers an
    /// ev.Timer with the executor's event loop. If the timer fires before
    /// notify(), the timer callback calls self.notify(), which bumps the
    /// generation and wakes the parked task — same path as a real wakeup.
    ///
    /// The race between timer-fire and notify() is safe: whichever calls
    /// task.wake() second sees `.ready` in scheduleTask()'s state.swap
    /// and returns immediately (no double-enqueue).
    ///
    /// Returns the current generation on wake (whether by notify or timeout).
    /// The caller must re-check its readiness condition to distinguish
    /// "real data arrived" from "timeout expired" — this function does not
    /// distinguish them (same as zio's Waiter.timedWait pattern).
    ///
    /// Modeled after zio's Waiter.timedWait (common.zig:153-168):
    ///   ev.Timer + callback → signal() → wake task,
    ///   defer loop.clearTimer() on exit.
    pub fn parkTimeout(self: *ReadySignal, last_seen_gen: u64, timeout: Timeout) !u64 {
        if (timeout == .none) {
            return self.park(last_seen_gen);
        }

        const task = zio.runtime.getCurrentTask();

        // Set up a timer whose callback calls self.notify().
        // When the timer fires, notify() bumps generation and wakes
        // the parked task — identical to a real data-ready notification.
        // This mirrors Waiter.timedWait's timer + callback pattern.
        var timer: ev.Timer = .init(timeout);
        timer.c.userdata = self;
        timer.c.callback = timerCallback;

        task.getExecutor().loop.setTimer(&timer, timeout);
        defer timer.c.loop.?.clearTimer(&timer);

        // Same handshake as park():
        self.parked_task.store(task, .release);
        task.state.store(.preparing_to_wait, .release);

        const current_gen = self.generation.load(.acquire);
        if (current_gen != last_seen_gen) {
            task.state.store(.ready, .release);
            self.parked_task.store(null, .release);
            return current_gen;
        }

        // Yield — either notify() or the timer callback will wake us.
        task.yield(.park, .allow_cancel) catch |err| {
            self.parked_task.store(null, .release);
            return err;
        };

        self.parked_task.store(null, .release);
        return self.generation.load(.acquire);
    }

    /// Timer callback for parkTimeout. Called by the event loop when
    /// the timeout expires. Calls notify() to bump generation and wake
    /// the parked task. Mirrors zio's Waiter.callback (common.zig:219).
    fn timerCallback(_: *ev.Loop, c: *ev.Completion) void {
        const self: *ReadySignal = @ptrCast(@alignCast(c.userdata.?));
        self.notify();
    }

    /// Read the current generation without side effects.
    pub fn currentGen(self: *ReadySignal) u64 {
        return self.generation.load(.acquire);
    }
};
```

**Why this is safe:**

1. **ReadySignal is a field of Pipe** (heap-allocated). Its lifetime equals
   the Pipe's lifetime. `notify()` accesses `self.generation` and
   `self.parked_task`, which are fields of this persistent struct — no stack
   dependency.

2. **`parked_task` points to `AnyTask`**, which is heap-allocated by zio's
   runtime when the coroutine is spawned. It lives until the coroutine
   completes and is freed by the runtime. `task.wake()` calls
   `task.getExecutor().scheduleTask(task)`, which is safe from any thread
   (Treiber stack push + `loop.wake()`).

3. **Generation counter is persistent**. `notify()` always increments it,
   regardless of whether anyone is parked. The counter never wraps in
   practice (2^64 increments at 1GHz = 584 years).

4. **No signal-after-free**: The notifier never accesses stack memory.
   It accesses Pipe fields (heap) and AnyTask (heap). Both outlive any
   individual send/recv call.

### Updated Pipe Integration

The Pipe struct from Layer 4 changes:

```zig
pub const Pipe = struct {
    // ... existing fields ...

    // REPLACE stack-allocated Waiter pointers with persistent ReadySignals
    // OLD: read_waiter: ?*Waiter = null,
    // OLD: write_waiter: ?*Waiter = null,
    read_signal: ReadySignal = .{},
    write_signal: ReadySignal = .{},

    // --- Write path (unchanged except wakeReader) ---

    pub fn flush(self: *Pipe) void {
        if (!self.out_pipe.flush()) {
            // Reader was sleeping (c was NULL). Notify.
            self.peer.read_signal.notify();
        }
    }

    fn wakeWriter(self: *Pipe) void {
        self.peer.write_signal.notify();
    }

    // --- Blocking recv (rewritten with ReadySignal) ---

    pub fn recvBlocking(self: *Pipe) !Msg {
        // Fast path: data already available
        if (self.recv()) |msg| return msg;

        // Adaptive spin: check for data without parking
        var spun: u32 = 0;
        while (spun < self.spin_count) : (spun += 1) {
            std.atomic.spinLoopHint();
            if (self.in_pipe.checkRead()) {
                self.spin_count = @min(
                    self.spin_count + (self.spin_count / 4),
                    max_spin_count,
                );
                return self.recv().?;
            }
        }

        // Spin exhausted — park via ReadySignal
        if (self.spin_count > min_spin_count) {
            self.spin_count -= self.spin_count / 8;
        }

        // Snapshot generation BEFORE checking data
        var gen = self.read_signal.currentGen();
        while (true) {
            // Check again (may have arrived during spin wind-down)
            if (self.in_pipe.checkRead()) return self.recv().?;

            // Park with generation check (prevents missed wakeup)
            gen = try self.read_signal.park(gen);

            // Woken — check for data
            if (self.in_pipe.checkRead()) return self.recv().?;
            // Spurious wake (generation bumped but data consumed
            // by another path) — re-park
        }
    }

    // --- Blocking send (rewritten with ReadySignal) ---

    pub fn sendBlocking(self: *Pipe, msg: *Msg) !void {
        var gen = self.write_signal.currentGen();
        while (true) {
            if (try self.send(msg)) return;
            gen = try self.write_signal.park(gen);
        }
    }
};
```

### Updated Poller Integration

The Socket-level Poller from Layer 7 changes similarly. Instead of
publishing a stack-allocated Waiter to multiple sockets, each Socket
owns a persistent `ReadySignal` that the Poller taps into:

```zig
pub const Socket = struct {
    // Pointer to the active ReadySignal.
    // Normally points to self.own_readiness_signal (embedded, heap-lifetime).
    // During Poller.poll(), temporarily redirected to a shared signal.
    // Using a pointer (not inline value) ensures that Poller's swap
    // actually shares one signal across all subscribed sockets.
    readiness_signal: *ReadySignal = undefined,  // set in init()
    own_readiness_signal: ReadySignal = .{},

    pub fn init(self: *Socket) void {
        // ... other initialization ...
        self.readiness_signal = &self.own_readiness_signal;
    }

    fn signalReadiness(self: *Socket) void {
        self.readiness_signal.notify();
    }

    pub fn sendBlocking(self: *Socket, msg: *Msg) !void {
        var gen = self.readiness_signal.currentGen();
        while (true) {
            if (self.hasOut()) return try self.send(msg);
            gen = try self.readiness_signal.park(gen);
        }
    }

    pub fn recvBlocking(self: *Socket) !Msg {
        var gen = self.readiness_signal.currentGen();
        while (true) {
            if (self.hasIn()) return try self.recv();
            gen = try self.readiness_signal.park(gen);
        }
    }
};
```

For multi-socket polling, we use a pointer-based shared signal.
Each Socket has a `readiness_signal: *ReadySignal` pointer (not an
inline value). Normally this points to the socket's own embedded signal.
During `poll()`, we temporarily redirect each socket's pointer to a
shared signal, so any socket's `signalReadiness()` wakes the poller.

```zig
pub const Poller = struct {
    pub const Item = struct {
        socket: *Socket,
        events: Events,
        revents: Events = .{},
    };

    pub const Events = packed struct(u8) {
        pollin: bool = false,
        pollout: bool = false,
        _pad: u6 = 0,
    };

    /// Multi-socket poll using generation-based wakeup.
    ///
    /// Protocol — subscribe/unsubscribe via pointer swap:
    ///
    /// 1. Create a stack-local ReadySignal ("shared").
    ///    (Stack-local is safe here because the Poller owns the entire
    ///    lifetime: we block until poll returns, and we unsubscribe
    ///    every socket before returning. No dangling pointer is possible.)
    ///
    /// 2. For each socket: swap its readiness_signal pointer to &shared.
    ///    Save the original pointer for restoration.
    ///
    /// 3. Re-check all sockets AFTER subscribing (prevents missed wakeup
    ///    between phase-1 check and subscription).
    ///
    /// 4. Park on the shared signal. Any socket's pipe flush will call
    ///    socket.signalReadiness() → shared.notify() → poller wakes.
    ///
    /// 5. Restore every socket's original signal pointer (unsubscribe).
    ///    This MUST happen before returning, even on error/cancellation.
    pub fn poll(items: []Item, timeout_ms: i64) !u32 {
        // Phase 1: non-blocking check (no subscription needed)
        var ready = checkAll(items);
        if (ready > 0 or timeout_ms == 0) return ready;

        // Phase 2: create shared signal and subscribe all sockets.
        // Dynamically allocate the originals array to support any
        // number of sockets (no hard cap).
        var shared = ReadySignal{};
        const allocator = std.heap.page_allocator;  // or arena
        const originals = try allocator.alloc(*ReadySignal, items.len);
        defer allocator.free(originals);

        for (items, 0..) |*item, i| {
            originals[i] = item.socket.readiness_signal;
            // Pointer swap: socket now notifies our shared signal.
            // This is safe because:
            // - We hold the poller's coroutine (won't return until phase 5)
            // - Socket.signalReadiness() just calls signal.notify()
            // - notify() only accesses fields of the pointed-to ReadySignal
            item.socket.readiness_signal = &shared;
        }
        defer {
            // Phase 5: unsubscribe — restore original signal pointers.
            // This runs even if park() returns error (cancellation).
            for (items, 0..) |*item, i| {
                item.socket.readiness_signal = originals[i];
            }
        }

        // Phase 3: re-check after subscribing (prevents missed wakeup)
        ready = checkAll(items);
        if (ready > 0) return ready;

        // Phase 4: park on the shared signal
        const gen = shared.currentGen();
        if (timeout_ms > 0) {
            // Timed wait — uses Timeout.fromMilliseconds (zio time.zig)
            _ = shared.parkTimeout(gen, Timeout.fromMilliseconds(@intCast(timeout_ms))) catch {};
        } else {
            // Infinite wait (timeout_ms == -1)
            _ = try shared.park(gen);
        }

        // Phase 5 (deferred): restore signal pointers
        return checkAll(items);
    }

    fn checkAll(items: []Item) u32 {
        var ready: u32 = 0;
        for (items) |*item| {
            item.revents = .{};
            if (item.events.pollin and item.socket.hasIn())
                item.revents.pollin = true;
            if (item.events.pollout and item.socket.hasOut())
                item.revents.pollout = true;
            if (@as(u8, @bitCast(item.revents)) != 0) ready += 1;
        }
        return ready;
    }
};
```

### Happens-Before Analysis

Every cross-thread data transfer in the system must be justified by a
happens-before chain. There are exactly four interacting paths:

#### Path 1: Data Send (Writer → Reader via YPipe)

```
Writer thread                    Reader thread
─────────────                    ─────────────
W1: write msg to ypipe slot
    (plain store to queue array)
W2: ypipe.flush()
    CAS(c, w, f) [acq_rel]      R1: ypipe.checkRead()
                                     CAS(c, front, null) [acq_rel]
                                 R2: ypipe.read() → msg
                                     (plain load from queue array)
```

**Happens-before chain**: W2's `acq_rel` CAS on `c` synchronizes-with
R1's `acq_rel` CAS on `c`. Therefore W1 (write to queue) happens-before
R2 (read from queue). The message data is correctly visible.

This is identical to libzmq's ypipe protocol. The CAS on `c` is the
single synchronization point.

#### Path 2: Backpressure (Reader → Writer via Pipe Counters)

```
Reader thread                    Writer thread
─────────────                    ─────────────
R1: msgs_read += 1
R2: peer.peers_msgs_read =
      self.msgs_read
    [plain store]                W1: load peers_msgs_read
                                     [plain load]
R3: write_signal.notify()        W2: check HWM
    generation.fetchAdd
    [release]
    load parked_task [acquire]
    task.wake()
```

**Issue**: R2 is a plain store and W1 is a plain load — no synchronization
between them. This is a **data race on `peers_msgs_read`**.

**Fix**: `peers_msgs_read` must be an atomic:

```zig
peers_msgs_read: std.atomic.Value(u64) = .init(0),

// Reader updates:
self.peer.peers_msgs_read.store(self.msgs_read, .release);

// Writer checks:
const peer_read = self.peers_msgs_read.load(.acquire);
return (self.msgs_written - peer_read) < self.hwm;
```

After fix: R2's `release` store synchronizes-with W1's `acquire` load.
R1 (msgs_read update) happens-before W2 (HWM check).

**Notification chain** (when writer was blocked on HWM):

R3's `generation.fetchAdd(.release)` happens-before the writer's
`generation.load(.acquire)` inside `park()`. R2 (store peers_msgs_read
with release) happens-before R3 (release on generation), which
happens-before writer's park-return (acquire on generation). So the
writer sees the updated `peers_msgs_read` after waking.

#### Path 3: ReadySignal (notify → park protocol)

There are four sub-cases. All must be correct.

**Case A: notify() before park() — fast path**

```
Writer                           Reader
──────                           ──────
                                 R1: check data → none
N1: write data to ypipe          R2: gen = currentGen() → G
N2: flush() → CAS on c
N3: notify():
    generation: G → G+1
    [fetchAdd, release]
    load parked_task → null      R3: re-check data → still none(?)
    [acquire]                    R4: park(G):
    // no wake (null)                store task [release]
                                     state → .preparing_to_wait [release]
                                     load generation [acquire] → G+1
                                     G+1 ≠ G → abort park:
                                       state → .ready [release]
                                       store null [release]
                                       return G+1
```

**Correctness**: R4 loads generation (acquire) and sees G+1 (written by
N3 with release). N2's flush (acq_rel CAS on `c`) happened before N3
(program order, same thread). R4's generation load (acquire)
synchronizes-with N3's fetchAdd (release). Therefore N2 (flush)
happens-before R4 (return from park). Reader's subsequent `checkRead()`
will see the flushed data.

**Case B: park() before notify() — wake path**

```
Writer                           Reader
──────                           ──────
                                 R1: check data → none
                                 R2: gen = currentGen() → G
                                 R3: park(G):
                                     store task [release]
                                     state → .preparing_to_wait [release]
                                     load generation [acquire] → G
                                     G == G → yield(.park)
                                     processCleanup: CAS
                                       .preparing_to_wait → .waiting
                                     (task is now suspended)
N1: write data to ypipe
N2: flush() → CAS on c
N3: notify():
    generation: G → G+1
    [fetchAdd, release]
    load parked_task → task
    [acquire]
    task.wake():
      state.swap(.ready) [acq_rel]
      (old = .waiting → enqueue)  R4: resume from yield
                                     store null to parked_task [release]
                                     return G+1
                                 R5: checkRead() → data!
```

**Correctness**: R3's `store task [release]` happens-before N3's
`load parked_task [acquire]` (release-acquire on `parked_task`).
N2's flush (acq_rel) happened before N3 (program order). N3's
`fetchAdd [release]` on generation happens-before R4's wakeup (the
executor's scheduling provides the acquire barrier — `scheduleTaskRemote`
uses a Treiber stack push with release, the executor's drain uses acquire).
N3's `task.wake()` does `state.swap(.ready, .acq_rel)`, which sees
`.waiting` (set by `processCleanup`'s CAS) and enqueues the task.
Therefore N2 (flush) happens-before R5 (checkRead).

**Case C: notify() races with park() setup — no park needed**

```
Writer                           Reader
──────                           ──────
                                 R1: check data → none
                                 R2: gen = currentGen() → G
N1: write data + flush
N2: notify():                   R3: park(G):
    generation: G → G+1              store task [release]
    [fetchAdd, release]              state → .preparing_to_wait [release]
    load parked_task → task
    [acquire]
    task.wake():                     load generation [acquire] → G+1
      state.swap(.ready) [acq_rel]   G+1 ≠ G → abort park:
      (old = .preparing_to_wait        state → .ready [release]
       → return, no enqueue)           store null to parked_task [release]
                                       return G+1
```

**Correctness**: N2's `fetchAdd [release]` synchronizes-with R3's
`load generation [acquire]`. The reader sees G+1, returns without
parking. N1's flush happened-before N2 (program order), so data is
visible when reader subsequently calls `checkRead()`.

The `task.wake()` call in this scenario is safe, but the reason is
precise — not a blanket "wake is idempotent" claim.

**Why this specific wake is safe — `scheduleTask()` state machine:**

`task.wake()` calls `executor.scheduleTask(task)`, which does
`task.state.swap(.ready, .acq_rel)`. The previous state determines
the outcome:

| Previous state | What happens | Safe? |
|---|---|---|
| `.preparing_to_wait` | Returns immediately. `processCleanup()` sees `.ready`, reschedules locally. | Yes — task never parks |
| `.waiting` | Enqueues task to ready queue. | Yes — normal wake |
| `.ready` | Swap is a no-op (already `.ready`). Returns without enqueuing. | Yes — deduped |
| `.finished` | **Panic** — task already completed. | **Unsafe** |

In Case C, the reader has stored `.preparing_to_wait` but has not yet
called `task.yield(.park)`. The `scheduleTask()` swap sees
`.preparing_to_wait` and swaps it to `.ready`, then returns immediately
(no enqueue needed). When the reader subsequently calls `yield(.park)`,
the executor's `processCleanup` CAS from `.preparing_to_wait` →
`.waiting` **fails** (state is already `.ready`), so `processCleanup`
calls `scheduleTaskLocal()` and the task runs immediately — no
actual suspension, no double-enqueue, no panic.

**The critical safety invariant**: `parked_task` is set to non-null
**only while the owning coroutine is alive** (between `park()` entry
and `park()` exit). The coroutine cannot reach `.finished` state
while `parked_task` is non-null, because:

1. `park()` always stores `null` to `parked_task` before returning
   (both normal and error paths, enforced by the code structure above).
2. Only the owning coroutine calls `park()`, so `parked_task` is
   non-null → coroutine is inside `park()` → coroutine is alive →
   task state is `.ready`, `.preparing_to_wait`, or `.waiting`.
3. Therefore `scheduleTask()` will never see `.finished` when called
   from `notify()` via a non-null `parked_task` load.

This rules out the dangerous case (wake-after-finish panic).

**Case D: notify() while reader is in spin phase**

```
Writer                           Reader
──────                           ──────
                                 R1: recv() → null
                                 R2: adaptive spin...
N1: write data + flush                spinLoopHint()
    flush returns true ← c was       checkRead() → true!
    not null (reader was              (spin catches data)
    spinning, not parked)             return recv().?
```

**Correctness**: flush() returns true because `c != NULL` (reader set it
to NULL via the checkRead CAS, but the CAS failed because the writer's
flush CAS already updated `c`). The reader's `checkRead()` CAS on `c`
provides the acquire barrier that makes the flushed data visible.

No ReadySignal involved — this is pure YPipe protocol.

### Cancellation Rules

A coroutine can be cancelled while parked inside `ReadySignal.park()`.
The cancellation rules are:

1. **Cancellation during park()**: `task.yield(.park, .allow_cancel)`
   returns `error.Canceled`. The reader stores `null` to `parked_task`
   and propagates the error. The writer may call `task.wake()` on the
   cancelled task — this is safe because:
   - The task struct lives until the coroutine completes
   - `scheduleTask()` on a cancelled-but-alive task is handled by the
     executor (state swap to `.ready` is safe in any pre-`.finished` state)
   - The reader will re-check data or propagate cancellation

2. **Cancellation during spin**: No special handling needed. The spin
   loop checks for data, not for cancellation. If the coroutine is
   cancelled during spin, the cancellation will be delivered when the
   next `task.yield(.park, .allow_cancel)` call happens (in `park()`).

3. **Cancellation during send/recv**: If `recvBlocking()` or
   `sendBlocking()` is cancelled, the error propagates to the caller.
   No partial state is left in the Pipe — the ReadySignal is always
   unpublished (parked_task = null) on exit, whether normal or cancelled.

4. **Socket close during park**: When `Socket.close()` terminates a pipe,
   the pipe's delimiter sentinel eventually wakes the parked coroutine
   (via the YPipe flush → ReadySignal notify path). The coroutine wakes,
   reads the delimiter, and the pipe transitions to terminated state.

### Memory Ordering Summary

Every atomic operation in the system and its required ordering:

| Field | Operation | Ordering | Justification |
|-------|-----------|----------|---------------|
| `YPipe.c` | CAS (flush) | `acq_rel` | Makes queue writes visible to reader |
| `YPipe.c` | CAS (checkRead) | `acq_rel` | Sees writer's flush, publishes NULL for sleep |
| `YPipe.c` | store (flush, CAS failed) | `release` | Makes queue writes visible after notify |
| `ReadySignal.generation` | fetchAdd (notify) | `release` | Makes prior data operations visible |
| `ReadySignal.generation` | load (park) | `acquire` | Sees notifier's prior data operations |
| `ReadySignal.parked_task` | store (park) | `release` | Publishes task pointer for notifier |
| `ReadySignal.parked_task` | load (notify) | `acquire` | Sees reader's task publication |
| `ReadySignal.parked_task` | store null (unpublish) | `release` | Makes unpublish visible to future notifiers |
| `Pipe.peers_msgs_read` | store (reader) | `release` | Makes read count visible to writer |
| `Pipe.peers_msgs_read` | load (writer) | `acquire` | Sees reader's updated count |
| `YQueue.spare_chunk` | swap (push/pop) | `acquire`/`release` | Chunk recycling between reader/writer |
| `Msg.Content.refcount` | fetchAdd (copy) | `monotonic` | Refcount increment (no ordering needed) |
| `Msg.Content.refcount` | fetchSub (release) | `release` + fence | Last decrement sees all prior uses |
| `Pipe.out_active` | store (send/terminate) | `release` | Writer publishes deactivation |
| `Pipe.out_active` | load (recv backpressure) | `acquire` | Reader sees writer's deactivation |
| `Pipe.out_active` | store (recv backpressure) | `release` | Reader publishes reactivation |
| `Pipe.state` | store (terminate) | `release` | Makes state transition visible |
| `Pipe.state` | load (processDelimiter) | `acquire` | Sees peer's state transition |

### No Relaxed Loads on Shared Mutable State

A deliberate design rule: **we never use `monotonic`/`relaxed` ordering
on fields that participate in cross-thread signaling protocols.** The
only `monotonic` operation is `Content.refcount.fetchAdd` (which only
needs atomicity, not ordering, because the last releaser uses `release` +
`acquire` fence).

This means we leave some performance on the table (an `acquire` load is
~0-1ns more than a `relaxed` load on x86, and ~2-5ns more on ARM).
The payoff is that **every cross-thread read sees a consistent snapshot
of the data it depends on**, and the correctness argument doesn't require
reasoning about which relaxed loads can be reordered past which stores.

### What About the `read_parked` Flag? (Why We Don't Need One)

An earlier design iteration used an explicit `read_parked: atomic(bool)`
flag to gate notify signals (only signal if reader is parked). We removed
it because:

1. **The generation counter subsumes it.** If the reader isn't parked,
   `parked_task` is null, and `notify()` just bumps the generation.
   The reader sees the bumped generation on its next `park()` call and
   returns immediately — no actual suspension.

2. **No signal accumulation.** Without a parked flag, `notify()` might
   call `task.wake()` on a task that's already running. This is safe
   because `scheduleTask()` does `state.swap(.ready, .acq_rel)`: if
   the task is already in `.ready` state, the swap is a no-op and the
   task is not double-enqueued (see the `scheduleTask()` state table
   in the Case C analysis above). This only happens when the reader is
   in the narrow window between publishing `parked_task` and actually
   calling `task.yield(.park)` — a window of ~2 instructions. In
   practice, double-wake is extremely rare.

3. **One fewer atomic operation on the hot path.** Removing the parked
   flag eliminates one atomic store (reader) and one atomic load (writer)
   per blocking recv. The generation check inside `park()` catches all
   cases the parked flag would have caught.

---

## Comparison: libzmq vs This Design

### Hot Path Operations (per-message costs)

| Operation | libzmq | This design |
|---|---|---|
| Write msg to pipe | ypipe::write + push (~10ns) | YPipe.write + push (~10ns) |
| Flush (CAS) | ypipe::flush CAS per msg (~20ns) | YPipe.flush CAS per batch (~20ns / N) |
| Notify sleeping reader | signaler.send() ~1000ns syscall | ReadySignal.notify() ~8ns atomic |
| | + mailbox mutex ~30ns | + Treiber push ~15ns |
| | + mailbox ypipe write ~10ns | + loop.wake() ~200-500ns (coalesced) |
| | + mailbox ypipe flush ~20ns | |
| Reader wakeup | signaler.recv() ~1000ns syscall | No separate wakeup path — |
| | + epoll_wait return | loop tick drains Treiber stack |
| Read msg from pipe | ypipe::read + checkRead (~25ns) | YPipe.read (~5ns array read) |
| | (CAS on every checkRead) | (CAS only at chunk boundary, 1/256) |
| Backpressure check | msgs_read % lwm ~30ns (modulo) | msgs_read >= threshold ~1ns |
| Backpressure notify | mailbox.send() ~1500ns | ReadySignal.notify() ~8ns |
| Command throttle | RDTSC ~100ns or tick count | Not needed (no command path) |
| Adaptive spin | N/A (reader blocks immediately) | ~0.3ns/iter (PAUSE instruction) |

### Throughput by Regime

| Scenario | libzmq | This design | Speedup |
|---|---|---|---|
| **Batch send (N=100)** | ~120ns/msg (flush+CAS per msg) | ~15ns/msg (one CAS, array reads) | ~8x |
| **Sustained (reader awake)** | ~100-200ns/msg | ~15-30ns/msg | ~5-7x |
| **Moderate (spin absorbs)** | ~3000-5000ns/msg (park/wake every msg) | ~30-50ns/msg (spin catches data, no park) | ~100x |
| **Ping-pong (worst case)** | ~3000-5000ns/msg | ~250-500ns/msg | ~6-10x |
| **Same executor** | ~100-200ns/msg (still uses signaler) | ~15-25ns/msg (CAS ~5ns L1 hit) | ~5-8x |

The biggest win is the **moderate throughput regime** — the regime that kills
naive implementations. libzmq parks and wakes the reader on every message
(~3000-5000ns). The adaptive spin catches data before parking, reducing the
cost to a CAS per flush (~20-25ns) plus spin iterations (~0.3ns each). This
is the regime where most real-world applications live.

### What We Eliminate

1. **Signaler** (eventfd/pipe): Replaced by persistent `ReadySignal` with
   atomic generation counter + `task.wake()`. Uses the executor's existing
   notification path (Treiber stack + `loop.wake()` with fetchOr coalescing).
   See "Concurrency Model" section for the happens-before proof.

2. **Mailbox** (mutex + SPSC ypipe + signaler): Eliminated entirely.
   `activate_read` and `activate_write` are direct `ReadySignal.notify()`.
   No command serialization, no command routing, no TID-based dispatch.

3. **Command system** (`object_t::send_command` → `ctx_t::send_command` →
   mailbox): Not needed. Pipe lifecycle commands (term, hiccup) are replaced
   by in-band delimiter sentinels through the ypipe itself (see Layer 7).

4. **RDTSC throttling** (`process_commands` in socket_base.cpp): Not needed.
   There's no command polling loop to throttle. Data flows directly through
   the ypipe, and notifications flow through zio's executor.

5. **Tick counter** (`inbound_poll_rate = 100`): Not needed. zio's executor
   already forces event loop ticks every 61 tasks (`EVENT_INTERVAL`).

### What We Retain

1. **YPipe CAS protocol**: Identical to libzmq. The lock-free SPSC queue
   with the `c` pointer sleep/wake mechanism is near-optimal and proven.

2. **YQueue chunk allocator**: Same design — N-element chunks with one
   spare chunk recycled via atomic exchange. N=256 for messages.

3. **Msg layout**: 64-byte cache-line-sized message with VSM/LMSG/CMSG
   variants, matching libzmq's msg_t semantics.

4. **HWM/LWM backpressure**: Same algorithm — writer blocks at HWM,
   reader sends activate_write at LWM crossings.

5. **Multi-part message atomicity**: Same rollback-on-failure semantics.

---

## Implementation Plan

### Phase 1: Core Data Structures

- `Msg` type with VSM/LMSG/CMSG variants, move/copy/deinit
- `YQueue(T, N)` with cache-line-aligned reader/writer/spare fields
- `YPipe(T, N)` with cache-line-aligned w/f, r, c fields
- Unit tests for each in isolation (single-threaded correctness)

### Phase 2: Pipe Integration

- `Pipe` struct with HWM/LWM backpressure
- `pipePair()` factory
- Threshold-based backpressure (comparison, not modulo)
- Batch send/recv APIs (`sendBatch`, `recvBatch`)
- Blocking send/recv with adaptive spin (`recvBlocking`, `sendBlocking`)
- Multi-part send/recv with rollback
- Cross-thread tests (two executors, producer/consumer on different threads)
- Benchmark: measure adaptive spin convergence across throughput regimes

### Phase 3: ZMTP Codec + Session

- `ZmtpCodec` with frame encode/decode (short + long frames)
- ZMTP 3.1 greeting and NULL mechanism handshake
- `Session` coroutine with concurrent send/receive loops
- Vectored I/O: batch encode → writev for send path
- Stream decode: read buffer → frame decode → batch pipe write for recv path
- Unit tests for codec (round-trip encode/decode)
- Integration tests: two sessions connected via TCP loopback

### Phase 4: Transport Bindings

- TCP transport: `tcpConnect`, `tcpAccept` with TCP_NODELAY
- IPC transport: `ipcConnect`, `ipcAccept` via UnixAddress
- inproc transport: direct pipe pair, no session
- TCP listener coroutine (accept loop, spawn sessions)
- Reconnection with exponential backoff
- Graceful shutdown (linger, drain pipe before close)

### Phase 5: Socket Layer (Layer 7)

- Pipe lifecycle state machine (4-state + delimiter sentinels)
- `FairQueue` round-robin recv with active/inactive swap
- `LoadBalancer` round-robin send with HWM deactivation and multipart atomicity
- `Dist` fan-out for PUB (send to all, refcount-based copy)
- Socket types via `Pattern` tagged union:
  - `PairPattern` (one pipe, reject extras)
  - `PushPattern` / `PullPattern` (LB send / FQ recv)
  - `PubPattern` / `SubPattern` (Dist fan-out / FQ recv with subscription filtering)
  - `ReqPattern` / `RepPattern` (strict alternation, delimiter envelope)
  - `DealerPattern` (async LB+FQ)
  - `RouterPattern` (identity-based routing, routing ID frame prepend)
- `Socket` base with pipe management, readiness callbacks, blocking send/recv
- `Poller` coroutine-native multi-socket wait (replaces zmq_poll)
- `MonitorChannel` typed event channel (replaces inproc monitor socket)
- Socket options (HWM, linger, connect timeout, routing_id)
- Transport-agnostic bind/connect API

### Phase 6: Benchmarks

- **Inproc latency**: single-message round-trip between two coroutines
- **Inproc throughput**: messages/sec at various sizes (VSM, 1KB, 64KB)
- **TCP latency**: round-trip over loopback
- **TCP throughput**: messages/sec over loopback at various sizes
- **IPC latency/throughput**: Unix domain socket performance
- **Fan-out**: PUB to N SUBs (inproc + TCP)
- **Fan-in**: N PUSHers to 1 PULLer (inproc + TCP)
- **Cross-executor delta**: same executor vs different executor for each transport
- **Adaptive spin tuning**: measure spin convergence under varying load
- Comparison with libzmq's `inproc_lat`, `inproc_thr`, `local_lat`, `local_thr`

---

## Appendix: zio Internals Referenced

### Runtime & Scheduling

| Component | File | Key Lines | Role |
|---|---|---|---|
| Executor.scheduleTask | src/runtime.zig | 477-517 | Task dispatch with state.swap(.ready) |
| Executor.scheduleTaskRemote | src/runtime.zig | 461-472 | Treiber stack + loop.wake() |
| Executor.run | src/runtime.zig | 367-410 | Main loop: tasks → drain remote → poll |
| Executor.getNextTask | src/runtime.zig | 416-443 | Fairness: EVENT_INTERVAL=61, tick guard |
| Executor.processCleanup | src/runtime.zig | 531-548 | Deferred park: CAS(.preparing_to_wait → .waiting) |
| AnyTask.yield | src/runtime/task.zig | (various) | Suspend coroutine: .park or .reschedule mode |
| AnyTask.state | src/runtime/task.zig | (various) | .new/.ready/.preparing_to_wait/.waiting/.finished |
| AnyTask.wake | src/runtime/task.zig | (various) | Calls executor.scheduleTask(self) |
| loop.wake | src/ev/loop.zig | 295-301 | fetchOr coalescing, one syscall |
| LoopState.markCompleted | src/ev/loop.zig | 135-159 | Atomic cancel coordination |
| AtomicStack (Treiber) | src/ev/loop.zig | 63-88 | Lock-free MPSC for cross-thread |
| Waiter | src/common.zig | (various) | Task parking / futex signaling (reference) |
| waitTask | src/common.zig | (various) | preparing_to_wait → waiting handshake pattern |
| Channel | src/sync/channel.zig | 24-100 | MPMC reference (mutex-based) |
| Completion lifecycle | src/ev/completion.zig | 55-100 | new→running→completed→dead |
| Context switch | src/coro/coroutines.zig | (asm) | ~100ns register save/restore |

### Network I/O

| Component | File | Key Lines | Role |
|---|---|---|---|
| IpAddress.listen | src/net.zig | 608-620 | TCP server socket (bind + listen) |
| IpAddress.connect | src/net.zig | 622-628 | TCP client connection |
| UnixAddress.listen | src/net.zig | 672-680 | Unix domain socket server |
| UnixAddress.connect | src/net.zig | 682-690 | Unix domain socket client |
| Server.accept | src/net.zig | 1075-1095 | Accept incoming connection → Stream |
| Stream.read/write | src/net.zig | 1097-1172 | Single-buffer async I/O |
| Stream.readVec/writeVec | src/net.zig | 1097-1172 | Vectored I/O (up to 16 iovecs) |
| Stream.readAll/writeAll | src/net.zig | 1097-1172 | Loop until complete |
| Socket.setNoDelay | src/net.zig | 893-895 | TCP_NODELAY for latency |
| Socket.setReuseAddress | src/net.zig | 848-850 | SO_REUSEADDR |
| ReadBuf / WriteBuf | src/ev/buf.zig | 1-37 | iovec wrappers for vectored I/O |
| NetAccept (io_uring) | src/ev/backends/io_uring.zig | 265-275 | Async accept submission |
| NetRecv/NetSend (io_uring) | src/ev/backends/io_uring.zig | 277-320 | Async recvmsg/sendmsg |
| waitForIo | src/common.zig | 139-153 | Park coroutine on I/O completion |

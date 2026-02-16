# ZMQ Pipe Implementation on Zig + zio

## Status: DESIGN PROPOSAL (v3)

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

    // Shared content block for large messages
    const Content = struct {
        data: [*]u8,
        len: usize,
        refcount: std.atomic.Value(u32),
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
                // deallocate Content struct itself
            }
        }
    };

    pub fn initSize(allocator: Allocator, size: usize) !Msg {
        if (size <= max_vsm_size) {
            return .{ .data = .{ .vsm = .{
                .bytes = undefined, .len = @intCast(size),
            } } };
        }
        // Allocate Content + data in single allocation
        const content = try allocator.create(Content);
        content.* = .{
            .data = (try allocator.alloc(u8, size)).ptr,
            .len = size,
            .refcount = .init(1),
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

**Size**: `@sizeOf(Msg)` = 64 bytes (one cache line), matching libzmq's msg_t.
The `Data` union is 49 bytes (48 inline + 1 len for vsm), flags + routing_id
+ tag + padding fill the rest.

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
    peers_msgs_read: u64 = 0,
    next_activate_threshold: u64 = 0,

    // Activity flags
    in_active: bool = true,
    out_active: bool = true,

    // Peer reference (the other end of the pipe pair)
    peer: *Pipe = undefined,

    // --- zio integration ---

    // The executor that owns this pipe endpoint. Set when the pipe
    // is attached to a socket/session on a specific executor.
    executor: *zio.Runtime.Executor = undefined,

    // Waiter for the read side: parked task waiting for data.
    // When flush() returns false, we wake this instead of going
    // through a mailbox.
    read_waiter: ?*Waiter = null,

    // Waiter for the write side: parked task waiting for HWM space.
    write_waiter: ?*Waiter = null,

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
        if (!self.out_active) return false;

        if (!self.checkHwm()) {
            self.out_active = false;
            return false;
        }

        const more = msg.flags.more;
        try self.out_pipe.write(msg.move(), more);

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
        const peer = self.peer;
        if (peer.read_waiter) |waiter| {
            // zio handles locality transparently:
            // - Same executor → scheduleTaskLocal: plain queue push, ~3 instructions
            // - Diff executor → scheduleTaskRemote: Treiber CAS + loop.wake()
            //   (~250-500ns, but coalesced — multiple wakes = one syscall)
            waiter.signal();
        }
        // If no waiter, reader isn't blocked — it will see data on
        // next checkRead(). No notification needed.
    }

    fn wakeWriter(self: *Pipe) void {
        const peer = self.peer;
        if (peer.write_waiter) |waiter| {
            waiter.signal();
        }
    }

    fn checkHwm(self: *const Pipe) bool {
        if (self.hwm == 0) return true;  // infinite
        return (self.msgs_written - self.peers_msgs_read) < self.hwm;
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
                self.peer.peers_msgs_read = self.msgs_read;
                if (!self.peer.out_active) {
                    self.peer.out_active = true;
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
        while (true) {
            if (try self.send(msg)) return;
            // Park until writer is activated
            var waiter = Waiter.init();
            self.write_waiter = &waiter;
            defer self.write_waiter = null;
            try waiter.wait(1, .allow_cancel);
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

        // Spin exhausted, no data — park the coroutine.
        // Decrease spin count: spinning wasn't worthwhile.
        if (self.spin_count > min_spin_count) {
            self.spin_count -= self.spin_count / 8;
        }

        var waiter = Waiter.init();
        self.read_waiter = &waiter;
        defer self.read_waiter = null;
        try waiter.wait(1, .allow_cancel);

        // Woken by writer's flush() → data must be available now
        return self.recv().?;
    }
};

/// Create a pipe pair for bidirectional communication.
pub fn pipePair(allocator: Allocator, hwm0: u32, hwm1: u32) !struct { Pipe, Pipe } {
    const pipe_a = try allocator.create(YPipe(Msg, 256));
    pipe_a.* = try YPipe(Msg, 256).init(allocator);
    const pipe_b = try allocator.create(YPipe(Msg, 256));
    pipe_b.* = try YPipe(Msg, 256).init(allocator);

    var p0 = Pipe{
        .in_pipe = pipe_a, .out_pipe = pipe_b,
        .hwm = hwm1, .lwm = computeLwm(hwm0),
    };
    var p1 = Pipe{
        .in_pipe = pipe_b, .out_pipe = pipe_a,
        .hwm = hwm0, .lwm = computeLwm(hwm1),
    };
    p0.peer = &p1;
    p1.peer = &p0;
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
    pipe.flushPipe();
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

## Comparison: libzmq vs This Design

### Hot Path Operations (per-message costs)

| Operation | libzmq | This design |
|---|---|---|
| Write msg to pipe | ypipe::write + push (~10ns) | YPipe.write + push (~10ns) |
| Flush (CAS) | ypipe::flush CAS per msg (~20ns) | YPipe.flush CAS per batch (~20ns / N) |
| Notify sleeping reader | signaler.send() ~1000ns syscall | waiter.signal() ~8ns atomic |
| | + mailbox mutex ~30ns | + Treiber push ~15ns |
| | + mailbox ypipe write ~10ns | + loop.wake() ~200-500ns (coalesced) |
| | + mailbox ypipe flush ~20ns | |
| Reader wakeup | signaler.recv() ~1000ns syscall | No separate wakeup path — |
| | + epoll_wait return | loop tick drains Treiber stack |
| Read msg from pipe | ypipe::read + checkRead (~25ns) | YPipe.read (~5ns array read) |
| | (CAS on every checkRead) | (CAS only at chunk boundary, 1/256) |
| Backpressure check | msgs_read % lwm ~30ns (modulo) | msgs_read >= threshold ~1ns |
| Backpressure notify | mailbox.send() ~1500ns | waiter.signal() ~8ns |
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

1. **Signaler** (eventfd/pipe): Replaced by zio's `Waiter.signal()` which
   uses the executor's existing notification path (Treiber stack +
   `loop.wake()` with fetchOr coalescing).

2. **Mailbox** (mutex + SPSC ypipe + signaler): Eliminated entirely.
   `activate_read` and `activate_write` are direct `waiter.signal()` calls.
   No command serialization, no command routing, no TID-based dispatch.

3. **Command system** (`object_t::send_command` → `ctx_t::send_command` →
   mailbox): Not needed. Pipe lifecycle commands (term, hiccup, hwm) can
   use zio's `Channel` for the rare cases where they're needed.

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
- Blocking send/recv using zio `Waiter`
- Multi-part send/recv with rollback
- Cross-thread tests (two executors, producer/consumer on different threads)

### Phase 3: Socket Layer

- Socket types (PUSH/PULL, PUB/SUB, REQ/REP, PAIR)
- Pipe management (attach, detach, terminate)
- Fair queuing (round-robin recv across multiple pipes)
- Load balancing (round-robin send across multiple pipes)
- inproc transport

### Phase 4: Network Transport

- TCP transport using zio's `net.IpAddress.listen/connect`
- ZMTP wire protocol (greeting, handshake, framing)
- Session management
- Reconnection logic

### Phase 5: Benchmarks

- Inproc latency: single-message round-trip between two coroutines
- Inproc throughput: messages/sec at various sizes (VSM, 1KB, 64KB)
- Fan-out: PUB to N SUBs
- Fan-in: N PUSHers to 1 PULLer
- Comparison with libzmq's `inproc_lat` and `inproc_thr` benchmarks
- Cross-executor vs same-executor performance delta

---

## Appendix: zio Internals Referenced

| Component | File | Key Lines | Role |
|---|---|---|---|
| Executor.scheduleTask | src/runtime.zig | 477-517 | Task dispatch with migration |
| Executor.scheduleTaskRemote | src/runtime.zig | 461-472 | Treiber stack + loop.wake() |
| Executor.run | src/runtime.zig | 367-410 | Main loop: tasks → drain remote → poll |
| Executor.getNextTask | src/runtime.zig | 416-443 | Fairness: EVENT_INTERVAL=61, tick guard |
| Executor.processCleanup | src/runtime.zig | 531-548 | Deferred park with CAS |
| loop.wake | src/ev/loop.zig | 295-301 | fetchOr coalescing, one syscall |
| LoopState.markCompleted | src/ev/loop.zig | 135-159 | Atomic cancel coordination |
| AtomicStack (Treiber) | src/ev/loop.zig | 63-88 | Lock-free MPSC for cross-thread |
| Waiter | src/common.zig | (various) | Task parking / futex signaling |
| Channel | src/sync/channel.zig | 24-100 | MPMC reference (mutex-based) |
| Completion lifecycle | src/ev/completion.zig | 55-100 | new→running→completed→dead |
| Context switch | src/coro/coroutines.zig | (asm) | ~100ns register save/restore |

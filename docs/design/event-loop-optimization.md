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

    // -------------------------------------------------------
    // Write path (called from producer coroutine)
    // -------------------------------------------------------

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

    pub fn flushPipe(self: *Pipe) void {
        if (!self.out_pipe.flush()) {
            // Reader is sleeping. Wake it directly through zio.
            self.wakeReader();
        }
    }

    fn wakeReader(self: *Pipe) void {
        const peer = self.peer;
        if (peer.read_waiter) |waiter| {
            // Wake the parked reader task.
            // If same executor: scheduleTaskLocal (~5ns, no syscall)
            // If different executor: push to Treiber stack + loop.wake()
            //   (~50-200ns, one coalesced syscall)
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

    pub fn recv(self: *Pipe) ?Msg {
        if (!self.in_active) return null;

        var msg = self.in_pipe.read() orelse {
            self.in_active = false;
            return null;
        };

        if (!msg.flags.more) {
            self.msgs_read += 1;

            // Threshold-based backpressure: comparison, not modulo
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

    // -------------------------------------------------------
    // Blocking variants (coroutine-aware)
    // -------------------------------------------------------

    /// Blocking send: parks coroutine if HWM reached, resumes when space available.
    pub fn sendBlocking(self: *Pipe, msg: *Msg) !void {
        while (true) {
            if (try self.send(msg)) {
                self.flushPipe();
                return;
            }
            // Park until writer is activated
            var waiter = Waiter.init();
            self.write_waiter = &waiter;
            defer self.write_waiter = null;
            try waiter.wait(1, .allow_cancel);
        }
    }

    /// Blocking recv: parks coroutine if no data, resumes when data flushed.
    pub fn recvBlocking(self: *Pipe) !Msg {
        while (true) {
            if (self.recv()) |msg| return msg;
            // Park until reader is activated
            var waiter = Waiter.init();
            self.read_waiter = &waiter;
            defer self.read_waiter = null;
            try waiter.wait(1, .allow_cancel);
        }
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

The most important thing to understand is how a `send` on one executor
thread wakes a `recvBlocking` on another:

```
Executor Thread A (producer coroutine):          Executor Thread B (consumer coroutine):
                                                  |
pipe.sendBlocking(&msg)                          pipe.recvBlocking()
  |                                                |
  pipe.send(&msg)                                  pipe.recv() → null (no data)
    ypipe.write(msg, more=false)                    ypipe.checkRead() → false, c set to NULL
    ypipe.push()                                    |
    f = &queue.back()                              in_active = false
  |                                                |
  pipe.flushPipe()                                 waiter = Waiter.init()
    ypipe.flush()                                  self.read_waiter = &waiter
      CAS(c, w, f) → fails, c was NULL            waiter.wait(1, .allow_cancel)
      c.store(f, .release)                           → task state = preparing_to_wait
      return false                                   → context switch to executor loop
    |                                                → cleanup: CAS preparing→waiting
    self.wakeReader()                                → task is now parked
      peer.read_waiter → waiter                      |
      waiter.signal()                                |
        → task.state = .ready                        |
        → if same executor: scheduleTaskLocal()      |
          if diff executor: push to Treiber stack    |
                           + loop.wake()             |
                             → fetchOr (coalesced)   |
                             → one backend syscall   |
                                                     |
                                                  [executor B loop tick]
                                                  drain remote ready queue
                                                  task.coro.step() → resume
                                                    |
                                                  waiter.wait returns
                                                  self.read_waiter = null
                                                  pipe.recv() → msg (data available!)
                                                  return msg
```

**Total cost breakdown (cross-thread)**:
- ypipe.write + push: ~5-10ns (store to array)
- ypipe.flush CAS: ~15-25ns (one CAS, acquire-release)
- waiter.signal(): ~5ns (atomic swap on task state)
- Treiber stack push: ~8-15ns (one CAS)
- loop.wake(): ~0ns if already woken (fetchOr fast path) or ~200-500ns (one backend syscall)
- **Total: ~35-55ns (reader already woken) or ~235-555ns (reader sleeping, amortized)**

Compare to libzmq cross-thread: ~3000-5000ns (mutex + ypipe command + eventfd write + eventfd read).

**Same-executor fast path**: When producer and consumer are on the same
executor thread (after task migration or by affinity), `waiter.signal()`
calls `scheduleTaskLocal()` which is a plain queue push — no atomic
operations on the remote stack, no syscall. Cost: ~10-15ns total for
the notification path.

**Wakeup coalescing under load**: If producer sends 1000 messages in a
burst, the first flush that finds the reader sleeping does the wakeup.
All subsequent flushes see `c != NULL` (reader not sleeping because
it hasn't drained yet) and `flush()` returns true — no notification
at all. The reader drains all 1000 messages in one pass. This is the
same self-batching behavior as libzmq's ypipe protocol, but without
the signaler syscall overhead.

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

### Hot Path Operations

| Operation | libzmq | This design |
|---|---|---|
| Write msg to pipe | ypipe::write + push (~10ns) | YPipe.write + push (~10ns) |
| Flush (CAS) | ypipe::flush CAS (~20ns) | YPipe.flush CAS (~20ns) |
| Notify sleeping reader | signaler.send() ~1000ns syscall | waiter.signal() ~8ns atomic |
| | + mailbox mutex ~30ns | (no mailbox, no mutex) |
| | + mailbox ypipe write ~10ns | |
| | + mailbox ypipe flush ~20ns | |
| Reader wakeup | signaler.recv() ~1000ns syscall | Treiber stack + loop.wake() |
| | + epoll_wait return | ~200ns (coalesced, amortized) |
| Read msg from pipe | ypipe::read CAS + pop (~25ns) | YPipe.read CAS + pop (~25ns) |
| Backpressure check | msgs_read % lwm ~30ns (modulo) | msgs_read >= threshold ~1ns |
| Backpressure notify | mailbox.send() ~1500ns | waiter.signal() ~8ns |
| Command throttle | RDTSC ~100ns or tick count | Not needed (no command path) |
| **Total (reader sleeping)** | **~3000-5000ns** | **~250-500ns** |
| **Total (reader awake)** | **~100-200ns** | **~40-60ns** |

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

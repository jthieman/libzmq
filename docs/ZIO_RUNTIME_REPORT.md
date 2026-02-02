# ZIO Runtime Report: Spawning, Joining, and std.Io Integration

This report analyzes how ZIO handles task spawning and joining, how it integrates with Zig's `std.Io` interface, and whether ZZMQ can manage the runtime internally while exposing async I/O interfaces.

## Executive Summary

**Key Findings:**

1. **ZIO manages all coroutine scheduling internally** - Users don't need to understand or manage spawning mechanics for typical I/O operations
2. **`std.Io` integration is seamless** - ZIO implements the full `std.Io.VTable`, allowing any code written for `std.Io` to work transparently
3. **ZZMQ CAN manage the runtime internally** - A library can initialize the runtime, perform operations, and expose `std.Io`-compatible interfaces
4. **Non-blocking is automatic** - All I/O operations in ZIO automatically yield to the event loop, no explicit async/await syntax needed

## 1. Runtime Architecture

### 1.1 Initialization and Lifecycle

```zig
// Create runtime with allocator and options
const rt = try zio.Runtime.init(allocator, .{
    .executors = .exact(1),      // Single-threaded (default)
    .lifo_slot_enabled = true,   // Cache locality optimization
    .stack_pool = .{...},        // Coroutine stack configuration
    .thread_pool = .{...},       // Blocking task pool configuration
});
defer rt.deinit();
```

The runtime owns:
- **Event loop** (`ev.Loop`) - epoll/kqueue/io_uring backend
- **Coroutine stack pool** - Reusable stacks for coroutines
- **Thread pool** - For blocking operations
- **Task scheduler** - LIFO slot + runnable queue

### 1.2 Executor Model

ZIO supports multi-executor mode (multiple threads each with their own event loop), but defaults to single-threaded:

```zig
pub const RuntimeOptions = struct {
    /// Number of executor threads (including main)
    /// .exact(1) = single-threaded, no worker threads
    executors: ExecutorCount = .exact(1),
};
```

For ZZMQ, single-threaded mode is ideal since ZeroMQ sockets are not thread-safe anyway.

## 2. Spawning and Joining

### 2.1 The `spawn()` Method

```zig
pub fn spawn(
    self: *Runtime,
    func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func))
) !JoinHandle(meta.ReturnType(func))
```

Creates a new coroutine that runs concurrently. Returns a `JoinHandle` for lifecycle management.

### 2.2 JoinHandle Lifecycle

```zig
pub fn JoinHandle(comptime T: type) type {
    return struct {
        /// Wait for completion, return result
        pub fn join(self: *Self, rt: *Runtime) T;

        /// Request cancellation and wait for completion
        pub fn cancel(self: *Self, rt: *Runtime) void;

        /// Detach - task runs independently, result discarded
        pub fn detach(self: *Self, rt: *Runtime) void;

        /// Check if result is available (non-blocking)
        pub fn hasResult(self: *const Self) bool;
    };
}
```

### 2.3 Groups for Multiple Tasks

```zig
var group: zio.Group = .init;
defer group.cancel(rt);

// Spawn multiple concurrent tasks
for (0..10) |i| {
    try group.spawn(rt, worker, .{ rt, i });
}

// Wait for all to complete
try group.wait(rt);
```

### 2.4 Cancellation

ZIO uses cooperative cancellation:
- `JoinHandle.cancel()` requests cancellation
- Tasks check cancellation via `rt.checkCancel()` or automatically at I/O points
- Cancellation propagates through child tasks

## 3. std.Io Integration

### 3.1 The Bridge: `runtime.io()`

```zig
// In runtime.zig
pub fn io(self: *Runtime) std.Io {
    return stdio.fromRuntime(self);
}

// In stdio.zig
pub fn fromRuntime(rt: *Runtime) Io {
    return Io{
        .userdata = @ptrCast(rt),
        .vtable = &vtable,
    };
}
```

### 3.2 Complete VTable Implementation

ZIO implements the **entire** `std.Io.VTable` (100+ operations):

```zig
pub const vtable = Io.VTable{
    // Task management
    .async = asyncImpl,
    .concurrent = concurrentImpl,
    .await = awaitImpl,
    .cancel = cancelImpl,
    .groupAsync = groupAsyncImpl,
    .groupConcurrent = groupConcurrentImpl,
    .groupAwait = groupAwaitImpl,
    .groupCancel = groupCancelImpl,

    // Synchronization
    .futexWait = futexWaitImpl,
    .futexWake = futexWakeImpl,
    .select = selectImpl,

    // File system (30+ operations)
    .dirCreateDir, .dirOpenDir, .dirStat, ...
    .fileRead, .fileWrite, .fileSeek, ...

    // Networking
    .netListenIp = netListenIpImpl,
    .netAccept = netAcceptImpl,
    .netConnectIp = netConnectIpImpl,
    .netSend = netSendImpl,
    .netReceive = netReceiveImpl,
    .netRead = netReadImpl,
    .netWrite = netWriteImpl,
    ...
};
```

### 3.3 How I/O Operations Work

When code calls `std.Io` operations, ZIO:

1. Translates to internal `ev.*` operation
2. Submits to event loop
3. **Suspends the coroutine** (yields to scheduler)
4. Event loop polls for completion
5. **Resumes coroutine** with result

```zig
fn netReadImpl(userdata: ?*anyopaque, socket: ..., buf: []u8, ...) ... {
    const rt: *Runtime = @ptrCast(@alignCast(userdata));

    // Create async I/O operation
    var op = ev.StreamRead.init(socket.handle, buf);

    // This suspends the current coroutine until I/O completes
    try waitForIo(rt, &op.c);

    return op.getResult();
}
```

## 4. Usage Patterns Compared

### 4.1 Direct ZIO API

```zig
pub fn main() !void {
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const server = try addr.listen(rt, .{});

    while (true) {
        const stream = try server.accept(rt);  // Suspends until client connects
        try group.spawn(rt, handleClient, .{ rt, stream });
    }
}

fn handleClient(rt: *zio.Runtime, stream: zio.net.Stream) !void {
    defer stream.close(rt);

    const data = try stream.read(rt, &buffer);  // Suspends until data arrives
    try stream.write(rt, data);  // Suspends until write completes
}
```

### 4.2 std.Io API (Generic, Runtime-Agnostic)

```zig
pub fn main() !void {
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    const io = rt.io();  // Get std.Io interface

    var server = try addr.listen(io, .{});

    while (true) {
        const stream = try server.accept(io);  // Same semantic, different type
        try group.concurrent(io, handleClient, .{ io, stream });
    }
}

fn handleClient(io: std.Io, stream: std.Io.net.Stream) std.Io.Cancelable!void {
    defer stream.close(io);

    const data = try stream.read(io, &buffer);  // Works identically
    try stream.write(io, data);
}
```

**Key Insight:** Code written against `std.Io` is **completely runtime-agnostic**. It can run on ZIO, on a threaded executor, or any future `std.Io` implementation.

## 5. Do Users Have to Manage Spawning/Joining?

### For Simple I/O: **NO**

Users just call I/O operations. The runtime handles everything:

```zig
// User code - no spawning visible
const data = try socket.recv(io);
try socket.send(io, response);
```

### For Concurrency: **Sometimes**

If users want concurrent operations within their code:

```zig
// User wants parallel sends
var group: std.Io.Group = .init;
defer group.cancel(io);

for (connections) |conn| {
    try group.concurrent(io, sendMessage, .{ io, conn, msg });
}
try group.await(io);  // Wait for all
```

But this is `std.Io` API - **not ZIO-specific**.

## 6. Can ZZMQ Manage Runtime Internally?

**YES.** Here's how:

### 6.1 Library-Managed Runtime Pattern

```zig
// zzmq/context.zig
pub const Context = struct {
    runtime: *zio.Runtime,
    allocator: Allocator,

    pub fn init(allocator: Allocator) !*Context {
        const ctx = try allocator.create(Context);
        ctx.* = .{
            .runtime = try zio.Runtime.init(allocator, .{}),
            .allocator = allocator,
        };
        return ctx;
    }

    pub fn deinit(self: *Context) void {
        self.runtime.deinit();
        self.allocator.destroy(self);
    }

    /// Expose std.Io for user code
    pub fn io(self: *Context) std.Io {
        return self.runtime.io();
    }
};
```

### 6.2 User-Facing API Options

**Option A: Pure std.Io Interface**

```zig
// ZZMQ creates and manages runtime
var ctx = try zzmq.Context.init(allocator);
defer ctx.deinit();

const io = ctx.io();

// User works with std.Io - completely standard
var socket = try ctx.socket(.req);
try socket.connect(io, "tcp://localhost:5555");
try socket.send(io, "Hello");
const reply = try socket.recv(io);
```

**Option B: Implicit I/O Context**

```zig
// ZZMQ Socket carries context reference internally
pub const Socket = struct {
    ctx: *Context,
    // ...

    pub fn send(self: *Socket, data: []const u8) !void {
        const io = self.ctx.io();
        // Internal I/O using std.Io
        try self.engine.write(io, data);
    }

    pub fn recv(self: *Socket) ![]u8 {
        const io = self.ctx.io();
        return try self.engine.read(io);
    }
};

// User code - runtime completely hidden
var socket = try ctx.socket(.req);
try socket.connect("tcp://localhost:5555");
try socket.send("Hello");
const reply = try socket.recv();
```

### 6.3 Hybrid Approach (Recommended for ZZMQ)

```zig
pub const Socket = struct {
    /// Operations that use internal runtime
    pub fn send(self: *Socket, data: []const u8) !void {
        return self.sendWithIo(self.ctx.io(), data);
    }

    /// Operations that accept user-provided std.Io
    /// (for users who manage their own runtime)
    pub fn sendWithIo(self: *Socket, io: std.Io, data: []const u8) !void {
        // Implementation
    }
};

// Usage 1: Library manages runtime (simple)
var socket = ctx.socket(.req);
try socket.send("Hello");

// Usage 2: User provides std.Io (advanced)
const my_runtime = try zio.Runtime.init(alloc, .{});
const io = my_runtime.io();
try socket.sendWithIo(io, "Hello");
```

## 7. Non-Blocking Event Loop Integration

### 7.1 How It Works

ZIO's coroutine model means ALL I/O is automatically non-blocking:

```
User calls socket.recv()
    ↓
ZZMQ calls engine.read(io)
    ↓
std.Io.netRead() → vtable.netRead()
    ↓
ZIO creates ev.StreamRead operation
    ↓
ZIO submits to event loop
    ↓
Coroutine SUSPENDS ← scheduler runs other tasks
    ↓
Event loop signals completion
    ↓
Coroutine RESUMES with data
    ↓
Data returned to user
```

### 7.2 Select/Poll Equivalent

```zig
// Wait for multiple events (equivalent to zmq_poll)
const ready = try zio.select(&.{
    socket1.recvFuture(),
    socket2.recvFuture(),
    timer.future(),
});

switch (ready.index) {
    0 => handleSocket1(ready.results[0]),
    1 => handleSocket2(ready.results[1]),
    2 => handleTimer(),
}
```

Or using `std.Io`:

```zig
var futures: [3]*std.Io.AnyFuture = .{...};
const idx = try io.select(&futures);
```

## 8. Implications for ZZMQ Design

### 8.1 Context

- Creates and owns `zio.Runtime`
- Exposes `io()` method for user code
- Manages socket registry, I/O threads (if multi-threaded)

### 8.2 Socket

- References parent Context for runtime access
- All blocking operations use `ctx.io()` internally
- Optionally accepts user-provided `std.Io` for flexibility

### 8.3 Poller

- Built on `zio.select()` or `std.Io.select()`
- Returns when any socket/timer is ready
- Fully non-blocking from user perspective

### 8.4 Memory and Buffers

- All allocations go through `std.mem.Allocator` (passed at init)
- Internal ring buffers for I/O
- Uses ZIO's built-in channel primitives for internal queues

## 9. Conclusion

**ZIO is an excellent foundation for ZZMQ because:**

1. **Runtime abstraction** - ZZMQ can hide all runtime management
2. **std.Io compatibility** - Users get a standard interface they may already know
3. **Automatic non-blocking** - No callback hell, no manual async/await
4. **Efficient I/O** - Uses io_uring/epoll/kqueue under the hood
5. **Cooperative cancellation** - Clean shutdown semantics
6. **No threading complexity** - Single-threaded mode works perfectly

**Recommended ZZMQ architecture:**

```
┌─────────────────────────────────────────────────────┐
│                    User Code                         │
│   socket.send(data), socket.recv(), poller.wait()   │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│                    ZZMQ API                          │
│   Context, Socket, Poller, Message                  │
└─────────────────────────────────────────────────────┘
                          │
                          ▼ std.Io interface
┌─────────────────────────────────────────────────────┐
│                  ZIO Runtime                         │
│   (managed by ZZMQ Context, hidden from user)       │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│              OS (epoll/kqueue/io_uring)             │
└─────────────────────────────────────────────────────┘
```

Users write straightforward synchronous-looking code. ZZMQ and ZIO handle all the async complexity internally.

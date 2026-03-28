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

## 10. Complete Example: TCP Echo Server with Library Abstraction

This example demonstrates a TCP echo server library that:
1. **Hides ZIO runtime** - Users never see `zio.Runtime`
2. **Exposes `std.Io`** - Users can use standard Zig async patterns
3. **Supports multiple styles** - Blocking, `io.async`, and `io.concurrent`

### 10.1 The Library (server.zig)

```zig
//! A simple TCP server library that abstracts ZIO runtime management.
//! Users interact only through std.Io interfaces.

const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;

/// TCP Server that manages its own ZIO runtime internally.
/// Users interact with it using std.Io interfaces.
pub const TcpServer = struct {
    runtime: *zio.Runtime,
    allocator: Allocator,
    listener: ?std.Io.net.Server,

    const Self = @This();

    /// Initialize the server. Creates and manages ZIO runtime internally.
    pub fn init(allocator: Allocator) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.* = .{
            .runtime = try zio.Runtime.init(allocator, .{}),
            .allocator = allocator,
            .listener = null,
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        if (self.listener) |*l| l.close(self.io());
        self.runtime.deinit();
        self.allocator.destroy(self);
    }

    /// Get std.Io interface for async operations.
    /// This is the key abstraction - users work with std.Io, not ZIO directly.
    pub fn io(self: *Self) std.Io {
        return self.runtime.io();
    }

    /// Bind and listen on an address.
    pub fn listen(self: *Self, address: std.net.Address, options: std.Io.net.ListenOptions) !void {
        self.listener = try address.listen(self.io(), options);
    }

    /// Accept a connection (blocking-style, but non-blocking internally).
    pub fn accept(self: *Self) !Connection {
        const listener = self.listener orelse return error.NotListening;
        const stream = try listener.accept(self.io(), .{});
        return Connection{
            .server = self,
            .stream = stream,
        };
    }

    /// Accept with explicit std.Io (for users managing concurrency themselves).
    pub fn acceptWithIo(self: *Self, user_io: std.Io) !Connection {
        const listener = self.listener orelse return error.NotListening;
        const stream = try listener.accept(user_io, .{});
        return Connection{
            .server = self,
            .stream = stream,
        };
    }
};

/// A client connection handle.
pub const Connection = struct {
    server: *TcpServer,
    stream: std.Io.net.Stream,

    const Self = @This();

    /// Read data (blocking-style).
    pub fn read(self: *Self, buffer: []u8) !usize {
        return self.readWithIo(self.server.io(), buffer);
    }

    /// Read with explicit std.Io.
    pub fn readWithIo(self: *Self, user_io: std.Io, buffer: []u8) !usize {
        return self.stream.read(user_io, buffer, .{});
    }

    /// Write data (blocking-style).
    pub fn write(self: *Self, data: []const u8) !usize {
        return self.writeWithIo(self.server.io(), data);
    }

    /// Write with explicit std.Io.
    pub fn writeWithIo(self: *Self, user_io: std.Io, data: []const u8) !usize {
        return self.stream.write(user_io, data, .{});
    }

    /// Close the connection.
    pub fn close(self: *Self) void {
        self.stream.close(self.server.io());
    }
};
```

### 10.2 Usage Style 1: Simple Blocking-Style

The simplest usage - looks synchronous but is non-blocking internally:

```zig
const std = @import("std");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    // Create server - ZIO runtime is created internally
    var srv = try server.TcpServer.init(gpa.allocator());
    defer srv.deinit();

    // Listen on port 8080
    const addr = try std.net.Address.parseIp4("127.0.0.1", 8080);
    try srv.listen(addr, .{ .reuse_address = true });

    std.debug.print("Echo server listening on :8080\n", .{});

    // Accept loop - each call suspends until a client connects
    while (true) {
        var conn = try srv.accept();  // Suspends, but looks blocking
        defer conn.close();

        // Echo loop
        var buf: [1024]u8 = undefined;
        while (true) {
            const n = conn.read(&buf) catch break;  // Suspends until data
            if (n == 0) break;
            _ = conn.write(buf[0..n]) catch break;  // Suspends until written
        }
    }
}
```

### 10.3 Usage Style 2: io.async for Background Work

Use `io.async` to start an operation and continue doing other work:

```zig
const std = @import("std");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var srv = try server.TcpServer.init(gpa.allocator());
    defer srv.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 8080);
    try srv.listen(addr, .{ .reuse_address = true });

    const io = srv.io();

    var conn = try srv.accept();
    defer conn.close();

    var buf: [1024]u8 = undefined;

    // Start async read - returns immediately with a future
    var read_future = io.async(struct {
        fn doRead(c: *server.Connection, b: []u8, user_io: std.Io) usize {
            return c.readWithIo(user_io, b) catch 0;
        }
    }.doRead, .{ &conn, &buf, io });

    // Do other work while read is in progress...
    std.debug.print("Read started, doing other work...\n", .{});
    doSomeOtherWork();

    // Now wait for the read to complete
    const bytes_read = read_future.await(io);
    std.debug.print("Read completed: {} bytes\n", .{bytes_read});

    if (bytes_read > 0) {
        _ = try conn.write(buf[0..bytes_read]);
    }
}

fn doSomeOtherWork() void {
    // Simulate other work
    std.debug.print("Other work done!\n", .{});
}
```

### 10.4 Usage Style 3: io.concurrent for Parallel Clients

Use `io.concurrent` with groups to handle multiple clients in parallel:

```zig
const std = @import("std");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var srv = try server.TcpServer.init(gpa.allocator());
    defer srv.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 8080);
    try srv.listen(addr, .{ .reuse_address = true });

    const io = srv.io();

    std.debug.print("Concurrent echo server on :8080\n", .{});

    // Group for managing concurrent client handlers
    var clients: std.Io.Group = .init;
    defer clients.cancel(io);

    // Accept loop
    while (true) {
        var conn = try srv.accept();

        // Spawn concurrent handler for this client
        // io.concurrent schedules it to run in parallel
        try clients.concurrent(io, handleClient, .{ io, conn });
    }
}

/// Handle a single client - runs concurrently with other handlers.
fn handleClient(io: std.Io, conn: server.Connection) std.Io.Cancelable!void {
    var c = conn;  // Make mutable copy
    defer c.close();

    var buf: [1024]u8 = undefined;

    while (true) {
        // Each read/write suspends this coroutine, allowing others to run
        const n = c.readWithIo(io, &buf) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => break,
        };
        if (n == 0) break;

        _ = c.writeWithIo(io, buf[0..n]) catch break;
    }
}
```

### 10.5 Usage Style 4: Mixed Patterns with Select

Combine multiple operations and wait for any to complete:

```zig
const std = @import("std");
const server = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var srv = try server.TcpServer.init(gpa.allocator());
    defer srv.deinit();

    const addr = try std.net.Address.parseIp4("127.0.0.1", 8080);
    try srv.listen(addr, .{ .reuse_address = true });

    const io = srv.io();

    var conn1 = try srv.accept();
    defer conn1.close();

    var conn2 = try srv.accept();
    defer conn2.close();

    var buf1: [1024]u8 = undefined;
    var buf2: [1024]u8 = undefined;

    // Start async reads on both connections
    var future1 = io.async(readFrom, .{ &conn1, &buf1, io });
    var future2 = io.async(readFrom, .{ &conn2, &buf2, io });

    // Wait for EITHER to complete (like select/poll)
    var futures = [_]*std.Io.AnyFuture{
        @ptrCast(&future1),
        @ptrCast(&future2),
    };

    const ready_idx = try io.select(&futures);

    switch (ready_idx) {
        0 => {
            const n = future1.await(io);
            std.debug.print("Connection 1 ready: {} bytes\n", .{n});
            _ = future2.cancel(io);  // Cancel the other
        },
        1 => {
            const n = future2.await(io);
            std.debug.print("Connection 2 ready: {} bytes\n", .{n});
            _ = future1.cancel(io);
        },
        else => unreachable,
    }
}

fn readFrom(conn: *server.Connection, buf: []u8, io: std.Io) usize {
    return conn.readWithIo(io, buf) catch 0;
}
```

### 10.6 Key Takeaways

| Pattern | When to Use | Complexity |
|---------|-------------|------------|
| **Blocking-style** | Simple sequential operations | Lowest |
| **io.async + await** | Start work, do something else, then collect result | Medium |
| **io.concurrent + Group** | Handle many things in parallel (e.g., multiple clients) | Medium |
| **io.select** | Wait for any of multiple events | Higher |

**The library hides all ZIO details:**
- User never imports `zio`
- User never creates `zio.Runtime`
- User works with standard `std.Io` interface
- All patterns work transparently

**Non-blocking is automatic:**
- Every `read()`, `write()`, `accept()` suspends the coroutine
- Other coroutines run while waiting
- Looks synchronous, behaves asynchronously

## 11. Deep Dive: How Suspension Works Without Users Knowing About ZIO

This section explains the internal mechanics that allow users to write blocking-style code that is actually non-blocking.

### 11.1 The Key Insight: Stackful Coroutines

ZIO uses **stackful coroutines** - each task has its own complete call stack. When a coroutine suspends, its entire call stack is frozen in place. This is different from async/await which requires explicit marking at every suspension point.

With stackful coroutines:
- Any function can suspend, not just `async` functions
- The suspension is invisible to callers
- No "function coloring" problem

### 11.2 The Suspension Chain

Here's what happens when a user calls `conn.read(&buf)`:

```
User code                    Library code                   ZIO internals
─────────────────────────────────────────────────────────────────────────────
conn.read(&buf)
    │
    └──► self.readWithIo(self.server.io(), buffer)
              │
              └──► self.stream.read(user_io, buffer, .{})
                        │
                        │   std.Io dispatches through vtable
                        ▼
                   vtable.netRead(userdata, ...)
                        │
                        └──► netReadImpl(userdata, ...)           [stdio.zig]
                                  │
                                  │   Cast userdata back to Runtime
                                  ▼
                             var op = ev.StreamRead.init(...);
                             try waitForIo(rt, &op.c);            [common.zig:125]
                                  │
                                  └──► waiter.wait(1, .allow_cancel)   [common.zig:137]
                                            │
                                            └──► executor.yield(...)   [common.zig:83]
                                                      │
                                                      │   CONTEXT SWITCH!
                                                      ▼
                                                 current_coro.yieldTo(&next_coro)
                                                      │
                                            ┌────────┴────────┐
                                            │   COROUTINE     │
                                            │   SUSPENDED     │
                                            │   (stack frozen)│
                                            └─────────────────┘
```

### 11.3 The Magic: `yieldTo()` Context Switch

The actual suspension happens in assembly:

```zig
// In coroutines.zig:869
pub fn yieldTo(self: *Coroutine, other: *Coroutine) void {
    switchContext(&self.context, &other.context);
}
```

`switchContext` is platform-specific assembly that:
1. Saves all CPU registers to current coroutine's stack
2. Switches the stack pointer to another coroutine's stack
3. Restores that coroutine's registers
4. **Returns into the middle of that coroutine's code**

The current coroutine is frozen mid-function. The scheduler runs other work.

### 11.4 The Critical Code: `waitForIo()`

This is the suspension point for all I/O operations:

```zig
// common.zig:125-153
pub fn waitForIo(rt: *Runtime, c: *ev.Completion) Cancelable!void {
    var waiter = Waiter.init(rt);
    c.userdata = &waiter;
    c.callback = Waiter.callback;      // When I/O done, call this

    // Submit to event loop (epoll/kqueue/io_uring)
    waiter.task.getExecutor().loop.add(c);

    // This suspends! Waits for callback to signal
    waiter.wait(1, .allow_cancel);     // ← SUSPENSION POINT

    // When we get here, I/O is complete
}
```

And `waiter.wait()` performs the actual yield:

```zig
// common.zig:67-94
pub fn wait(self: *Waiter, expected: u32, ...) ... {
    while (true) {
        task.state.store(.preparing_to_wait, .release);

        // Already signaled? Don't suspend
        if (self.signaled.load(.acquire) >= expected) {
            return;
        }

        // SUSPEND THE COROUTINE - this is where the magic happens
        executor.yield(.preparing_to_wait, .waiting, ...);

        // When we resume, check if actually done
        if (self.signaled.load(.acquire) >= expected) {
            return;  // Done!
        }
        // Spurious wakeup, loop again
    }
}
```

### 11.5 The Resume Path

When the OS signals I/O completion:

```
Event loop detects readable socket (epoll_wait/kevent returns)
    │
    └──► Calls completion callback
              │
              └──► Waiter.callback()                    [common.zig:117]
                        │
                        └──► waiter.signal()           [common.zig:56]
                                  │
                                  ├──► signaled.fetchAdd(1)   // Mark ready
                                  └──► task.wake()            // Schedule task
                                            │
                                            └──► Adds task to runnable queue

Later, scheduler picks up the task:
    │
    └──► scheduler.yieldTo(&suspended_task.coro)
              │
              │   CONTEXT SWITCH back!
              ▼
         // Execution resumes EXACTLY where it left off
         // Inside waiter.wait(), after the yield() call

         waiter.wait() returns
              │
         waitForIo() returns
              │
         netReadImpl() returns data
              │
         vtable dispatch returns
              │
         stream.read() returns
              │
         conn.readWithIo() returns
              │
         conn.read() returns to user with data!
```

### 11.6 The Abstraction Layers

```
┌─────────────────────────────────────────────────────────────┐
│  User Code                                                  │
│  conn.read(&buf)  ← Looks like a normal blocking call       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  Library (e.g., server.zig, ZZMQ)                           │
│  Calls self.stream.read(self.server.io(), ...)              │
│  ← Gets std.Io from internal runtime, user doesn't know     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  std.Io Interface                                           │
│  Dispatches through vtable: vtable.netRead(...)             │
│  ← Standard Zig interface, implementation-agnostic          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  ZIO's stdio.zig vtable implementation                      │
│  Casts userdata → Runtime, calls waitForIo()                │
│  ← User never imports this, never sees it                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  ZIO Runtime (coroutines + event loop)                      │
│  Suspends coroutine, polls OS, resumes when ready           │
│  ← Completely hidden, just makes the call "take time"       │
└─────────────────────────────────────────────────────────────┘
```

### 11.7 Why This Works Transparently

| Layer | What User Sees | What Actually Happens |
|-------|----------------|----------------------|
| `conn.read()` | "Blocking" call | Calls through to std.Io |
| `std.Io` | Standard interface | Dispatches via vtable |
| vtable impl | Hidden | Calls `waitForIo()` |
| `waitForIo` | Hidden | Submits I/O, calls `waiter.wait()` |
| `waiter.wait` | Hidden | Calls `executor.yield()` |
| `yield()` | Hidden | `yieldTo()` - assembly context switch |
| Coroutine | Hidden | Stack frozen, scheduler runs others |
| I/O completion | Hidden | Callback signals, task rescheduled |
| Resume | Hidden | Context switch back, stack unfreezes |
| Return | Data arrives! | Looks like call just "finished" |

### 11.8 Comparison with Other Async Models

| Model | Suspension | Pros | Cons |
|-------|------------|------|------|
| **ZIO (stackful coroutines)** | Invisible, any function can suspend | No function coloring, natural code | Stack memory per coroutine |
| **async/await** | Explicit `await` keyword | Clear suspension points | Function coloring, viral async |
| **Callbacks** | Manual via closures | No runtime overhead | Callback hell, hard to follow |
| **Threads** | OS-managed preemption | True parallelism | Heavy, synchronization needed |

### 11.9 Memory Layout

Each coroutine has its own stack (default ~64KB, configurable):

```
┌─────────────────────────────────────────┐
│           Coroutine A Stack             │
├─────────────────────────────────────────┤
│  main()                                 │
│    └─► handleClient()                   │
│          └─► conn.read()                │
│                └─► stream.read()        │
│                      └─► waitForIo()    │
│                            └─► yield()  │  ← Suspended here
│                                 [saved registers]
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│           Coroutine B Stack             │
├─────────────────────────────────────────┤
│  main()                                 │
│    └─► handleClient()                   │
│          └─► conn.write()               │  ← Currently running
└─────────────────────────────────────────┘
```

When the scheduler switches from B to A:
1. Save B's registers to B's stack
2. Switch stack pointer to A's stack
3. Restore A's registers
4. `ret` instruction returns into A's `yield()` call
5. A continues as if nothing happened

### 11.10 Key Takeaway

**The user writes `const data = conn.read(&buf)` and it "just works":**
- Looks like a blocking call
- Actually non-blocking underneath
- Other coroutines run while waiting
- No special syntax needed
- No knowledge of ZIO required

This is the power of stackful coroutines combined with the `std.Io` abstraction layer.

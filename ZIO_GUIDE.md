# ZIO - Complete Guide

**Version:** 0.5.1 (zig-0.16 branch)
**Zig Compatibility:** 0.16.0+
**License:** MIT

ZIO is an async I/O framework for Zig that provides stackful coroutines (fibers/green threads) with a runtime that makes asynchronous operations look synchronous. It's similar to Go's goroutines but implemented in Zig with manual memory management.

---

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Core Concepts](#core-concepts)
4. [Runtime](#runtime)
5. [Tasks](#tasks)
6. [Groups](#groups)
7. [Networking](#networking)
8. [File I/O](#file-io)
9. [Synchronization Primitives](#synchronization-primitives)
10. [Channels](#channels)
11. [Select](#select)
12. [Signals](#signals)
13. [Time](#time)
14. [Error Handling & Cancellation](#error-handling--cancellation)
15. [std.Io Integration](#stdio-integration)
16. [Complete Examples](#complete-examples)
17. [Platform Support](#platform-support)
18. [io_uring Deep Dive](#io_uring-deep-dive)
19. [FAQ](#faq)

---

## Overview

ZIO provides:

- **Stackful coroutines** - User-mode context switching for `x86_64`, `aarch64`, `riscv64`, and `loongarch64`
- **Async I/O** - Operations look blocking but use event-driven OS APIs (`io_uring`, `epoll`, `iocp`, `kqueue`, `poll`)
- **Multi-threaded scheduler** - Run tasks across multiple CPU threads with automatic load balancing
- **Synchronization primitives** - `Mutex`, `Condition`, `Semaphore`, `Channel`, `Barrier`, etc.
- **Cancellation support** - All operations can be canceled
- **std.Io integration** - Works with Zig standard library interfaces

### How It Works

1. You create a `Runtime` that manages one or more executor threads
2. You `spawn()` tasks (coroutines) that run concurrently
3. When a task performs I/O or waits, it suspends and other tasks run
4. The scheduler multiplexes thousands of tasks onto a few OS threads

---

## Installation

### 1. Add ZIO to `build.zig.zon`

```bash
zig fetch --save "git+https://github.com/lalinsky/zio#zig-0.16"
```

### 2. Configure `build.zig`

```zig
const zio = b.dependency("zio", .{
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("zio", zio.module("zio"));
```

---

## Core Concepts

### Imports

```zig
const std = @import("std");
const zio = @import("zio");
```

### Public Types

| Type | Description |
|------|-------------|
| `zio.Runtime` | Main runtime that manages executors and tasks |
| `zio.JoinHandle(T)` | Handle to wait for/cancel a spawned task |
| `zio.Group` | Structured concurrency for managing multiple tasks |
| `zio.net.*` | Networking types (Stream, Socket, IpAddress, etc.) |
| `zio.File` / `zio.Dir` | File system operations |
| `zio.Mutex` | Async-aware mutual exclusion lock |
| `zio.Condition` | Condition variable for signaling |
| `zio.Semaphore` | Counting semaphore |
| `zio.Channel(T)` | Bounded FIFO channel for task communication |
| `zio.BroadcastChannel(T)` | Multi-producer multi-consumer broadcast channel |
| `zio.Barrier` | Synchronization barrier |
| `zio.Notify` | One-shot notification |
| `zio.ResetEvent` | Manual/auto reset event |
| `zio.Future(T)` | One-shot value container |
| `zio.Signal` | OS signal handler |
| `zio.time.Duration` | Time duration |
| `zio.time.Timestamp` | Point in time |
| `zio.time.Stopwatch` | High-performance timer |

---

## Runtime

The `Runtime` is the core of ZIO. It manages the event loop, task scheduling, and resource pools.

### Creating a Runtime

```zig
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
defer _ = gpa.deinit();

const rt = try zio.Runtime.init(gpa.allocator(), .{});
defer rt.deinit();
```

### Runtime Options

```zig
const rt = try zio.Runtime.init(allocator, .{
    // Number of executor threads (default: 1 = single-threaded)
    .executors = .exact(4),  // or .auto for CPU count

    // Stack pool configuration
    .stack_pool = .{
        .maximum_size = 8 * 1024 * 1024,  // 8MB reserved per stack
        .committed_size = 64 * 1024,       // 64KB initial commit
        .max_unused_stacks = 16,
        .max_age = .fromSeconds(60),
    },

    // LIFO slot optimization (default: true)
    .lifo_slot_enabled = true,
});
```

### Runtime Methods

```zig
// Spawn a new task
var handle = try rt.spawn(myFunction, .{ arg1, arg2 });

// Spawn a blocking task (runs in thread pool)
var handle = try rt.spawnBlocking(blockingFunction, .{ arg1 });

// Sleep the current task
try rt.sleep(.fromMilliseconds(100));

// Yield to other tasks
try rt.yield();

// Get current monotonic time
const now = rt.now();

// Cancellation shield (prevent cancel during critical section)
rt.beginShield();
defer rt.endShield();

// Check if canceled
try rt.checkCancel();

// Get std.Io interface
const io = rt.io();
```

---

## Tasks

Tasks are the fundamental unit of concurrency in ZIO. They're stackful coroutines that suspend when waiting and resume when ready.

### Spawning Tasks

```zig
fn myTask(rt: *zio.Runtime, value: i32) !i32 {
    try rt.sleep(.fromMilliseconds(100));
    return value * 2;
}

// Spawn and get a handle
var handle = try rt.spawn(myTask, .{ rt, 21 });

// Wait for result
const result = handle.join(rt);  // Returns 42
```

### Task Return Types

Tasks can return any type, including error unions:

```zig
// Void task
fn voidTask() void { }

// Value-returning task
fn valueTask() i32 { return 42; }

// Error-returning task
fn errorTask() !void { return error.SomethingFailed; }

// Error union with value
fn errorValueTask() !i32 { return 42; }

// With Cancelable error
fn cancelableTask(rt: *zio.Runtime) zio.Cancelable!void {
    try rt.sleep(.fromSeconds(10));
}
```

### JoinHandle Operations

```zig
var handle = try rt.spawn(myTask, .{ rt });

// Wait for completion and get result
const result = handle.join(rt);

// Cancel the task and wait
handle.cancel(rt);

// Detach (fire and forget)
handle.detach(rt);

// Check if result is available
if (handle.hasResult()) {
    const result = handle.getResult();
}
```

### Common Pattern: Cancel in Defer

```zig
var handle = try rt.spawn(myTask, .{ rt });
defer handle.cancel(rt);  // Safe even after join()

const result = handle.join(rt);
```

### Stack Size

Default stack size is 256 KiB. Configure for tasks needing more:

```zig
// Note: Stack size is set via the task closure, not spawn options
// The default is usually sufficient for most tasks
```

---

## Groups

Groups provide structured concurrency - spawn multiple tasks and wait for all to complete.

### Basic Usage

```zig
var group: zio.Group = .init;
defer group.cancel(rt);  // Cancel any remaining tasks

// Spawn tasks into the group
try group.spawn(rt, task1, .{ rt });
try group.spawn(rt, task2, .{ rt });
try group.spawn(rt, task3, .{ rt });

// Wait for all to complete
try group.wait(rt);

// Check if any task failed
if (group.hasFailed()) {
    // Handle failure
}
```

### Error Handling in Groups

When a task in a group returns an error:
- If it returns `error.Canceled`, the group's `isCanceled()` flag is set
- For any other error, the group's `hasFailed()` flag is set

```zig
fn worker(rt: *zio.Runtime) !void {
    try doWork();  // If this fails, group.hasFailed() becomes true
}

var group: zio.Group = .init;
defer group.cancel(rt);

for (0..10) |_| {
    try group.spawn(rt, worker, .{rt});
}

try group.wait(rt);
if (group.hasFailed()) {
    std.log.err("One or more workers failed", .{});
}
```

### Blocking Tasks in Groups

```zig
try group.spawnBlocking(rt, blockingWork, .{ data });
```

---

## Networking

ZIO provides high-level networking with full async support.

### IP Addresses

```zig
// Parse IPv4
const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 8080);

// Parse IPv6
const addr6 = try zio.net.IpAddress.parseIp6("::1", 8080);

// Parse either
const addr = try zio.net.IpAddress.parseIp("192.168.1.1", 8080);

// Parse with port string
const addr = try zio.net.IpAddress.parseIpAndPort("127.0.0.1:8080");

// Unspecified (0.0.0.0)
const addr = zio.net.IpAddress.unspecified(8080);
```

### TCP Server

```zig
const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 8080);
const server = try addr.listen(rt, .{});
defer server.close(rt);

std.log.info("Listening on {f}", .{server.socket.address});

var group: zio.Group = .init;
defer group.cancel(rt);

while (true) {
    const stream = try server.accept(rt);
    errdefer stream.close(rt);

    try group.spawn(rt, handleClient, .{ rt, stream });
}
```

### TCP Client

```zig
const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 8080);
var stream = try addr.connect(rt, .{});
defer stream.close(rt);
```

### Stream I/O

```zig
fn handleClient(rt: *zio.Runtime, stream: zio.net.Stream) !void {
    defer stream.close(rt);
    defer stream.shutdown(rt, .both) catch {};

    // Buffered reader/writer
    var read_buffer: [4096]u8 = undefined;
    var reader = stream.reader(rt, &read_buffer);

    var write_buffer: [4096]u8 = undefined;
    var writer = stream.writer(rt, &write_buffer);

    // Read a line
    const line = reader.interface.takeDelimiterInclusive('\n') catch |err| switch (err) {
        error.EndOfStream => return,
        else => return err,
    };

    // Write response
    try writer.interface.writeAll(line);
    try writer.interface.flush();
}
```

### UDP

```zig
const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 8080);
const socket = try addr.bind(rt, .{});
defer socket.close(rt);

var buffer: [1024]u8 = undefined;

while (true) {
    const result = try socket.receiveFrom(rt, &buffer);
    std.log.info("Received {d} bytes from {f}", .{ result.len, result.from });

    _ = try socket.sendTo(rt, result.from, buffer[0..result.len]);
}
```

### DNS Resolution

```zig
const hostname = try zio.net.HostName.init("example.com");
var iter = try hostname.lookup(rt, .{ .port = 80 });
defer iter.deinit();

while (iter.next()) |result| {
    switch (result) {
        .address => |addr| std.log.info("Address: {f}", .{addr}),
        .canonical_name => |name| std.log.info("Canonical: {s}", .{name.bytes}),
    }
}

// Or connect directly
var stream = try hostname.connect(rt, 80, .{});
```

---

## File I/O

File I/O is truly asynchronous on Linux and Windows, simulated via thread pool on other platforms.

### Opening Files

```zig
// Open existing file
var file = try zio.openFile(rt, "path/to/file.txt");
defer file.close(rt);

// Create new file
var file = try zio.createFile(rt, "path/to/file.txt", .{});
defer file.close(rt);

// With directory handle
const dir = zio.Dir.cwd();
var file = try dir.openFile(rt, "file.txt", .{ .mode = .read_only });
```

### Reading and Writing

```zig
// Positional read
var buf: [1024]u8 = undefined;
const n = try file.read(rt, &buf, 0);  // Read at offset 0

// Positional write
const written = try file.write(rt, "Hello, World!", 0);

// Vectored I/O
const slices = [_][]const u8{ "Hello, ", "World!" };
_ = try file.writeVec(rt, &slices, 0);
```

### File Reader/Writer (std.Io Interface)

```zig
var read_buffer: [256]u8 = undefined;
var reader = file.reader(rt, &read_buffer);

var result: [20]u8 = undefined;
const bytes_read = try reader.interface.readSliceShort(&result);

var write_buffer: [256]u8 = undefined;
var writer = file.writer(rt, &write_buffer);
try writer.interface.writeAll("Hello!");
try writer.interface.flush();
```

### Directory Operations

```zig
// Create directory
try zio.createDir(rt, "newdir", 0o755);

// Delete directory
try zio.deleteDir(rt, "olddir");

// Delete file
try zio.deleteFile(rt, "file.txt");

// Rename
try zio.rename(rt, "old.txt", "new.txt");

// File stats
const info = try zio.stat(rt, "file.txt");
std.log.info("Size: {d}, Mode: {o}", .{ info.size, info.mode });

// Check access
try zio.access(rt, "file.txt", .{ .read = true });
```

### Symbolic and Hard Links

```zig
// Create symlink
try dir.symLink(rt, "target.txt", "link.txt", .{});

// Read symlink
var buf: [256]u8 = undefined;
const target = try dir.readLink(rt, "link.txt", &buf);

// Create hard link
try dir.hardLink(rt, "original.txt", dir, "hardlink.txt", .{});
```

---

## Synchronization Primitives

All synchronization primitives are async-aware and will suspend the current task instead of blocking the thread.

### Mutex

```zig
var mutex: zio.Mutex = .init;

try mutex.lock(rt);
defer mutex.unlock(rt);

// Critical section
shared_data += 1;
```

Non-blocking try:

```zig
if (mutex.tryLock()) {
    defer mutex.unlock(rt);
    // Got the lock
}
```

Uncancelable lock (for cleanup operations):

```zig
mutex.lockUncancelable(rt);
defer mutex.unlock(rt);
```

### Condition Variable

```zig
var mutex: zio.Mutex = .init;
var cond: zio.Condition = .init;
var ready = false;

// Waiting side
try mutex.lock(rt);
while (!ready) {
    try cond.wait(rt, &mutex);
}
mutex.unlock(rt);

// Signaling side
try mutex.lock(rt);
ready = true;
cond.signal(rt);
mutex.unlock(rt);
```

### Semaphore

```zig
var sem = zio.Semaphore{ .permits = 3 };

// Acquire permit (blocks if none available)
try sem.wait(rt);
defer sem.post(rt);

// Do work with limited concurrency
```

Timed wait:

```zig
sem.timedWait(rt, .fromMilliseconds(100)) catch |err| {
    if (err == error.Timeout) {
        // Handle timeout
    }
};
```

### Barrier

```zig
var barrier = zio.Barrier{ .threshold = 4 };

// In each of 4 tasks:
try barrier.wait(rt);  // All tasks must reach here before any proceed
```

### Notify

One-shot notification:

```zig
var notify: zio.Notify = .init;

// Waiting side
try notify.wait(rt);

// Signaling side
notify.set(rt);
```

### ResetEvent

```zig
var event: zio.ResetEvent = .init;

// Wait for event
try event.wait(rt);

// Signal event
event.set(rt);

// Reset for reuse
event.reset();
```

### Future

One-shot value container:

```zig
var future = zio.Future(i32).init;

// Setting side
future.set(42);

// Waiting side (in select or directly)
const value = try zio.wait(rt, &future);
```

---

## Channels

Channels provide communication between tasks with optional buffering.

### Creating Channels

```zig
// Buffered channel (capacity = buffer length)
var buffer: [8]i32 = undefined;
var channel = zio.Channel(i32).init(&buffer);

// Unbuffered channel (synchronous rendezvous)
var channel = zio.Channel(i32).init(&.{});
```

### Sending and Receiving

```zig
// Send (blocks if full)
try channel.send(rt, 42);

// Receive (blocks if empty)
const value = try channel.receive(rt);
```

Non-blocking:

```zig
// Try send
channel.trySend(99) catch |err| switch (err) {
    error.ChannelFull => { /* handle */ },
    error.ChannelClosed => { /* handle */ },
};

// Try receive
const value = channel.tryReceive() catch |err| switch (err) {
    error.ChannelEmpty => { /* handle */ },
    error.ChannelClosed => { /* handle */ },
};
```

### Closing Channels

```zig
// Graceful close - receivers can drain remaining items
channel.close(.graceful);

// Immediate close - clears all buffered items
channel.close(.immediate);
```

### Producer-Consumer Example

```zig
fn producer(rt: *zio.Runtime, ch: *zio.Channel(i32)) !void {
    for (0..10) |i| {
        try ch.send(rt, @intCast(i));
    }
}

fn consumer(rt: *zio.Runtime, ch: *zio.Channel(i32)) !void {
    while (true) {
        const value = ch.receive(rt) catch |err| switch (err) {
            error.ChannelClosed => break,
            else => return err,
        };
        std.log.info("Received: {}", .{value});
    }
}
```

---

## Select

`select()` waits for multiple operations and returns whichever completes first.

### Basic Usage

```zig
var task1 = try rt.spawn(slowTask, .{rt});
defer task1.cancel(rt);

var task2 = try rt.spawn(fastTask, .{rt});
defer task2.cancel(rt);

const result = try zio.select(rt, .{
    .slow = &task1,
    .fast = &task2,
});

switch (result) {
    .slow => |val| std.log.info("Slow won: {}", .{val}),
    .fast => |val| std.log.info("Fast won: {}", .{val}),
}
```

### With Channels

```zig
const result = try zio.select(rt, .{
    .recv = channel.asyncReceive(),
    .timeout = zio.time.Timeout{ .duration = .fromSeconds(5) },
});

switch (result) {
    .recv => |val| {
        const value = try val;  // Handle potential ChannelClosed error
        std.log.info("Received: {}", .{value});
    },
    .timeout => {
        std.log.info("Timed out", .{});
    },
}
```

### With Signals

```zig
var sigint = try zio.Signal.init(.INT);
defer sigint.deinit();

var sigterm = try zio.Signal.init(.TERM);
defer sigterm.deinit();

const result = try zio.select(rt, .{
    .sigint = &sigint,
    .sigterm = &sigterm,
});

switch (result) {
    .sigint => std.log.info("Received SIGINT", .{}),
    .sigterm => std.log.info("Received SIGTERM", .{}),
}
```

### Wait for Single Future

```zig
// Using wait() for a single future
const result = try zio.wait(rt, &future);
const value = result.value;

// waitUntilComplete never returns error.Canceled
const value = zio.waitUntilComplete(rt, &future);
```

---

## Signals

Handle OS signals (Unix) or console control events (Windows).

### Creating Signal Handlers

```zig
var sig = try zio.Signal.init(.INT);  // SIGINT / Ctrl+C
defer sig.deinit();

// Wait for signal
try sig.wait(rt);
std.log.info("Received signal!", .{});
```

### Available Signal Types

**Unix:** All `std.posix.SIG` values (INT, TERM, USR1, USR2, etc.)

**Windows:**
- `.INT` - Ctrl+C (CTRL_C_EVENT)
- `.TERM` - Console close (CTRL_CLOSE_EVENT)

### Timed Wait

```zig
sig.timedWait(rt, .fromSeconds(5)) catch |err| switch (err) {
    error.Timeout => std.log.info("No signal received", .{}),
    else => return err,
};
```

### Graceful Shutdown Example

```zig
fn signalHandler(rt: *zio.Runtime, shutdown: *std.atomic.Value(bool)) !void {
    var sig = try zio.Signal.init(.INT);
    defer sig.deinit();

    try sig.wait(rt);
    std.log.info("Shutdown requested", .{});
    shutdown.store(true, .release);
}

fn serverTask(rt: *zio.Runtime, shutdown: *std.atomic.Value(bool)) !void {
    while (!shutdown.load(.acquire)) {
        // Do work
        try rt.sleep(.fromMilliseconds(100));
    }
    std.log.info("Server shutting down", .{});
}

var shutdown = std.atomic.Value(bool).init(false);
var group: zio.Group = .init;
defer group.cancel(rt);

try group.spawn(rt, serverTask, .{ rt, &shutdown });
try group.spawn(rt, signalHandler, .{ rt, &shutdown });

try group.wait(rt);
```

---

## Time

### Duration

```zig
const d1 = zio.time.Duration.fromNanoseconds(1000);
const d2 = zio.time.Duration.fromMicroseconds(500);
const d3 = zio.time.Duration.fromMilliseconds(100);
const d4 = zio.time.Duration.fromSeconds(5);
const d5 = zio.time.Duration.fromMinutes(2);

// Convert back
const ms = d3.toMilliseconds();  // 100

// Parse from string (Go-style)
const d = try zio.time.Duration.parse("1h30m45s");

// Format
std.log.info("Duration: {f}", .{d});  // "1h30m45s"
```

### Timestamp

```zig
// Current monotonic time
const now = rt.now();

// Duration between timestamps
const elapsed = start.durationTo(end);

// Add/subtract duration
const later = now.addDuration(.fromSeconds(10));
const earlier = now.subDuration(.fromSeconds(5));

// Format
std.log.info("Time: {f}", .{now});  // "2024-01-15 12:40:45"
```

### Stopwatch

```zig
var timer = zio.time.Stopwatch.start();

// Do work...

const elapsed = timer.read();
std.log.info("Took {f}", .{elapsed});

// Reset
timer.reset();

// Lap (read and reset)
const lap = timer.lap();
```

### Timeout

Used with `select()`:

```zig
const result = try zio.select(rt, .{
    .work = &workFuture,
    .timeout = zio.time.Timeout{ .duration = .fromSeconds(5) },
});
```

---

## Error Handling & Cancellation

### Cancelable Error Type

Many ZIO operations can return `error.Canceled`:

```zig
pub const Cancelable = error{Canceled};

fn myTask(rt: *zio.Runtime) zio.Cancelable!void {
    try rt.sleep(.fromSeconds(10));  // Returns error.Canceled if task is canceled
}
```

### Handling Cancellation

```zig
fn handleClient(rt: *zio.Runtime, stream: zio.net.Stream) !void {
    defer stream.close(rt);  // Always closes, even on cancel

    while (true) {
        const data = stream.read(rt, &buf) catch |err| switch (err) {
            error.Canceled => {
                std.log.info("Task canceled, cleaning up", .{});
                return error.Canceled;  // Propagate cancellation
            },
            else => return err,
        };
        // Process data...
    }
}
```

### Cancellation Shielding

Prevent cancellation during critical sections:

```zig
rt.beginShield();
defer rt.endShield();

// This code cannot be interrupted by cancellation
try doImportantCleanup();

// After endShield(), check if we were canceled
try rt.checkCancel();  // Throws error.Canceled if we were
```

### Uncancelable Operations

Some operations have "uncancelable" variants for cleanup:

```zig
// Always acquires lock, even if canceled
mutex.lockUncancelable(rt);
defer mutex.unlock(rt);
```

---

## std.Io Integration

ZIO implements `std.Io.Reader` and `std.Io.Writer` interfaces.

### Getting std.Io from Runtime

```zig
const io = rt.io();

// Use with std.http.Server
var server = std.http.Server.init(&reader.interface, &writer.interface);
```

### Stream Reader/Writer

```zig
var read_buffer: [4096]u8 = undefined;
var reader = stream.reader(rt, &read_buffer);

var write_buffer: [4096]u8 = undefined;
var writer = stream.writer(rt, &write_buffer);

// reader.interface and writer.interface are std.Io types
```

### HTTP Server Example

```zig
fn handleClient(rt: *zio.Runtime, stream: zio.net.Stream) !void {
    defer stream.close(rt);

    var read_buffer: [64 * 1024]u8 = undefined;
    var reader = stream.reader(rt, &read_buffer);

    var write_buffer: [4096]u8 = undefined;
    var writer = stream.writer(rt, &write_buffer);

    var server = std.http.Server.init(&reader.interface, &writer.interface);

    while (true) {
        var request = server.receiveHead() catch break;

        const html = "<html><body><h1>Hello!</h1></body></html>";
        try request.respond(html, .{
            .status = .ok,
            .extra_headers = &.{
                .{ .name = "content-type", .value = "text/html" },
            },
        });

        if (!request.head.keep_alive) break;
    }
}
```

---

## Complete Examples

### TCP Echo Server

```zig
const std = @import("std");
const zio = @import("zio");

fn handleClient(rt: *zio.Runtime, stream: zio.net.Stream) !void {
    defer stream.close(rt);
    defer stream.shutdown(rt, .both) catch {};

    var read_buffer: [1024]u8 = undefined;
    var reader = stream.reader(rt, &read_buffer);

    var write_buffer: [1024]u8 = undefined;
    var writer = stream.writer(rt, &write_buffer);

    while (true) {
        const line = reader.interface.takeDelimiterInclusive('\n') catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        try writer.interface.writeAll(line);
        try writer.interface.flush();
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const rt = try zio.Runtime.init(gpa.allocator(), .{});
    defer rt.deinit();

    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 8080);
    const server = try addr.listen(rt, .{});
    defer server.close(rt);

    std.log.info("Listening on {f}", .{server.socket.address});

    var group: zio.Group = .init;
    defer group.cancel(rt);

    while (true) {
        const stream = try server.accept(rt);
        errdefer stream.close(rt);
        try group.spawn(rt, handleClient, .{ rt, stream });
    }
}
```

### Producer-Consumer with Channels

```zig
const std = @import("std");
const zio = @import("zio");

fn producer(rt: *zio.Runtime, channel: *zio.Channel(i32), id: u32) !void {
    for (0..5) |i| {
        const item: i32 = @intCast(id * 100 + i);
        channel.send(rt, item) catch |err| switch (err) {
            error.ChannelClosed => return,
            error.Canceled => return,
        };
        std.log.info("Producer {}: sent {}", .{ id, item });
    }
}

fn consumer(rt: *zio.Runtime, channel: *zio.Channel(i32), id: u32) !void {
    while (true) {
        const item = channel.receive(rt) catch |err| switch (err) {
            error.ChannelClosed => return,
            error.Canceled => return,
        };
        std.log.info("Consumer {}: received {}", .{ id, item });
        try rt.sleep(.fromMilliseconds(100));
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var rt = try zio.Runtime.init(gpa.allocator(), .{});
    defer rt.deinit();

    var buffer: [8]i32 = undefined;
    var channel = zio.Channel(i32).init(&buffer);

    var group: zio.Group = .init;
    defer group.cancel(rt);

    // Start producers and consumers
    for (0..2) |i| {
        try group.spawn(rt, producer, .{ rt, &channel, @intCast(i) });
        try group.spawn(rt, consumer, .{ rt, &channel, @intCast(i) });
    }

    try group.wait(rt);
}
```

### Multi-threaded Runtime

```zig
const rt = try zio.Runtime.init(allocator, .{
    .executors = .auto,  // Use all CPU cores
});
defer rt.deinit();

// Tasks automatically distribute across executor threads
```

---

## Platform Support

### Operating Systems

| OS | Event Backend | File I/O | Notes |
|----|---------------|----------|-------|
| Linux | io_uring, epoll | Async | Full support |
| Windows | IOCP | Async | Full support |
| macOS | kqueue | Thread pool | BSD socket API |
| FreeBSD/OpenBSD | kqueue | Thread pool | Should work |
| Others | poll | Thread pool | Fallback |

### Architectures

- `x86_64` - Full support
- `aarch64` - Full support
- `riscv64` - Full support
- `loongarch64` - Full support

### Features by Platform

| Feature | Linux | Windows | macOS |
|---------|-------|---------|-------|
| Async network I/O | Yes | Yes | Yes |
| True async file I/O | Yes (io_uring) | Yes (IOCP) | Via thread pool |
| Cancelable I/O | Yes | Yes | Stop polling only |
| Unix sockets | Yes | Win10+ | Yes |
| Signal handling | Yes | Console events | Yes |

---

## io_uring Deep Dive

This section covers ZIO's io_uring backend in detail, with insights relevant for building high-performance messaging libraries like ZeroMQ.

### ZIO's io_uring Implementation

ZIO uses io_uring on Linux as its primary I/O backend. The implementation is in `src/ev/backends/io_uring.zig`.

#### Initialization Flags

ZIO initializes io_uring with performance-optimized flags:

```zig
// From ZIO's io_uring.zig
flags |= linux.IORING_SETUP_SINGLE_ISSUER;   // Single thread submits SQEs
flags |= linux.IORING_SETUP_DEFER_TASKRUN;   // Defer task work to submission
flags |= linux.IORING_SETUP_COOP_TASKRUN;    // Cooperative task running
```

- **SINGLE_ISSUER**: Optimizes for single-threaded SQE submission (common pattern)
- **DEFER_TASKRUN**: Reduces kernel thread wakeups by deferring work to io_uring_enter
- **COOP_TASKRUN**: Allows cooperative scheduling of io_uring tasks

#### Supported Operations

ZIO's io_uring backend supports these async operations natively:

| Operation | io_uring Op | Notes |
|-----------|-------------|-------|
| TCP connect | `CONNECT` | Non-blocking connection |
| TCP accept | `ACCEPT` | Accepts new connections |
| recv/send | `RECVMSG`/`SENDMSG` | Vectored I/O via msghdr |
| recvfrom/sendto | `RECVMSG`/`SENDMSG` | UDP with address |
| recvmsg/sendmsg | `RECVMSG`/`SENDMSG` | Full control messages |
| poll | `POLL_ADD` | Level-triggered readiness |
| shutdown | `SHUTDOWN` | Socket shutdown |
| close | `CLOSE` | Async close |
| File read/write | `READV`/`WRITEV` | Vectored positional I/O |
| File open/create | `OPENAT` | Async file open |
| File sync | `FSYNC` | With DATASYNC option |
| ftruncate | `FTRUNCATE` | Set file size |
| mkdir | `MKDIRAT` | Create directory |
| rename | `RENAMEAT` | Atomic rename |
| unlink | `UNLINKAT` | Delete files/dirs |
| statx | `STATX` | Extended file stats |

#### Cancellation

ZIO supports true async cancellation via io_uring:

```zig
// ZIO cancels in-flight operations using IORING_OP_ASYNC_CANCEL
sqe.prep_cancel(@intFromPtr(target), 0);
```

This generates two CQEs:
1. Cancel CQE (result: 0 or -ENOENT)
2. Target CQE (result: -ECANCELED or natural completion)

#### Wakeup Mechanism

ZIO uses io_uring's `FUTEX_WAIT` operation for efficient cross-thread wakeups:

```zig
// Wait on wake_requested flag
sqe.opcode = .FUTEX_WAIT;
sqe.fd = @bitCast(linux.FUTEX2_FLAGS{ .size = .U32, .private = true });

// Wake from another thread
linux.futex_3arg(&state.wake_requested.raw, .{ .cmd = .WAKE, .private = true }, 1);
```

This is more efficient than eventfd for signaling between threads.

### High-Performance Networking Considerations

For building a ZeroMQ-like library with ZIO, here are key considerations:

#### 1. Vectored I/O (Scatter-Gather)

ZIO supports vectored I/O through `ReadBuf` and `WriteBuf`:

```zig
// Send multiple buffers in one syscall
const slices = [_][]const u8{ header, payload, trailer };
_ = try stream.sendVec(rt, &slices, .{});

// Receive into multiple buffers
var header_buf: [64]u8 = undefined;
var payload_buf: [4096]u8 = undefined;
const iovecs = [_]std.posix.iovec{
    .{ .base = &header_buf, .len = header_buf.len },
    .{ .base = &payload_buf, .len = payload_buf.len },
};
const n = try stream.recvVec(rt, &iovecs, .{});
```

#### 2. Low-Latency Wakeups

For intra-process signaling (like ZeroMQ's inproc transport), use `zio.Notify`:

```zig
var notify: zio.Notify = .init;

// Signaling side (can be called from any thread)
notify.set(rt);

// Waiting side
try notify.wait(rt);
```

Or use `zio.Channel` for passing data with backpressure:

```zig
// Unbuffered channel = synchronous rendezvous (lowest latency)
var channel = zio.Channel(Message).init(&.{});
```

#### 3. Timeouts

ZIO integrates timeouts naturally with `select()`:

```zig
const result = try zio.select(rt, .{
    .recv = channel.asyncReceive(),
    .timeout = zio.time.Timeout{ .duration = .fromMilliseconds(100) },
});

switch (result) {
    .recv => |val| { /* got message */ },
    .timeout => { /* timed out */ },
}
```

For socket-level timeouts:

```zig
// Timed operations via select with socket operations
var recv_op = stream.asyncRecv(&buffer, .{});
const result = try zio.select(rt, .{
    .data = &recv_op,
    .timeout = zio.time.Timeout{ .duration = .fromSeconds(5) },
});
```

#### 4. Concurrent Send/Receive

ZIO's coroutine model makes concurrent bidirectional communication natural:

```zig
fn connectionHandler(rt: *zio.Runtime, stream: zio.net.Stream) !void {
    defer stream.close(rt);

    var send_queue = zio.Channel(Message).init(&send_buffer);
    var recv_queue = zio.Channel(Message).init(&recv_buffer);

    var group: zio.Group = .init;
    defer group.cancel(rt);

    // Spawn concurrent sender and receiver
    try group.spawn(rt, sendLoop, .{ rt, stream, &send_queue });
    try group.spawn(rt, recvLoop, .{ rt, stream, &recv_queue });

    try group.wait(rt);
}

fn sendLoop(rt: *zio.Runtime, stream: zio.net.Stream, queue: *zio.Channel(Message)) !void {
    while (true) {
        const msg = try queue.receive(rt);
        try stream.sendAll(rt, msg.data, .{});
    }
}

fn recvLoop(rt: *zio.Runtime, stream: zio.net.Stream, queue: *zio.Channel(Message)) !void {
    var buffer: [65536]u8 = undefined;
    while (true) {
        const n = try stream.recv(rt, &buffer, .{});
        if (n == 0) break;
        try queue.send(rt, Message{ .data = buffer[0..n] });
    }
}
```

#### 5. High Throughput Patterns

**Batch Processing with Groups:**

```zig
fn processBatch(rt: *zio.Runtime, connections: []Connection) !void {
    var group: zio.Group = .init;
    defer group.cancel(rt);

    // Launch all operations concurrently
    for (connections) |conn| {
        try group.spawn(rt, processOne, .{ rt, conn });
    }

    // Wait for all to complete
    try group.wait(rt);
}
```

**Multi-threaded Runtime for CPU Parallelism:**

```zig
const rt = try zio.Runtime.init(allocator, .{
    .executors = .auto,  // One executor per CPU core
});
```

### io_uring Features NOT Currently in ZIO

ZIO's io_uring backend provides a solid foundation, but doesn't expose all advanced io_uring features. If you're building a ZeroMQ-like library and need these, you'd need to extend ZIO or use the ev layer directly.

#### Zero-Copy Networking

io_uring supports [zero-copy send](https://lwn.net/Articles/879724/) which can improve performance by 200%+ over MSG_ZEROCOPY:

- **IORING_OP_SEND_ZC**: Zero-copy send with notification when buffer is safe to reuse
- **Registered buffers**: Pre-registered buffers eliminate page pinning overhead
- **ZC Rx (kernel 6.x+)**: [Zero-copy receive](https://docs.kernel.org/networking/iou-zcrx.html) directly into userspace memory

**Status in ZIO:** Not currently implemented. ZIO uses standard RECVMSG/SENDMSG.

#### Multishot Operations

io_uring supports [multishot recv/accept](https://lwn.net/Articles/899498/) which can improve performance by ~8%:

- **Multishot recv**: Single SQE generates multiple CQEs for incoming data
- **Multishot accept**: Single SQE accepts multiple connections
- **Provided buffers**: Kernel picks buffers from a pool, reducing submission overhead

**Status in ZIO:** Not currently implemented. Each recv/accept is a single-shot operation.

#### Ring-Mapped Buffers

- **IORING_REGISTER_BUFFERS**: Pre-register buffers to avoid per-operation setup
- **IORING_REGISTER_FILES**: Pre-register file descriptors (fixed files)
- **Buffer rings**: Circular buffer pools managed by kernel

**Status in ZIO:** Not currently implemented.

#### SQE Linking

- **IOSQE_IO_LINK**: Chain operations (e.g., read then write)
- **IOSQE_IO_HARDLINK**: Hard-linked operations (continue even on error)
- **IOSQE_IO_DRAIN**: Wait for all prior operations to complete

**Status in ZIO:** Not exposed at the high-level API.

### Recommendations for a ZeroMQ-Like Library

Based on ZIO's current capabilities:

| Feature | ZIO Support | Recommendation |
|---------|-------------|----------------|
| TCP/UDP | ✅ Full | Use ZIO directly |
| Unix sockets | ✅ Full | Use ZIO directly |
| inproc (in-process) | ✅ Via Channel | Use unbuffered Channel for lowest latency |
| Pub/Sub | ✅ Via BroadcastChannel | Use BroadcastChannel |
| Request/Reply | ✅ Patterns possible | Build on Channel + Group |
| Timeouts | ✅ Via select() | Use select() with Timeout |
| Concurrent I/O | ✅ Via tasks/groups | Spawn tasks per direction |
| Load balancing | ✅ Multi-executor | Use `.executors = .auto` |
| Zero-copy | ⚠️ Use workarounds | Buffer pools + large buffers (see below) |
| Multishot recv | ⚠️ Not critical | ~8% gain; use larger buffers instead |
| Kernel bypass | ❌ Not applicable | Use DPDK/AF_XDP directly |

#### Why Missing Features Are Often Not Critical

**Zero-copy send (SEND_ZC):** The 200%+ improvement applies vs MSG_ZEROCOPY, not vs regular send. For messages under 16KB, the copy overhead is minimal. Use buffer pooling and large buffers to get most of the benefit.

**Multishot recv:** Only ~8% improvement. You can achieve similar gains by using larger receive buffers (64KB+) which amortizes the per-operation overhead.

**Registered buffers:** Eliminates page pinning overhead, but userspace buffer pooling eliminates allocation overhead which is often the larger cost.

#### Recommended Architecture for High-Performance Messaging

```zig
const MessagingSocket = struct {
    stream: zio.net.Stream,
    recv_pool: *BufferPool,
    send_pool: *BufferPool,
    send_batcher: MessageBatcher,

    pub fn send(self: *MessagingSocket, rt: *zio.Runtime, msg: []const u8) !void {
        // Coalesce small messages
        if (msg.len < 1024 and self.send_batcher.add(msg)) {
            if (self.send_batcher.shouldFlush()) {
                try self.send_batcher.flush(rt, self.stream);
            }
            return;
        }
        // Large messages: send directly with pooled buffer if needed
        try self.stream.sendAll(rt, msg, .{});
    }

    pub fn recv(self: *MessagingSocket, rt: *zio.Runtime) ![]u8 {
        const buf = self.recv_pool.acquire() orelse
            return error.NoBuffersAvailable;
        errdefer self.recv_pool.release(buf);

        const n = try self.stream.recv(rt, buf, .{});
        if (n == 0) return error.ConnectionClosed;
        return buf[0..n];
    }
};

### Performance Tuning

**Executor Count:**
```zig
// For I/O-bound workloads, 1 executor is often sufficient
.executors = .exact(1)

// For mixed CPU/I/O, match core count
.executors = .auto
```

**Stack Size:**
```zig
// Default 256KB is conservative; messaging handlers may need less
.stack_pool = .{
    .committed_size = 32 * 1024,  // 32KB initial commit
}
```

**io_uring Queue Size:**
The queue size affects how many operations can be in-flight. ZIO uses a default of 256 entries. For extreme throughput you can adjust this via `Loop.Options.queue_size`.

**LIFO Slot Optimization:**
ZIO enables a LIFO slot optimization by default (`.lifo_slot_enabled = true`) which allows newly spawned tasks to run immediately on the same executor, improving cache locality for request-response patterns.

### Userspace Workarounds for Missing Features

Since ZIO doesn't expose registered buffers or zero-copy APIs, here are effective userspace alternatives:

#### Buffer Pooling

Avoid allocation overhead by reusing buffers:

```zig
const BufferPool = struct {
    free_list: std.ArrayList([]u8),
    allocator: std.mem.Allocator,
    buffer_size: usize,

    pub fn init(allocator: std.mem.Allocator, count: usize, size: usize) !BufferPool {
        var pool = BufferPool{
            .free_list = std.ArrayList([]u8).init(allocator),
            .allocator = allocator,
            .buffer_size = size,
        };
        for (0..count) |_| {
            const buf = try allocator.alloc(u8, size);
            try pool.free_list.append(buf);
        }
        return pool;
    }

    pub fn acquire(self: *BufferPool) ?[]u8 {
        return self.free_list.popOrNull();
    }

    pub fn release(self: *BufferPool, buf: []u8) void {
        self.free_list.append(buf) catch {
            self.allocator.free(buf);
        };
    }
};
```

#### Message Coalescing

Reduce syscall overhead by batching small messages:

```zig
const MessageBatcher = struct {
    buffer: []u8,
    offset: usize = 0,
    flush_threshold: usize,

    pub fn add(self: *MessageBatcher, msg: []const u8) bool {
        if (self.offset + msg.len > self.buffer.len) return false;
        @memcpy(self.buffer[self.offset..][0..msg.len], msg);
        self.offset += msg.len;
        return true;
    }

    pub fn shouldFlush(self: *const MessageBatcher) bool {
        return self.offset >= self.flush_threshold;
    }

    pub fn flush(self: *MessageBatcher, rt: *zio.Runtime, stream: zio.net.Stream) !void {
        if (self.offset > 0) {
            try stream.sendAll(rt, self.buffer[0..self.offset], .{});
            self.offset = 0;
        }
    }
};
```

#### Larger Buffers

Amortize per-operation overhead with larger buffers (benefits diminish after ~64KB):

```zig
// Good: Large buffer reduces syscalls per byte
var buffer: [65536]u8 = undefined;
const n = try stream.recv(rt, &buffer, .{});

// Less efficient: Many small reads
var small_buf: [1024]u8 = undefined;
// Each recv has fixed overhead regardless of size
```

### Understanding ZIO's Architecture

For advanced users, understanding ZIO's internal architecture helps with optimization:

#### Completion Model

ZIO uses a **1:1 SQE:CQE model** - each submitted operation produces exactly one completion. This is important because:

- Every `recv()`/`send()` call submits one io_uring SQE
- The coroutine suspends until the CQE arrives
- No batching of completions at the io_uring level

This means for very high message rates, consider:
- Batching at the application level (message coalescing)
- Using larger buffers to reduce operation count
- Using vectored I/O to send multiple buffers per syscall

#### The ev Layer

ZIO's async operations flow through these layers:

```
High-level API (zio.net.Stream)
       ↓
Runtime (task scheduling)
       ↓
ev/loop.zig (completion management)
       ↓
ev/backends/io_uring.zig (SQE submission)
```

For very advanced use cases, you can access the ev layer directly:

```zig
// Access the underlying loop (advanced)
const loop = rt.executor.loop;

// The loop processes completions via tick()
// Each backend (io_uring, epoll, iocp) implements submit() and poll()
```

#### Cross-Thread Wakeups

ZIO's cross-thread signaling uses io_uring's `FUTEX_WAIT`/`FUTEX_WAKE` operations, which are more efficient than eventfd. This makes `zio.Notify` and cross-thread channel operations very fast.

### Further Reading

- [io_uring and networking in 2023](https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023) - Comprehensive overview of io_uring networking features
- [Zero-copy network transmission with io_uring](https://lwn.net/Articles/879724/) - LWN article on zero-copy send
- [io_uring multishot recv](https://lwn.net/Articles/899498/) - LWN article on multishot operations
- [Efficient zero-copy networking using io_uring](https://kernel-recipes.org/en/2024/schedule/efficient-zero-copy-networking-using-io_uring/) - Kernel Recipes 2024 presentation
- [io_uring zero copy Rx](https://docs.kernel.org/networking/iou-zcrx.html) - Kernel documentation on zero-copy receive

---

## FAQ

### How is this different from other Zig async I/O projects?

ZIO provides:
- Complete cross-platform support (Linux, Windows, macOS)
- Thread pool for blocking operations with coroutine integration
- Advanced synchronization primitives (channels, barriers, etc.)
- Full cancellation support
- `std.Io` interface compatibility

### What about the future std.Io interface?

ZIO implements `std.Io.Reader` and `std.Io.Writer`. When Zig 0.16 is released with the full `std.Io` interface, ZIO will be one implementation of it.

### How do I handle errors in spawned tasks?

Tasks can return error unions. Use `join()` to get the result:

```zig
var handle = try rt.spawn(mayFailTask, .{});
const result = handle.join(rt);  // Returns error union
const value = try result;  // Propagate error or get value
```

### Can I use ZIO with existing blocking code?

Yes, use `spawnBlocking()` for CPU-intensive or blocking operations:

```zig
var handle = try rt.spawnBlocking(blockingOperation, .{ args });
const result = handle.join(rt);
```

### What's the maximum number of concurrent tasks?

Limited primarily by memory (each task needs a stack). The default 256KB stack means ~4000 tasks per GB of RAM. You can reduce stack size for tasks that don't need deep call stacks.

### How do I debug deadlocks?

- Ensure all `lock()` calls have matching `unlock()` (use `defer`)
- Don't wait on yourself (ZIO detects this with a panic)
- Use Groups for structured concurrency instead of manual task management
- Consider using `tryLock()` with timeouts for debugging

---

## Resources

- **GitHub:** https://github.com/lalinsky/zio
- **Example Project:** https://github.com/lalinsky/zio-mini-redis
- **HTTP Library:** https://github.com/lalinsky/dusty

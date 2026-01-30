# ZZMQ: ZeroMQ Semantics on Zig with ZIO

A high-performance messaging library implementing ZeroMQ semantics, built idiomatically on Zig and the ZIO async I/O framework.

## Table of Contents

1. [Design Goals](#design-goals)
2. [Why Zig + ZIO](#why-zig--zio)
3. [ZIO Deep Dive](#zio-deep-dive-actual-apis-for-zzmq)
4. [Architecture Overview](#architecture-overview)
5. [Core Types](#core-types)
6. [Message System](#message-system)
7. [Socket Architecture](#socket-architecture)
8. [Pipe System](#pipe-system)
9. [Select and Multiplexing](#select-and-multiplexing)
10. [Connection Management](#connection-management)
11. [Transport Layer](#transport-layer)
12. [ZMTP Codec](#zmtp-codec)
13. [Socket Patterns](#socket-patterns)
14. [Options and Configuration](#options-and-configuration)
15. [Error Handling](#error-handling)
16. [API Design](#api-design)
17. [C FFI Layer](#c-ffi-layer)
18. [Performance Considerations: Hot Path Deep Dive](#performance-considerations-hot-path-deep-dive)
19. [Monitoring and Events](#monitoring-and-events)
20. [libzmq Reference: Critical Behaviors](#libzmq-reference-critical-behaviors-and-edge-cases)
21. [libzmq Reference: Signaling, Connection, Heartbeat](#libzmq-reference-signaling-connection-readiness-heartbeat-disconnect)
22. [libzmq Reference: Level vs Edge Triggering and Polling](#libzmq-reference-level-vs-edge-triggering-and-polling)
23. [libzmq Deep Dives: Protocol, Patterns, Internals](#libzmq-deep-dives-protocol-patterns-and-internals)
24. [Testing Strategy and Coverage](#testing-strategy-and-coverage)
25. [Implementation Roadmap](#implementation-roadmap)
26. [Summary](#summary)

---

## Design Goals

ZZMQ has three primary goals, in order of priority:

### Goal 1: Idiomatic Zig and ZIO

ZZMQ should feel native to Zig programmers and leverage ZIO's strengths naturally.

| Principle | Implementation |
|-----------|----------------|
| **Explicit allocation** | All allocators passed explicitly; no global state |
| **Error handling** | Zig error unions; no exceptions or panics for recoverable errors |
| **Resource cleanup** | `defer` patterns; `deinit()` methods; no hidden cleanup |
| **Comptime generics** | Socket types parameterized by pattern at compile time |
| **No hidden control flow** | Coroutine suspension is explicit (ZIO runtime calls) |
| **Embrace ZIO primitives** | Use `Channel`, `Group`, `select` directly; don't reinvent |

**Anti-patterns we avoid:**
- Hidden threads or background work
- Implicit allocations
- Global mutable state
- Blocking the OS thread (all blocking is coroutine suspension)
- Fighting ZIO's model with our own synchronization

```zig
// Idiomatic ZZMQ usage
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
defer _ = gpa.deinit();

const rt = try zio.Runtime.init(gpa.allocator(), .{});
defer rt.deinit();

const ctx = try zzmq.Context.init(rt, gpa.allocator(), .{});
defer ctx.deinit();

var socket = try ctx.socket(zzmq.Push);
defer socket.close();

try socket.bind("tcp://127.0.0.1:5555");

var msg = try zzmq.Message.init(gpa.allocator(), "hello");
defer msg.deinit();

try socket.send(&msg);  // May suspend coroutine, never blocks OS thread
```

### Goal 2: Better Than libzmq Performance

ZZMQ should match or exceed libzmq's throughput and latency.

**Performance targets:**

| Metric | libzmq | ZZMQ Target | How We Achieve It |
|--------|--------|-------------|-------------------|
| Inproc latency | ~500ns | <300ns | Direct channel, no ypipe overhead |
| TCP latency (loopback) | ~15μs | <10μs | io_uring, fewer syscalls |
| Small msg throughput | ~4M/s | >6M/s | Inline messages, no allocation |
| Large msg throughput | ~2GB/s | >3GB/s | Zero-copy, vectored I/O |
| PUB fan-out (1:1000) | ~500K/s | >1M/s | Refcounted messages, no copies |
| Memory per connection | ~8KB | <4KB | Simpler state, shared buffers |

**Why we expect better performance:**

1. **No ypipe overhead**: libzmq's lock-free queue requires careful CAS operations and signaling. ZIO's channels use cooperative scheduling - when HWM is hit, the coroutine simply suspends. No atomic operations on the hot path.

2. **Modern I/O**: ZIO uses io_uring on Linux, which batches syscalls and can operate in kernel-polled mode. libzmq uses epoll with one syscall per operation.

3. **Simpler architecture**: libzmq has separate I/O threads, mailboxes, and command queues. We have coroutines on a single runtime - less cross-thread coordination.

4. **Inline small messages**: Messages ≤48 bytes are stored inline in the Message struct. No allocation, no indirection. (libzmq uses ~30 bytes; ZZMQ increases this for better small-message performance.)

5. **No hidden allocations**: Every allocation is explicit and can use custom allocators. libzmq allocates internally with global malloc.

**Performance-critical paths:**

```
HOT PATH - Send (called millions of times/sec):
  socket.send(msg)
    → pattern.selectPipe()      // O(1) round-robin
    → pipe.outbound.send(msg)   // zio.Channel.send - may suspend

HOT PATH - Recv (called millions of times/sec):
  socket.recv()
    → pattern.selectPipe()      // O(1) fair-queue
    → pipe.inbound.receive()    // zio.Channel.receive - may suspend

WARM PATH - Engine writer (per-message, but async):
  channel.receive()             // Wake on data
    → codec.encode(msg)         // In-place, no copy
    → stream.send(buf)          // ZIO async send

WARM PATH - Engine reader (per-message, but async):
  stream.recv(buf)              // ZIO async recv
    → codec.decode(buf)         // Parse in place
    → channel.send(msg)         // May suspend on HWM
```

### Goal 3: Match libzmq Semantics and Guarantees

ZZMQ should be a drop-in conceptual replacement for libzmq with identical behavior.

**Semantics we preserve:**

| Feature | libzmq Behavior | ZZMQ Behavior |
|---------|-----------------|---------------|
| **HWM (high water mark)** | Block or drop when queue full | Block (via coroutine suspend) or drop |
| **Linger** | Wait for pending messages on close | Same - configurable timeout |
| **Reconnection** | Automatic with exponential backoff | Same - IVL and IVL_MAX options |
| **Heartbeat** | PING/PONG with configurable interval | Same - IVL, TIMEOUT, TTL options |
| **Message atomicity** | Multipart delivered atomically | Same - all-or-nothing delivery |
| **Fair queuing** | Round-robin across peers | Same algorithm |
| **Subscriptions** | Prefix-based filtering | Same - trie-based matching |
| **Routing IDs** | Assigned or explicit identity | Same - for ROUTER sockets |
| **Conflate** | Keep only last message | Same - per-pipe option |

**Socket options we support:**

```zig
// All standard ZMQ options
pub const SocketOption = enum {
    // Flow control
    send_hwm,           // ZMQ_SNDHWM
    recv_hwm,           // ZMQ_RCVHWM

    // Timeouts
    send_timeout,       // ZMQ_SNDTIMEO
    recv_timeout,       // ZMQ_RCVTIMEO

    // Connection
    linger,             // ZMQ_LINGER
    reconnect_ivl,      // ZMQ_RECONNECT_IVL
    reconnect_ivl_max,  // ZMQ_RECONNECT_IVL_MAX
    connect_timeout,    // ZMQ_CONNECT_TIMEOUT

    // Identity
    routing_id,         // ZMQ_ROUTING_ID

    // Heartbeat
    heartbeat_ivl,      // ZMQ_HEARTBEAT_IVL
    heartbeat_timeout,  // ZMQ_HEARTBEAT_TIMEOUT
    heartbeat_ttl,      // ZMQ_HEARTBEAT_TTL

    // Pattern-specific
    subscribe,          // ZMQ_SUBSCRIBE
    unsubscribe,        // ZMQ_UNSUBSCRIBE
    req_relaxed,        // ZMQ_REQ_RELAXED
    req_correlate,      // ZMQ_REQ_CORRELATE
    router_mandatory,   // ZMQ_ROUTER_MANDATORY

    // Behavior
    conflate,           // ZMQ_CONFLATE
    immediate,          // ZMQ_IMMEDIATE

    // TCP
    tcp_keepalive,      // ZMQ_TCP_KEEPALIVE
    // ...
};
```

**Wire compatibility:**

- ZMTP 3.1 protocol for interoperability with libzmq
- Can communicate with libzmq peers over TCP
- Same framing, commands, and handshake

**Guarantees we maintain:**

1. **Message ordering**: Messages from A to B arrive in send order
2. **No message loss** (within HWM): If send succeeds, message will be delivered or linger timeout
3. **Atomic multipart**: All frames of a multipart message delivered together or not at all
4. **Backpressure propagation**: HWM on any socket eventually slows the sender
5. **Graceful degradation**: Peer disconnect doesn't crash; reconnection is automatic

---

## Why Zig + ZIO

### Why Zig?

| Feature | Benefit for ZZMQ |
|---------|------------------|
| **Comptime** | Zero-cost pattern abstractions; inline small messages |
| **No hidden allocations** | Predictable performance; custom allocators |
| **Explicit error handling** | No surprise panics in library code |
| **C interop** | Easy to expose C API for other languages |
| **No runtime** | Minimal footprint; embeddable |
| **Safety without GC** | Memory safety via conventions; no pause times |

### Why ZIO?

| Feature | Benefit for ZZMQ |
|---------|------------------|
| **Stackful coroutines** | Natural blocking-style API that's actually async |
| **io_uring support** | Best-in-class Linux I/O performance |
| **Cross-platform** | Same code works on Linux, macOS, Windows, BSDs |
| **Bounded channels** | Direct mapping to HWM; built-in backpressure |
| **Structured concurrency** | Clean connection lifecycle management |
| **Select** | Multiplexing without callbacks or state machines |

### Key Insight: ZIO Channels Are Our Pipes

libzmq's architecture centers on `ypipe_t`, a carefully crafted lock-free queue with:
- Single-CAS synchronization
- "Reader sleeping" detection via cursor==NULL
- Chunked memory allocation
- Separate signaler for cross-thread wakeup

This was brilliant engineering for 2010. But ZIO gives us something better:

```zig
// libzmq: complex lock-free queue + signaler
ypipe_t<msg_t> pipe;
signaler_t signaler;
// ... hundreds of lines of careful atomic code ...

// ZZMQ: just use ZIO's channel
outbound: zio.Channel(Message),  // capacity = HWM
inbound: zio.Channel(Message),   // capacity = HWM
```

When HWM is reached:
- libzmq: CAS fails, signaler notified, thread wakes, checks queue...
- ZZMQ: `channel.send()` suspends coroutine. That's it.

The ZIO runtime handles all the complexity of efficient wakeup. We don't need to think about it.

---

## ZIO Deep Dive: Actual APIs for ZZMQ

This section documents the actual ZIO APIs we'll use, based on source code analysis of the ZIO library.

### Channel API (`zio.sync.Channel`)

ZIO channels are bounded FIFO queues with coroutine-aware blocking.

```zig
const Channel = @import("zio").sync.Channel;

// Create channel with buffer (capacity = buffer.len = HWM)
var buffer: [1000]Message = undefined;
var channel = Channel(Message).init(&buffer);

// Unbuffered channel (synchronous rendezvous)
var unbuffered = Channel(Message).init(&.{});

// Blocking operations (suspend coroutine if needed)
try channel.send(rt, msg);           // Block if full
const msg = try channel.receive(rt); // Block if empty

// Non-blocking operations (return immediately)
channel.trySend(msg) catch |err| switch (err) {
    error.ChannelFull => {},   // Would block
    error.ChannelClosed => {}, // Channel closed
};

const msg = channel.tryReceive() catch |err| switch (err) {
    error.ChannelEmpty => {},  // Would block
    error.ChannelClosed => {}, // Channel closed
};

// Check state (not atomic with operations!)
if (channel.isEmpty()) { ... }
if (channel.isFull()) { ... }

// Close channel
channel.close(.graceful);   // Allow draining buffered items
channel.close(.immediate);  // Clear buffer, receivers get ChannelClosed
```

**Key Insight**: Channel uses `std.Thread.Mutex` internally, not lock-free. For ZZMQ, this is fine because:
1. Each pipe is SPSC (single socket writes, single engine reads)
2. Contention is rare (only when HWM triggers backpressure)
3. The coroutine suspend/resume is the dominant cost, not the mutex

### Select API (`zio.select`)

Wait on multiple operations simultaneously (like Go's select).

```zig
const select = @import("zio").select;
const Timeout = @import("zio").time.Timeout;

// Create async operations for select
var recv1 = channel1.asyncReceive();
var recv2 = channel2.asyncReceive();
var send_op = channel3.asyncSend(msg);

// Select returns tagged union with winner's result
const result = try select(rt, .{
    .ch1 = &recv1,
    .ch2 = &recv2,
    .send = &send_op,
    .timeout = Timeout{ .duration = Duration.fromMilliseconds(100) },
});

switch (result) {
    .ch1 => |val| {
        const msg = try val;  // val is error union!
        // Handle message from ch1
    },
    .ch2 => |val| {
        const msg = try val;
        // Handle message from ch2
    },
    .send => |res| {
        try res;  // Check if send succeeded
    },
    .timeout => {
        // Timeout expired, no message received
    },
}
```

**Key Pattern for ZZMQ Fair Queue**:
```zig
fn selectFromPipes(pipes: []Pipe, rt: *Runtime, timeout: ?Duration) !Message {
    // Build async receivers for all pipes
    var receivers: [MAX_PIPES]Channel(Message).AsyncReceive = undefined;
    for (pipes, 0..) |pipe, i| {
        receivers[i] = pipe.inbound.asyncReceive();
    }

    // Use selectAwaitables for runtime-sized array
    const awaitables = buildAwaitables(receivers[0..pipes.len]);
    const winner_idx = try zio.selectAwaitables(rt, awaitables);

    return receivers[winner_idx].getResult();
}
```

### Time and Timeout API

```zig
const time = @import("zio").time;

// Duration (stored as nanoseconds)
const d1 = time.Duration.fromMilliseconds(100);
const d2 = time.Duration.fromSeconds(5);
const d3 = time.Duration.fromNanoseconds(1000);

// Sleep current coroutine
try rt.sleep(time.Duration.fromMilliseconds(100));

// Timeout as a future (for select)
const timeout = time.Timeout{ .duration = time.Duration.fromMilliseconds(100) };
const no_timeout = time.Timeout.none;  // Wait forever

// Timestamp (point in time) - use runtime for monotonic time
const now = rt.now();  // Returns time.Instant
const deadline = now.addDuration(d1);

// Stopwatch for measuring elapsed time
var stopwatch = time.Stopwatch.start();
// ... do work ...
const elapsed = stopwatch.read();
```

### Network API

ZIO uses an address-centric API where addresses have methods for connection/listening:

```zig
const zio = @import("zio");

// TCP Client - address.connect() returns a stream
const addr = try zio.IpAddress.parse("127.0.0.1", 5555);
const stream = try addr.connect(rt, .{});
defer stream.close();

// Stream I/O - send/recv (not write/read)
const bytes_sent = try stream.send(rt, data, .{});
try stream.sendAll(rt, data, .{});  // Send all bytes
const bytes_received = try stream.recv(rt, &buffer, .{});

// TCP Server - address.listen() returns an Acceptor
const bind_addr = try zio.IpAddress.parse("0.0.0.0", 5555);
var acceptor = try bind_addr.listen(rt, .{ .backlog = 128 });
defer acceptor.close();

while (true) {
    const client_stream = try acceptor.accept(rt);
    try group.spawn(rt, handleClient, .{ rt, client_stream });
}

// Address types
const ipv4_addr = try zio.IpAddress.parse("127.0.0.1", 5555);
const ipv6_addr = try zio.IpAddress.parse("::1", 5555);
```

### Vectored I/O

ZIO supports scatter/gather I/O for efficient multipart message handling:

```zig
// Vectored send - multiple buffers in single syscall
const slices: []const []const u8 = &.{
    header_bytes,
    body_bytes,
    trailer_bytes,
};
const bytes_sent = try stream.sendVec(rt, slices, .{});
try stream.sendAllVec(rt, slices, .{});  // Send all

// Vectored receive
var buffers: [3][]u8 = .{
    &header_buf,
    &body_buf,
    &trailer_buf,
};
const bytes_received = try stream.recvVec(rt, &buffers, .{});
```

This is critical for ZZMQ multipart messages - we can send all frames with minimal syscalls.

### Runtime and Coroutine Management

```zig
const Runtime = @import("zio").Runtime;
const Group = @import("zio").runtime.Group;

// Initialize runtime
const rt = try Runtime.init(allocator, .{});
defer rt.deinit();

// Spawn a coroutine
var handle = try rt.spawn(myFunction, .{ arg1, arg2 });

// Wait for result
const result = try handle.join(rt);

// Cancel a coroutine
handle.cancel(rt);

// Yield to other coroutines
try rt.yield();

// Group: manage multiple coroutines
var group: Group = .init;
defer group.cancel(rt);

try group.spawn(rt, task1, .{});
try group.spawn(rt, task2, .{});
try group.spawn(rt, task3, .{});

// Wait for all to complete
try group.wait(rt);

if (group.hasFailed()) {
    // At least one task failed
}
```

### io_uring Integration

ZIO uses io_uring on Linux with optimized flags:
- `IORING_SETUP_SINGLE_ISSUER` - Single thread submits (our model)
- `IORING_SETUP_DEFER_TASKRUN` - Defer completion processing
- `IORING_SETUP_COOP_TASKRUN` - Cooperative task running

**Implications for ZZMQ**:
1. Syscalls are automatically batched by io_uring
2. Multiple network operations can be submitted in one syscall
3. Falls back to epoll on older kernels, kqueue on macOS/BSD

### Notify: One-Shot Signaling

ZIO provides `Notify` for efficient one-shot notifications between coroutines:

```zig
const zio = @import("zio");

// Create a notify (typically stored in a struct)
var notify: zio.Notify = .{};

// Sender side: wake up exactly one waiter
notify.set();

// Receiver side: wait until signaled
try notify.wait(rt);

// Can also use in select for non-blocking check
var notify_op = notify.asyncWait();
const result = try zio.select(rt, .{
    .notify = &notify_op,
    .timeout = Timeout{ .duration = Duration.fromMilliseconds(100) },
});
```

**ZZMQ uses Notify for:**
1. **Engine wakeup**: When user adds message after engine was idle
2. **Shutdown signaling**: Context tells sockets to start closing
3. **Linger completion**: Signal when pending messages are flushed
4. **Reconnect trigger**: Session tells connector to initiate reconnect

```zig
pub const Pipe = struct {
    outbound: zio.Channel(Message),
    inbound: zio.Channel(Message),
    engine_wakeup: zio.Notify = .{},  // Wake engine when user sends after idle

    pub fn send(self: *Pipe, rt: *zio.Runtime, msg: Message) !void {
        try self.outbound.send(rt, msg);
        self.engine_wakeup.set();  // Signal engine in case it was waiting
    }
};
```

### Cancellation and Shielding

ZIO supports cooperative cancellation with shielding for critical sections:

```zig
// Cancel a group (signals all coroutines)
group.cancel(rt);

// Inside a coroutine, check for cancellation
if (rt.isCancelled()) {
    return error.Cancelled;
}

// Shield a critical section from cancellation
// (useful for linger: must complete pending sends)
rt.beginShield();
defer rt.endShield();

// This code runs even if cancellation was requested
for (pending_messages) |msg| {
    try self.stream.sendAll(rt, msg.data(), .{});
}
```

**ZZMQ linger with shielding:**
```zig
pub fn closeWithLinger(self: *Socket, rt: *zio.Runtime, linger_ms: i32) void {
    if (linger_ms == 0) {
        // Drop immediately
        self.dropAllPending();
        return;
    }

    // Shield from cancellation - we need to try sending pending messages
    rt.beginShield();
    defer rt.endShield();

    if (linger_ms < 0) {
        // Infinite linger - wait until all sent
        self.flushAllPending(rt) catch {};
    } else {
        // Timed linger - use deadline
        const deadline = rt.now().addDuration(
            Duration.fromMilliseconds(@intCast(linger_ms))
        );
        self.flushWithDeadline(rt, deadline) catch {
            // Timeout - drop remaining
            self.dropAllPending();
        };
    }
}
```

### ZZMQ-Specific Usage Patterns

**Pattern 1: Engine Reader/Writer Pair**
```zig
pub fn runEngine(self: *Engine, rt: *Runtime) void {
    var group: Group = .init;
    defer group.cancel(rt);

    try group.spawn(rt, readerLoop, .{ self, rt });
    try group.spawn(rt, writerLoop, .{ self, rt });

    group.wait(rt) catch {};
}
```

**Pattern 2: Socket Send with Timeout**
```zig
pub fn sendWithTimeout(self: *Socket, rt: *Runtime, msg: Message, timeout_ms: u32) !void {
    const pipe = self.selectPipeForSend();

    var send_op = pipe.outbound.asyncSend(msg);
    const result = try select(rt, .{
        .send = &send_op,
        .timeout = Timeout{ .duration = Duration.fromMilliseconds(timeout_ms) },
    });

    switch (result) {
        .send => |res| try res,
        .timeout => return error.Timeout,
    }
}
```

**Pattern 3: Fair Queue Receive**
```zig
pub fn recvFairQueue(self: *FairQueue, rt: *Runtime) !Message {
    // Try non-blocking first (fast path)
    while (self.active > 0) {
        const pipe = self.pipes.items[self.current];
        if (pipe.inbound.tryReceive()) |msg| {
            self.current = (self.current + 1) % self.active;
            return msg;
        }
        self.deactivatePipe(self.current);
    }

    // All empty - must wait (slow path)
    // Re-activate all pipes and use select
    self.reactivateAll();
    return self.selectFromAllPipes(rt);
}
```

**Pattern 4: Heartbeat Timer Loop**
```zig
fn heartbeatLoop(self: *Engine, rt: *Runtime) !void {
    while (!self.terminated) {
        try rt.sleep(Duration.fromMilliseconds(self.heartbeat_interval));

        const now = rt.now();  // Runtime provides monotonic time
        const since_recv = self.last_recv.durationTo(now);

        if (since_recv.toMilliseconds() > self.heartbeat_timeout) {
            return error.HeartbeatTimeout;
        }

        if (since_recv.toMilliseconds() > self.heartbeat_interval) {
            try self.sendPing();
        }
    }
}
```

### Design Corrections from ZIO Analysis

Based on ZIO source analysis, here are corrections to earlier assumptions:

| Original Assumption | Actual ZIO Behavior |
|---------------------|---------------------|
| Lock-free channels | Mutex-based (fine for SPSC) |
| `zio.time.Timer` type | Use `Timeout` as select future |
| `zio.select(&channels, timeout)` | `select(rt, .{ .name = future })` struct-based |
| Implicit coroutine switching | Explicit `rt` parameter everywhere |
| `channel.send(msg)` | `channel.send(rt, msg)` - needs runtime |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Code                                       │
│                                                                              │
│   const msg = try socket.recv();                                            │
│   try socket.send(response);                                                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               Socket Layer                                   │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        Socket(PatternType)                             │  │
│  │                                                                        │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │  │
│  │  │   Options   │  │   Pattern   │  │    Pipes    │                   │  │
│  │  │  (hwm,etc)  │  │   State     │  │  (Channel)  │                   │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘                   │  │
│  │                                           │                           │  │
│  │  send()/recv() ──► Pattern logic ──► select across pipes             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                Pipe Layer                                    │
│                                                                              │
│    ┌─────────────────────────────────────────────────────────────────┐     │
│    │                          Pipe                                    │     │
│    │                                                                  │     │
│    │   ┌─────────────────┐              ┌─────────────────┐         │     │
│    │   │ outbound_channel│   ◄────────  │ inbound_channel │         │     │
│    │   │ (socket→engine) │              │ (engine→socket) │         │     │
│    │   │ zio.Channel(Msg)│              │ zio.Channel(Msg)│         │     │
│    │   │ capacity = HWM  │              │ capacity = HWM  │         │     │
│    │   └────────┬────────┘              └────────▲────────┘         │     │
│    │            │                                │                   │     │
│    └────────────┼────────────────────────────────┼───────────────────┘     │
│                 │                                │                          │
└─────────────────┼────────────────────────────────┼──────────────────────────┘
                  │                                │
                  ▼                                │
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Connection Layer                                  │
│                                                                              │
│    ┌─────────────────────────────────────────────────────────────────┐     │
│    │                    Engine (coroutine)                            │     │
│    │                                                                  │     │
│    │   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐        │     │
│    │   │    Codec    │    │   Stream    │    │   State     │        │     │
│    │   │   (ZMTP)    │◄──►│ (zio.net)   │    │  Machine    │        │     │
│    │   └─────────────┘    └─────────────┘    └─────────────┘        │     │
│    │                                                                  │     │
│    │   Loop: read from outbound_channel → encode → write to network  │     │
│    │         read from network → decode → write to inbound_channel   │     │
│    └─────────────────────────────────────────────────────────────────┘     │
│                                                                              │
│    ┌──────────────────────┐    ┌──────────────────────┐                    │
│    │  Listener (coroutine)│    │ Connector (coroutine)│                    │
│    │  accept → spawn      │    │ connect → reconnect  │                    │
│    │  engine              │    │ with backoff         │                    │
│    └──────────────────────┘    └──────────────────────┘                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ZIO Runtime                                         │
│                                                                              │
│    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│    │  Coroutine  │  │  Coroutine  │  │  Coroutine  │  │     ...     │     │
│    │  Scheduler  │  │   Stacks    │  │   Channels  │  │             │     │
│    └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘     │
│                                                                              │
│    ┌─────────────────────────────────────────────────────────────────┐     │
│    │              Event Backend (io_uring/epoll/kqueue)               │     │
│    └─────────────────────────────────────────────────────────────────┘     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Threading Model and Safety

### ZIO's Single-Threaded Runtime Model

ZIO uses a single-threaded runtime model: one OS thread runs one `zio.Runtime`, which
manages many coroutines cooperatively. This is similar to Node.js's event loop or
Tokio's single-threaded runtime mode.

**Key implications for ZZMQ:**

1. **No locks within a runtime** - All coroutines in one runtime share memory safely
2. **Sockets bound to their runtime** - A socket uses its context's runtime
3. **Cross-runtime communication via channels** - Thread-safe channel for multi-runtime

### ZZMQ Threading Rules

| Resource | Thread Safety | Notes |
|----------|--------------|-------|
| **Context** | Create/destroy on one thread | `init()`/`deinit()` not thread-safe |
| **Socket** | Single runtime only | Cannot be passed between threads |
| **Message** | Move semantics | Transfer ownership, don't share |
| **Context.runtime** | One thread owns it | Run event loop on one thread |

### Multi-Core Scaling Pattern

For multi-core scaling, use multiple contexts (each with own runtime) and connect via
TCP or inproc. This matches libzmq's recommended pattern.

```zig
// Main thread: coordinator
var main_ctx = try zzmq.Context.init(.{});
var frontend = try main_ctx.socket(.router);
try frontend.bind("tcp://*:5555");

// Worker threads: each with own context
const workers = try std.Thread.spawn(.{}, workerThread, .{});

fn workerThread() void {
    var ctx = try zzmq.Context.init(.{});
    defer ctx.deinit();

    var worker = try ctx.socket(.dealer);
    try worker.connect("tcp://127.0.0.1:5555");

    // Process messages...
}
```

### Inproc Transport Threading

Inproc (`inproc://`) can connect sockets within the same context. Since all sockets
in a context share the same runtime (single-threaded), inproc is always safe:

```zig
var ctx = try zzmq.Context.init(.{});

var sender = try ctx.socket(.push);
try sender.bind("inproc://workers");

var receiver = try ctx.socket(.pull);
try receiver.connect("inproc://workers");

// Both sockets in same context, same runtime - safe
// Data flows through ZIO channels, no thread boundary
```

### Cross-Runtime Channel (Advanced)

For users who need multiple runtimes in one process communicating without network,
we could provide a thread-safe channel wrapper:

```zig
pub const CrossRuntimePipe = struct {
    /// Thread-safe channel (uses mutex internally)
    channel: ThreadSafeChannel(Message),

    /// Notify for waking up receiving runtime
    recv_notify: std.Thread.ResetEvent = .{},

    pub fn send(self: *CrossRuntimePipe, msg: Message) void {
        self.channel.send(msg);
        self.recv_notify.set();
    }

    pub fn recv(self: *CrossRuntimePipe, rt: *zio.Runtime) !Message {
        // TODO: Integrate with ZIO's event loop via fd/eventfd
        // For now, this is a placeholder design
    }
};
```

**Note:** This is out of scope for initial implementation. Use TCP between runtimes.

### Comparison with libzmq

| Aspect | libzmq | ZZMQ |
|--------|--------|------|
| Context | Thread-safe | Single-thread create/destroy |
| Sockets | NOT thread-safe | Same (single runtime) |
| Inproc | Lock-free pipes | ZIO channels (no locks) |
| Multi-core | I/O threads + worker threads | Multiple contexts |
| Cross-thread send | Mailbox + signaler | Not supported (use TCP) |

**Design decision:** ZZMQ simplifies by embracing ZIO's single-threaded model fully.
This eliminates lock contention and signaling complexity. Multi-core scaling uses the
same pattern libzmq recommends: multiple processes/contexts connected via network.

---

## Scope and Feature Decisions

### In Scope (MVP)

| Feature | Priority | Notes |
|---------|----------|-------|
| **TCP transport** | P0 | Primary transport |
| **IPC transport** | P0 | Unix domain sockets |
| **Inproc transport** | P0 | Same-context fast path |
| **Core patterns** | P0 | PUSH/PULL, PUB/SUB, REQ/REP, DEALER/ROUTER, PAIR |
| **XPUB/XSUB** | P1 | For proxy pattern |
| **Proxy** | P1 | Frontend/backend bridging |
| **HWM/backpressure** | P0 | Strict libzmq semantics |
| **Multipart messages** | P0 | Atomic message groups |
| **ZMTP 3.1** | P0 | Wire protocol |
| **NULL security** | P0 | No authentication |
| **Heartbeat** | P1 | PING/PONG keepalive |
| **Reconnection** | P0 | With exponential backoff |
| **C FFI** | P1 | libzmq-compatible C API |
| **Socket monitoring** | P2 | BroadcastChannel-based |

### In Scope (Post-MVP)

| Feature | Priority | Notes |
|---------|----------|-------|
| **PLAIN security** | P2 | Username/password |
| **CURVE security** | P3 | Encrypted, authenticated |
| **ZAP authentication** | P3 | External authenticator |
| **Message metadata** | P2 | ZMTP 3.1 properties |
| **Socket options** | P1+ | Add as needed |

### Out of Scope

| Feature | Reason |
|---------|--------|
| **PGM/EPGM multicast** | Complex, rarely used, requires kernel support |
| **GSSAPI security** | Complex, rarely used outside enterprise |
| **NORM transport** | Obscure, no demand |
| **VMCI transport** | VMware-specific |
| **Draft sockets** (SERVER/CLIENT, RADIO/DISH, etc.) | Unstable API, add later if needed |
| **SOCKS proxy** | Can add later if demanded |
| **WebSocket transport** | Can add later; different use case |
| **Thread-safe sockets** | Complexity; use multiple contexts instead |

### Explicit Non-Goals

1. **100% libzmq compatibility** - We match semantics, not every option/edge case
2. **Drop-in binary replacement** - Different ABI, but C FFI provides compatibility layer
3. **Backward compatibility during development** - Will break APIs until 1.0
4. **Windows support initially** - Linux first, then macOS, then Windows

---

## Core Types

### Context

The context owns shared resources and provides the ZIO runtime for all sockets.

**Key design decision**: Users provide an allocator to the context. This follows Zig idioms and enables:
- Custom allocators for performance (arena, pool)
- Testing with failing allocators
- Memory tracking and debugging
- Embedded systems with fixed buffers

```zig
pub const Context = struct {
    /// The ZIO runtime - ALL I/O goes through this
    runtime: *zio.Runtime,

    /// Allocator for ALL dynamic allocations (messages, buffers, subscriptions)
    /// User-provided, enabling custom memory strategies
    allocator: std.mem.Allocator,

    /// Context-wide options
    options: ContextOptions,

    /// Inproc endpoint registry
    inproc_endpoints: InprocRegistry,

    /// All sockets in this context (for shutdown coordination)
    sockets: SocketList,

    /// State
    state: State,

    const State = enum {
        active,
        shutting_down,
        terminated,
    };

    /// Initialize context with user-provided allocator
    /// All ZZMQ allocations for this context use this allocator
    pub fn init(options: ContextOptions) !*Context {
        const allocator = options.allocator orelse std.heap.page_allocator;
        const runtime = options.runtime orelse try zio.Runtime.init(allocator);

        const self = try allocator.create(Context);
        self.* = .{
            .runtime = runtime,
            .allocator = allocator,
            .options = options,
            .inproc_endpoints = InprocRegistry.init(allocator),
            .sockets = SocketList.init(allocator),
            .state = .active,
        };
        return self;
    }

    pub fn deinit(self: *Context) void {
        self.inproc_endpoints.deinit();
        self.sockets.deinit();
        if (self.options.owns_runtime) {
            self.runtime.deinit();
        }
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    /// Create a socket of the specified type
    pub fn socket(self: *Context, comptime Pattern: type) !*Socket(Pattern);

    /// Shutdown: stop accepting new operations, start draining
    pub fn shutdown(self: *Context) void;

    /// Terminate: block until all sockets closed and drained
    pub fn terminate(self: *Context) void;
};

pub const ContextOptions = struct {
    /// User-provided allocator (null = use page_allocator)
    allocator: ?std.mem.Allocator = null,

    /// User-provided ZIO runtime (null = create internal runtime)
    runtime: ?*zio.Runtime = null,

    /// Maximum concurrent sockets
    max_sockets: u32 = 1024,

    /// Maximum message size (0 = no limit)
    max_message_size: usize = 0,

    /// I/O thread hint (ZIO manages actual threading)
    io_threads: u32 = 1,

    /// Internal tracking
    owns_runtime: bool = false,
};
```

**Allocator Usage Throughout ZZMQ**:

| Component | Allocations |
|-----------|------------|
| Message (>48 bytes) | Payload buffer + refcount |
| Pipe | Channel backing arrays |
| Socket | Pattern state, pipe list |
| Subscriptions | Trie nodes, prefix copies |
| Engine | Codec buffers, connection state |

**Example: Custom Allocator for High-Throughput**:

```zig
// Use arena allocator that resets periodically
var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
defer arena.deinit();

var ctx = try zzmq.Context.init(.{
    .allocator = arena.allocator(),
});
defer ctx.deinit();

// All ZZMQ allocations use the arena
var socket = try ctx.socket(.push);
```

**Example: Testing with Failing Allocator**:

```zig
var failing = std.testing.FailingAllocator.init(std.heap.page_allocator, .{
    .fail_index = 100,  // Fail after 100 allocations
});

var ctx = zzmq.Context.init(.{
    .allocator = failing.allocator(),
}) catch |err| {
    // Verify graceful handling of OOM
    try std.testing.expectEqual(error.OutOfMemory, err);
    return;
};
```

### Buffer Strategy

Following Zig idioms: explicit allocation, arenas for batch operations, no hidden pooling.

**Design Principles:**
1. **User controls memory** - All allocations flow through context allocator
2. **No hidden pools** - If user wants pooling, they provide a pooling allocator
3. **Arenas for batch operations** - Multipart receive uses arena for clean cleanup
4. **Predictable per-connection memory** - Fixed buffers in Engine, known sizes

**Buffer Categories and Strategy:**

| Buffer Type | Size | Strategy | Rationale |
|-------------|------|----------|-----------|
| Engine read buffer | 64KB | Allocated from `ctx.allocator` at init | Simple, user controls source |
| Engine write buffer | 64KB | Allocated from `ctx.allocator` at init | Simple, user controls source |
| Message data (≤48B) | Inline | No allocation (VSM) | Fast path optimization |
| Message data (>48B) | Variable | `ctx.allocator` | User controls, can use pool |
| Multipart recv | Variable | Arena per multipart | Batch free on `deinit()` |
| Channel backing | HWM × sizeof(Message) | `ctx.allocator` | Sized at pipe creation |

**Engine Buffer Allocation:**

```zig
pub const Engine = struct {
    /// Read buffer - allocated from context allocator
    read_buf: []u8,

    /// Write buffer - allocated from context allocator
    write_buf: []u8,

    /// Context reference (for allocator access)
    ctx: *Context,

    // ... other fields ...

    pub fn init(ctx: *Context, stream: zio.net.Stream, pipe: *Pipe) !Engine {
        const allocator = ctx.allocator;

        return .{
            .read_buf = try allocator.alloc(u8, ctx.options.engine_buffer_size),
            .write_buf = try allocator.alloc(u8, ctx.options.engine_buffer_size),
            .ctx = ctx,
            .stream = stream,
            .pipe = pipe,
            // ...
        };
    }

    pub fn deinit(self: *Engine) void {
        const allocator = self.ctx.allocator;
        allocator.free(self.read_buf);
        allocator.free(self.write_buf);
        // ...
    }
};
```

**Message Allocation (send side - user controls):**

```zig
// User creates messages - they choose allocation strategy
const msg = try zzmq.Message.init(ctx.allocator, data);
defer msg.deinit();
try socket.send(rt, msg);

// Or with external buffer (zero-copy)
const msg = zzmq.Message.initExternal(user_buffer, freeCallback, hint);
try socket.send(rt, msg);

// Or inline for small messages (no allocation)
const msg = zzmq.Message.initInline("hello");
try socket.send(rt, msg);
```

**Message Allocation (recv side - context allocator):**

```zig
pub fn recv(self: *Socket, rt: *zio.Runtime) !Message {
    // Single message uses context allocator
    // Caller responsible for calling msg.deinit()
    return self.recvWithAllocator(rt, self.ctx.allocator);
}
```

**Multipart Receive with Arena:**

```zig
/// Receive a complete multipart message
/// All frames allocated from internal arena - freed together on deinit()
pub fn recvMultipart(self: *Socket, rt: *zio.Runtime) !MultipartMessage {
    // Arena backed by context allocator
    var arena = std.heap.ArenaAllocator.init(self.ctx.allocator);
    errdefer arena.deinit();

    var frames = std.ArrayList([]const u8).init(arena.allocator());

    while (true) {
        const msg = try self.pipe.inbound.receive(rt);
        defer msg.deinit();  // Original message cleaned up

        // Copy data into arena
        const frame_data = try arena.allocator().dupe(u8, msg.data());
        try frames.append(frame_data);

        if (!msg.flags.more) break;
    }

    return .{
        .frames = try frames.toOwnedSlice(),
        .arena = arena,
    };
}

pub const MultipartMessage = struct {
    frames: []const []const u8,
    arena: std.heap.ArenaAllocator,

    /// Free all frames with single arena deinit
    pub fn deinit(self: *MultipartMessage) void {
        self.arena.deinit();
    }

    pub fn frameCount(self: MultipartMessage) usize {
        return self.frames.len;
    }

    pub fn frame(self: MultipartMessage, index: usize) []const u8 {
        return self.frames[index];
    }

    /// Iterator over frames
    pub fn iterator(self: *const MultipartMessage) FrameIterator {
        return .{ .frames = self.frames, .index = 0 };
    }
};
```

**User-Provided Pooling (if needed):**

Users who need pooling provide their own allocator:

```zig
// User creates a pooling allocator
var pool = MyMessagePool.init(std.heap.page_allocator, .{
    .size_classes = &.{ 64, 256, 1024, 4096, 65536 },
    .slabs_per_class = 16,
});
defer pool.deinit();

// Pass pool's allocator interface to context
var ctx = try zzmq.Context.init(.{
    .allocator = pool.allocator(),
});

// All ZZMQ allocations now go through the pool
```

**ContextOptions for Buffer Tuning:**

```zig
pub const ContextOptions = struct {
    /// User-provided allocator (null = use page_allocator)
    allocator: ?std.mem.Allocator = null,

    /// Engine read/write buffer size (default 64KB)
    engine_buffer_size: usize = 65536,

    /// Maximum message size (0 = no limit, enforced on recv)
    max_message_size: usize = 0,

    // ... other options ...
};
```

**Why No Built-in Buffer Pool:**

1. **Zig idiom**: "If you want pooling, bring a pooling allocator"
2. **Simplicity**: Less code to maintain, fewer edge cases
3. **Flexibility**: User can choose pool strategy (slab, arena, fixed)
4. **Predictability**: No hidden caching behavior
5. **Testing**: Easy to test with std.testing.FailingAllocator

**io_uring Buffer Registration:**

Deferred - adds complexity with unclear benefit:
- Requires fixed buffer addresses (can't resize)
- Needs ZIO-specific hooks
- User can achieve similar with FixedBufferAllocator
- Revisit only if profiling shows syscall overhead is bottleneck

### Socket Handle

Opaque handle to a socket, parameterized by pattern type.

```zig
pub fn Socket(comptime Pattern: type) type {
    return struct {
        const Self = @This();

        /// Internal state
        inner: *SocketInner(Pattern),

        // === Core API ===

        pub fn send(self: *Self, msg: *Message) SendError!void;
        pub fn sendWithTimeout(self: *Self, msg: *Message, timeout: Timeout) SendError!void;
        pub fn trySend(self: *Self, msg: *Message) SendError!bool;

        pub fn recv(self: *Self) RecvError!Message;
        pub fn recvWithTimeout(self: *Self, timeout: Timeout) RecvError!Message;
        pub fn tryRecv(self: *Self) RecvError!?Message;

        // === Endpoint management ===

        pub fn bind(self: *Self, endpoint: []const u8) !void;
        pub fn connect(self: *Self, endpoint: []const u8) !void;
        pub fn unbind(self: *Self, endpoint: []const u8) !void;
        pub fn disconnect(self: *Self, endpoint: []const u8) !void;

        // === Options ===

        pub fn setOption(self: *Self, comptime opt: SocketOption, value: opt.Type) !void;
        pub fn getOption(self: *Self, comptime opt: SocketOption) opt.Type;

        // === Lifecycle ===

        pub fn close(self: *Self) void;
    };
}
```

---

## Message System

Messages are the unit of data transfer. Design goals:
- Small message optimization (inline storage)
- Zero-copy for large messages
- Reference counting for fan-out
- Multipart message support

### Message Structure

```zig
/// Maximum inline data size (avoid allocation for small messages)
/// libzmq's max_vsm_size is ~30 bytes (platform-dependent). ZZMQ uses 48 bytes
/// because: (1) many real-world messages are 32-64 bytes, (2) the Message struct
/// is 64 bytes anyway for cache alignment, (3) larger inline = fewer allocations.
const INLINE_SIZE = 48;

/// A single message (may be part of multipart)
pub const Message = struct {
    /// Message data storage
    data: Data,

    /// Flags
    flags: Flags,

    /// Routing ID (for ROUTER sockets)
    routing_id: ?RoutingId = null,

    /// Group (for RADIO/DISH)
    group: ?Group = null,

    pub const Flags = packed struct {
        more: bool = false,        // More parts follow
        shared: bool = false,      // Data is shared (refcounted)
        command: bool = false,     // ZMTP command frame
        _padding: u5 = 0,
    };

    pub const Data = union(enum) {
        /// Small message: data stored inline
        inline_data: InlineData,

        /// Large message: heap-allocated, owned
        owned: OwnedData,

        /// Shared message: reference-counted
        shared: *SharedData,

        /// External: user-provided buffer with custom free
        external: ExternalData,

        /// Empty message
        empty: void,
    };

    pub const InlineData = struct {
        len: u8,
        bytes: [INLINE_SIZE]u8,
    };

    pub const OwnedData = struct {
        ptr: [*]u8,
        len: usize,
        capacity: usize,
        allocator: std.mem.Allocator,
    };

    pub const SharedData = struct {
        refcount: std.atomic.Value(u32),
        ptr: [*]u8,
        len: usize,
        capacity: usize,
        allocator: std.mem.Allocator,

        pub fn acquire(self: *SharedData) *SharedData {
            _ = self.refcount.fetchAdd(1, .monotonic);
            return self;
        }

        pub fn release(self: *SharedData) void {
            if (self.refcount.fetchSub(1, .release) == 1) {
                self.refcount.fence(.acquire);
                self.allocator.free(self.ptr[0..self.capacity]);
                self.allocator.destroy(self);
            }
        }
    };

    pub const ExternalData = struct {
        ptr: [*]const u8,
        len: usize,
        hint: ?*anyopaque,
        free_fn: *const fn (?*anyopaque, [*]const u8, usize) void,
    };

    // === Construction ===

    /// Create empty message
    pub fn init() Message {
        return .{ .data = .{ .empty = {} }, .flags = .{} };
    }

    /// Create message from slice (copies data)
    pub fn initFromSlice(allocator: std.mem.Allocator, data: []const u8) !Message {
        if (data.len <= INLINE_SIZE) {
            var msg = Message{ .data = .{ .inline_data = undefined }, .flags = .{} };
            msg.data.inline_data.len = @intCast(data.len);
            @memcpy(msg.data.inline_data.bytes[0..data.len], data);
            return msg;
        }

        const buf = try allocator.alloc(u8, data.len);
        @memcpy(buf, data);
        return .{
            .data = .{ .owned = .{
                .ptr = buf.ptr,
                .len = data.len,
                .capacity = data.len,
                .allocator = allocator,
            }},
            .flags = .{},
        };
    }

    /// Create message with pre-allocated buffer (takes ownership)
    pub fn initOwned(ptr: [*]u8, len: usize, capacity: usize, allocator: std.mem.Allocator) Message {
        return .{
            .data = .{ .owned = .{
                .ptr = ptr,
                .len = len,
                .capacity = capacity,
                .allocator = allocator,
            }},
            .flags = .{},
        };
    }

    /// Create message from external buffer (zero-copy)
    pub fn initExternal(
        ptr: [*]const u8,
        len: usize,
        hint: ?*anyopaque,
        free_fn: *const fn (?*anyopaque, [*]const u8, usize) void,
    ) Message {
        return .{
            .data = .{ .external = .{
                .ptr = ptr,
                .len = len,
                .hint = hint,
                .free_fn = free_fn,
            }},
            .flags = .{},
        };
    }

    // === Access ===

    pub fn slice(self: *const Message) []const u8 {
        return switch (self.data) {
            .inline_data => |d| d.bytes[0..d.len],
            .owned => |d| d.ptr[0..d.len],
            .shared => |s| s.ptr[0..s.len],
            .external => |e| e.ptr[0..e.len],
            .empty => &[_]u8{},
        };
    }

    pub fn len(self: *const Message) usize {
        return self.slice().len;
    }

    // === Sharing (for PUB fan-out) ===

    /// Convert to shared and return a new reference
    /// Original message becomes shared too
    pub fn share(self: *Message, allocator: std.mem.Allocator) !Message {
        switch (self.data) {
            .shared => |s| {
                // Already shared, just acquire
                return .{
                    .data = .{ .shared = s.acquire() },
                    .flags = self.flags,
                    .routing_id = self.routing_id,
                    .group = self.group,
                };
            },
            .owned => |o| {
                // Convert to shared
                const shared = try allocator.create(SharedData);
                shared.* = .{
                    .refcount = std.atomic.Value(u32).init(2), // One for self, one for return
                    .ptr = o.ptr,
                    .len = o.len,
                    .capacity = o.capacity,
                    .allocator = o.allocator,
                };
                self.data = .{ .shared = shared };
                self.flags.shared = true;
                return .{
                    .data = .{ .shared = shared },
                    .flags = self.flags,
                    .routing_id = self.routing_id,
                    .group = self.group,
                };
            },
            .inline_data => {
                // Small message: just copy (cheaper than refcount overhead)
                return self.*;
            },
            .external => {
                // Can't share external, copy it
                return try Message.initFromSlice(allocator, self.slice());
            },
            .empty => {
                return self.*;
            },
        }
    }

    // === Cleanup ===

    pub fn deinit(self: *Message) void {
        switch (self.data) {
            .owned => |o| o.allocator.free(o.ptr[0..o.capacity]),
            .shared => |s| s.release(),
            .external => |e| e.free_fn(e.hint, e.ptr, e.len),
            .inline_data, .empty => {},
        }
        self.* = undefined;
    }

    // === Multipart helpers ===

    pub fn hasMore(self: *const Message) bool {
        return self.flags.more;
    }

    pub fn setMore(self: *Message, more: bool) void {
        self.flags.more = more;
    }
};

/// Routing ID for ROUTER sockets
pub const RoutingId = struct {
    bytes: [256]u8,
    len: u8,

    pub fn fromSlice(data: []const u8) !RoutingId {
        if (data.len > 255) return error.RoutingIdTooLong;
        var id = RoutingId{ .bytes = undefined, .len = @intCast(data.len) };
        @memcpy(id.bytes[0..data.len], data);
        return id;
    }

    pub fn slice(self: *const RoutingId) []const u8 {
        return self.bytes[0..self.len];
    }
};
```

### Multipart Messages

ZMQ guarantees atomic delivery of multipart messages - either all frames are delivered or none. This is critical for request envelopes (ROUTER) and complex protocols.

**Multipart API:**

```zig
/// Send a multipart message (all frames at once)
pub fn sendMultipart(self: *Socket, frames: []const Message) !void {
    // Mark all but last with MORE flag
    for (frames[0 .. frames.len - 1]) |*frame| {
        frame.flags.more = true;
    }
    frames[frames.len - 1].flags.more = false;

    // Send all frames
    for (frames) |*frame| {
        try self.send(frame);
    }
}

/// Receive a complete multipart message
pub fn recvMultipart(self: *Socket, allocator: std.mem.Allocator) !MultipartMessage {
    var frames = std.ArrayList(Message).init(allocator);
    errdefer {
        for (frames.items) |*f| f.deinit();
        frames.deinit();
    }

    while (true) {
        var msg = try self.recv();
        errdefer msg.deinit();

        const has_more = msg.flags.more;
        try frames.append(msg);

        if (!has_more) break;
    }

    return MultipartMessage{ .frames = frames };
}

/// Multipart message container
pub const MultipartMessage = struct {
    frames: std.ArrayList(Message),

    pub fn deinit(self: *MultipartMessage) void {
        for (self.frames.items) |*f| f.deinit();
        self.frames.deinit();
    }

    /// Get frame by index
    pub fn get(self: *const MultipartMessage, index: usize) ?*const Message {
        if (index >= self.frames.items.len) return null;
        return &self.frames.items[index];
    }

    /// Number of frames
    pub fn frameCount(self: *const MultipartMessage) usize {
        return self.frames.items.len;
    }
};
```

**Atomic Delivery Guarantee:**

The atomic delivery is ensured at the pipe/engine level:

```zig
/// Engine writer: send multipart atomically using vectored I/O
fn writeMultipart(self: *Engine, rt: *zio.Runtime) !void {
    var multipart_frames = std.ArrayList([]u8).init(self.allocator);
    defer {
        for (multipart_frames.items) |frame| self.allocator.free(frame);
        multipart_frames.deinit();
    }

    // Collect all frames of multipart
    while (true) {
        const msg = try self.pipe.outbound.receive(rt);
        defer msg.deinit();

        const encoded = try self.codec.encodeMessage(&self.write_buf, &msg);
        try multipart_frames.append(try self.allocator.dupe(u8, encoded));

        if (!msg.flags.more) break;
    }

    // Use vectored I/O to send all frames with minimal syscalls
    // ZIO's sendAllVec batches into single io_uring submission
    const slices = @as([]const []const u8, multipart_frames.items);
    try self.stream.sendAllVec(rt, slices, .{});
}

/// Engine reader: buffer multipart before delivering
fn readMultipart(self: *Engine, rt: *zio.Runtime) !void {
    var multipart = std.ArrayList(Message).init(self.allocator);
    defer {
        for (multipart.items) |*m| m.deinit();
        multipart.deinit();
    }

    // Read all frames of multipart
    while (true) {
        const result = self.codec.decode(self.read_buf, self.allocator);
        const msg = switch (result) {
            .frame => |f| f.content.message,
            else => return error.ProtocolError,
        };

        const has_more = msg.flags.more;
        try multipart.append(msg);

        if (!has_more) break;
    }

    // Deliver complete multipart atomically
    for (multipart.items) |msg| {
        try self.pipe.inbound.send(rt, msg);
    }
    multipart.clearRetainingCapacity();  // Ownership transferred
}
```

**Backpressure with Multipart:**

When HWM is reached mid-multipart:
1. We've already committed to sending the multipart
2. Channel.send() will suspend until space available
3. The entire multipart blocks together

This maintains atomicity: either the whole multipart fits or we wait.

### Message Copying Rules

| Scenario | Behavior |
|----------|----------|
| `send()` on PUSH/REQ/DEALER | Move (caller loses ownership) |
| `send()` on PUB/RADIO | Share (refcount) or copy for inline |
| `recv()` | Returns owned message |
| Internal pipe transfer | Move (no copy) |
| ZMTP encode | Read-only access, no copy |
| ZMTP decode | Allocate new message |

---

## Socket Architecture

### Socket Internal Structure

```zig
fn SocketInner(comptime Pattern: type) type {
    return struct {
        const Self = @This();

        /// Parent context
        context: *Context,

        /// Socket options
        options: SocketOptions,

        /// Pattern-specific state
        pattern: Pattern.State,

        /// All pipes (one per peer connection)
        pipes: PipeSet,

        /// Bound endpoints (listeners)
        listeners: std.ArrayList(*Listener),

        /// Connected endpoints (connectors)
        connectors: std.ArrayList(*Connector),

        /// Socket state
        state: State,

        /// Monitor (for socket events)
        monitor: ?*Monitor,

        const State = enum {
            active,
            closing,
            closed,
        };

        // === Internal operations ===

        fn attachPipe(self: *Self, pipe: *Pipe) void {
            self.pipes.add(pipe);
            Pattern.onPipeAttached(&self.pattern, pipe);
        }

        fn detachPipe(self: *Self, pipe: *Pipe) void {
            Pattern.onPipeDetached(&self.pattern, pipe);
            self.pipes.remove(pipe);
        }

        fn sendImpl(self: *Self, msg: *Message, timeout: Timeout) SendError!void {
            if (self.state != .active) return error.SocketClosed;
            return Pattern.send(&self.pattern, &self.pipes, msg, timeout, self.context.runtime);
        }

        fn recvImpl(self: *Self, timeout: Timeout) RecvError!Message {
            if (self.state != .active) return error.SocketClosed;
            return Pattern.recv(&self.pattern, &self.pipes, timeout, self.context.runtime);
        }
    };
}
```

### Pipe Set

Collection of pipes with pattern-specific indexing:

```zig
pub const PipeSet = struct {
    /// All pipes by ID
    pipes: std.AutoHashMap(PipeId, *Pipe),

    /// Pipes with data available (for recv)
    readable: std.ArrayList(*Pipe),

    /// Pipes that can accept data (for send)
    writable: std.ArrayList(*Pipe),

    /// For ROUTER: routing ID → pipe mapping
    routing_table: ?std.StringHashMap(*Pipe),

    /// Round-robin index for fair queuing
    rr_index: usize,

    pub fn init(allocator: std.mem.Allocator, needs_routing: bool) PipeSet;
    pub fn deinit(self: *PipeSet) void;

    pub fn add(self: *PipeSet, pipe: *Pipe) void;
    pub fn remove(self: *PipeSet, pipe: *Pipe) void;

    /// Mark pipe as readable (has data)
    pub fn markReadable(self: *PipeSet, pipe: *Pipe) void;
    pub fn markNotReadable(self: *PipeSet, pipe: *Pipe) void;

    /// Mark pipe as writable (can accept data)
    pub fn markWritable(self: *PipeSet, pipe: *Pipe) void;
    pub fn markNotWritable(self: *PipeSet, pipe: *Pipe) void;

    /// Get next writable pipe (round-robin)
    pub fn nextWritable(self: *PipeSet) ?*Pipe;

    /// Get next readable pipe (fair queue)
    pub fn nextReadable(self: *PipeSet) ?*Pipe;

    /// Get pipe by routing ID (ROUTER only)
    pub fn getByRoutingId(self: *PipeSet, id: []const u8) ?*Pipe;

    /// Iterate all pipes
    pub fn iterator(self: *PipeSet) Iterator;
};
```

---

## Pipe System

A pipe represents the bidirectional connection between a socket and a peer (either remote via network, or local via inproc). This section describes ZZMQ's pipe architecture, which follows libzmq semantics while leveraging ZIO's coroutine model.

### Core Design Decisions

Based on analysis of libzmq's `pipe.cpp`, `ypipe.hpp`, `fq.cpp`, and `lb.cpp`:

| Decision | libzmq | ZZMQ | Rationale |
|----------|--------|------|-----------|
| Queue contents | Frames | Frames | Enables streaming large multipart messages |
| HWM enforcement | Counter check | Counter check | HWM counts messages, not frames |
| Queue capacity | Unbounded (linked chunks) | Growable | "Unlimited" means unlimited |
| Credit flow | Explicit commands | Implicit (blocking) | ZIO coroutines handle backpressure |
| Flush semantics | Explicit flush() | Explicit flush() | Batching for throughput |

### Frame vs Message: Why Frames Matter

libzmq pipes store **frames**, not complete messages. This is critical for:

1. **Streaming large messages** - A 1GB file sent as 1000 frames shouldn't be buffered entirely
2. **Memory efficiency** - Process frames as they arrive
3. **Latency** - Start work before complete message received
4. **libzmq compatibility** - `ZMQ_RCVMORE` pattern is widely used

```zig
// Low-level API: frame streaming (matches libzmq)
while (true) {
    const frame = try socket.recvFrame(rt);
    try processFrame(frame);
    if (!frame.hasMore()) break;  // End of message
}

// High-level API: convenience aggregation
const msg = try socket.recv(rt);  // Returns complete MultipartMessage
defer msg.deinit();
```

### HWM Semantics (libzmq Reference)

From `src/pipe.cpp:533-538`:
```cpp
bool zmq::pipe_t::check_hwm () const {
    const bool full = _hwm > 0 && _msgs_written - _peers_msgs_read >= uint64_t(_hwm);
    return !full;
}
```

From `src/pipe.cpp:198-199` - HWM counts **complete messages**:
```cpp
if (!(msg_->flags () & msg_t::more) && !msg_->is_routing_id ())
    _msgs_read++;
```

**Key behaviors:**
- Default HWM = 1000 per direction
- HWM = 0 means **unlimited** (truly unbounded)
- HWM counts complete messages, not individual frames
- Per-pipe HWM (each connection independent)
- Inproc: HWM = sender's sndhwm + receiver's rcvhwm

### Growable Frame Queue

Unlike fixed-capacity channels, ZZMQ uses growable queues to support truly unlimited HWM:

```zig
/// Growable frame queue - allocates in chunks like libzmq's ypipe
pub const FrameQueue = struct {
    /// Linked list of frame chunks
    chunks: ChunkList,
    allocator: Allocator,

    /// Read/write positions
    head: usize = 0,
    tail: usize = 0,

    /// Signaling for async operations
    readable: zio.Notify = .{},

    /// Closed flag
    closed: bool = false,

    /// Like libzmq's message_pipe_granularity = 256
    pub const chunk_size = 256;

    pub const Chunk = struct {
        frames: [chunk_size]Frame,
        next: ?*Chunk = null,
    };

    const ChunkList = struct {
        first: ?*Chunk = null,
        last: ?*Chunk = null,
        count: usize = 0,
    };

    pub fn init(allocator: Allocator) FrameQueue {
        return .{ .chunks = .{}, .allocator = allocator };
    }

    /// Enqueue a frame - grows as needed (truly unbounded)
    pub fn enqueue(self: *FrameQueue, frame: Frame) !void {
        if (self.closed) return error.QueueClosed;

        // Allocate new chunk if needed
        if (self.needsNewChunk()) {
            const chunk = try self.allocator.create(Chunk);
            chunk.* = .{ .frames = undefined, .next = null };

            if (self.chunks.last) |last| {
                last.next = chunk;
            } else {
                self.chunks.first = chunk;
            }
            self.chunks.last = chunk;
            self.chunks.count += 1;
        }

        // Write frame
        self.writeFrame(frame);
    }

    /// Dequeue a frame - blocks if empty
    pub fn dequeue(self: *FrameQueue, rt: *zio.Runtime) !Frame {
        while (self.isEmpty() and !self.closed) {
            try self.readable.wait(rt);
        }

        if (self.isEmpty()) return error.QueueClosed;
        return self.readFrame();
    }

    /// Non-blocking dequeue
    pub fn tryDequeue(self: *FrameQueue) ?Frame {
        if (self.isEmpty()) return null;
        return self.readFrame();
    }

    /// Signal that data is available (called after flush)
    pub fn signal(self: *FrameQueue) void {
        self.readable.set();
    }

    /// Close the queue
    pub fn close(self: *FrameQueue) void {
        self.closed = true;
        self.readable.set();  // Wake any waiters
    }

    pub fn isEmpty(self: *FrameQueue) bool {
        return self.head == self.tail;
    }

    pub fn deinit(self: *FrameQueue) void {
        // Free all chunks
        var chunk = self.chunks.first;
        while (chunk) |c| {
            const next = c.next;
            self.allocator.destroy(c);
            chunk = next;
        }
    }

    // ... internal helpers
    fn needsNewChunk(self: *FrameQueue) bool { ... }
    fn writeFrame(self: *FrameQueue, frame: Frame) void { ... }
    fn readFrame(self: *FrameQueue) Frame { ... }
};
```

### Pipe Structure

```zig
pub const Pipe = struct {
    id: PipeId,

    // === Frame Queues (growable, truly unbounded) ===

    /// Frames from socket to engine (outbound)
    outbound: FrameQueue,

    /// Frames from engine to socket (inbound)
    inbound: FrameQueue,

    // === HWM Tracking (counter-based, separate from queue capacity) ===

    /// Complete messages written to outbound (for send HWM)
    msgs_written: u64 = 0,

    /// Complete messages read from inbound (for recv HWM)
    msgs_read: u64 = 0,

    /// Last known peer read count (for credit flow)
    peers_msgs_read: u64 = 0,

    /// Configured HWM (null = unlimited)
    send_hwm: ?u32,
    recv_hwm: ?u32,

    // === Multipart State ===

    /// Frames in current outbound multipart (not yet complete)
    outbound_multipart_frames: u32 = 0,

    /// Currently receiving a multipart message
    inbound_multipart_active: bool = false,

    // === Flush State ===

    /// Frames written since last flush
    unflushed_frames: u32 = 0,

    // === Metadata ===

    /// Peer identity (assigned during handshake or by ROUTER)
    routing_id: ?RoutingId = null,

    /// Associated engine (null for inproc)
    engine: ?*Engine = null,

    /// Pipe state
    state: State = .active,

    /// Statistics
    stats: Stats = .{},

    /// Options
    options: PipeOptions,

    // === Types ===

    const State = enum {
        active,
        closing,        // Graceful shutdown initiated
        draining,       // Flushing remaining frames
        closed,
    };

    const Stats = struct {
        messages_sent: u64 = 0,
        messages_recv: u64 = 0,
        bytes_sent: u64 = 0,
        bytes_recv: u64 = 0,
        frames_sent: u64 = 0,
        frames_recv: u64 = 0,
    };

    const PipeOptions = struct {
        conflate: bool = false,
        // LWM for credit batching (default: HWM/2)
        lwm_divisor: u32 = 2,
    };

    // === HWM Enforcement ===

    /// Check if we can start a new message (HWM not reached)
    /// Called BEFORE writing first frame of a message
    pub fn canStartMessage(self: *Pipe) bool {
        const hwm = self.send_hwm orelse return true;  // Unlimited
        return self.msgs_written - self.peers_msgs_read < hwm;
    }

    /// Check if HWM is reached (for pattern logic)
    pub fn isHwmReached(self: *Pipe) bool {
        return !self.canStartMessage();
    }

    // === Frame Writing (Socket → Engine) ===

    /// Write a single frame to the pipe
    /// For multipart: write all frames, then call flush()
    pub fn writeFrame(self: *Pipe, frame: Frame) !void {
        if (self.state != .active) return error.PipeClosed;

        try self.outbound.enqueue(frame);
        self.outbound_multipart_frames += 1;
        self.unflushed_frames += 1;
        self.stats.frames_sent += 1;
        self.stats.bytes_sent += frame.data.len;

        // Track message completion
        if (!frame.flags.more) {
            self.msgs_written += 1;
            self.stats.messages_sent += 1;
            self.outbound_multipart_frames = 0;
        }
    }

    /// Write a complete multipart message atomically
    pub fn writeMessage(self: *Pipe, msg: MultipartMessage) !void {
        if (self.state != .active) return error.PipeClosed;

        // HWM check before starting
        if (!self.canStartMessage()) {
            return error.HighWaterMark;
        }

        // Write all frames
        for (msg.frames, 0..) |frame, i| {
            var f = frame;
            f.flags.more = (i < msg.frames.len - 1);
            try self.writeFrame(f);
        }

        // Flush to make visible to engine
        self.flush();
    }

    /// Flush written frames to make them visible to reader
    /// Like libzmq's pipe_t::flush()
    pub fn flush(self: *Pipe) void {
        if (self.unflushed_frames > 0) {
            self.outbound.signal();  // Wake engine reader
            self.unflushed_frames = 0;
        }
    }

    /// Rollback incomplete multipart (discard unflushed frames)
    /// Like libzmq's pipe_t::rollback()
    pub fn rollback(self: *Pipe) void {
        // Remove frames from current incomplete multipart
        while (self.outbound_multipart_frames > 0) {
            if (self.outbound.tryDequeue()) |frame| {
                frame.deinit();
                self.outbound_multipart_frames -= 1;
                self.unflushed_frames -|= 1;
            } else break;
        }
    }

    // === Frame Reading (Engine → Socket) ===

    /// Read a single frame (for streaming)
    pub fn readFrame(self: *Pipe, rt: *zio.Runtime) !Frame {
        const frame = try self.inbound.dequeue(rt);
        self.stats.frames_recv += 1;
        self.stats.bytes_recv += frame.data.len;

        // Track message completion for stats
        if (!frame.flags.more) {
            self.msgs_read += 1;
            self.stats.messages_recv += 1;
            self.inbound_multipart_active = false;

            // Credit flow: notify peer every LWM messages
            self.maybeSendCredit();
        } else {
            self.inbound_multipart_active = true;
        }

        return frame;
    }

    /// Read a complete multipart message (convenience)
    pub fn readMessage(self: *Pipe, rt: *zio.Runtime, allocator: Allocator) !MultipartMessage {
        var frames = std.ArrayList(Frame).init(allocator);
        errdefer {
            for (frames.items) |f| f.deinit();
            frames.deinit();
        }

        while (true) {
            const frame = try self.readFrame(rt);
            try frames.append(frame);
            if (!frame.flags.more) break;
        }

        return MultipartMessage{
            .frames = try frames.toOwnedSlice(),
            .allocator = allocator,
        };
    }

    /// Try to read without blocking
    pub fn tryReadFrame(self: *Pipe) ?Frame {
        const frame = self.inbound.tryDequeue() orelse return null;
        self.stats.frames_recv += 1;
        self.stats.bytes_recv += frame.data.len;

        if (!frame.flags.more) {
            self.msgs_read += 1;
            self.stats.messages_recv += 1;
            self.inbound_multipart_active = false;
            self.maybeSendCredit();
        } else {
            self.inbound_multipart_active = true;
        }

        return frame;
    }

    /// Check if pipe has data to read
    pub fn hasData(self: *Pipe) bool {
        return !self.inbound.isEmpty();
    }

    // === Credit Flow ===

    /// Send credit to peer (batched at LWM intervals)
    fn maybeSendCredit(self: *Pipe) void {
        const hwm = self.recv_hwm orelse return;  // No credit for unlimited
        const lwm = hwm / self.options.lwm_divisor;

        if (lwm > 0 and self.msgs_read % lwm == 0) {
            // In ZZMQ, credit is implicit via channel backpressure
            // This is mainly for statistics/monitoring
            // For explicit credit (future): self.sendCreditCommand()
        }
    }

    /// Update peer's read count (for HWM calculation)
    pub fn updatePeerCredit(self: *Pipe, msgs_read: u64) void {
        self.peers_msgs_read = msgs_read;
    }

    // === Lifecycle ===

    /// Begin graceful close
    pub fn beginClose(self: *Pipe) void {
        if (self.state != .active) return;
        self.state = .closing;

        // Rollback any incomplete multipart
        self.rollback();

        // Close outbound (engine will drain then close)
        self.outbound.close();
    }

    /// Force immediate close
    pub fn forceClose(self: *Pipe) void {
        self.state = .closed;
        self.outbound.close();
        self.inbound.close();
    }

    pub fn deinit(self: *Pipe, allocator: Allocator) void {
        self.outbound.deinit();
        self.inbound.deinit();
        allocator.destroy(self);
    }
};

pub const PipeId = u32;
```

### HWM Modes: Per-Pipe vs Aggregate

ZZMQ supports both libzmq-compatible per-pipe HWM and an additional aggregate mode:

```zig
pub const HwmMode = union(enum) {
    /// Each pipe has independent HWM (libzmq default)
    /// Total memory = HWM × num_peers
    per_pipe: struct {
        send: ?u32 = 1000,
        recv: ?u32 = 1000,
    },

    /// Shared HWM across all pipes (ZZMQ extension)
    /// Bounds total memory regardless of peer count
    aggregate: struct {
        send_total: ?u32,
        recv_total: ?u32,
        tracker: *AggregateHwmTracker,
    },

    /// No limit (truly unlimited)
    unlimited,
};

/// Tracks aggregate HWM across all pipes
pub const AggregateHwmTracker = struct {
    /// Total outstanding messages across all pipes
    total_outstanding: Atomic(u64) = .{ .value = 0 },

    /// Configured limit
    limit: ?u64,

    /// Try to reserve a slot for a new message
    pub fn tryReserve(self: *AggregateHwmTracker) bool {
        const lim = self.limit orelse return true;  // Unlimited

        while (true) {
            const current = self.total_outstanding.load(.acquire);
            if (current >= lim) return false;  // At limit

            // Try to increment
            if (self.total_outstanding.cmpxchgWeak(
                current, current + 1, .acq_rel, .acquire
            )) |_| {
                continue;  // Retry
            } else {
                return true;  // Reserved
            }
        }
    }

    /// Release a slot when message is consumed
    pub fn release(self: *AggregateHwmTracker) void {
        _ = self.total_outstanding.fetchSub(1, .release);
    }

    /// Get current outstanding count
    pub fn outstanding(self: *AggregateHwmTracker) u64 {
        return self.total_outstanding.load(.acquire);
    }
};
```

**Use cases:**

| Mode | Use Case | Trade-off |
|------|----------|-----------|
| Per-pipe (default) | Most applications, libzmq compat | Memory scales with connections |
| Aggregate | Memory-constrained, many peers | One slow peer can impact others |
| Unlimited | High-throughput, trusted peers | Risk of OOM |

### Two-Tier Architecture: Comptime vs Runtime

ZZMQ provides both compile-time optimized and runtime-flexible APIs:

```zig
// ============================================
// Tier 1: Comptime-Optimized (Zig users)
// ============================================

/// Comptime-configured pipe with optimizations
pub fn TypedPipe(comptime config: PipeConfig) type {
    return struct {
        const Self = @This();

        // Comptime-known HWM enables branch elimination
        const send_hwm = config.send_hwm;
        const recv_hwm = config.recv_hwm;

        core: Pipe,

        /// HWM check with comptime optimization
        pub inline fn canStartMessage(self: *Self) bool {
            if (send_hwm == null) {
                // Comptime-known unlimited: no check needed
                return true;
            }
            return self.core.canStartMessage();
        }

        /// Write with comptime-optimized HWM check
        pub fn writeMessage(self: *Self, msg: MultipartMessage) !void {
            if (send_hwm) |hwm| {
                // Comptime-known bounded: inline check
                if (self.core.msgs_written - self.core.peers_msgs_read >= hwm) {
                    return error.HighWaterMark;
                }
            }
            // else: comptime-known unlimited, check eliminated

            try self.core.writeMessage(msg);
        }
    };
}

pub const PipeConfig = struct {
    send_hwm: ?u32 = 1000,
    recv_hwm: ?u32 = 1000,
    conflate: bool = false,
};

// Usage (Zig):
const MyPipe = TypedPipe(.{ .send_hwm = 1000, .recv_hwm = null });
var pipe: MyPipe = ...;

// ============================================
// Tier 2: Runtime-Flexible (C FFI, dynamic config)
// ============================================

/// Runtime-configured pipe
pub const DynamicPipe = struct {
    core: Pipe,

    pub fn init(allocator: Allocator, config: DynamicPipeConfig) !*DynamicPipe {
        const self = try allocator.create(DynamicPipe);
        self.* = .{
            .core = .{
                .outbound = FrameQueue.init(allocator),
                .inbound = FrameQueue.init(allocator),
                .send_hwm = config.send_hwm,
                .recv_hwm = config.recv_hwm,
                .options = .{ .conflate = config.conflate },
            },
        };
        return self;
    }

    /// Runtime HWM check
    pub fn canStartMessage(self: *DynamicPipe) bool {
        return self.core.canStartMessage();
    }

    /// Update HWM at runtime (for setsockopt)
    pub fn setHwm(self: *DynamicPipe, send: ?u32, recv: ?u32) void {
        self.core.send_hwm = send;
        self.core.recv_hwm = recv;
    }
};

pub const DynamicPipeConfig = struct {
    send_hwm: ?u32 = 1000,
    recv_hwm: ?u32 = 1000,
    conflate: bool = false,
};

// C FFI wrapper:
export fn zzmq_pipe_create(send_hwm: i32, recv_hwm: i32) ?*DynamicPipe {
    const config = DynamicPipeConfig{
        .send_hwm = if (send_hwm <= 0) null else @intCast(send_hwm),
        .recv_hwm = if (recv_hwm <= 0) null else @intCast(recv_hwm),
    };
    return DynamicPipe.init(c_allocator, config) catch null;
}
```

### Flush Semantics and Batching

Like libzmq, ZZMQ uses explicit flush for throughput optimization:

```zig
// Writer side (socket → pipe → engine)
pub fn sendMultipart(socket: *Socket, frames: []const Frame) !void {
    const pipe = socket.selectPipeForSend() orelse return error.NoRoute;

    // HWM check before starting
    if (!pipe.canStartMessage()) {
        // Either block, drop, or return error based on pattern
        return error.HighWaterMark;
    }

    // Write all frames (accumulates in pipe)
    for (frames, 0..) |frame, i| {
        var f = frame;
        f.flags.more = (i < frames.len - 1);
        try pipe.writeFrame(f);
    }

    // Flush to make visible to engine
    // This is the only point that signals the reader
    pipe.flush();
}

// Engine side (reads flushed frames)
fn engineWriterLoop(engine: *Engine) !void {
    while (engine.state == .ready) {
        // Wait for flushed data
        const frame = try engine.pipe.outbound.dequeue(engine.rt);

        // Encode and send to network
        try engine.sendZmtpFrame(frame);
    }
}
```

**Why flush semantics matter:**
- Batches signaling (one wake per message, not per frame)
- Ensures multipart atomicity (all frames visible together)
- Matches libzmq behavior exactly
- Reduces context switch overhead

### Inproc HWM Calculation

For inproc connections, libzmq sums both sides' HWM:

```zig
/// Calculate effective HWM for inproc connection
/// libzmq: if either side is unlimited, total is unlimited
/// otherwise: total = sender's sndhwm + receiver's rcvhwm
pub fn calculateInprocHwm(sender_hwm: ?u32, receiver_hwm: ?u32) ?u32 {
    const send = sender_hwm orelse return null;    // Unlimited
    const recv = receiver_hwm orelse return null;  // Unlimited
    return send + recv;
}

/// Create pipe pair for inproc connection
pub fn createInprocPipePair(
    allocator: Allocator,
    connector_opts: *const SocketOptions,
    binder_opts: *const SocketOptions,
) !struct { connector: *Pipe, binder: *Pipe } {
    // Direction: connector → binder
    const c2b_hwm = calculateInprocHwm(
        connector_opts.send_hwm,
        binder_opts.recv_hwm,
    );

    // Direction: binder → connector
    const b2c_hwm = calculateInprocHwm(
        binder_opts.send_hwm,
        connector_opts.recv_hwm,
    );

    // Create shared frame queues
    var c2b_queue = FrameQueue.init(allocator);
    var b2c_queue = FrameQueue.init(allocator);

    // Connector's pipe
    const connector = try allocator.create(Pipe);
    connector.* = .{
        .id = generatePipeId(),
        .outbound = c2b_queue,
        .inbound = b2c_queue,
        .send_hwm = c2b_hwm,
        .recv_hwm = b2c_hwm,
        .options = .{},
    };

    // Binder's pipe (reversed queues)
    const binder = try allocator.create(Pipe);
    binder.* = .{
        .id = generatePipeId(),
        .outbound = b2c_queue,
        .inbound = c2b_queue,
        .send_hwm = b2c_hwm,
        .recv_hwm = c2b_hwm,
        .options = .{},
    };

    return .{ .connector = connector, .binder = binder };
}
```

### Conflate Mode

libzmq's conflate mode keeps only the latest message:

```zig
/// Conflating frame queue - keeps only latest complete message
pub const ConflatingFrameQueue = struct {
    latest_message: ?[]Frame = null,
    allocator: Allocator,
    readable: zio.Notify = .{},
    closed: bool = false,

    /// Enqueue overwrites any existing message
    pub fn enqueue(self: *ConflatingFrameQueue, frames: []Frame) !void {
        // Free old message
        if (self.latest_message) |old| {
            for (old) |f| f.deinit();
            self.allocator.free(old);
        }

        // Store new message
        self.latest_message = try self.allocator.dupe(Frame, frames);
        self.readable.set();
    }

    /// Dequeue gets latest (and only) message
    pub fn dequeue(self: *ConflatingFrameQueue, rt: *zio.Runtime) ![]Frame {
        while (self.latest_message == null and !self.closed) {
            try self.readable.wait(rt);
        }

        if (self.latest_message) |msg| {
            self.latest_message = null;
            return msg;
        }

        return error.QueueClosed;
    }
};
```

### Data Flow Summary

```
User send(MultipartMessage)
         │
         ▼
┌─────────────────────────┐
│   Pattern Logic         │  LoadBalancer/Router selects pipe
│   (check canStartMsg)   │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Pipe.writeFrame()     │  Accumulates frames
│   (for each frame)      │  HWM tracked via counter
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Pipe.flush()          │  Signals engine
│                         │  Makes frames visible
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   FrameQueue            │  Growable storage
│   (outbound)            │  No fixed capacity limit
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Engine Writer         │  Dequeues frames
│   Coroutine             │  Encodes ZMTP
└───────────┬─────────────┘
            │
            ▼
         Network


         Network
            │
            ▼
┌─────────────────────────┐
│   Engine Reader         │  Decodes ZMTP
│   Coroutine             │  Enqueues frames
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   FrameQueue            │  Growable storage
│   (inbound)             │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Pipe.readFrame()      │  Streaming API
│   or Pipe.readMessage() │  Aggregating API
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Pattern Logic         │  FairQueue selects pipe
│   (tracks multipart)    │  Sticks to pipe mid-message
└───────────┬─────────────┘
            │
            ▼
User recv() → Frame or MultipartMessage
```

---

## Select and Multiplexing

A critical operation for ZMQ patterns is waiting on multiple pipes simultaneously. ZIO provides `zio.select` for this purpose.

### The Problem

Consider a PULL socket with 3 connected peers. When the user calls `recv()`:
- We need to check all 3 inbound channels
- If all are empty, we need to wait until ANY has data
- We want fair-queuing (round-robin) among ready channels

Similarly for PUSH with HWM reached on all pipes - we wait for ANY to have space.

### ZIO Select

ZIO's `select` function waits on multiple async operations simultaneously:

```zig
const result = try zio.select(rt, .{
    .pipe1 = pipe1.inbound.asyncReceive(),
    .pipe2 = pipe2.inbound.asyncReceive(),
    .pipe3 = pipe3.inbound.asyncReceive(),
});

switch (result) {
    .pipe1 => |msg| return msg,
    .pipe2 => |msg| return msg,
    .pipe3 => |msg| return msg,
}
```

Key properties:
- Returns as soon as ANY operation completes
- Cancels other pending operations automatically
- Zero-cost when one is immediately ready (fast path)
- Supports timeouts via additional timer channel

### Helper: Wait for Any Readable Pipe

```zig
/// Block until any pipe has data, return that message.
/// Used by PULL, SUB, DEALER patterns.
fn blockOnReadable(
    pipes: *PipeSet,
    timeout: Timeout,
    rt: *zio.Runtime,
) RecvError!Message {
    // Build async receives for all pipes
    const pipe_count = pipes.count();
    if (pipe_count == 0) return error.NoRoute;

    // For small pipe counts, use inline select
    if (pipe_count <= 8) {
        return blockOnReadableSmall(pipes, timeout, rt);
    }

    // For large pipe counts, use polling approach
    return blockOnReadableLarge(pipes, timeout, rt);
}

/// Optimized for common case of few connections
fn blockOnReadableSmall(
    pipes: *PipeSet,
    timeout: Timeout,
    rt: *zio.Runtime,
) RecvError!Message {
    // ZIO select with up to 8 channels
    // Using comptime-generated switch based on actual count

    var iter = pipes.iterator();

    // Collect async receives
    var ops: [8]AsyncReceiveOp = undefined;
    var count: usize = 0;

    while (iter.next()) |pipe| : (count += 1) {
        if (count >= 8) break;
        ops[count] = pipe.inbound.asyncReceive();
    }

    // Select based on count
    return switch (count) {
        1 => {
            const result = try zio.select(rt, .{ .p0 = ops[0] });
            return result.p0;
        },
        2 => {
            const result = try zio.select(rt, .{ .p0 = ops[0], .p1 = ops[1] });
            return switch (result) {
                .p0 => |m| m,
                .p1 => |m| m,
            };
        },
        // ... etc for 3-8
        else => unreachable,
    };
}
```

### Helper: Wait for Any Writable Pipe

```zig
/// Block until any pipe can accept a write, then write.
/// Used by PUSH, DEALER patterns.
fn blockOnWritable(
    pipes: *PipeSet,
    msg: *Message,
    timeout: Timeout,
    rt: *zio.Runtime,
) SendError!void {
    const pipe_count = pipes.count();
    if (pipe_count == 0) return error.NoRoute;

    // Try each pipe once first (fast path)
    var iter = pipes.iterator();
    while (iter.next()) |pipe| {
        if (pipe.tryWrite(msg.*)) |_| {
            return;  // Success
        } else |_| {}
    }

    // All full - wait for space on any
    // Build async send operations
    var wait_ops = std.ArrayList(AsyncSendOp).init(rt.allocator);
    defer wait_ops.deinit();

    iter.reset();
    while (iter.next()) |pipe| {
        try wait_ops.append(pipe.outbound.asyncSend(msg.*));
    }

    // Select waits for first available
    // When one succeeds, others are automatically cancelled
    // The ZIO channel handles the send atomically

    _ = try selectFromSlice(rt, wait_ops.items, timeout);
}
```

### Pattern-Specific Select Usage

**ROUTER recv (with routing ID tracking):**
```zig
pub fn recv(state: *State, pipes: *PipeSet, timeout: Timeout, rt: *zio.Runtime) !Message {
    // We need to know WHICH pipe the message came from
    // to attach the routing ID

    // Try non-blocking first
    var iter = pipes.iterator();
    while (iter.next()) |pipe| {
        if (pipe.tryRead()) |msg| {
            var result = msg;
            result.routing_id = pipe.routing_id;
            return result;
        } else |_| {}
    }

    // All empty - use select to find which becomes ready
    const idx = try selectReadable(pipes, timeout, rt);
    const pipe = pipes.getByIndex(idx).?;

    var msg = try pipe.read(rt);
    msg.routing_id = pipe.routing_id;
    return msg;
}
```

**PUB/SUB subscription matching:**
```zig
pub fn recv(state: *State, pipes: *PipeSet, timeout: Timeout, rt: *zio.Runtime) !Message {
    while (true) {
        // Fair queue from any pipe
        const msg = try blockOnReadable(pipes, timeout, rt);

        // Check subscription filter
        if (state.subscriptions.matches(msg.slice())) {
            return msg;
        }

        // Doesn't match subscription - drop and try again
        msg.deinit();
    }
}
```

### Timeout Integration

```zig
/// Receive with timeout support
fn recvWithTimeout(
    pipes: *PipeSet,
    timeout: Timeout,
    rt: *zio.Runtime,
) RecvError!Message {
    if (timeout == .infinite) {
        return blockOnReadable(pipes, timeout, rt);
    }

    // Race against timer
    var timer = zio.time.Timer.init(timeout.duration);

    const result = try zio.select(rt, .{
        .message = blockOnReadableAsync(pipes),
        .timeout = timer.asyncWait(),
    });

    switch (result) {
        .message => |msg| return msg,
        .timeout => return error.Timeout,
    }
}
```

---

## Structured Concurrency with ZIO Groups

ZZMQ uses ZIO Groups for structured concurrency, matching libzmq's explicit lifecycle management while leveraging ZIO's cooperative scheduling.

### Design Principles

1. **Groups at Engine level** - Reader/writer/heartbeat coroutines in a group
2. **Explicit Session list at Socket level** - More control for linger, matches libzmq
3. **Graceful shutdown by default** - Set state, let coroutines exit naturally
4. **Forceful cancel only for `linger=0`** - Context termination with immediate drop

### Hierarchy

```
Context
  └── Socket (list, not group)
        └── Session (list, not group - for linger control)
              └── Engine
                    └── Group: coroutines
                          ├── readerLoop
                          ├── writerLoop
                          └── heartbeatLoop (optional)
```

**Why lists instead of groups at Socket/Session level:**

| Level | Uses Group? | Rationale |
|-------|-------------|-----------|
| Engine coroutines | Yes | All exit together when connection ends |
| Session list | No | Need per-session linger, ordered shutdown |
| Socket sessions | No | Different endpoints may have different linger |
| Context sockets | No | Each socket closes independently |

### Failure Propagation: Explicit State Coordination

When reader detects disconnect, it sets state; writer checks and exits gracefully:

```zig
pub const Engine = struct {
    state: State,
    group: zio.Group = .{},
    disconnect_reason: ?DisconnectReason = null,

    const State = enum {
        handshaking,
        ready,
        closing,  // Graceful shutdown initiated
        closed,
    };

    const DisconnectReason = enum {
        peer_closed,
        protocol_error,
        heartbeat_timeout,
        local_close,
    };

    pub fn run(self: *Engine, rt: *zio.Runtime) void {
        defer self.cleanup(rt);

        // Handshake
        self.performHandshake(rt) catch |err| {
            self.disconnect_reason = .protocol_error;
            return;
        };

        self.state = .ready;

        // Spawn coroutines into group
        self.group.spawn(rt, Engine.readerLoop, .{ self, rt }) catch return;
        self.group.spawn(rt, Engine.writerLoop, .{ self, rt }) catch return;

        if (self.heartbeat.interval != null) {
            self.group.spawn(rt, Engine.heartbeatLoop, .{ self, rt }) catch return;
        }

        // Wait for all to finish (any exit causes others to see state change)
        self.group.wait(rt) catch {};

        // Notify session of disconnect
        if (self.disconnect_reason) |reason| {
            self.session.handleDisconnect(reason);
        }
    }

    /// Reader: sets state on disconnect, others check and exit
    fn readerLoop(self: *Engine, rt: *zio.Runtime) void {
        while (self.state == .ready) {
            const n = self.stream.recv(rt, self.read_buf, .{}) catch |err| {
                self.disconnect_reason = switch (err) {
                    error.ConnectionReset, error.BrokenPipe => .peer_closed,
                    error.Timeout => .heartbeat_timeout,
                    else => .protocol_error,
                };
                self.state = .closing;  // Signal others to exit
                return;
            };

            if (n == 0) {
                self.disconnect_reason = .peer_closed;
                self.state = .closing;
                return;
            }

            // Process data...
        }
    }

    /// Writer: checks state before each operation
    fn writerLoop(self: *Engine, rt: *zio.Runtime) void {
        while (self.state == .ready) {
            // Receive from channel - will unblock when state changes
            // because channel will be closed
            const msg = self.pipe.outbound.receive(rt) catch |err| {
                if (err == error.ChannelClosed) return;
                self.state = .closing;
                return;
            };
            defer msg.deinit();

            // Check state again before sending
            if (self.state != .ready) {
                // Re-queue message if linger allows
                if (self.session.linger_ms != 0) {
                    self.pipe.outbound.trySend(msg) catch {};
                }
                return;
            }

            self.stream.sendAll(rt, self.encodeMessage(msg), .{}) catch |err| {
                self.state = .closing;
                return;
            };
        }
    }

    fn heartbeatLoop(self: *Engine, rt: *zio.Runtime) void {
        const interval = self.heartbeat.interval orelse return;

        while (self.state == .ready) {
            rt.sleep(Duration.fromMilliseconds(interval)) catch return;

            if (self.state != .ready) return;

            const now = rt.now();
            if (now.since(self.heartbeat.last_recv).toMilliseconds() > self.heartbeat.timeout) {
                self.disconnect_reason = .heartbeat_timeout;
                self.state = .closing;
                return;
            }

            if (now.since(self.heartbeat.last_send).toMilliseconds() > interval) {
                self.sendPing() catch {
                    self.state = .closing;
                    return;
                };
            }
        }
    }
};
```

### Graceful vs Forceful Shutdown

```zig
pub const Session = struct {
    engine: ?*Engine = null,
    linger_ms: i32,

    /// Graceful shutdown: let engine finish current work
    pub fn beginGracefulShutdown(self: *Session, rt: *zio.Runtime) void {
        if (self.engine) |eng| {
            eng.state = .closing;
            // Engine checks state and exits loops naturally
            eng.group.wait(rt) catch {};
            eng.deinit();
            self.engine = null;
        }
    }

    /// Forceful shutdown: cancel immediately (linger=0)
    pub fn forceShutdown(self: *Session, rt: *zio.Runtime) void {
        if (self.engine) |eng| {
            eng.group.cancel(rt);  // Immediate cancellation
            eng.deinit();
            self.engine = null;
        }
    }
};

pub const Socket = struct {
    sessions: std.ArrayList(*Session),
    ctx: *Context,

    /// Close socket with linger
    pub fn close(self: *Socket, rt: *zio.Runtime) void {
        const linger = self.options.linger_ms;

        for (self.sessions.items) |session| {
            if (linger == 0) {
                session.forceShutdown(rt);
            } else {
                session.beginGracefulShutdown(rt);
            }
        }

        self.sessions.deinit();
        self.ctx.unregisterSocket(self);
    }
};
```

### Listener with Accept Group

Listener uses a group for accepted connections (all engines for this endpoint):

```zig
pub const Listener = struct {
    acceptor: zio.net.Acceptor,
    engine_group: zio.Group = .{},  // All engines from this listener
    active: bool = true,

    pub fn run(self: *Listener, rt: *zio.Runtime) void {
        defer self.cleanup(rt);

        while (self.active) {
            const stream = self.acceptor.accept(rt) catch |err| {
                if (err == error.Cancelled) break;
                continue;  // Transient error, retry
            };

            // Create engine for this connection
            const engine = Engine.create(self.socket, stream, .server) catch {
                stream.close();
                continue;
            };

            // Spawn into group - will be cancelled when listener stops
            self.engine_group.spawn(rt, Engine.run, .{ engine, rt }) catch {
                engine.deinit();
                continue;
            };
        }
    }

    pub fn stop(self: *Listener, rt: *zio.Runtime) void {
        self.active = false;
        self.acceptor.close();  // Unblocks accept()
        self.engine_group.cancel(rt);  // Cancel all engines
        self.engine_group.wait(rt) catch {};  // Wait for cleanup
    }
};
```

### Connector with Reconnection

Connector doesn't use a group - it manages a single engine with reconnection logic:

```zig
pub const Connector = struct {
    endpoint: Endpoint,
    engine: ?*Engine = null,
    reconnect: ReconnectState,
    state: State = .disconnected,

    const State = enum { disconnected, connecting, connected };

    pub fn run(self: *Connector, rt: *zio.Runtime) void {
        while (self.state != .disconnected) {
            // Connect
            const stream = self.endpoint.connect(rt) catch |err| {
                self.scheduleReconnect(rt);
                continue;
            };

            // Create and run engine
            self.engine = Engine.create(self.socket, stream, .client) catch {
                stream.close();
                self.scheduleReconnect(rt);
                continue;
            };

            self.state = .connected;
            self.reconnect.reset();

            // Run engine (blocks until disconnect)
            self.engine.?.run(rt);

            // Engine exited - cleanup and maybe reconnect
            self.engine.?.deinit();
            self.engine = null;

            if (self.state != .disconnected) {
                self.scheduleReconnect(rt);
            }
        }
    }

    fn scheduleReconnect(self: *Connector, rt: *zio.Runtime) void {
        const delay = self.reconnect.nextDelay();
        rt.sleep(Duration.fromMilliseconds(delay)) catch return;
    }

    pub fn stop(self: *Connector) void {
        self.state = .disconnected;
        if (self.engine) |eng| {
            eng.state = .closing;
        }
    }
};
```

### Comparison with libzmq

| Aspect | libzmq | ZZMQ |
|--------|--------|------|
| Engine coordination | Mailbox commands | State enum + channel close |
| Reader/writer sync | Lock-free queues + signaler | ZIO channels + groups |
| Shutdown signal | Command via mailbox | `state = .closing` |
| Forceful cancel | `terminate(false)` | `group.cancel(rt)` |
| Reconnection | Timer + state machine | Loop with `rt.sleep()` |
| Linger | Timer callback | Shielded block + deadline |

**Key insight:** libzmq uses commands and signaling because it's poll-based. ZZMQ uses state + channels because ZIO coroutines can simply check state and exit naturally.

---

## State Change Propagation

A key challenge in ZZMQ is propagating state changes to blocked coroutines. When a coroutine is blocked waiting on a channel or I/O, how does it learn that another coroutine set `state = .closing`?

### The Problem

```zig
// Writer is blocked here - won't see state change!
const msg = self.pipe.outbound.receive(rt) catch |err| { ... };

// Meanwhile, reader sets:
self.state = .closing;  // Writer doesn't wake up
```

### Hybrid Solution: Notify + Select

ZZMQ uses a hybrid approach combining `zio.Notify` for signaling and `zio.select()` for multiplexing:

```zig
pub const Engine = struct {
    state: State = .handshaking,
    shutdown_notify: zio.Notify = .{},  // One-shot notification
    group: zio.Group = .{},
    disconnect_reason: ?DisconnectReason = null,

    const State = enum {
        handshaking,
        ready,
        closing,
        closed,
    };

    const DisconnectReason = enum {
        peer_closed,
        protocol_error,
        heartbeat_timeout,
        local_close,
    };

    /// Initiate graceful shutdown - wakes all waiting coroutines
    fn initiateShutdown(self: *Engine, reason: DisconnectReason) void {
        if (self.state == .ready) {
            self.state = .closing;
            self.disconnect_reason = reason;
            self.shutdown_notify.set();  // Wake writer and heartbeat
        }
    }
```

*(Full State Change Propagation content continues in the existing section below)*

---

## Connection Management

### Listener (for `bind()`)

```zig
pub const Listener = struct {
    /// Endpoint this listener is bound to
    endpoint: Endpoint,

    /// The listening socket
    server: zio.net.Server,

    /// Parent socket
    socket: *anyopaque,  // Type-erased SocketInner

    /// Accept group (structured concurrency)
    group: zio.Group,

    /// State
    active: bool,

    /// Accept loop coroutine
    pub fn run(self: *Listener, rt: *zio.Runtime) void {
        defer self.cleanup(rt);

        while (self.active) {
            const stream = self.server.accept(rt) catch |err| {
                // Handle accept errors (log, maybe backoff)
                continue;
            };

            // Spawn engine for this connection
            const engine = Engine.create(self.socket, stream, .server) catch |err| {
                stream.close(rt);
                continue;
            };

            self.group.spawn(rt, Engine.run, .{ engine, rt }) catch |err| {
                engine.destroy();
                continue;
            };
        }
    }

    pub fn stop(self: *Listener, rt: *zio.Runtime) void {
        self.active = false;
        self.group.cancel(rt);
    }
};
```

### Connector (for `connect()`)

```zig
pub const Connector = struct {
    /// Endpoint to connect to
    endpoint: Endpoint,

    /// Parent socket
    socket: *anyopaque,

    /// Reconnection state
    reconnect: ReconnectState,

    /// Current engine (if connected)
    engine: ?*Engine,

    /// State
    state: State,

    const State = enum {
        disconnected,
        connecting,
        connected,
        terminating,
    };

    pub fn run(self: *Connector, rt: *zio.Runtime) void {
        while (self.state != .terminating) {
            self.state = .connecting;

            // Attempt connection
            const stream = self.endpoint.connect(rt) catch |err| {
                self.handleConnectError(rt, err);
                continue;
            };

            // Create engine
            self.engine = Engine.create(self.socket, stream, .client) catch |err| {
                stream.close(rt);
                self.scheduleReconnect(rt);
                continue;
            };

            self.state = .connected;
            self.reconnect.reset();

            // Run engine (blocks until disconnect)
            self.engine.?.run(rt);

            // Engine exited
            self.engine.?.destroy();
            self.engine = null;

            if (self.state != .terminating) {
                self.scheduleReconnect(rt);
            }
        }
    }

    fn scheduleReconnect(self: *Connector, rt: *zio.Runtime) void {
        self.state = .disconnected;
        const delay = self.reconnect.nextDelay();
        rt.sleep(Duration.fromMilliseconds(delay)) catch return;
    }
};
```

---

## Transport Layer

### Endpoint Parsing

```zig
pub const Endpoint = union(enum) {
    tcp: TcpEndpoint,
    ipc: IpcEndpoint,
    inproc: InprocEndpoint,

    pub fn parse(uri: []const u8) !Endpoint {
        if (std.mem.startsWith(u8, uri, "tcp://")) {
            return .{ .tcp = try TcpEndpoint.parse(uri[6..]) };
        } else if (std.mem.startsWith(u8, uri, "ipc://")) {
            return .{ .ipc = try IpcEndpoint.parse(uri[6..]) };
        } else if (std.mem.startsWith(u8, uri, "inproc://")) {
            return .{ .inproc = try InprocEndpoint.parse(uri[9..]) };
        } else {
            return error.InvalidEndpoint;
        }
    }

    pub fn connect(self: Endpoint, rt: *zio.Runtime) !zio.net.Stream {
        return switch (self) {
            .tcp => |e| e.connect(rt),
            .ipc => |e| e.connect(rt),
            .inproc => error.NotNetworkTransport,
        };
    }

    pub fn listen(self: Endpoint, rt: *zio.Runtime) !zio.net.Server {
        return switch (self) {
            .tcp => |e| e.listen(rt),
            .ipc => |e| e.listen(rt),
            .inproc => error.NotNetworkTransport,
        };
    }
};

pub const TcpEndpoint = struct {
    address: zio.net.IpAddress,

    pub fn parse(addr: []const u8) !TcpEndpoint {
        // Parse "host:port" or "*:port"
        // ...
    }

    pub fn connect(self: TcpEndpoint, rt: *zio.Runtime) !zio.net.Stream {
        return self.address.connect(rt, .{});
    }

    pub fn listen(self: TcpEndpoint, rt: *zio.Runtime) !zio.net.Server {
        return self.address.listen(rt, .{});
    }
};
```

### IPC Transport (Unix Domain Sockets)

IPC transport uses Unix domain sockets for efficient local inter-process communication. This transport is significantly faster than TCP for same-machine communication as it bypasses the network stack.

#### libzmq IPC Reference

From `src/ipc_address.cpp`:
- Uses `struct sockaddr_un` with `AF_UNIX` family
- Path stored in `sun_path` (max 108 bytes on Linux, varies by platform)
- **Abstract sockets**: Path starting with `@` is converted to `\0` prefix (Linux-only, no filesystem entry)
- **Wildcard binding**: Path `*` creates temp directory with unique socket file

From `src/ipc_listener.cpp`:
- **File cleanup**: `unlink()` before bind to remove stale socket files
- **Peer credentials**: `SO_PEERCRED` (Linux) or `LOCAL_PEERCRED` (BSD) for UID/GID/PID filtering
- Socket file deleted on close (unless using `ZMQ_USE_FD`)

#### ZZMQ IPC Implementation

```zig
pub const IpcEndpoint = struct {
    path: []const u8,
    is_abstract: bool,

    /// Maximum path length (platform-dependent)
    pub const MAX_PATH_LEN = 108; // Linux sockaddr_un.sun_path

    pub fn parse(path: []const u8) !IpcEndpoint {
        if (path.len == 0) return error.InvalidEndpoint;

        // Check for abstract socket prefix
        if (path[0] == '@') {
            if (path.len == 1) return error.InvalidEndpoint; // "@" alone invalid
            if (path.len > MAX_PATH_LEN) return error.PathTooLong;
            return .{ .path = path[1..], .is_abstract = true };
        }

        if (path.len >= MAX_PATH_LEN) return error.PathTooLong;
        return .{ .path = path, .is_abstract = false };
    }

    pub fn connect(self: IpcEndpoint, rt: *zio.Runtime) !zio.net.Stream {
        const addr = self.toSocketAddress();
        return try zio.net.Stream.connect(rt, .{ .unix = addr });
    }

    pub fn listen(self: IpcEndpoint, rt: *zio.Runtime, options: ListenOptions) !IpcListener {
        return try IpcListener.init(rt, self, options);
    }

    fn toSocketAddress(self: IpcEndpoint) std.os.sockaddr.un {
        var addr: std.os.sockaddr.un = .{
            .family = std.os.AF.UNIX,
            .path = undefined,
        };

        if (self.is_abstract) {
            // Abstract socket: first byte is NUL
            addr.path[0] = 0;
            @memcpy(addr.path[1..][0..self.path.len], self.path);
        } else {
            @memcpy(addr.path[0..self.path.len], self.path);
            addr.path[self.path.len] = 0; // NUL terminate
        }

        return addr;
    }
};

pub const IpcListener = struct {
    server: zio.net.Server,
    endpoint: IpcEndpoint,
    owns_file: bool,
    temp_dir: ?[]const u8,
    allocator: std.mem.Allocator,

    pub const ListenOptions = struct {
        /// Pre-created socket FD (for systemd socket activation)
        use_fd: ?std.os.fd_t = null,
        /// Backlog for listen()
        backlog: u31 = 128,
        /// UID filter (empty = accept all)
        uid_filter: []const std.os.uid_t = &.{},
        /// GID filter (empty = accept all)
        gid_filter: []const std.os.gid_t = &.{},
        /// PID filter (empty = accept all) - Linux only
        pid_filter: []const std.os.pid_t = &.{},
    };

    pub fn init(
        rt: *zio.Runtime,
        endpoint: IpcEndpoint,
        options: ListenOptions,
    ) !IpcListener {
        var self = IpcListener{
            .server = undefined,
            .endpoint = endpoint,
            .owns_file = false,
            .temp_dir = null,
            .allocator = rt.allocator,
        };

        if (options.use_fd) |fd| {
            // Use pre-created FD (systemd socket activation)
            self.server = zio.net.Server.fromFd(fd);
            self.owns_file = false;
        } else {
            // Handle wildcard binding
            var actual_path = endpoint.path;
            if (std.mem.eql(u8, endpoint.path, "*")) {
                const temp = try self.createWildcardSocket();
                actual_path = temp.path;
                self.temp_dir = temp.dir;
            }

            // Remove stale socket file (like libzmq)
            if (!endpoint.is_abstract) {
                std.fs.cwd().deleteFile(actual_path) catch {};
            }

            // Bind and listen
            const addr = (IpcEndpoint{ .path = actual_path, .is_abstract = endpoint.is_abstract }).toSocketAddress();
            self.server = try zio.net.Server.init(rt, .{ .unix = addr }, .{
                .backlog = options.backlog,
            });
            self.owns_file = !endpoint.is_abstract;
        }

        return self;
    }

    pub fn accept(self: *IpcListener, rt: *zio.Runtime) !zio.net.Stream {
        while (true) {
            const stream = try self.server.accept(rt);

            // Apply peer credential filters
            if (try self.filterConnection(stream)) {
                return stream;
            }

            // Connection rejected by filter
            stream.close();
        }
    }

    fn filterConnection(self: *IpcListener, stream: zio.net.Stream) !bool {
        // No filters = accept all
        if (self.options.uid_filter.len == 0 and
            self.options.gid_filter.len == 0 and
            self.options.pid_filter.len == 0)
        {
            return true;
        }

        // Get peer credentials (platform-specific)
        const creds = try getPeerCredentials(stream.handle);

        // Check UID
        for (self.options.uid_filter) |allowed_uid| {
            if (creds.uid == allowed_uid) return true;
        }

        // Check GID
        for (self.options.gid_filter) |allowed_gid| {
            if (creds.gid == allowed_gid) return true;
        }

        // Check PID (Linux only)
        for (self.options.pid_filter) |allowed_pid| {
            if (creds.pid == allowed_pid) return true;
        }

        return false;
    }

    pub fn deinit(self: *IpcListener) void {
        self.server.deinit();

        // Clean up socket file
        if (self.owns_file) {
            std.fs.cwd().deleteFile(self.endpoint.path) catch {};
        }

        // Clean up temp directory for wildcard sockets
        if (self.temp_dir) |dir| {
            std.fs.cwd().deleteTree(dir) catch {};
            self.allocator.free(dir);
        }
    }

    fn createWildcardSocket(self: *IpcListener) !struct { path: []const u8, dir: []const u8 } {
        // Create unique temp directory like libzmq
        const template = "/tmp/zzmq-XXXXXX";
        const dir = try std.fs.makeTempDir(self.allocator, template);
        const path = try std.fmt.allocPrint(self.allocator, "{s}/socket", .{dir});
        return .{ .path = path, .dir = dir };
    }
};

/// Get peer credentials from Unix socket (platform-specific)
fn getPeerCredentials(fd: std.os.fd_t) !PeerCredentials {
    if (@hasDecl(std.os, "SO") and @hasDecl(std.os.SO, "PEERCRED")) {
        // Linux: SO_PEERCRED
        var cred: extern struct {
            pid: std.os.pid_t,
            uid: std.os.uid_t,
            gid: std.os.gid_t,
        } = undefined;
        var len: std.os.socklen_t = @sizeOf(@TypeOf(cred));

        try std.os.getsockopt(fd, std.os.SOL.SOCKET, std.os.SO.PEERCRED, std.mem.asBytes(&cred), &len);

        return .{ .pid = cred.pid, .uid = cred.uid, .gid = cred.gid };
    } else if (comptime @hasDecl(std.os, "LOCAL_PEERCRED")) {
        // BSD: LOCAL_PEERCRED
        // ... BSD-specific implementation
    }

    return error.PeerCredentialsNotSupported;
}

pub const PeerCredentials = struct {
    uid: std.os.uid_t,
    gid: std.os.gid_t,
    pid: ?std.os.pid_t = null, // Linux only
};
```

#### IPC Transport Features

| Feature | libzmq | ZZMQ |
|---------|--------|------|
| Regular socket path | `ipc:///tmp/sock` | ✓ Same |
| Abstract sockets | `ipc://@abstract` | ✓ `@` prefix → `\0` prefix |
| Wildcard binding | `ipc://*` | ✓ Temp dir + unique socket |
| File cleanup | Unlink before bind, on close | ✓ Same |
| UID filtering | `ZMQ_IPC_FILTER_UID` | ✓ `ListenOptions.uid_filter` |
| GID filtering | `ZMQ_IPC_FILTER_GID` | ✓ `ListenOptions.gid_filter` |
| PID filtering | `ZMQ_IPC_FILTER_PID` | ✓ `ListenOptions.pid_filter` (Linux) |
| Socket activation | `ZMQ_USE_FD` | ✓ `ListenOptions.use_fd` |

#### Platform Considerations

**Linux:**
- Full support for abstract sockets (no filesystem entry)
- `SO_PEERCRED` provides pid, uid, gid

**macOS/BSD:**
- No abstract sockets (use regular paths)
- `LOCAL_PEERCRED` provides uid, gid (no pid)

**Windows:**
- AF_UNIX supported since Windows 10 1803
- No peer credentials available
- Path format: `ipc://C:\Users\...\socket`

### Inproc Transport

Inproc bypasses network entirely - just connects pipes directly:

```zig
pub const InprocEndpoint = struct {
    name: []const u8,

    pub fn parse(name: []const u8) !InprocEndpoint {
        if (name.len == 0 or name.len > 256) return error.InvalidEndpoint;
        return .{ .name = name };
    }
};

pub const InprocRegistry = struct {
    /// Bound endpoints: name → bound socket
    bound: std.StringHashMap(BoundEndpoint),

    /// Pending connections waiting for bind
    pending: std.StringHashMap(std.ArrayList(PendingConnect)),

    mutex: std.Thread.Mutex,

    const BoundEndpoint = struct {
        socket: *anyopaque,
        options: SocketOptions,
    };

    const PendingConnect = struct {
        socket: *anyopaque,
        pipe: *Pipe,
    };

    /// Called when socket binds to inproc
    pub fn bind(self: *InprocRegistry, name: []const u8, socket: *anyopaque, options: SocketOptions) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.bound.contains(name)) return error.AddressInUse;

        try self.bound.put(name, .{ .socket = socket, .options = options });

        // Connect any pending connectors
        if (self.pending.get(name)) |pending_list| {
            for (pending_list.items) |pending| {
                self.connectInproc(socket, options, pending.socket, pending.pipe);
            }
            pending_list.clearAndFree();
        }
    }

    /// Called when socket connects to inproc
    pub fn connect(self: *InprocRegistry, name: []const u8, socket: *anyopaque, allocator: std.mem.Allocator) !*Pipe {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Create pipe pair
        const pipe = try createPipe(allocator, getSocketOptions(socket));

        if (self.bound.get(name)) |bound| {
            // Immediate connection
            self.connectInproc(bound.socket, bound.options, socket, pipe);
            return pipe;
        }

        // No bound socket yet - queue for later
        var pending = self.pending.get(name) orelse blk: {
            const list = std.ArrayList(PendingConnect).init(allocator);
            try self.pending.put(name, list);
            break :blk self.pending.get(name).?;
        };

        try pending.append(.{ .socket = socket, .pipe = pipe });
        return pipe;
    }

    fn connectInproc(bind_socket: *anyopaque, bind_opts: SocketOptions, connect_socket: *anyopaque, pipe: *Pipe) void {
        // Create peer pipe (reverse direction)
        const peer_pipe = createPeerPipe(pipe, bind_opts);

        // Cross-connect the channels
        // pipe.outbound → peer_pipe.inbound (connect sends to bind)
        // peer_pipe.outbound → pipe.inbound (bind sends to connect)

        // Attach pipes to sockets
        attachPipeToSocket(bind_socket, peer_pipe);
        attachPipeToSocket(connect_socket, pipe);
    }
};
```

---

## ZMTP Codec

ZMTP (ZeroMQ Message Transport Protocol) is the wire protocol for ZMQ. We implement ZMTP 3.1 for compatibility with libzmq.

### Protocol Overview

```
Connection lifecycle:
1. Greeting exchange (64 bytes each direction)
2. Handshake (mechanism-specific: NULL, PLAIN, CURVE)
3. Ready command with metadata
4. Message frames
```

### Frame Format

```
┌──────────┬──────────┬───────────────────────────────────────┐
│  Flags   │  Size    │              Body                      │
│ (1 byte) │ (1-8 b)  │          (Size bytes)                  │
└──────────┴──────────┴───────────────────────────────────────┘

Flags byte:
  bit 0: MORE (1 = more frames follow)
  bit 1: LONG (1 = 8-byte size, 0 = 1-byte size)
  bit 2: COMMAND (1 = command frame, 0 = message frame)
```

### Codec Structure

```zig
pub const ZmtpCodec = struct {
    /// Protocol state machine
    state: State,

    /// Negotiated properties
    properties: Properties,

    /// Decode buffer for partial frames
    decode_buffer: std.ArrayList(u8),

    /// Partial frame state
    partial: ?PartialFrame,

    const State = enum {
        /// Waiting to send/receive greeting
        greeting,
        /// Performing security handshake
        handshaking,
        /// Exchanging READY commands
        ready_exchange,
        /// Normal message flow
        traffic,
        /// Error state
        failed,
    };

    const Properties = struct {
        /// Peer's socket type
        peer_socket_type: ?SocketType = null,
        /// Peer's identity
        peer_identity: ?[]const u8 = null,
        /// Security mechanism
        mechanism: Mechanism = .null_,
        /// As-server flag
        as_server: bool = false,
    };

    const PartialFrame = struct {
        flags: u8,
        size: u64,
        body_received: usize,
    };

    /// Initialize codec for a new connection
    pub fn init(allocator: std.mem.Allocator, role: Role, socket_type: SocketType) ZmtpCodec {
        return .{
            .state = .greeting,
            .properties = .{},
            .decode_buffer = std.ArrayList(u8).init(allocator),
            .partial = null,
        };
    }

    pub fn deinit(self: *ZmtpCodec) void {
        self.decode_buffer.deinit();
    }
};
```

### Greeting

```zig
/// ZMTP 3.1 greeting (64 bytes)
pub const Greeting = extern struct {
    signature: [10]u8,      // 0xFF, size[8], 0x7F
    version: [2]u8,         // major, minor (3, 1)
    mechanism: [20]u8,      // "NULL" + padding
    as_server: u8,          // 0 or 1
    filler: [31]u8,         // zeros

    pub fn init(mechanism: Mechanism, as_server: bool) Greeting {
        var g: Greeting = undefined;

        // Signature
        g.signature[0] = 0xFF;
        @memset(g.signature[1..9], 0);
        g.signature[9] = 0x7F;

        // Version 3.1
        g.version[0] = 3;
        g.version[1] = 1;

        // Mechanism (NULL, PLAIN, CURVE)
        @memset(&g.mechanism, 0);
        const mech_name = mechanism.name();
        @memcpy(g.mechanism[0..mech_name.len], mech_name);

        // As-server
        g.as_server = if (as_server) 1 else 0;

        // Filler
        @memset(&g.filler, 0);

        return g;
    }

    pub fn validate(self: *const Greeting) !void {
        if (self.signature[0] != 0xFF or self.signature[9] != 0x7F) {
            return error.InvalidSignature;
        }
        if (self.version[0] < 3) {
            return error.UnsupportedVersion;
        }
    }
};
```

### Encoding Messages

```zig
/// Encode a message to wire format
/// Returns slice of encoded data (written to buf)
pub fn encodeMessage(self: *ZmtpCodec, buf: []u8, msg: *const Message) ![]u8 {
    const data = msg.slice();
    var offset: usize = 0;

    // Flags byte
    var flags: u8 = 0;
    if (msg.flags.more) flags |= 0x01;
    if (data.len > 255) flags |= 0x02;  // LONG flag
    // Command flag (0x04) not set for messages

    buf[offset] = flags;
    offset += 1;

    // Size
    if (data.len > 255) {
        // 8-byte size (big-endian)
        std.mem.writeInt(u64, buf[offset..][0..8], data.len, .big);
        offset += 8;
    } else {
        // 1-byte size
        buf[offset] = @intCast(data.len);
        offset += 1;
    }

    // Body
    if (offset + data.len > buf.len) return error.BufferTooSmall;
    @memcpy(buf[offset .. offset + data.len], data);
    offset += data.len;

    return buf[0..offset];
}

/// Encode a command frame (READY, PING, PONG, etc.)
pub fn encodeCommand(self: *ZmtpCodec, buf: []u8, cmd: Command) ![]u8 {
    var offset: usize = 0;

    // Flags: COMMAND bit set
    const flags: u8 = 0x04 | (if (cmd.bodyLen() > 255) @as(u8, 0x02) else 0);
    buf[offset] = flags;
    offset += 1;

    // Size
    const total_len = 1 + cmd.name.len + cmd.bodyLen();
    if (total_len > 255) {
        std.mem.writeInt(u64, buf[offset..][0..8], total_len, .big);
        offset += 8;
    } else {
        buf[offset] = @intCast(total_len);
        offset += 1;
    }

    // Command name (length-prefixed)
    buf[offset] = @intCast(cmd.name.len);
    offset += 1;
    @memcpy(buf[offset .. offset + cmd.name.len], cmd.name);
    offset += cmd.name.len;

    // Command body
    offset += try cmd.encodeBody(buf[offset..]);

    return buf[0..offset];
}
```

### Decoding Messages

```zig
/// Decode result
pub const DecodeResult = union(enum) {
    /// Successfully decoded a frame
    frame: DecodedFrame,
    /// Need more data (returns bytes needed)
    need_more: usize,
    /// Protocol error
    protocol_error: ProtocolError,
};

pub const DecodedFrame = struct {
    /// Frame type
    frame_type: FrameType,
    /// Bytes consumed from input
    consumed: usize,
    /// Decoded content
    content: union(enum) {
        message: Message,
        command: Command,
    },

    const FrameType = enum { message, command };
};

/// Decode a frame from wire data
/// May return need_more if buffer doesn't contain complete frame
pub fn decode(self: *ZmtpCodec, data: []const u8, allocator: std.mem.Allocator) DecodeResult {
    if (data.len == 0) return .{ .need_more = 1 };

    var offset: usize = 0;

    // Parse flags
    const flags = data[offset];
    offset += 1;

    const has_more = (flags & 0x01) != 0;
    const is_long = (flags & 0x02) != 0;
    const is_command = (flags & 0x04) != 0;

    // Parse size
    const size_bytes: usize = if (is_long) 8 else 1;
    if (data.len < offset + size_bytes) {
        return .{ .need_more = offset + size_bytes - data.len };
    }

    const body_len: u64 = if (is_long)
        std.mem.readInt(u64, data[offset..][0..8], .big)
    else
        data[offset];
    offset += size_bytes;

    // Check we have full body
    if (data.len < offset + body_len) {
        return .{ .need_more = offset + body_len - data.len };
    }

    const body = data[offset .. offset + body_len];
    offset += body_len;

    // Decode based on frame type
    if (is_command) {
        const cmd = Command.decode(body) catch |err| {
            return .{ .protocol_error = .{ .command_decode = err } };
        };
        return .{ .frame = .{
            .frame_type = .command,
            .consumed = offset,
            .content = .{ .command = cmd },
        } };
    } else {
        // Message frame
        var msg = Message.initFromSlice(allocator, body) catch |err| {
            return .{ .protocol_error = .{ .allocation = err } };
        };
        msg.flags.more = has_more;
        return .{ .frame = .{
            .frame_type = .message,
            .consumed = offset,
            .content = .{ .message = msg },
        } };
    }
}
```

### Commands

```zig
pub const Command = struct {
    name: []const u8,
    properties: std.StringHashMap([]const u8),

    // Standard commands
    pub const READY = "READY";
    pub const ERROR = "ERROR";
    pub const SUBSCRIBE = "SUBSCRIBE";
    pub const CANCEL = "CANCEL";
    pub const PING = "PING";
    pub const PONG = "PONG";

    /// Create READY command with socket metadata
    pub fn ready(allocator: std.mem.Allocator, socket_type: SocketType, identity: ?[]const u8) !Command {
        var props = std.StringHashMap([]const u8).init(allocator);
        try props.put("Socket-Type", socket_type.name());
        if (identity) |id| {
            try props.put("Identity", id);
        }
        return .{ .name = READY, .properties = props };
    }

    /// Create PING command
    pub fn ping(context: []const u8) Command {
        return .{ .name = PING, .context = context };
    }

    /// Create SUBSCRIBE command (for XPUB)
    pub fn subscribe(prefix: []const u8) Command {
        return .{ .name = SUBSCRIBE, .subscription = prefix };
    }
};
```

### Engine Integration

The codec integrates with Engine's reader/writer loops:

```zig
fn readerLoop(self: *Engine, rt: *zio.Runtime) void {
    var read_buf: [65536]u8 = undefined;
    var decode_offset: usize = 0;

    while (self.state == .ready) {
        // Read more data from network (ZIO stream.recv API)
        const n = self.stream.recv(rt, read_buf[decode_offset..], .{}) catch |err| {
            self.handleReadError(err);
            break;
        };

        if (n == 0) break;  // EOF

        const available = decode_offset + n;

        // Decode all complete frames
        var consumed: usize = 0;
        while (consumed < available) {
            const result = self.codec.decode(
                read_buf[consumed..available],
                self.allocator,
            );

            switch (result) {
                .frame => |frame| {
                    consumed += frame.consumed;
                    self.handleFrame(rt, frame);
                },
                .need_more => break,  // Wait for more data
                .protocol_error => |err| {
                    self.handleProtocolError(err);
                    return;
                },
            }
        }

        // Move unconsumed data to start of buffer
        if (consumed > 0 and consumed < available) {
            std.mem.copyForwards(u8, &read_buf, read_buf[consumed..available]);
        }
        decode_offset = available - consumed;
    }
}
```

---

## Socket Patterns

### Pattern Interface

Each pattern implements:

```zig
pub fn PatternInterface(comptime Self: type) type {
    return struct {
        /// Called when pipe attached
        pub fn onPipeAttached(state: *Self.State, pipe: *Pipe) void;

        /// Called when pipe detached
        pub fn onPipeDetached(state: *Self.State, pipe: *Pipe) void;

        /// Send implementation
        pub fn send(
            state: *Self.State,
            pipes: *PipeSet,
            msg: *Message,
            timeout: Timeout,
            rt: *zio.Runtime,
        ) SendError!void;

        /// Receive implementation
        pub fn recv(
            state: *Self.State,
            pipes: *PipeSet,
            timeout: Timeout,
            rt: *zio.Runtime,
        ) RecvError!Message;

        /// Check if send would succeed
        pub fn canSend(state: *const Self.State, pipes: *const PipeSet) bool;

        /// Check if recv would succeed
        pub fn canRecv(state: *const Self.State, pipes: *const PipeSet) bool;
    };
}
```

### PUSH Pattern

```zig
pub const Push = struct {
    pub const State = struct {
        /// Load balancer index
        lb_index: usize = 0,
    };

    pub fn onPipeAttached(state: *State, pipe: *Pipe) void {
        _ = state;
        _ = pipe;
        // PUSH accepts all pipes for sending
    }

    pub fn onPipeDetached(state: *State, pipe: *Pipe) void {
        _ = state;
        _ = pipe;
    }

    pub fn send(
        state: *State,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        // Round-robin load balance across writable pipes
        const start_index = state.lb_index;
        var attempts: usize = 0;

        while (attempts < pipes.count()) : (attempts += 1) {
            const pipe = pipes.getByIndex(state.lb_index) orelse continue;
            state.lb_index = (state.lb_index + 1) % pipes.count();

            // Try this pipe
            if (pipe.tryWrite(msg.*)) |_| {
                return;  // Success
            } else |err| switch (err) {
                error.PipeClosed => continue,  // Try next
                else => {},  // HWM reached, try next
            }
        }

        // All pipes full or closed - block on first available
        // Use select to wait on any pipe becoming writable
        return blockOnWritable(pipes, msg, timeout, rt);
    }

    pub fn recv(state: *State, pipes: *PipeSet, timeout: Timeout, rt: *zio.Runtime) RecvError!Message {
        _ = state;
        _ = pipes;
        _ = timeout;
        _ = rt;
        return error.NotSupported;  // PUSH can't receive
    }

    pub fn canSend(state: *const State, pipes: *const PipeSet) bool {
        _ = state;
        return pipes.hasWritable();
    }

    pub fn canRecv(state: *const State, pipes: *const PipeSet) bool {
        _ = state;
        _ = pipes;
        return false;
    }
};
```

### PULL Pattern

```zig
pub const Pull = struct {
    pub const State = struct {
        /// Fair queue index
        fq_index: usize = 0,
    };

    pub fn send(state: *State, pipes: *PipeSet, msg: *Message, timeout: Timeout, rt: *zio.Runtime) SendError!void {
        _ = state;
        _ = pipes;
        _ = msg;
        _ = timeout;
        _ = rt;
        return error.NotSupported;
    }

    pub fn recv(
        state: *State,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        // Fair queue across readable pipes
        const start_index = state.fq_index;
        var attempts: usize = 0;

        while (attempts < pipes.count()) : (attempts += 1) {
            const pipe = pipes.getByIndex(state.fq_index) orelse continue;
            state.fq_index = (state.fq_index + 1) % pipes.count();

            if (pipe.tryRead()) |msg| {
                return msg;
            } else |err| switch (err) {
                error.PipeClosed => continue,
                else => {},  // Empty, try next
            }
        }

        // No data available - block waiting for any pipe
        return blockOnReadable(pipes, timeout, rt);
    }

    pub fn canSend(state: *const State, pipes: *const PipeSet) bool {
        _ = state;
        _ = pipes;
        return false;
    }

    pub fn canRecv(state: *const State, pipes: *const PipeSet) bool {
        _ = state;
        return pipes.hasReadable();
    }
};
```

### PUB Pattern

```zig
pub const Pub = struct {
    pub const State = struct {
        // PUB has no special state (broadcasts to all)
    };

    pub fn send(
        state: *State,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        _ = state;
        _ = timeout;

        var iter = pipes.iterator();
        var first = true;

        while (iter.next()) |pipe| {
            // Share message (refcount) for all but last
            const to_send = if (first) blk: {
                first = false;
                // Check if there's a next - if so, share
                if (iter.peek() != null) {
                    break :blk try msg.share(rt.allocator);
                } else {
                    break :blk msg.*;  // Move last one
                }
            } else try msg.share(rt.allocator);

            // PUB drops messages when HWM reached (no backpressure)
            _ = pipe.tryWrite(to_send) catch |err| switch (err) {
                error.PipeClosed => continue,
                else => continue,  // HWM drop
            };
        }
    }

    pub fn recv(state: *State, pipes: *PipeSet, timeout: Timeout, rt: *zio.Runtime) RecvError!Message {
        _ = state;
        _ = pipes;
        _ = timeout;
        _ = rt;
        return error.NotSupported;
    }
};
```

### SUB Pattern

```zig
pub const Sub = struct {
    pub const State = struct {
        /// Subscription filters (prefix trie)
        subscriptions: SubscriptionTrie,
        /// Fair queue index
        fq_index: usize = 0,
    };

    pub fn recv(
        state: *State,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        // Fair queue, but filter by subscription
        while (true) {
            const msg = try Pull.recv(@ptrCast(state), pipes, timeout, rt);

            // Check subscription filter
            if (state.subscriptions.matches(msg.slice())) {
                return msg;
            }

            // Doesn't match - drop and try again
            msg.deinit();
        }
    }

    pub fn subscribe(state: *State, prefix: []const u8) !void {
        try state.subscriptions.insert(prefix);
        // TODO: Send subscription to connected PUBs
    }

    pub fn unsubscribe(state: *State, prefix: []const u8) void {
        state.subscriptions.remove(prefix);
        // TODO: Send unsubscription to connected PUBs
    }
};
```

### REQ Pattern

```zig
pub const Req = struct {
    pub const State = struct {
        /// Currently expecting reply from this pipe
        expecting_reply: ?*Pipe = null,
        /// Load balancer for requests
        lb_index: usize = 0,
        /// Options
        relaxed: bool = false,
        correlate: bool = false,
        /// Request ID for correlation
        request_id: u32 = 0,
    };

    pub fn send(
        state: *State,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        // Strict REQ/REP: must recv before next send
        if (!state.relaxed and state.expecting_reply != null) {
            return error.InvalidState;
        }

        // Add empty delimiter frame (REQ envelope)
        var envelope = Message.init();
        envelope.setMore(true);

        // Find pipe via load balancer
        const pipe = pipes.nextWritable() orelse return error.NoRoute;

        // Send envelope + message
        try pipe.write(rt, envelope);
        try pipe.write(rt, msg.*);

        state.expecting_reply = pipe;
    }

    pub fn recv(
        state: *State,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        const pipe = state.expecting_reply orelse {
            if (state.relaxed) {
                // Relaxed mode: recv from any
                return Pull.recv(@ptrCast(state), pipes, timeout, rt);
            }
            return error.InvalidState;
        };

        // Read from expected pipe
        const envelope = try pipe.read(rt);
        if (envelope.len() != 0) return error.ProtocolError;
        envelope.deinit();

        const msg = try pipe.read(rt);
        state.expecting_reply = null;
        return msg;
    }
};
```

### REP Pattern

REP is the server side of REQ/REP. It inherits from ROUTER but adds a strict state machine
that enforces recv-send-recv-send alternation. Key insight: it automatically copies the
routing envelope on recv and uses it on send, so the user never sees routing IDs.

**libzmq reference** (`src/rep.cpp`):
- Inherits from `router_t`
- Two flags: `_sending_reply` and `_request_begins`
- On recv: reads envelope frames until empty delimiter, copies them to reply pipe
- On send: uses ROUTER's send which routes via the copied envelope
- State flips when complete message (MORE=false) is processed

```zig
pub const Rep = struct {
    pub const State = struct {
        /// State machine phase
        phase: Phase = .receiving,

        /// Envelope frames copied from request (for routing reply)
        /// Stored in arena, cleared after each reply
        envelope: std.ArrayList(Message),

        /// Arena for envelope storage
        envelope_arena: std.heap.ArenaAllocator,

        const Phase = enum {
            /// Waiting for request (recv allowed, send blocked)
            receiving,
            /// Request received, waiting to send reply (send allowed, recv blocked)
            sending,
        };
    };

    pub fn init(allocator: std.mem.Allocator) State {
        return .{
            .envelope = std.ArrayList(Message).init(allocator),
            .envelope_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn recv(
        state: *State,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        // State check: can't recv while sending
        if (state.phase == .sending) {
            return error.InvalidState;  // EFSM in libzmq
        }

        // If this is start of new request, read and store envelope
        if (state.envelope.items.len == 0) {
            try state.readEnvelope(pipes, rt);
        }

        // Read the actual request frame(s)
        const msg = try Router.recv(undefined, pipes, timeout, rt);

        // If complete message, transition to sending phase
        if (!msg.flags.more) {
            state.phase = .sending;
        }

        return msg;
    }

    pub fn send(
        state: *State,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        // State check: can't send while receiving
        if (state.phase == .receiving) {
            return error.InvalidState;  // EFSM in libzmq
        }

        // First send of reply: write stored envelope first
        if (state.envelope.items.len > 0) {
            for (state.envelope.items) |envelope_frame| {
                var frame = envelope_frame;
                frame.flags.more = true;
                try Router.send(undefined, pipes, &frame, timeout, rt);
            }
            state.envelope.clearRetainingCapacity();
            _ = state.envelope_arena.reset(.retain_capacity);
        }

        // Send the reply frame
        try Router.send(undefined, pipes, msg, timeout, rt);

        // If complete message, transition back to receiving phase
        if (!msg.flags.more) {
            state.phase = .receiving;
        }
    }

    /// Read envelope frames (routing IDs + empty delimiter) from request
    fn readEnvelope(state: *State, pipes: *PipeSet, rt: *zio.Runtime) !void {
        const alloc = state.envelope_arena.allocator();

        while (true) {
            var frame = try Router.recv(undefined, pipes, .{}, rt);

            if (!frame.flags.more) {
                // Malformed: no delimiter before payload
                // Discard and reset
                frame.deinit();
                state.envelope.clearRetainingCapacity();
                return error.ProtocolError;
            }

            // Empty frame = delimiter, we're done with envelope
            if (frame.len() == 0) {
                // Store the empty delimiter too
                try state.envelope.append(frame);
                break;
            }

            // Copy frame data to arena (original may be transient)
            const data_copy = try alloc.dupe(u8, frame.data());
            var stored = frame;
            stored.setData(data_copy);
            try state.envelope.append(stored);
        }
    }

    pub fn hasIn(state: *State, pipes: *PipeSet) bool {
        if (state.phase == .sending) return false;
        return Router.hasIn(undefined, pipes);
    }

    pub fn hasOut(state: *State, pipes: *PipeSet) bool {
        if (state.phase == .receiving) return false;
        return Router.hasOut(undefined, pipes);
    }
};
```

**Zig/ZIO optimizations:**
1. **Tagged enum for state** - Compile-time checked, clearer than boolean flags
2. **Arena for envelope** - Batch deallocation after each reply cycle
3. **No polling loops** - ZIO channels suspend coroutine when waiting

### DEALER Pattern

DEALER is the async counterpart to REQ. It has no state machine restrictions - you can
send and recv freely. It load-balances sends (round-robin) and fair-queues receives.

**libzmq reference** (`src/dealer.cpp`):
- Uses `fq_t` for fair-queue receives
- Uses `lb_t` for load-balanced sends
- `_probe_router` option: send empty message on connect (for ROUTER peer awareness)
- `sendpipe`/`recvpipe` variants expose which pipe was used

```zig
pub const Dealer = struct {
    pub const State = struct {
        /// Fair queue state for receives
        fq: FairQueue = .{},

        /// Load balancer state for sends
        lb: LoadBalancer = .{},

        /// Send empty message to routers on connect
        probe_router: bool = false,
    };

    pub fn onPipeAttached(state: *State, pipe: *Pipe, rt: *zio.Runtime) void {
        state.fq.attach(pipe);
        state.lb.attach(pipe);

        // Probe router: send empty message so ROUTER assigns routing ID
        if (state.probe_router) {
            var probe = Message.initEmpty();
            pipe.trySend(probe) catch {};
        }
    }

    pub fn onPipeDetached(state: *State, pipe: *Pipe) void {
        state.fq.detach(pipe);
        state.lb.detach(pipe);
    }

    pub fn send(
        state: *State,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        return state.lb.send(pipes, msg, timeout, rt);
    }

    pub fn recv(
        state: *State,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        return state.fq.recv(pipes, timeout, rt);
    }

    /// Send and return which pipe was used (for correlating replies)
    pub fn sendPipe(
        state: *State,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!*Pipe {
        return state.lb.sendPipe(pipes, msg, timeout, rt);
    }

    /// Recv and return which pipe it came from
    pub fn recvPipe(
        state: *State,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!struct { msg: Message, pipe: *Pipe } {
        return state.fq.recvPipe(pipes, timeout, rt);
    }

    pub fn hasIn(state: *State) bool {
        return state.fq.hasIn();
    }

    pub fn hasOut(state: *State) bool {
        return state.lb.hasOut();
    }
};

/// Fair queue: round-robin receives from active pipes
/// libzmq: src/fq.cpp
pub const FairQueue = struct {
    /// Current pipe index for round-robin
    current: usize = 0,

    /// Multipart state: if true, must continue from same pipe
    receiving_multipart: bool = false,

    /// Pipe that current multipart is coming from
    multipart_pipe: ?*Pipe = null,

    pub fn recv(
        self: *FairQueue,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        const result = try self.recvPipe(pipes, timeout, rt);
        return result.msg;
    }

    pub fn recvPipe(
        self: *FairQueue,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!struct { msg: Message, pipe: *Pipe } {
        // If mid-multipart, MUST read from same pipe (atomicity)
        if (self.receiving_multipart) {
            const pipe = self.multipart_pipe.?;
            const msg = try pipe.inbound.receive(rt);

            if (!msg.flags.more) {
                self.receiving_multipart = false;
                self.multipart_pipe = null;
                self.advanceRoundRobin(pipes);
            }

            return .{ .msg = msg, .pipe = pipe };
        }

        // Try current pipe first (fast path)
        const active = pipes.activePipes();
        if (active.len == 0) {
            return error.NoConnections;
        }

        // Try non-blocking from current
        const start = self.current % active.len;
        var idx = start;

        while (true) {
            const pipe = active[idx];
            if (pipe.inbound.tryReceive()) |msg| {
                if (msg.flags.more) {
                    self.receiving_multipart = true;
                    self.multipart_pipe = pipe;
                } else {
                    self.current = (idx + 1) % active.len;
                }
                return .{ .msg = msg, .pipe = pipe };
            }

            idx = (idx + 1) % active.len;
            if (idx == start) break;  // Checked all, none ready
        }

        // All empty - wait with select
        const ready_pipe = try pipes.waitAnyReadable(timeout, rt);
        const msg = try ready_pipe.inbound.receive(rt);

        if (msg.flags.more) {
            self.receiving_multipart = true;
            self.multipart_pipe = ready_pipe;
        } else {
            self.current = (pipes.indexOf(ready_pipe) + 1) % active.len;
        }

        return .{ .msg = msg, .pipe = ready_pipe };
    }

    fn advanceRoundRobin(self: *FairQueue, pipes: *PipeSet) void {
        const active = pipes.activePipes();
        if (active.len > 0) {
            self.current = (self.current + 1) % active.len;
        }
    }

    pub fn hasIn(self: *FairQueue, pipes: *PipeSet) bool {
        if (self.receiving_multipart) return true;

        for (pipes.activePipes()) |pipe| {
            if (pipe.inbound.canReceive()) return true;
        }
        return false;
    }
};

/// Load balancer: round-robin sends to active pipes
/// libzmq: src/lb.cpp
pub const LoadBalancer = struct {
    /// Current pipe index for round-robin
    current: usize = 0,

    /// Multipart state
    state: SendState = .idle,

    /// Pipe that current multipart is going to
    multipart_pipe: ?*Pipe = null,

    const SendState = enum {
        idle,
        sending_multipart,
        /// Mid-multipart pipe disconnected, drop remaining frames
        dropping,
    };

    pub fn send(
        self: *LoadBalancer,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        _ = try self.sendPipe(pipes, msg, timeout, rt);
    }

    pub fn sendPipe(
        self: *LoadBalancer,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!*Pipe {
        // Handle dropping mode (mid-multipart disconnect)
        if (self.state == .dropping) {
            if (!msg.flags.more) {
                self.state = .idle;
                self.multipart_pipe = null;
            }
            // Silently discard, return success (libzmq compat)
            return self.multipart_pipe.?;
        }

        // If mid-multipart, MUST send to same pipe (atomicity)
        if (self.state == .sending_multipart) {
            const pipe = self.multipart_pipe.?;

            // Check if pipe still active
            if (!pipe.isActive()) {
                // Pipe disconnected mid-multipart - enter dropping mode
                pipe.rollback();
                self.state = .dropping;
                if (!msg.flags.more) {
                    self.state = .idle;
                    self.multipart_pipe = null;
                }
                return pipe;
            }

            try pipe.outbound.send(rt, msg.*);

            if (!msg.flags.more) {
                pipe.flush();
                self.state = .idle;
                self.multipart_pipe = null;
                self.advanceRoundRobin(pipes);
            }

            return pipe;
        }

        // Find writable pipe via round-robin
        const active = pipes.activePipes();
        if (active.len == 0) {
            return error.NoConnections;
        }

        const start = self.current % active.len;
        var idx = start;

        while (true) {
            const pipe = active[idx];
            if (pipe.outbound.canSend()) {
                try pipe.outbound.send(rt, msg.*);

                if (msg.flags.more) {
                    self.state = .sending_multipart;
                    self.multipart_pipe = pipe;
                } else {
                    pipe.flush();
                    self.current = (idx + 1) % active.len;
                }

                return pipe;
            }

            idx = (idx + 1) % active.len;
            if (idx == start) break;  // All full
        }

        // All pipes full - wait with select
        const ready_pipe = try pipes.waitAnyWritable(timeout, rt);
        try ready_pipe.outbound.send(rt, msg.*);

        if (msg.flags.more) {
            self.state = .sending_multipart;
            self.multipart_pipe = ready_pipe;
        } else {
            ready_pipe.flush();
            self.current = (pipes.indexOf(ready_pipe) + 1) % active.len;
        }

        return ready_pipe;
    }

    fn advanceRoundRobin(self: *LoadBalancer, pipes: *PipeSet) void {
        const active = pipes.activePipes();
        if (active.len > 0) {
            self.current = (self.current + 1) % active.len;
        }
    }

    pub fn hasOut(self: *LoadBalancer, pipes: *PipeSet) bool {
        // If mid-multipart, we must continue (can't switch pipes)
        if (self.state == .sending_multipart) return true;

        for (pipes.activePipes()) |pipe| {
            if (pipe.outbound.canSend()) return true;
        }
        return false;
    }
};
```

**Zig/ZIO optimizations:**
1. **Tagged enum for send state** - `idle`, `sending_multipart`, `dropping` is clearer than booleans
2. **Struct return** - `recvPipe` returns `{msg, pipe}` - no out-parameters
3. **ZIO select for waiting** - No polling loops, just `waitAnyReadable/Writable`
4. **Index arithmetic** - Simple modulo, no atomic operations needed (single-threaded)

### PAIR Pattern

PAIR is the simplest pattern - exactly one peer, no routing, no load balancing.
Often used for inproc coordination between threads/coroutines.

**libzmq reference** (`src/pair.cpp`):
- Single `_pipe` pointer (nullable)
- Rejects additional connections (`pipe_->terminate(false)`)
- Direct read/write to the pipe
- Flush only on complete message (MORE=false)

```zig
pub const Pair = struct {
    pub const State = struct {
        /// The single peer pipe (null if disconnected)
        pipe: ?*Pipe = null,
    };

    pub fn onPipeAttached(state: *State, pipe: *Pipe) void {
        if (state.pipe == null) {
            state.pipe = pipe;
        } else {
            // PAIR only allows one connection - reject additional
            pipe.terminate(false);
        }
    }

    pub fn onPipeDetached(state: *State, pipe: *Pipe) void {
        if (state.pipe == pipe) {
            state.pipe = null;
        }
    }

    pub fn send(
        state: *State,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        const pipe = state.pipe orelse return error.NoConnection;

        try pipe.outbound.send(rt, msg.*);

        // Flush on complete message
        if (!msg.flags.more) {
            pipe.flush();
        }
    }

    pub fn recv(
        state: *State,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        const pipe = state.pipe orelse return error.NoConnection;

        return pipe.inbound.receive(rt);
    }

    pub fn trySend(state: *State, msg: *Message) SendError!bool {
        const pipe = state.pipe orelse return error.NoConnection;

        if (pipe.outbound.trySend(msg.*)) {
            if (!msg.flags.more) {
                pipe.flush();
            }
            return true;
        }
        return false;
    }

    pub fn tryRecv(state: *State) RecvError!?Message {
        const pipe = state.pipe orelse return error.NoConnection;
        return pipe.inbound.tryReceive();
    }

    pub fn hasIn(state: *State) bool {
        const pipe = state.pipe orelse return false;
        return pipe.inbound.canReceive();
    }

    pub fn hasOut(state: *State) bool {
        const pipe = state.pipe orelse return false;
        return pipe.outbound.canSend();
    }
};
```

**Zig/ZIO optimizations:**
1. **Optional type** - `?*Pipe` is idiomatic Zig, clear null handling
2. **Minimal abstraction** - No FQ/LB, just direct pipe operations
3. **Good for inproc** - Fast path for same-context communication

### Pattern Summary

| Pattern | Send Strategy | Recv Strategy | State Machine | Key Feature |
|---------|--------------|---------------|---------------|-------------|
| **PUSH** | Load balance | N/A | None | Fire-and-forget |
| **PULL** | N/A | Fair queue | None | Collect from many |
| **PUB** | Filtered multicast | N/A | None | Topic filtering |
| **SUB** | N/A | Filtered recv | None | Subscriptions |
| **REQ** | Load balance | From same pipe | Send→Recv | Correlation |
| **REP** | To envelope pipe | Fair queue | Recv→Send | Auto-routing |
| **DEALER** | Load balance | Fair queue | None | Async REQ |
| **ROUTER** | By routing ID | Fair queue | None | Manual routing |
| **PAIR** | Single pipe | Single pipe | None | 1:1 only |

### ROUTER Pattern

```zig
pub const Router = struct {
    pub const State = struct {
        /// Next routing ID to assign
        next_routing_id: u32 = 1,
        /// Options
        mandatory: bool = false,
    };

    pub fn onPipeAttached(state: *State, pipe: *Pipe) void {
        // Assign routing ID if not set
        if (pipe.routing_id == null) {
            var id_bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &id_bytes, state.next_routing_id, .big);
            pipe.routing_id = RoutingId.fromSlice(&id_bytes) catch unreachable;
            state.next_routing_id += 1;
        }
    }

    pub fn send(
        state: *State,
        pipes: *PipeSet,
        msg: *Message,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) SendError!void {
        // First frame is routing ID
        const routing_id = msg.routing_id orelse return error.InvalidMessage;

        // Look up pipe by routing ID
        const pipe = pipes.getByRoutingId(routing_id.slice()) orelse {
            if (state.mandatory) {
                return error.HostUnreachable;
            }
            return;  // Silently drop
        };

        // Remove routing ID from message for wire
        const wire_msg = msg.*;
        wire_msg.routing_id = null;

        try pipe.write(rt, wire_msg);
    }

    pub fn recv(
        state: *State,
        pipes: *PipeSet,
        timeout: Timeout,
        rt: *zio.Runtime,
    ) RecvError!Message {
        _ = state;

        // Fair queue from all pipes
        const pipe = pipes.nextReadable() orelse {
            return blockOnReadable(pipes, timeout, rt);
        };

        var msg = try pipe.read(rt);

        // Prepend routing ID
        msg.routing_id = pipe.routing_id;

        return msg;
    }
};
```

### XPUB/XSUB Patterns

XPUB and XSUB are the "extended" versions of PUB and SUB that expose subscription messages to the application layer, enabling subscription forwarding through intermediary proxies.

#### Key Differences from PUB/SUB

| Aspect | PUB/SUB | XPUB/XSUB |
|--------|---------|-----------|
| Subscription API | `setsockopt(ZMQ_SUBSCRIBE)` | `send()` subscription message |
| Subscription visibility | Hidden | Exposed via `recv()` on XPUB |
| Upstream data | Not supported | XSUB can send user messages |
| Filtering | Automatic at SUB | Configurable at XSUB |
| Use case | Simple pub/sub | Proxies, logging, manual control |

#### Subscription Message Formats

ZZMQ supports both legacy wire format and ZMTP 3.1 commands:

```
Legacy Wire Format (sent by application):
  Subscribe:   [0x01][prefix bytes...]
  Unsubscribe: [0x00][prefix bytes...]

ZMTP 3.1 Commands (internal, on wire):
  SUBSCRIBE command with body = prefix
  CANCEL command with body = prefix

Both formats are equivalent - ZMTP 3.1 is used on the wire,
legacy format is used at the API level for backwards compatibility.
```

#### libzmq XPUB/XSUB Reference

From `src/xpub.cpp` and `src/xsub.cpp`:

**XPUB (Extended Publisher):**
- Receives subscription messages from subscribers as readable messages
- `_subscriptions` mtrie tracks which pipes subscribe to which prefixes
- On send: matches topic against subscriptions, distributes to matching pipes
- Options:
  - `ZMQ_XPUB_VERBOSE`: Report all subscribe/unsubscribe (not just first/last)
  - `ZMQ_XPUB_VERBOSER`: Also report unsubscribes verbosely
  - `ZMQ_XPUB_MANUAL`: Application controls subscription acceptance
  - `ZMQ_XPUB_NODROP`: Block instead of drop when HWM reached
  - `ZMQ_XPUB_WELCOME_MSG`: Send message to new subscribers
  - `ZMQ_ONLY_FIRST_SUBSCRIBE`: Only first frame considered for subscription
  - `ZMQ_INVERT_MATCHING`: Send to pipes NOT matching
  - `ZMQ_TOPICS_COUNT`: Query number of unique subscription prefixes

**XSUB (Extended Subscriber):**
- Sends subscription messages upstream (via `send()`, not setsockopt)
- Caches subscriptions locally in `_subscriptions` trie
- On reconnect (`xhiccuped`): resends all cached subscriptions
- `options.filter`: When true (default for SUB), filters incoming messages
- Options:
  - `ZMQ_ONLY_FIRST_SUBSCRIBE`: Only first frame triggers subscription logic
  - `ZMQ_XSUB_VERBOSE_UNSUBSCRIBE`: Forward all unsubscribes (draft API)
  - `ZMQ_TOPICS_COUNT`: Query number of local subscriptions

**Key Insight**: XPUB/XSUB enable building subscription-forwarding proxies. The proxy receives subscription messages from XPUB, forwards them to XSUB, which sends them upstream to the real publisher.

#### ZZMQ XPUB Implementation

```zig
pub const XPub = struct {
    /// Subscription trie: prefix → set of subscribed pipes
    subscriptions: SubscriptionTrie,

    /// Pending subscription notifications to deliver via recv()
    pending_notifications: std.ArrayList(SubscriptionNotification),

    /// Pending upstream data messages (non-subscription)
    pending_data: std.ArrayList(Message),

    /// Distributor for fan-out
    dist: Distributor,

    /// Options
    verbose_subs: bool = false,      // ZMQ_XPUB_VERBOSE
    verbose_unsubs: bool = false,    // ZMQ_XPUB_VERBOSER
    manual_mode: bool = false,       // ZMQ_XPUB_MANUAL
    lossy: bool = true,              // !ZMQ_XPUB_NODROP
    invert_matching: bool = false,   // ZMQ_INVERT_MATCHING
    only_first_subscribe: bool = false,  // ZMQ_ONLY_FIRST_SUBSCRIBE
    welcome_msg: ?Message = null,    // ZMQ_XPUB_WELCOME_MSG

    /// Currently sending multipart
    more_send: bool = false,
    more_recv: bool = false,

    /// For manual mode: tracks which pipe sent the subscription we just delivered
    last_pipe: ?*Pipe = null,

    /// For manual mode with last value: send to specific pipe
    send_last_pipe: bool = false,

    const SubscriptionNotification = struct {
        subscribe: bool,  // true=subscribe, false=unsubscribe
        prefix: []const u8,
        pipe: ?*Pipe,  // For manual mode
    };

    pub fn init(allocator: std.mem.Allocator) XPub {
        return .{
            .subscriptions = SubscriptionTrie.init(allocator),
            .pending_notifications = std.ArrayList(SubscriptionNotification).init(allocator),
            .pending_data = std.ArrayList(Message).init(allocator),
            .dist = Distributor.init(allocator),
        };
    }

    /// Called when new pipe attaches (subscriber connects)
    pub fn attachPipe(self: *XPub, pipe: *Pipe) void {
        self.dist.attach(pipe);

        // Send welcome message if configured
        if (self.welcome_msg) |*welcome| {
            var copy = welcome.copy() catch return;
            pipe.write(copy) catch return;
            pipe.flush();
        }

        // Process any subscription messages from this pipe
        self.processIncoming(pipe);
    }

    /// Process incoming messages from a pipe (subscriptions + upstream data)
    fn processIncoming(self: *XPub, pipe: *Pipe) void {
        while (pipe.tryRead()) |msg| {
            const data = msg.data();
            const first_part = !self.more_recv;
            self.more_recv = msg.hasMore();

            // Determine if this is a subscription message
            var is_subscription = false;
            var subscribe: bool = undefined;
            var prefix: []const u8 = undefined;

            if (first_part or !self.only_first_subscribe) {
                // Check for ZMTP 3.1 command format
                if (msg.isSubscribeCommand()) {
                    is_subscription = true;
                    subscribe = true;
                    prefix = msg.commandBody();
                } else if (msg.isCancelCommand()) {
                    is_subscription = true;
                    subscribe = false;
                    prefix = msg.commandBody();
                }
                // Check for legacy wire format
                else if (data.len > 0 and (data[0] == 0x01 or data[0] == 0x00)) {
                    is_subscription = true;
                    subscribe = data[0] == 0x01;
                    prefix = data[1..];
                }
            }

            if (is_subscription) {
                self.handleSubscription(pipe, subscribe, prefix, msg);
            } else {
                // Non-subscription upstream data - queue for recv()
                self.pending_data.append(msg) catch {
                    msg.deinit();
                };
            }
        }
    }

    fn handleSubscription(
        self: *XPub,
        pipe: *Pipe,
        subscribe: bool,
        prefix: []const u8,
        original_msg: Message,
    ) void {
        defer original_msg.deinit();

        if (self.manual_mode) {
            // Queue for application to approve
            const notif = SubscriptionNotification{
                .subscribe = subscribe,
                .prefix = self.allocator.dupe(u8, prefix) catch return,
                .pipe = pipe,
            };
            self.pending_notifications.append(notif) catch return;
        } else {
            // Auto-accept subscription
            const notify = if (subscribe) blk: {
                const first_added = self.subscriptions.subscribe(prefix, pipe) catch return;
                break :blk first_added or self.verbose_subs;
            } else blk: {
                const result = self.subscriptions.unsubscribe(prefix, pipe);
                break :blk result == .removed or self.verbose_unsubs;
            };

            // Queue notification for recv()
            if (notify) {
                const notif = SubscriptionNotification{
                    .subscribe = subscribe,
                    .prefix = self.allocator.dupe(u8, prefix) catch return,
                    .pipe = null,
                };
                self.pending_notifications.append(notif) catch return;
            }
        }
    }

    /// Manual mode: accept a pending subscription
    pub fn acceptSubscription(self: *XPub, prefix: []const u8) void {
        if (!self.manual_mode or self.last_pipe == null) return;
        self.subscriptions.subscribe(prefix, self.last_pipe.?) catch return;
    }

    /// Manual mode: reject a pending subscription (unsubscribe)
    pub fn rejectSubscription(self: *XPub, prefix: []const u8) void {
        if (!self.manual_mode or self.last_pipe == null) return;
        _ = self.subscriptions.unsubscribe(prefix, self.last_pipe.?);
    }

    /// Send message to matching subscribers
    pub fn send(self: *XPub, msg: *Message, rt: *zio.Runtime) SendError!void {
        // For first frame, find matching pipes
        if (!self.more_send) {
            self.dist.unmatch();
            self.subscriptions.matchUnique(msg.data(), &self.dist.matched);

            // Invert matching if configured
            if (self.invert_matching) {
                self.dist.reverseMatch();
            }
        }

        // Check HWM
        if (!self.lossy and !self.dist.checkHwm()) {
            return error.WouldBlock;
        }

        // Send to matching pipes
        try self.dist.sendToMatching(msg, rt);

        self.more_send = msg.hasMore();
        if (!self.more_send) {
            self.dist.unmatch();
        }
    }

    /// Receive subscription notification or upstream data
    pub fn recv(self: *XPub) RecvError!Message {
        // First return subscription notifications
        if (self.pending_notifications.items.len > 0) {
            const notif = self.pending_notifications.orderedRemove(0);

            // Track last pipe for manual mode
            self.last_pipe = notif.pipe;

            // Create notification message: [0x00|0x01][prefix]
            var msg = try Message.initSize(self.allocator, 1 + notif.prefix.len);
            msg.data()[0] = if (notif.subscribe) 0x01 else 0x00;
            @memcpy(msg.data()[1..], notif.prefix);
            self.allocator.free(notif.prefix);
            return msg;
        }

        // Then return upstream data (if any)
        if (self.pending_data.items.len > 0) {
            return self.pending_data.orderedRemove(0);
        }

        return error.WouldBlock;
    }

    pub fn hasIn(self: *XPub) bool {
        return self.pending_notifications.items.len > 0 or
               self.pending_data.items.len > 0;
    }

    pub fn hasOut(self: *XPub) bool {
        return self.lossy or self.dist.checkHwm();
    }

    /// Query number of unique subscription prefixes
    pub fn getTopicsCount(self: *XPub) usize {
        return self.subscriptions.total_subscriptions;
    }
};
```

#### ZZMQ XSUB Implementation

```zig
pub const XSub = struct {
    /// Local subscription cache (for reconnect replay and filtering)
    subscriptions: SubscriptionCache,

    /// Fair queue for receiving messages
    fq: FairQueue,

    /// Distributor for sending subscriptions upstream
    dist: Distributor,

    /// Options
    filter: bool = false,  // When true, filter incoming by subscriptions (SUB sets this true)
    only_first_subscribe: bool = false,
    verbose_unsubs: bool = false,

    /// Currently receiving/sending multipart
    more_recv: bool = false,
    more_send: bool = false,
    process_subscribe: bool = false,

    /// Cached message for poll efficiency (hasIn check)
    has_message: bool = false,
    cached_message: Message,

    /// Subscription cache for XSUB (different from Trie - just tracks prefixes, no pipes)
    pub const SubscriptionCache = struct {
        /// Set of subscribed prefixes
        prefixes: std.StringHashMap(void),
        /// For iteration during replay
        allocator: std.mem.Allocator,

        pub fn init(allocator: std.mem.Allocator) SubscriptionCache {
            return .{
                .prefixes = std.StringHashMap(void).init(allocator),
                .allocator = allocator,
            };
        }

        pub fn add(self: *SubscriptionCache, prefix: []const u8) !void {
            const owned = try self.allocator.dupe(u8, prefix);
            try self.prefixes.put(owned, {});
        }

        pub fn remove(self: *SubscriptionCache, prefix: []const u8) bool {
            if (self.prefixes.fetchRemove(prefix)) |entry| {
                self.allocator.free(entry.key);
                return true;
            }
            return false;
        }

        /// Check if topic matches any subscription (prefix match)
        pub fn matches(self: *SubscriptionCache, topic: []const u8) bool {
            var iter = self.prefixes.keyIterator();
            while (iter.next()) |prefix| {
                if (std.mem.startsWith(u8, topic, prefix.*)) {
                    return true;
                }
            }
            return false;
        }

        pub fn iterator(self: *SubscriptionCache) std.StringHashMap(void).KeyIterator {
            return self.prefixes.keyIterator();
        }

        pub fn count(self: *SubscriptionCache) usize {
            return self.prefixes.count();
        }
    };

    pub fn init(allocator: std.mem.Allocator) XSub {
        return .{
            .subscriptions = SubscriptionCache.init(allocator),
            .fq = FairQueue.init(allocator),
            .dist = Distributor.init(allocator),
            .cached_message = Message.empty(),
        };
    }

    /// Called when new pipe attaches
    pub fn attachPipe(self: *XSub, pipe: *Pipe) void {
        self.fq.attach(pipe);
        self.dist.attach(pipe);

        // Send all cached subscriptions to new upstream peer
        self.replaySubscriptions(pipe);
    }

    /// Called on reconnect (pipe hiccup)
    pub fn onHiccup(self: *XSub, pipe: *Pipe) void {
        // Resend all subscriptions to reconnected peer
        self.replaySubscriptions(pipe);
    }

    fn replaySubscriptions(self: *XSub, pipe: *Pipe) void {
        var iter = self.subscriptions.iterator();
        while (iter.next()) |prefix| {
            var msg = Message.initSize(self.allocator, 1 + prefix.len) catch continue;
            msg.data()[0] = 0x01;  // Subscribe
            @memcpy(msg.data()[1..], prefix.*);

            pipe.write(msg) catch {
                msg.deinit();
                continue;
            };
        }
        pipe.flush();
    }

    /// Send subscription message upstream (or user data)
    pub fn send(self: *XSub, msg: *Message, rt: *zio.Runtime) SendError!void {
        const data = msg.data();
        const first_part = !self.more_send;
        self.more_send = msg.hasMore();

        // Determine if we should process this as subscription
        if (first_part) {
            self.process_subscribe = !self.only_first_subscribe;
        }

        // Check for subscription message
        var is_subscription = false;
        var subscribe: bool = undefined;
        var prefix: []const u8 = undefined;

        if (self.process_subscribe or first_part) {
            if (msg.isSubscribeCommand()) {
                is_subscription = true;
                subscribe = true;
                prefix = msg.commandBody();
            } else if (msg.isCancelCommand()) {
                is_subscription = true;
                subscribe = false;
                prefix = msg.commandBody();
            } else if (data.len > 0 and data[0] == 0x01) {
                is_subscription = true;
                subscribe = true;
                prefix = data[1..];
            } else if (data.len > 0 and data[0] == 0x00) {
                is_subscription = true;
                subscribe = false;
                prefix = data[1..];
            }
        }

        if (is_subscription) {
            // Update local cache
            if (subscribe) {
                try self.subscriptions.add(prefix);
            } else {
                const removed = self.subscriptions.remove(prefix);
                // If verbose_unsubs is false and this wasn't the last, don't forward
                if (!removed and !self.verbose_unsubs) {
                    msg.deinit();
                    return;  // Don't forward duplicate unsubscribe
                }
            }
            self.process_subscribe = true;
        }

        // Forward upstream to XPUB/PUB
        try self.dist.sendToAll(msg, rt);
    }

    /// Receive message (optionally filtered by subscriptions)
    pub fn recv(self: *XSub, rt: *zio.Runtime) RecvError!Message {
        // Return cached message if available (from hasIn check)
        if (self.has_message) {
            self.has_message = false;
            self.more_recv = self.cached_message.hasMore();
            return self.cached_message;
        }

        while (true) {
            const msg = try self.fq.recv(rt);

            // Pass through if:
            // - Continuation of multipart (more_recv)
            // - Filtering disabled
            // - Matches subscription
            if (self.more_recv or !self.filter or self.subscriptions.matches(msg.data())) {
                self.more_recv = msg.hasMore();
                return msg;
            }

            // Message doesn't match - skip entire multipart
            msg.deinit();
            while (msg.hasMore()) {
                const part = try self.fq.recv(rt);
                part.deinit();
            }
        }
    }

    pub fn hasIn(self: *XSub) bool {
        // Continuation of multipart
        if (self.more_recv) return true;

        // Already have cached message
        if (self.has_message) return true;

        // Try to get a matching message
        while (true) {
            const msg = self.fq.tryRecv() orelse return false;

            if (!self.filter or self.subscriptions.matches(msg.data())) {
                self.cached_message = msg;
                self.has_message = true;
                return true;
            }

            // Skip non-matching multipart
            msg.deinit();
            while (msg.hasMore()) {
                if (self.fq.tryRecv()) |part| {
                    part.deinit();
                } else {
                    break;
                }
            }
        }
    }

    pub fn hasOut(self: *XSub) bool {
        _ = self;
        return true;  // Subscriptions can always be sent
    }

    /// Query number of local subscriptions
    pub fn getTopicsCount(self: *XSub) usize {
        return self.subscriptions.count();
    }
};
```

#### SUB as XSUB Wrapper

SUB is implemented as a thin wrapper around XSUB with `filter = true`:

```zig
pub const Sub = struct {
    inner: XSub,

    pub fn init(allocator: std.mem.Allocator) Sub {
        var sub = Sub{ .inner = XSub.init(allocator) };
        sub.inner.filter = true;  // SUB always filters
        return sub;
    }

    /// SUB uses setsockopt for subscriptions (not send)
    pub fn subscribe(self: *Sub, prefix: []const u8) !void {
        // Create subscription message
        var msg = try Message.initSize(self.allocator, 1 + prefix.len);
        msg.data()[0] = 0x01;
        @memcpy(msg.data()[1..], prefix);

        // Send upstream
        try self.inner.send(&msg, self.rt);
    }

    pub fn unsubscribe(self: *Sub, prefix: []const u8) !void {
        var msg = try Message.initSize(self.allocator, 1 + prefix.len);
        msg.data()[0] = 0x00;
        @memcpy(msg.data()[1..], prefix);
        try self.inner.send(&msg, self.rt);
    }

    pub fn recv(self: *Sub, rt: *zio.Runtime) !Message {
        return self.inner.recv(rt);
    }

    // SUB cannot send user messages
    pub fn send(self: *Sub, msg: *Message, rt: *zio.Runtime) !void {
        _ = self;
        _ = msg;
        _ = rt;
        return error.NotSupported;
    }

    pub fn hasIn(self: *Sub) bool {
        return self.inner.hasIn();
    }

    pub fn hasOut(self: *Sub) bool {
        return false;  // SUB cannot send
    }
};
```

#### XPUB Manual Mode Workflow

Manual mode allows applications to approve/reject subscriptions:

```
┌─────────────────────────────────────────────────────────────────┐
│                    XPUB MANUAL MODE WORKFLOW                     │
└─────────────────────────────────────────────────────────────────┘

1. Subscriber connects and sends subscription
   │
   ▼
2. XPUB queues notification (doesn't apply to trie yet)
   │
   ▼
3. Application calls recv()
   │   ┌──────────────────────────────────────────────┐
   │   │ Returns: [0x01]["weather."]                   │
   │   │ XPUB stores last_pipe internally              │
   │   └──────────────────────────────────────────────┘
   │
   ▼
4. Application decides to accept or reject
   │
   ├─── Accept: socket.setOption(.subscribe, "weather.")
   │    │   → subscriptions.add("weather.", last_pipe)
   │    │   → Future publishes to "weather.*" go to this subscriber
   │
   └─── Reject: socket.setOption(.unsubscribe, "weather.")
        │   → Subscription not added
        │   → Subscriber won't receive these messages
```

#### XPUB/XSUB Proxy Pattern

The canonical use case for XPUB/XSUB:

```zig
/// Subscription-forwarding proxy
pub fn pubSubProxy(
    xpub: *Socket(.XPUB),  // Subscribers connect here
    xsub: *Socket(.XSUB),  // Publishers connect here
    rt: *zio.Runtime,
) !void {
    while (true) {
        // Wait for activity on either socket
        const ready = try zio.select(.{
            xpub.pollable(.recv),
            xsub.pollable(.recv),
        }, rt);

        // Forward subscriptions: XPUB → XSUB
        if (ready[0]) {
            // Subscription notifications from XPUB
            while (xpub.hasIn()) {
                var sub_msg = try xpub.recv();
                // Forward to XSUB (sends upstream to publishers)
                try xsub.send(&sub_msg, rt);
            }
        }

        // Forward publications: XSUB → XPUB
        if (ready[1]) {
            while (xsub.hasIn()) {
                var pub_msg = try xsub.recv(rt);
                // Forward to XPUB (distributes to matching subscribers)
                try xpub.send(&pub_msg, rt);
            }
        }
    }
}
```

#### Subscription Message Flow Diagram

```
Publishers          Proxy                 Subscribers
    │                 │                        │
    │    ┌────────────┴────────────┐           │
    │    │  XSUB           XPUB    │           │
    │    │   ▲               │     │           │
    │    │   │               ▼     │           │
    │    │   │    ┌─────────────┐  │           │
    │    │   │    │ Subscription │  │           │
    │    │   │    │    Trie     │  │           │
    │    │   │    └─────────────┘  │           │
    │    │   │               │     │           │
    │    └───┼───────────────┼─────┘           │
    │        │               │                 │
    │        │               │                 │
◄───┼────────┼───────────────┼─────────────────┤
    │  Publications flow     │   Subscriptions  │
    │  from left to right    │   flow from      │
    │                        │   right to left  │
    │        │               │                 │
    ▼        │               ▼                 │
┌───────┐    │          ┌───────┐          ┌───────┐
│ PUB   │────┼─────────▶│ Proxy │◀─────────│ SUB   │
│       │    │          │       │──────────▶│       │
└───────┘    │          └───────┘          └───────┘
             │               │
             │  [0x01]topic  │
             │ ◄──────────── │
             │               │
        topic:data           │
         ─────────────────▶  │
                             │
                        topic:data
                         ─────────────▶
```
---

## Subscription Matching System

### Semantic Requirements (libzmq Compatibility)

Subscription matching in ZMQ is **prefix-based**: a subscriber subscribes to a prefix (e.g., `"weather."`), and all messages whose first frame starts with that prefix are delivered. Key semantics:

1. **Prefix Matching**: Topic `"weather.nyc.temp"` matches subscriptions `""`, `"w"`, `"weather"`, `"weather."`, `"weather.nyc"`, etc.
2. **Empty Prefix**: Subscribing to `""` matches ALL messages (wildcard)
3. **Multiple Subscriptions**: A pipe can have multiple subscriptions; message delivered if ANY matches
4. **Subscription Counting**: Same prefix subscribed twice requires two unsubscribes to remove
5. **First Frame Only**: Only the first frame of a multipart message is matched against subscriptions
6. **Binary Safe**: Prefixes are byte arrays, not strings (can contain NUL bytes)

### ZZMQ Design: Prefix Trie with Zig Idioms

We implement a prefix trie (similar to libzmq's MTrie) but with Zig-idiomatic design:

```zig
pub const SubscriptionTrie = struct {
    const Self = @This();

    /// Node in the prefix trie
    pub const Node = struct {
        /// Pipes subscribed at this exact prefix
        /// Uses small-vector optimization: inline storage for common case (1-4 pipes)
        pipes: PipeSet,

        /// Child nodes, keyed by next byte
        /// null means no children (leaf or partial leaf)
        children: ?*Children,

        /// Reference count for this prefix (same pipe can subscribe multiple times)
        /// Maps pipe -> subscription count
        ref_counts: std.AutoHashMap(*Pipe, u32),

        pub const Children = struct {
            /// Sparse child storage for typical case (few children)
            /// Falls back to dense array if many children
            storage: ChildStorage,

            const ChildStorage = union(enum) {
                /// Sparse: up to 8 children stored inline
                sparse: struct {
                    keys: [8]u8,
                    nodes: [8]?*Node,
                    count: u8,
                },
                /// Dense: 256-entry array for nodes with many children
                dense: [256]?*Node,
            };
        };
    };

    root: Node,
    allocator: Allocator,

    /// Total number of unique subscriptions (for stats/debugging)
    total_subscriptions: usize = 0,

    /// Add a subscription for a pipe
    /// Returns true if this is the FIRST subscription for this prefix (any pipe)
    pub fn subscribe(self: *Self, prefix: []const u8, pipe: *Pipe) !bool {
        var node = &self.root;

        // Walk/create path to prefix
        for (prefix) |byte| {
            node = try self.getOrCreateChild(node, byte);
        }

        // Add pipe to this node
        const first_for_prefix = node.pipes.count() == 0;
        const prev_count = node.ref_counts.get(pipe) orelse 0;
        try node.ref_counts.put(pipe, prev_count + 1);

        if (prev_count == 0) {
            try node.pipes.add(pipe);
        }

        self.total_subscriptions += 1;
        return first_for_prefix;
    }

    /// Remove a subscription for a pipe
    /// Returns: .removed if last subscription for this prefix, .remaining if others exist, .not_found
    pub fn unsubscribe(self: *Self, prefix: []const u8, pipe: *Pipe) UnsubscribeResult {
        var node = &self.root;

        // Walk to prefix
        for (prefix) |byte| {
            node = self.getChild(node, byte) orelse return .not_found;
        }

        // Decrement reference count
        const count = node.ref_counts.get(pipe) orelse return .not_found;
        if (count == 1) {
            _ = node.ref_counts.remove(pipe);
            node.pipes.remove(pipe);
            self.total_subscriptions -= 1;

            // Clean up empty nodes (optional, for memory efficiency)
            self.maybeCompact(prefix);

            return if (node.pipes.count() == 0) .removed else .remaining;
        } else {
            node.ref_counts.put(pipe, count - 1) catch unreachable;
            self.total_subscriptions -= 1;
            return .remaining;
        }
    }

    pub const UnsubscribeResult = enum { removed, remaining, not_found };

    /// Find all pipes matching a topic (prefix match)
    /// Calls callback for each matching pipe (may be called multiple times for same pipe
    /// if subscribed at multiple prefix levels - caller should deduplicate if needed)
    pub fn match(self: *Self, topic: []const u8, callback: *const fn(*Pipe) void) void {
        var node = &self.root;

        // Root node pipes (empty prefix "") match everything
        for (node.pipes.slice()) |pipe| {
            callback(pipe);
        }

        // Walk topic, collecting matching pipes at each level
        for (topic) |byte| {
            node = self.getChild(node, byte) orelse break;
            for (node.pipes.slice()) |pipe| {
                callback(pipe);
            }
        }
    }

    /// Optimized: collect unique matching pipes into a set
    pub fn matchUnique(self: *Self, topic: []const u8, result: *PipeSet) void {
        var node = &self.root;

        // Root matches
        result.addAll(node.pipes);

        // Walk and collect
        for (topic) |byte| {
            node = self.getChild(node, byte) orelse break;
            result.addAll(node.pipes);
        }
    }

    /// Remove ALL subscriptions for a pipe (called when pipe disconnects)
    pub fn removeAllForPipe(self: *Self, pipe: *Pipe) void {
        self.removeFromSubtree(&self.root, pipe);
    }

    fn removeFromSubtree(self: *Self, node: *Node, pipe: *Pipe) void {
        // Remove from this node
        if (node.ref_counts.remove(pipe)) |count| {
            node.pipes.remove(pipe);
            self.total_subscriptions -= count;
        }

        // Recurse to children
        if (node.children) |children| {
            switch (children.storage) {
                .sparse => |*s| {
                    for (s.nodes[0..s.count]) |maybe_child| {
                        if (maybe_child) |child| {
                            self.removeFromSubtree(child, pipe);
                        }
                    }
                },
                .dense => |*d| {
                    for (d) |maybe_child| {
                        if (maybe_child) |child| {
                            self.removeFromSubtree(child, pipe);
                        }
                    }
                },
            }
        }
    }
};
```

### Design Rationale

**Why Prefix Trie (not HashMap)?**
- `match()` is O(topic_length), touching only relevant nodes
- HashMap would require checking every subscription prefix against topic
- Memory sharing for common prefixes (e.g., "weather.nyc" and "weather.london")

**Sparse vs Dense Children:**
- Most nodes have few children (typical topic hierarchies)
- Sparse storage (8 inline slots) avoids 256-byte allocation per node
- Automatic promotion to dense when >8 children

**Reference Counting:**
- Same pipe subscribing twice to same prefix is valid
- Must unsubscribe twice to fully remove
- Separate from pipe set membership (pipe in set iff ref_count > 0)

**Deduplication Strategy:**
- `match()` may return same pipe multiple times (subscribed at multiple levels)
- `matchUnique()` deduplicates into PipeSet for send operations
- PUB pattern uses `matchUnique()` to build send list

### Integration with PUB Pattern

```zig
pub const Pub = struct {
    subscriptions: SubscriptionTrie,
    dist: Distributor,
    match_buffer: PipeSet,  // Reused buffer for match results

    pub fn send(self: *Pub, msg: *Message, rt: *zio.Runtime) !void {
        // Match first frame against subscriptions
        const topic = msg.firstFrame().data;

        self.match_buffer.clear();
        self.subscriptions.matchUnique(topic, &self.match_buffer);

        // Send to all matching pipes
        for (self.match_buffer.slice()) |pipe| {
            // Copy message to each pipe (COW optimization kicks in)
            try pipe.write(msg.copy());
        }
    }

    pub fn onPipeAttached(self: *Pub, pipe: *Pipe) void {
        self.dist.attach(pipe);
        // New pipe has no subscriptions yet
    }

    pub fn onPipeDetached(self: *Pub, pipe: *Pipe) void {
        self.subscriptions.removeAllForPipe(pipe);
        self.dist.detach(pipe);
    }

    pub fn handleSubscription(self: *Pub, pipe: *Pipe, data: []const u8) !void {
        if (data.len == 0) return error.InvalidSubscription;

        const subscribe = data[0] == 1;
        const prefix = data[1..];

        if (subscribe) {
            _ = try self.subscriptions.subscribe(prefix, pipe);
        } else {
            _ = self.subscriptions.unsubscribe(prefix, pipe);
        }
    }
};
```

### XPUB Subscription Notifications

XPUB exposes subscription events to the application:

```zig
pub const XPub = struct {
    subscriptions: SubscriptionTrie,

    /// Pending subscription notifications for recv()
    pending_notifications: std.ArrayList(Notification),

    /// Options
    verbose_subs: bool = false,    // Notify on every sub, not just first
    verbose_unsubs: bool = false,  // Notify on every unsub, not just last

    const Notification = struct {
        subscribe: bool,  // true = subscribe, false = unsubscribe
        prefix: []const u8,
    };

    pub fn handleSubscription(self: *XPub, pipe: *Pipe, data: []const u8) !void {
        const subscribe = data[0] == 1;
        const prefix = data[1..];

        if (subscribe) {
            const first = try self.subscriptions.subscribe(prefix, pipe);
            if (first or self.verbose_subs) {
                try self.pending_notifications.append(.{
                    .subscribe = true,
                    .prefix = try self.allocator.dupe(u8, prefix),
                });
            }
        } else {
            const result = self.subscriptions.unsubscribe(prefix, pipe);
            if (result == .removed or self.verbose_unsubs) {
                try self.pending_notifications.append(.{
                    .subscribe = false,
                    .prefix = try self.allocator.dupe(u8, prefix),
                });
            }
        }
    }

    pub fn recv(self: *XPub) ?Message {
        if (self.pending_notifications.popOrNull()) |notif| {
            // Return subscription event as message: [0/1][prefix]
            var msg = Message.init(self.allocator);
            const frame = msg.addFrame(notif.prefix.len + 1);
            frame[0] = if (notif.subscribe) 1 else 0;
            @memcpy(frame[1..], notif.prefix);
            self.allocator.free(notif.prefix);
            return msg;
        }
        return null;
    }
};
```

---

## Identity and Routing ID Lifecycle

Routing IDs (also called "identities") are critical for ROUTER sockets to address specific peers. Understanding the lifecycle is essential for correct implementation.

### Identity Sources

A pipe's routing ID can come from three sources, in priority order:

```
1. Explicit ZMQ_ROUTING_ID option (set before connect)
2. ZMTP handshake Identity property (sent by peer)
3. Auto-generated ID (ROUTER assigns unique ID)
```

### Lifecycle Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    ROUTING ID LIFECYCLE                          │
└─────────────────────────────────────────────────────────────────┘

CONNECTING SOCKET (e.g., DEALER connecting to ROUTER):
─────────────────────────────────────────────────────

[1] Socket Creation
    │
    ▼
[2] Optional: socket.setOption(.routing_id, "my-identity")
    │                     │
    │                     └── Stored in socket.options.routing_id
    │
    ▼
[3] socket.connect("tcp://server:5555")
    │
    ▼
[4] ZMTP Handshake
    │   ┌──────────────────────────────────────────────┐
    │   │ READY command includes:                       │
    │   │   Identity: <routing_id or empty>            │
    │   │   Socket-Type: DEALER                        │
    │   └──────────────────────────────────────────────┘
    │
    ▼
[5] Pipe created with routing_id attached


ROUTER SOCKET (receiving connection):
─────────────────────────────────────

[1] Incoming connection accepted
    │
    ▼
[2] ZMTP Handshake
    │   ┌──────────────────────────────────────────────┐
    │   │ Receive peer's READY command                  │
    │   │   Identity: "my-identity" (or empty)         │
    │   └──────────────────────────────────────────────┘
    │
    ▼
[3] Determine Routing ID:
    │
    │   if peer sent non-empty Identity:
    │       routing_id = peer's Identity
    │       ┌────────────────────────────────────────┐
    │       │ Check for collision!                    │
    │       │ If routing_id already exists:          │
    │       │   - ZMQ_ROUTER_HANDOVER=1: disconnect  │
    │       │     old pipe, use new                   │
    │       │   - ZMQ_ROUTER_HANDOVER=0: reject new  │
    │       │     connection (disconnect it)         │
    │       └────────────────────────────────────────┘
    │   else:
    │       routing_id = generate_unique_id()
    │       // Format: 0x00 + 4-byte counter (binary, not string)
    │
    ▼
[4] Pipe attached with routing_id
    │   pipes.addWithRoutingId(pipe, routing_id)
    │
    ▼
[5] Ready for send/recv
    │
    │   recv() → prepends routing_id to message
    │   send() → first frame is routing_id, routes to that pipe
```

### ZZMQ Implementation

```zig
pub const RoutingId = struct {
    /// Routing IDs are 1-255 bytes
    data: [255]u8,
    len: u8,

    pub const MAX_LEN = 255;

    pub fn fromSlice(slice: []const u8) !RoutingId {
        if (slice.len == 0 or slice.len > MAX_LEN) {
            return error.InvalidRoutingId;
        }
        var id: RoutingId = undefined;
        @memcpy(id.data[0..slice.len], slice);
        id.len = @intCast(slice.len);
        return id;
    }

    pub fn slice(self: *const RoutingId) []const u8 {
        return self.data[0..self.len];
    }

    /// Auto-generated IDs start with 0x00 (cannot conflict with user IDs)
    pub fn isAutoGenerated(self: *const RoutingId) bool {
        return self.len >= 1 and self.data[0] == 0x00;
    }
};

pub const RoutingIdGenerator = struct {
    counter: u32 = 0,

    pub fn next(self: *RoutingIdGenerator) RoutingId {
        self.counter += 1;
        var id: RoutingId = undefined;
        id.data[0] = 0x00;  // Auto-generated marker
        std.mem.writeInt(u32, id.data[1..5], self.counter, .big);
        id.len = 5;
        return id;
    }
};

pub const Router = struct {
    /// Pipe lookup by routing ID
    pipes_by_id: std.HashMap(RoutingIdKey, *Pipe, RoutingIdContext, 80),

    /// ID generator for auto-assigned IDs
    id_generator: RoutingIdGenerator,

    /// Options
    mandatory: bool = false,      // ZMQ_ROUTER_MANDATORY
    handover: bool = false,       // ZMQ_ROUTER_HANDOVER

    const RoutingIdKey = struct {
        data: [255]u8,
        len: u8,
    };

    /// Called when ZMTP handshake completes
    pub fn onPipeReady(self: *Router, pipe: *Pipe, peer_identity: ?[]const u8) !void {
        const routing_id = if (peer_identity) |id| blk: {
            if (id.len > 0) {
                // Peer provided explicit identity
                break :blk try RoutingId.fromSlice(id);
            }
            // Empty identity = generate one
            break :blk self.id_generator.next();
        } else self.id_generator.next();

        // Check for collision
        const key = RoutingIdKey{ .data = routing_id.data, .len = routing_id.len };
        if (self.pipes_by_id.get(key)) |existing_pipe| {
            if (self.handover) {
                // Disconnect old pipe, use new
                existing_pipe.terminate();
                _ = self.pipes_by_id.remove(key);
            } else {
                // Reject new connection
                pipe.terminate();
                return error.RoutingIdCollision;
            }
        }

        pipe.routing_id = routing_id;
        try self.pipes_by_id.put(key, pipe);
    }

    pub fn onPipeDetached(self: *Router, pipe: *Pipe) void {
        if (pipe.routing_id) |id| {
            const key = RoutingIdKey{ .data = id.data, .len = id.len };
            _ = self.pipes_by_id.remove(key);
        }
    }

    pub fn send(self: *Router, msg: *Message) !void {
        // First frame must be routing ID
        const routing_id = msg.routing_id orelse return error.NoRoutingId;

        const key = RoutingIdKey{ .data = routing_id.data, .len = routing_id.len };
        const pipe = self.pipes_by_id.get(key) orelse {
            if (self.mandatory) {
                return error.HostUnreachable;
            }
            // Silently drop (default ZMQ behavior)
            return;
        };

        // Send message (without routing ID frame on wire)
        try pipe.write(msg);
    }

    pub fn recv(self: *Router, pipes: *PipeSet, rt: *zio.Runtime) !Message {
        // Fair queue from all pipes
        const pipe = try pipes.waitReadable(rt);
        var msg = try pipe.read(rt);

        // Prepend routing ID so user knows who sent it
        msg.routing_id = pipe.routing_id;
        return msg;
    }
};
```

### Identity Constraints

```zig
pub fn validateRoutingId(id: []const u8) !void {
    // Length: 1-255 bytes
    if (id.len == 0) return error.RoutingIdEmpty;
    if (id.len > 255) return error.RoutingIdTooLong;

    // User-provided IDs cannot start with 0x00 (reserved for auto-generated)
    if (id[0] == 0x00) return error.RoutingIdReservedPrefix;
}
```

### Use Cases

**Named Services:**
```zig
// Worker identifies itself
worker.setOption(.routing_id, "worker-1");
worker.connect("tcp://broker:5555");

// Broker can route to specific worker
var msg = Message.init(allocator);
msg.routing_id = try RoutingId.fromSlice("worker-1");
msg.addFrame("specific task for worker-1");
broker.send(&msg);
```

**Extracting Sender Identity:**
```zig
// Broker receives request
var request = try broker.recv();
const sender = request.routing_id.?.slice();  // Who sent this?

// Reply to same sender
var reply = Message.init(allocator);
reply.routing_id = request.routing_id;
reply.addFrame("response");
try broker.send(&reply);
```

---

## Memory Pressure and Graceful Degradation

### Philosophy

Memory pressure handling is a **shared responsibility**:
- **ZZMQ** provides mechanisms and sensible defaults
- **Users** configure behavior appropriate to their use case
- **Explicit failures** are preferred over silent data loss

### Memory Failure Points

```
┌─────────────────────────────────────────────────────────────────┐
│                    MEMORY ALLOCATION POINTS                      │
└─────────────────────────────────────────────────────────────────┘

1. Message Creation
   └── User calls Message.init() or recv()
       └── Frame data allocation

2. Pipe Write (send path)
   └── Frame queue chunk allocation (when queue grows)

3. Subscription Trie
   └── New node creation for new prefixes

4. Connection Establishment
   └── Pipe allocation, engine buffers

5. ZMTP Codec
   └── Decode buffer allocation for large frames
```

### Strategy: Fail-Fast with User Control

```zig
pub const MemoryPolicy = struct {
    /// What to do when send() can't allocate
    on_send_oom: SendOomAction = .return_error,

    /// What to do when recv() can't allocate
    on_recv_oom: RecvOomAction = .return_error,

    /// Maximum memory for pipe queues (0 = unlimited)
    max_queue_memory: usize = 0,

    /// Pre-allocate queue chunks to avoid OOM during operation
    preallocate_chunks: u32 = 0,

    pub const SendOomAction = enum {
        return_error,    // Return error.OutOfMemory (default)
        block_until_mem, // Block until memory available (risky!)
        drop_message,    // Drop this message, return success (lossy)
    };

    pub const RecvOomAction = enum {
        return_error,    // Return error.OutOfMemory (default)
        disconnect_peer, // Disconnect the peer that sent too-large message
    };
};
```

### Pipe Queue Memory Limits

```zig
pub const Pipe = struct {
    // Existing fields...

    /// Memory tracking
    queue_memory_used: usize = 0,
    queue_memory_limit: usize,  // 0 = unlimited

    pub fn writeFrame(self: *Pipe, frame: Frame) !void {
        const frame_size = frame.data.len + @sizeOf(FrameHeader);

        // Check memory limit (separate from HWM!)
        if (self.queue_memory_limit > 0) {
            if (self.queue_memory_used + frame_size > self.queue_memory_limit) {
                return error.QueueMemoryExceeded;
            }
        }

        // Try to enqueue
        try self.outbound.enqueue(frame);
        self.queue_memory_used += frame_size;
    }

    pub fn readFrame(self: *Pipe, rt: *zio.Runtime) !Frame {
        const frame = try self.inbound.dequeue(rt);
        const frame_size = frame.data.len + @sizeOf(FrameHeader);
        self.queue_memory_used -= frame_size;
        return frame;
    }
};
```

### Handling Large Messages

```zig
pub const Codec = struct {
    /// Maximum frame size we'll decode (protection against malicious peers)
    max_frame_size: usize = 1024 * 1024 * 1024,  // 1GB default

    /// Decode buffer (reused between frames)
    decode_buffer: std.ArrayList(u8),

    pub fn decodeFrame(self: *Codec, reader: anytype) !Frame {
        const size = try self.readFrameSize(reader);

        // Check limit
        if (size > self.max_frame_size) {
            return error.FrameTooLarge;
        }

        // Allocate
        self.decode_buffer.resize(size) catch |err| {
            // Can't allocate decode buffer
            return error.OutOfMemory;
        };

        try reader.readAll(self.decode_buffer.items);
        return Frame.fromSlice(self.decode_buffer.items);
    }
};
```

### Pre-allocation Strategy

For latency-critical applications, pre-allocate resources:

```zig
pub fn createPreallocatedPipe(allocator: Allocator, config: PipeConfig) !*Pipe {
    var pipe = try allocator.create(Pipe);

    // Pre-allocate queue chunks
    const chunks_needed = config.preallocate_chunks;
    for (0..chunks_needed) |_| {
        const chunk = try allocator.create(FrameQueue.Chunk);
        pipe.outbound.chunk_pool.push(chunk);
        pipe.inbound.chunk_pool.push(chunk);
    }

    return pipe;
}
```

### Error Propagation

```zig
pub const Socket = struct {
    pub fn send(self: *Socket, msg: *Message) SendError!void {
        // Allocation failures during send are explicit errors
        return self.inner.send(msg) catch |err| switch (err) {
            error.OutOfMemory => {
                // User must handle this:
                // - Retry later
                // - Drop message (if acceptable)
                // - Shut down gracefully
                return error.OutOfMemory;
            },
            error.QueueMemoryExceeded => {
                // Queue memory limit hit (not HWM!)
                return error.OutOfMemory;
            },
            else => |e| return e,
        };
    }

    pub fn recv(self: *Socket) RecvError!Message {
        return self.inner.recv() catch |err| switch (err) {
            error.OutOfMemory => {
                // Couldn't allocate space for received message
                // Options:
                // - Return error (user retries later)
                // - If policy is disconnect_peer, the pipe is terminated
                return error.OutOfMemory;
            },
            error.FrameTooLarge => {
                // Peer sent frame exceeding our limit
                // Pipe is disconnected (protocol violation)
                return error.PeerViolation;
            },
            else => |e| return e,
        };
    }
};
```

### Monitoring Memory Usage

```zig
pub const Context = struct {
    pub fn getMemoryStats(self: *Context) MemoryStats {
        var stats = MemoryStats{};

        for (self.sockets.items) |socket| {
            for (socket.pipes.items) |pipe| {
                stats.queue_memory += pipe.queue_memory_used;
                stats.pipe_count += 1;
            }
        }

        return stats;
    }

    pub const MemoryStats = struct {
        queue_memory: usize = 0,
        pipe_count: usize = 0,
        message_count: usize = 0,
    };
};
```

### Summary: Memory Pressure Design

| Scenario | Default Behavior | User Override |
|----------|------------------|---------------|
| send() OOM | Return `error.OutOfMemory` | `drop_message` for lossy |
| recv() OOM | Return `error.OutOfMemory` | `disconnect_peer` |
| Frame too large | Disconnect peer | Configure `max_frame_size` |
| Queue memory limit | Return `error.OutOfMemory` | Set `queue_memory_limit` |
| Pre-allocation | None | Set `preallocate_chunks` |

**Key Principle:** Never silently lose data by default. Users opt into lossy behavior explicitly.

---

## Configuration Validation

### Philosophy

Configuration errors should be caught **as early as possible** with **clear, actionable error messages**. This dramatically improves developer experience.

### Validation Timing

```
┌─────────────────────────────────────────────────────────────────┐
│                    VALIDATION TIMING                             │
└─────────────────────────────────────────────────────────────────┘

[1] SOCKET CREATION (comptime where possible)
    - Socket type validity
    - Invalid option combinations caught at compile time (Zig)

[2] OPTION SETTING (socket.setOption)
    - Value range validation
    - Type-specific constraints
    - Option compatibility with socket type

[3] BIND/CONNECT (socket.bind/connect)
    - Endpoint format validation
    - Transport-specific validation
    - Socket type allows bind/connect

[4] OPERATION TIME (send/recv)
    - Message structure requirements
    - State machine violations (e.g., REQ waiting for reply)
```

### Zig Comptime Validation

```zig
/// Socket type constraints validated at compile time
pub fn Socket(comptime socket_type: SocketType) type {
    return struct {
        const Self = @This();

        // Comptime-check socket capabilities
        const can_send = switch (socket_type) {
            .PULL, .SUB => false,
            else => true,
        };
        const can_recv = switch (socket_type) {
            .PUSH, .PUB => false,
            else => true,
        };
        const can_bind = true;  // All types can bind
        const can_connect = true;  // All types can connect
        const requires_peer = switch (socket_type) {
            .PAIR => true,
            else => false,
        };

        pub fn send(self: *Self, msg: *Message) !void {
            comptime if (!can_send) {
                @compileError("Cannot send on " ++ @tagName(socket_type) ++ " socket");
            };
            return self.inner.send(msg);
        }

        pub fn recv(self: *Self) !Message {
            comptime if (!can_recv) {
                @compileError("Cannot recv on " ++ @tagName(socket_type) ++ " socket");
            };
            return self.inner.recv();
        }

        /// Subscribe only valid for SUB/XSUB
        pub fn subscribe(self: *Self, prefix: []const u8) !void {
            comptime if (socket_type != .SUB and socket_type != .XSUB) {
                @compileError("subscribe() only valid for SUB/XSUB sockets");
            };
            return self.inner.subscribe(prefix);
        }
    };
}

// Usage - compile errors for invalid operations:
var push = try context.socket(.PUSH);
// push.recv();  // COMPILE ERROR: Cannot recv on PUSH socket

var sub = try context.socket(.SUB);
// sub.send(&msg);  // COMPILE ERROR: Cannot send on SUB socket
```

### Runtime Option Validation

```zig
pub const SocketOptions = struct {
    pub fn set(self: *SocketOptions, socket_type: SocketType, option: Option, value: anytype) !void {
        // Validate option is applicable to this socket type
        if (!option.validFor(socket_type)) {
            return ConfigError.optionNotApplicable(option, socket_type);
        }

        // Validate value
        try self.validateValue(option, value);

        // Apply
        self.applyOption(option, value);
    }

    fn validateValue(self: *SocketOptions, option: Option, value: anytype) !void {
        switch (option) {
            .sndhwm, .rcvhwm => {
                // HWM: 0 (unlimited) or positive
                // Already valid by type (u32)
            },
            .routing_id => {
                const id = value;
                if (id.len == 0) return ConfigError.routingIdEmpty();
                if (id.len > 255) return ConfigError.routingIdTooLong(id.len);
                if (id[0] == 0x00) return ConfigError.routingIdReservedPrefix();
            },
            .subscribe => {
                // Prefix can be any bytes, including empty
                // (empty = subscribe to all)
            },
            .linger => {
                // -1 = infinite, 0 = drop immediately, >0 = milliseconds
                const linger: i32 = value;
                if (linger < -1) return ConfigError.lingerInvalid(linger);
            },
            .tcp_keepalive => {
                const ka: i32 = value;
                if (ka < -1 or ka > 1) return ConfigError.boolOptionInvalid("tcp_keepalive", ka);
            },
            else => {},
        }
    }
};
```

### Detailed Error Messages

```zig
pub const ConfigError = struct {
    kind: Kind,
    details: Details,

    pub const Kind = enum {
        option_not_applicable,
        value_out_of_range,
        invalid_format,
        incompatible_options,
        invalid_state,
    };

    pub const Details = union {
        option_not_applicable: struct {
            option: Option,
            socket_type: SocketType,
        },
        value_out_of_range: struct {
            option: Option,
            value: i64,
            min: i64,
            max: i64,
        },
        // ... etc
    };

    pub fn format(self: ConfigError, writer: anytype) !void {
        switch (self.kind) {
            .option_not_applicable => {
                const d = self.details.option_not_applicable;
                try writer.print(
                    "Option '{s}' is not applicable to {s} sockets. " ++
                    "This option is only valid for: {s}",
                    .{
                        @tagName(d.option),
                        @tagName(d.socket_type),
                        d.option.validSocketTypes(),
                    }
                );
            },
            .value_out_of_range => {
                const d = self.details.value_out_of_range;
                try writer.print(
                    "Value {d} for option '{s}' is out of range. " ++
                    "Valid range: {d} to {d}",
                    .{ d.value, @tagName(d.option), d.min, d.max }
                );
            },
            // ...
        }
    }

    // Convenience constructors with good messages
    pub fn optionNotApplicable(option: Option, socket_type: SocketType) ConfigError {
        return .{
            .kind = .option_not_applicable,
            .details = .{ .option_not_applicable = .{
                .option = option,
                .socket_type = socket_type,
            }},
        };
    }

    pub fn routingIdEmpty() ConfigError {
        return .{
            .kind = .invalid_format,
            .details = .{ .message = "Routing ID cannot be empty. Provide 1-255 bytes." },
        };
    }

    pub fn routingIdReservedPrefix() ConfigError {
        return .{
            .kind = .invalid_format,
            .details = .{ .message =
                "Routing ID cannot start with 0x00 (reserved for auto-generated IDs). " ++
                "Use any other starting byte for explicit identities."
            },
        };
    }
};
```

### Endpoint Validation

```zig
pub const Endpoint = struct {
    pub fn parse(uri: []const u8) !Endpoint {
        // Format: transport://address
        const sep = std.mem.indexOf(u8, uri, "://") orelse {
            return ConfigError.invalidEndpoint(uri,
                "Missing '://'. Expected format: transport://address (e.g., tcp://127.0.0.1:5555)");
        };

        const transport = uri[0..sep];
        const address = uri[sep + 3..];

        return switch (transport) {
            "tcp" => .{ .tcp = try TcpEndpoint.parse(address) },
            "ipc" => .{ .ipc = try IpcEndpoint.parse(address) },
            "inproc" => .{ .inproc = try InprocEndpoint.parse(address) },
            else => ConfigError.unknownTransport(transport),
        };
    }
};

pub const TcpEndpoint = struct {
    pub fn parse(address: []const u8) !TcpEndpoint {
        // Format: host:port or [ipv6]:port or *:port (bind any)

        // Find last colon (port separator)
        const colon = std.mem.lastIndexOf(u8, address, ":") orelse {
            return ConfigError.invalidEndpoint(address,
                "TCP address must include port. Expected: host:port (e.g., 127.0.0.1:5555 or *:5555)");
        };

        const host = address[0..colon];
        const port_str = address[colon + 1..];

        const port = std.fmt.parseInt(u16, port_str, 10) catch {
            return ConfigError.invalidEndpoint(address,
                std.fmt.allocPrint(allocator,
                    "Invalid port '{s}'. Port must be a number 1-65535.", .{port_str}));
        };

        if (port == 0) {
            return ConfigError.invalidEndpoint(address,
                "Port 0 is not valid. Use a specific port (1-65535) or ephemeral port binding.");
        }

        // Parse host
        if (host.len == 0) {
            return ConfigError.invalidEndpoint(address, "Host cannot be empty.");
        }

        return .{ .host = host, .port = port };
    }
};
```

### Option Compatibility Matrix

```zig
pub const Option = enum {
    // Common
    sndhwm,
    rcvhwm,
    linger,

    // Pattern-specific
    routing_id,      // DEALER, REQ, ROUTER
    subscribe,       // SUB, XSUB
    unsubscribe,     // SUB, XSUB
    router_mandatory, // ROUTER only
    router_handover, // ROUTER only
    req_correlate,   // REQ only
    req_relaxed,     // REQ only

    pub fn validFor(self: Option, socket_type: SocketType) bool {
        return switch (self) {
            .sndhwm, .rcvhwm, .linger => true,  // Valid for all

            .routing_id => switch (socket_type) {
                .DEALER, .REQ, .REP, .ROUTER => true,
                else => false,
            },

            .subscribe, .unsubscribe => switch (socket_type) {
                .SUB, .XSUB => true,
                else => false,
            },

            .router_mandatory, .router_handover => socket_type == .ROUTER,

            .req_correlate, .req_relaxed => socket_type == .REQ,
        };
    }

    pub fn validSocketTypes(self: Option) []const u8 {
        return switch (self) {
            .routing_id => "DEALER, REQ, REP, ROUTER",
            .subscribe, .unsubscribe => "SUB, XSUB",
            .router_mandatory, .router_handover => "ROUTER",
            .req_correlate, .req_relaxed => "REQ",
            else => "all socket types",
        };
    }
};
```

### State Validation

```zig
pub const ReqSocket = struct {
    state: State = .ready,

    const State = enum { ready, waiting_reply };

    pub fn send(self: *ReqSocket, msg: *Message) !void {
        if (self.state != .ready) {
            return error.InvalidState;
            // Better: return ConfigError.invalidState(
            //     "REQ socket already sent a request. " ++
            //     "Must call recv() before sending another request.");
        }

        try self.inner.send(msg);
        self.state = .waiting_reply;
    }

    pub fn recv(self: *ReqSocket) !Message {
        if (self.state != .waiting_reply) {
            return error.InvalidState;
            // Better: return ConfigError.invalidState(
            //     "REQ socket has no pending request. " ++
            //     "Must call send() before recv().");
        }

        const reply = try self.inner.recv();
        self.state = .ready;
        return reply;
    }
};
```

### Summary: Configuration Validation

| When | What | How |
|------|------|-----|
| Compile time | send/recv capability | Zig comptime checks |
| Compile time | Method availability (subscribe on SUB) | Type-specific Socket |
| Option set | Value ranges | Runtime validation |
| Option set | Socket type compatibility | `validFor()` lookup |
| Bind/Connect | Endpoint format | `Endpoint.parse()` |
| Operation | State machine | Pattern-specific checks |

**Key Principle:** Catch errors early, provide clear messages, leverage Zig's comptime for impossible-to-misuse APIs.

---

## Proxy Pattern

The proxy (also called "device" or "forwarder") is a built-in message forwarding pattern that connects two sockets bidirectionally, optionally capturing all messages.

### libzmq Proxy Reference

From `src/proxy.cpp`:

**Basic Proxy**:
```
zmq_proxy(frontend, backend, capture)
```
- Forwards messages: frontend → backend and backend → frontend
- Preserves multipart atomicity (all parts forwarded together)
- Optional capture socket receives copy of all messages
- Blocks until context terminated

**Steerable Proxy**:
```
zmq_proxy_steerable(frontend, backend, capture, control)
```
- Same as basic proxy plus control socket
- Control commands: `PAUSE`, `RESUME`, `TERMINATE`, `STATISTICS`
- Statistics returns 8-part message with counts and bytes

**Key Implementation Details**:
- Uses `zmq_poll` (or socket_poller) for event multiplexing
- `forward()` function handles burst of messages (up to `proxy_burst_size`)
- Careful handling of POLLIN/POLLOUT to avoid blocking
- When one direction is blocked (HWM), disables polling for input from that direction

### ZZMQ Proxy Implementation

```zig
pub const Proxy = struct {
    frontend: *Socket,
    backend: *Socket,
    capture: ?*Socket,
    control: ?*Socket,

    state: State = .active,
    stats: Statistics = .{},

    const State = enum { active, paused, terminated };

    const Statistics = struct {
        frontend_recv: EndpointStats = .{},
        frontend_send: EndpointStats = .{},
        backend_recv: EndpointStats = .{},
        backend_send: EndpointStats = .{},

        const EndpointStats = struct {
            count: u64 = 0,
            bytes: u64 = 0,
        };
    };

    pub fn init(
        frontend: *Socket,
        backend: *Socket,
        capture: ?*Socket,
        control: ?*Socket,
    ) Proxy {
        return .{
            .frontend = frontend,
            .backend = backend,
            .capture = capture,
            .control = control,
        };
    }

    /// Run the proxy (blocks until terminated)
    pub fn run(self: *Proxy, rt: *zio.Runtime) !void {
        while (self.state != .terminated) {
            // Build poll items
            var items = std.BoundedArray(zzmq.PollItem, 4){};

            try items.append(.{ .socket = self.frontend, .events = .{ .pollin = true, .pollout = true } });
            try items.append(.{ .socket = self.backend, .events = .{ .pollin = true, .pollout = true } });
            if (self.control) |ctrl| {
                try items.append(.{ .socket = ctrl, .events = .{ .pollin = true } });
            }

            // Poll for events
            const ready = try zzmq.poll(items.slice(), -1);
            if (ready == 0) continue;

            // Check control socket first
            if (self.control) |ctrl| {
                if (items.get(2).revents.pollin) {
                    try self.handleControl(ctrl, rt);
                }
            }

            if (self.state != .active) continue;

            // Forward frontend → backend
            if (items.get(0).revents.pollin and items.get(1).revents.pollout) {
                try self.forward(self.frontend, self.backend, &self.stats.frontend_recv, &self.stats.backend_send, rt);
            }

            // Forward backend → frontend
            if (items.get(1).revents.pollin and items.get(0).revents.pollout) {
                try self.forward(self.backend, self.frontend, &self.stats.backend_recv, &self.stats.frontend_send, rt);
            }
        }
    }

    /// Forward messages from one socket to another
    fn forward(
        self: *Proxy,
        from: *Socket,
        to: *Socket,
        recv_stats: *Statistics.EndpointStats,
        send_stats: *Statistics.EndpointStats,
        rt: *zio.Runtime,
    ) !void {
        // Forward burst of messages
        const burst_size = 8;

        for (0..burst_size) |_| {
            // Forward all parts of one message
            while (true) {
                var msg = from.tryRecv() orelse return;  // No more messages
                defer if (msg.needsDeinit()) msg.deinit();

                const nbytes = msg.size();
                recv_stats.count += 1;
                recv_stats.bytes += nbytes;

                const has_more = msg.hasMore();

                // Copy to capture socket if present
                if (self.capture) |capture| {
                    var copy = try msg.copy();
                    try capture.send(&copy, if (has_more) .{ .sndmore = true } else .{});
                }

                // Forward to destination
                try to.send(&msg, if (has_more) .{ .sndmore = true } else .{});
                send_stats.count += 1;
                send_stats.bytes += nbytes;

                if (!has_more) break;
            }
        }
    }

    fn handleControl(self: *Proxy, control: *Socket, rt: *zio.Runtime) !void {
        var msg = try control.recv(rt);
        defer msg.deinit();

        const cmd = msg.data();

        if (std.mem.eql(u8, cmd, "PAUSE")) {
            self.state = .paused;
        } else if (std.mem.eql(u8, cmd, "RESUME")) {
            self.state = .active;
        } else if (std.mem.eql(u8, cmd, "TERMINATE")) {
            self.state = .terminated;
        } else if (std.mem.eql(u8, cmd, "STATISTICS")) {
            // Send 8-part reply with statistics
            const stat_values = [8]u64{
                self.stats.frontend_recv.count,
                self.stats.frontend_recv.bytes,
                self.stats.frontend_send.count,
                self.stats.frontend_send.bytes,
                self.stats.backend_recv.count,
                self.stats.backend_recv.bytes,
                self.stats.backend_send.count,
                self.stats.backend_send.bytes,
            };

            for (stat_values, 0..) |value, i| {
                var reply = try Message.initSize(@sizeOf(u64));
                std.mem.writeInt(u64, reply.data()[0..8], value, .little);
                try control.send(&reply, if (i < 7) .{ .sndmore = true } else .{});
            }
            return;
        }

        // For REP socket, send empty reply
        if (control.getSocketType() == .rep) {
            var reply = Message.init();
            try control.send(&reply, .{});
        }
    }
};

/// Convenience function matching libzmq API
pub fn proxy(frontend: *Socket, backend: *Socket, capture: ?*Socket) !void {
    var p = Proxy.init(frontend, backend, capture, null);
    try p.run(frontend.runtime);
}

/// Steerable proxy with control socket
pub fn proxySteerable(
    frontend: *Socket,
    backend: *Socket,
    capture: ?*Socket,
    control: *Socket,
) !void {
    var p = Proxy.init(frontend, backend, capture, control);
    try p.run(frontend.runtime);
}
```

### Proxy Use Cases

**1. PUB/SUB Forwarder (XPUB-XSUB)**:
```zig
// Publishers connect to frontend (XSUB)
// Subscribers connect to backend (XPUB)
// Subscriptions flow: XPUB → proxy → XSUB → publisher
var frontend = try ctx.socket(.xsub);
var backend = try ctx.socket(.xpub);

try frontend.connect("tcp://publisher:5555");
try backend.bind("tcp://*:5556");

try zzmq.proxy(&frontend, &backend, null);
```

**2. Request/Reply Broker (ROUTER-DEALER)**:
```zig
// Clients connect to frontend (ROUTER)
// Workers connect to backend (DEALER)
var frontend = try ctx.socket(.router);
var backend = try ctx.socket(.dealer);

try frontend.bind("tcp://*:5555");
try backend.bind("tcp://*:5556");

try zzmq.proxy(&frontend, &backend, null);
```

**3. Load Balancer (ROUTER-ROUTER)**:
```zig
// More control over routing
var frontend = try ctx.socket(.router);
var backend = try ctx.socket(.router);
// Custom routing logic in between
```

**4. Tap/Monitor (with capture)**:
```zig
var capture = try ctx.socket(.pub);
try capture.bind("tcp://*:5557");

// All proxied messages also sent to capture socket
try zzmq.proxy(&frontend, &backend, &capture);
```

### Proxy Flow Diagram

```
                    ┌─────────────────────────────────────┐
                    │             PROXY                   │
                    │                                     │
  Clients ──────────┼─► Frontend ────────► Backend ──────┼──► Workers
                    │      ▲                    │         │
                    │      │                    ▼         │
  Workers ──────────┼─► Backend ─────────► Frontend ─────┼──► Clients
                    │                                     │
                    │      │         Capture              │
                    │      └──────────┬──────────         │
                    └─────────────────┼───────────────────┘
                                      │
                                      ▼
                               Monitor/Tap
```

---

## Options and Configuration

### Socket Options Reference

Complete list of socket options with libzmq compatibility notes.

#### High Water Marks

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `send_hwm` | u32 | 1000 | Send high water mark (messages) |
| `recv_hwm` | u32 | 1000 | Receive high water mark (messages) |
| `maxmsgsize` | i64 | -1 | Max message size (-1 = no limit) |

```zig
try socket.setOption(.send_hwm, 100);
try socket.setOption(.recv_hwm, 100);
try socket.setOption(.maxmsgsize, 1024 * 1024);  // 1MB max
```

#### Timeouts

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `send_timeout` | i32 | -1 | Send timeout in ms (-1 = infinite) |
| `recv_timeout` | i32 | -1 | Receive timeout in ms (-1 = infinite) |
| `connect_timeout` | u32 | 0 | Connection timeout in ms (0 = no timeout) |
| `handshake_ivl` | u32 | 30000 | ZMTP handshake timeout in ms |

```zig
try socket.setOption(.send_timeout, 5000);  // 5 second timeout
try socket.setOption(.recv_timeout, 5000);
try socket.setOption(.connect_timeout, 3000);  // 3s connect timeout
```

#### Connection Lifecycle

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `linger` | i32 | -1 | Linger time on close (-1 = infinite, 0 = drop) |
| `reconnect_ivl` | u32 | 100 | Initial reconnect interval in ms |
| `reconnect_ivl_max` | u32 | 0 | Max reconnect interval (0 = no max, use exponential) |
| `backlog` | u32 | 100 | Listen backlog for TCP |
| `immediate` | bool | false | Only queue to completed connections |

```zig
try socket.setOption(.linger, 1000);  // Wait 1s for pending messages
try socket.setOption(.reconnect_ivl, 100);
try socket.setOption(.reconnect_ivl_max, 30000);  // Cap at 30s
try socket.setOption(.immediate, true);  // Don't queue until connected
```

#### Identity and Routing

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `routing_id` | []u8 | null | Socket identity (1-255 bytes) |
| `connect_routing_id` | []u8 | null | Set routing ID for next connect() |
| `probe_router` | bool | false | Send empty message on connect (DEALER) |
| `router_mandatory` | bool | false | Error if routing ID not found (ROUTER) |
| `router_handover` | bool | false | Take over existing routing ID (ROUTER) |

```zig
try socket.setOption(.routing_id, "worker-1");
try socket.setOption(.router_mandatory, true);
```

#### Heartbeat (ZMTP keepalive)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `heartbeat_ivl` | u32 | 0 | Heartbeat interval in ms (0 = disabled) |
| `heartbeat_timeout` | u32 | 0 | Heartbeat timeout (default: ivl * 2) |
| `heartbeat_ttl` | u32 | 0 | Heartbeat TTL for remote peer |

```zig
try socket.setOption(.heartbeat_ivl, 10000);     // Ping every 10s
try socket.setOption(.heartbeat_timeout, 30000); // 30s timeout
```

#### Pattern-Specific Options

| Option | Patterns | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `subscribe` | SUB, XSUB | []u8 | - | Subscribe to topic prefix |
| `unsubscribe` | SUB, XSUB | []u8 | - | Unsubscribe from topic |
| `req_relaxed` | REQ | bool | false | Allow out-of-order recv |
| `req_correlate` | REQ | bool | false | Match replies by request ID |
| `conflate` | PULL, SUB, DEALER | bool | false | Keep only last message |
| `invert_matching` | XPUB, PUB | bool | false | Invert subscription matching |
| `xpub_verbose` | XPUB | bool | false | Pass all subscriptions |
| `xpub_manual` | XPUB | bool | false | Manual subscription handling |

```zig
// SUB socket
try socket.setOption(.subscribe, "topic.");
try socket.setOption(.subscribe, "");  // Subscribe to all

// REQ socket - relaxed mode
try socket.setOption(.req_relaxed, true);
try socket.setOption(.req_correlate, true);

// Conflate - keep only latest
try socket.setOption(.conflate, true);
```

#### TCP Tuning

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `tcp_keepalive` | i32 | -1 | OS keepalive (-1 = OS default, 0 = off, 1 = on) |
| `tcp_keepalive_idle` | i32 | -1 | Idle time before keepalive |
| `tcp_keepalive_cnt` | i32 | -1 | Keepalive probe count |
| `tcp_keepalive_intvl` | i32 | -1 | Keepalive probe interval |
| `sndbuf` | i32 | -1 | OS send buffer size (-1 = OS default) |
| `rcvbuf` | i32 | -1 | OS receive buffer size |
| `tos` | i32 | 0 | IP Type of Service |
| `ipv6` | bool | false | Prefer IPv6 |

```zig
try socket.setOption(.tcp_keepalive, 1);
try socket.setOption(.tcp_keepalive_idle, 60);
try socket.setOption(.sndbuf, 128 * 1024);  // 128KB
try socket.setOption(.ipv6, true);
```

#### Security Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `plain_server` | bool | false | Enable PLAIN server mode |
| `plain_username` | []u8 | null | PLAIN username |
| `plain_password` | []u8 | null | PLAIN password |
| `curve_server` | bool | false | Enable CURVE server mode |
| `curve_publickey` | [32]u8 | null | CURVE public key |
| `curve_secretkey` | [32]u8 | null | CURVE secret key |
| `curve_serverkey` | [32]u8 | null | CURVE server public key (client) |
| `zap_domain` | []u8 | "" | ZAP authentication domain |

```zig
// PLAIN server
try socket.setOption(.plain_server, true);

// PLAIN client
try socket.setOption(.plain_username, "admin");
try socket.setOption(.plain_password, "secret");

// CURVE (post-MVP)
try socket.setOption(.curve_server, true);
try socket.setOption(.curve_secretkey, &server_secret);
try socket.setOption(.curve_publickey, &server_public);
```

#### Read-Only Options

| Option | Type | Description |
|--------|------|-------------|
| `fd` | i32 | Underlying file descriptor (for external polling) |
| `events` | u32 | Current poll events (POLLIN, POLLOUT) |
| `type` | SocketType | Socket type |
| `last_endpoint` | []u8 | Last endpoint bound/connected |
| `mechanism` | Mechanism | Current security mechanism |
| `rcvmore` | bool | More message parts available |

```zig
const socket_fd = socket.getOption(.fd);
const events = socket.getOption(.events);
const has_input = (events & zzmq.POLLIN) != 0;
const last_ep = socket.getOption(.last_endpoint);
```

### Socket Options Implementation

```zig
pub const SocketOption = enum {
    // High water marks
    send_hwm,
    recv_hwm,
    maxmsgsize,

    // Timeouts
    send_timeout,
    recv_timeout,
    connect_timeout,
    handshake_ivl,

    // Connection lifecycle
    linger,
    reconnect_ivl,
    reconnect_ivl_max,
    backlog,
    immediate,

    // Identity and routing
    routing_id,
    connect_routing_id,
    probe_router,
    router_mandatory,
    router_handover,

    // Heartbeat
    heartbeat_ivl,
    heartbeat_timeout,
    heartbeat_ttl,

    // Pattern-specific
    subscribe,
    unsubscribe,
    req_relaxed,
    req_correlate,
    conflate,
    invert_matching,
    xpub_verbose,
    xpub_manual,

    // TCP tuning
    tcp_keepalive,
    tcp_keepalive_idle,
    tcp_keepalive_cnt,
    tcp_keepalive_intvl,
    sndbuf,
    rcvbuf,
    tos,
    ipv6,

    // Security
    plain_server,
    plain_username,
    plain_password,
    curve_server,
    curve_publickey,
    curve_secretkey,
    curve_serverkey,
    zap_domain,

    // Read-only
    fd,
    events,
    socket_type,
    last_endpoint,
    mechanism,
    rcvmore,

    pub fn Type(comptime self: SocketOption) type {
        return switch (self) {
            .send_hwm, .recv_hwm, .backlog => u32,
            .maxmsgsize => i64,
            .send_timeout, .recv_timeout, .linger => i32,
            .connect_timeout, .handshake_ivl => u32,
            .reconnect_ivl, .reconnect_ivl_max => u32,
            .heartbeat_ivl, .heartbeat_timeout, .heartbeat_ttl => u32,
            .routing_id, .connect_routing_id => []const u8,
            .subscribe, .unsubscribe => []const u8,
            .plain_username, .plain_password => []const u8,
            .zap_domain, .last_endpoint => []const u8,
            .curve_publickey, .curve_secretkey, .curve_serverkey => *const [32]u8,
            .tcp_keepalive, .tcp_keepalive_idle,
            .tcp_keepalive_cnt, .tcp_keepalive_intvl => i32,
            .sndbuf, .rcvbuf, .tos => i32,
            .fd => std.posix.fd_t,
            .events => u32,
            .probe_router, .router_mandatory, .router_handover,
            .req_relaxed, .req_correlate, .conflate,
            .invert_matching, .xpub_verbose, .xpub_manual,
            .plain_server, .curve_server, .ipv6, .immediate, .rcvmore => bool,
            .socket_type => SocketType,
            .mechanism => Mechanism,
        };
    }

    pub fn isReadOnly(self: SocketOption) bool {
        return switch (self) {
            .fd, .events, .socket_type, .last_endpoint, .mechanism, .rcvmore => true,
            else => false,
        };
    }
};

pub const SocketOptions = struct {
    // High water marks
    send_hwm: u32 = 1000,
    recv_hwm: u32 = 1000,
    maxmsgsize: i64 = -1,

    // Timeouts
    send_timeout: i32 = -1,
    recv_timeout: i32 = -1,
    connect_timeout: u32 = 0,
    handshake_ivl: u32 = 30000,

    // Connection lifecycle
    linger: i32 = -1,
    reconnect_ivl: u32 = 100,
    reconnect_ivl_max: u32 = 0,
    backlog: u32 = 100,
    immediate: bool = false,

    // Identity
    routing_id: ?RoutingId = null,
    probe_router: bool = false,
    router_mandatory: bool = false,
    router_handover: bool = false,

    // Heartbeat
    heartbeat_ivl: u32 = 0,
    heartbeat_timeout: u32 = 0,
    heartbeat_ttl: u32 = 0,

    // Pattern-specific
    req_relaxed: bool = false,
    req_correlate: bool = false,
    conflate: bool = false,
    invert_matching: bool = false,
    xpub_verbose: bool = false,
    xpub_manual: bool = false,

    // TCP tuning
    tcp_keepalive: i32 = -1,
    tcp_keepalive_idle: i32 = -1,
    tcp_keepalive_cnt: i32 = -1,
    tcp_keepalive_intvl: i32 = -1,
    sndbuf: i32 = -1,
    rcvbuf: i32 = -1,
    tos: i32 = 0,
    ipv6: bool = false,

    // Security
    plain_server: bool = false,
    plain_username: ?[]const u8 = null,
    plain_password: ?[]const u8 = null,
    curve_server: bool = false,
    curve_publickey: ?*const [32]u8 = null,
    curve_secretkey: ?*const [32]u8 = null,
    curve_serverkey: ?*const [32]u8 = null,
    zap_domain: []const u8 = "",
};
```

### Message Properties (ZMTP Metadata)

ZMTP 3.1 allows peers to exchange metadata during handshake and attach properties to messages.

#### Standard Properties

| Property | Description | When Available |
|----------|-------------|----------------|
| `Socket-Type` | Peer's socket type (e.g., "DEALER") | After handshake |
| `Identity` | Peer's routing ID | After handshake (if set) |
| `User-Id` | Authenticated user ID | After ZAP authentication |
| `Peer-Address` | Peer's network address | Always |
| `Routing-Id` | Message routing ID (ROUTER) | On received messages |

#### Accessing Properties

```zig
pub const Message = struct {
    // ... existing fields ...

    /// Metadata properties from ZMTP handshake
    properties: ?*MessageProperties = null,

    /// Get a property value
    pub fn getProperty(self: *const Message, name: []const u8) ?[]const u8 {
        const props = self.properties orelse return null;
        return props.get(name);
    }
};

pub const MessageProperties = struct {
    allocator: std.mem.Allocator,
    map: std.StringHashMap([]const u8),

    pub fn get(self: *const MessageProperties, name: []const u8) ?[]const u8 {
        return self.map.get(name);
    }

    pub fn set(self: *MessageProperties, name: []const u8, value: []const u8) !void {
        const name_copy = try self.allocator.dupe(u8, name);
        const value_copy = try self.allocator.dupe(u8, value);
        try self.map.put(name_copy, value_copy);
    }

    pub fn deinit(self: *MessageProperties) void {
        var iter = self.map.iterator();
        while (iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit();
    }
};
```

#### Usage Examples

```zig
// Receive message and check properties
const msg = try socket.recv(rt);
defer msg.deinit();

// Get peer's socket type
if (msg.getProperty("Socket-Type")) |socket_type| {
    std.log.info("Received from {s} socket", .{socket_type});
}

// Get authenticated user (if ZAP used)
if (msg.getProperty("User-Id")) |user_id| {
    std.log.info("Message from user: {s}", .{user_id});
}

// Get peer's address
if (msg.getProperty("Peer-Address")) |addr| {
    std.log.info("Peer address: {s}", .{addr});
}
```

#### Setting Properties on Send (ZMTP 3.1)

```zig
// Create message with custom properties
var msg = try Message.init(allocator, data);
defer msg.deinit();

// Attach application metadata
try msg.setProperty("X-Request-Id", request_id);
try msg.setProperty("X-Timestamp", timestamp_str);

try socket.send(rt, &msg);
```

#### Property Propagation

Properties are set at different points:

| Property | Set By | Propagated To |
|----------|--------|---------------|
| `Socket-Type` | ZMTP handshake | All messages from this peer |
| `Identity` | ZMTP handshake | All messages from this peer |
| `User-Id` | ZAP authentication | All messages from this peer |
| `Peer-Address` | Engine | All messages from this peer |
| `Routing-Id` | ROUTER pattern | Received messages only |
| `X-*` (custom) | Sender | Single message |

#### Engine Property Handling

```zig
pub const Engine = struct {
    /// Properties from ZMTP handshake (apply to all messages)
    peer_properties: MessageProperties,

    fn performHandshake(self: *Engine, rt: *zio.Runtime) !void {
        // Exchange ZMTP greetings and handshakes...

        // Extract properties from READY command
        if (self.codec.ready_properties) |props| {
            try self.peer_properties.set("Socket-Type", props.socket_type);
            if (props.identity) |id| {
                try self.peer_properties.set("Identity", id);
            }
        }

        // Add peer address
        const addr_str = try std.fmt.allocPrint(
            self.allocator,
            "{}",
            .{self.stream.peerAddress()},
        );
        try self.peer_properties.set("Peer-Address", addr_str);
    }

    fn attachProperties(self: *Engine, msg: *Message) !void {
        // Clone peer properties to message
        if (msg.properties == null) {
            msg.properties = try self.peer_properties.clone(msg.allocator);
        }
    }
};
```

### STREAM Socket (Raw TCP)

STREAM is a special socket type for raw TCP communication, not following ZMQ patterns.
Useful for implementing custom protocols or HTTP servers.

**libzmq reference:** `ZMQ_STREAM` (type 11)

```zig
pub const Stream = struct {
    pub const State = struct {
        /// Map of routing ID to pipe (like ROUTER)
        pipes: std.AutoHashMap(RoutingId, *Pipe),
        next_routing_id: u32 = 1,
        notify: bool = true,  // ZMQ_STREAM_NOTIFY
    };

    pub fn onPipeAttached(state: *State, pipe: *Pipe) void {
        // Assign routing ID
        const id = state.next_routing_id;
        state.next_routing_id += 1;
        pipe.routing_id = RoutingId.fromInt(id);
        state.pipes.put(pipe.routing_id.?, pipe) catch {};

        // Send connect notification (empty message with routing ID)
        if (state.notify) {
            var notification = Message.initEmpty();
            notification.routing_id = pipe.routing_id;
            pipe.inbound.trySend(notification) catch {};
        }
    }

    pub fn onPipeDetached(state: *State, pipe: *Pipe) void {
        if (pipe.routing_id) |id| {
            _ = state.pipes.remove(id);

            // Send disconnect notification
            if (state.notify) {
                var notification = Message.initEmpty();
                notification.routing_id = id;
                // Queue to socket somehow...
            }
        }
    }

    /// Send raw data to a specific peer
    pub fn send(
        state: *State,
        msg: *Message,
        rt: *zio.Runtime,
    ) SendError!void {
        // First frame must be routing ID
        const routing_id = msg.routing_id orelse return error.InvalidMessage;

        const pipe = state.pipes.get(routing_id) orelse return error.HostUnreachable;

        // Send raw (no ZMTP framing)
        try pipe.sendRaw(rt, msg.data());
    }

    /// Receive raw data (returns routing ID + data)
    pub fn recv(
        state: *State,
        rt: *zio.Runtime,
    ) RecvError!Message {
        // Fair queue from all pipes
        // Returns message with routing_id set
        // ...
    }
};
```

**Use cases:**
- HTTP server implementation
- Custom protocol handling
- Bridging ZMQ to non-ZMQ systems

---

## Error Handling

### Error Types

```zig
pub const SendError = error{
    SocketClosed,
    NotSupported,      // Pattern doesn't support send
    InvalidState,      // REQ waiting for reply
    NoRoute,           // No connected peers
    HostUnreachable,   // ROUTER mandatory, peer gone
    Timeout,
    Canceled,
};

pub const RecvError = error{
    SocketClosed,
    NotSupported,      // Pattern doesn't support recv
    InvalidState,      // REQ hasn't sent yet
    Timeout,
    Canceled,
};

pub const BindError = error{
    InvalidEndpoint,
    AddressInUse,
    PermissionDenied,
    OutOfMemory,
};

pub const ConnectError = error{
    InvalidEndpoint,
    OutOfMemory,
};
```

---

## API Design

### Complete Example

```zig
const std = @import("std");
const zio = @import("zio");
const zzmq = @import("zzmq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize ZIO runtime
    const rt = try zio.Runtime.init(allocator, .{});
    defer rt.deinit();

    // Create ZZMQ context with this runtime
    const ctx = try zzmq.Context.init(rt, allocator, .{});
    defer ctx.deinit();

    // Create sockets
    var pusher = try ctx.socket(zzmq.Push);
    defer pusher.close();

    var puller = try ctx.socket(zzmq.Pull);
    defer puller.close();

    // Configure
    try pusher.setOption(.send_hwm, 100);
    try puller.setOption(.recv_hwm, 100);

    // Bind/Connect
    try puller.bind("tcp://127.0.0.1:5555");
    try pusher.connect("tcp://127.0.0.1:5555");

    // Send messages
    var msg = try zzmq.Message.initFromSlice(allocator, "Hello, World!");
    try pusher.send(&msg);

    // Receive
    var received = try puller.recv();
    defer received.deinit();

    std.debug.print("Received: {s}\n", .{received.slice()});
}
```

### PUB/SUB Example

```zig
fn publisher(ctx: *zzmq.Context) !void {
    var pub_socket = try ctx.socket(zzmq.Pub);
    defer pub_socket.close();

    try pub_socket.bind("tcp://127.0.0.1:5556");

    var i: u32 = 0;
    while (true) : (i += 1) {
        var msg = try zzmq.Message.initFromSlice(
            ctx.allocator,
            std.fmt.bufPrint(&buf, "Message {}", .{i}),
        );
        try pub_socket.send(&msg);
        zio.time.sleep(ctx.runtime, .fromMilliseconds(100));
    }
}

fn subscriber(ctx: *zzmq.Context) !void {
    var sub_socket = try ctx.socket(zzmq.Sub);
    defer sub_socket.close();

    try sub_socket.setOption(.subscribe, "");  // Subscribe to all
    try sub_socket.connect("tcp://127.0.0.1:5556");

    while (true) {
        var msg = try sub_socket.recv();
        defer msg.deinit();
        std.debug.print("Got: {s}\n", .{msg.slice()});
    }
}
```

---

## C FFI Layer

ZZMQ provides a C-compatible API layer for interoperability with existing ZeroMQ language bindings and applications. The goal is source-level compatibility with libzmq's public API.

### Design Principles

1. **Opaque Handles**: All objects (context, socket, message) are opaque `void*` pointers
2. **Error Codes**: Return -1 on error, set errno (via thread-local storage)
3. **Memory Ownership**: Clear ownership semantics matching libzmq exactly
4. **ABI Stability**: Struct layouts and function signatures match libzmq 4.3.x
5. **Zero-Copy Bridge**: Minimal overhead when crossing FFI boundary

### C Header (zzmq.h)

```c
/* ZZMQ - ZeroMQ-compatible messaging for Zig/ZIO */
/* API-compatible with libzmq 4.3.x */

#ifndef __ZZMQ_H_INCLUDED__
#define __ZZMQ_H_INCLUDED__

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

/*  Version macros                                                            */
#define ZZMQ_VERSION_MAJOR 1
#define ZZMQ_VERSION_MINOR 0
#define ZZMQ_VERSION_PATCH 0

/*  For libzmq compatibility, also define ZMQ_VERSION                         */
#define ZMQ_VERSION_MAJOR 4
#define ZMQ_VERSION_MINOR 3
#define ZMQ_VERSION_PATCH 6

/*  Symbol visibility                                                         */
#if defined _WIN32
#  if defined ZZMQ_STATIC
#    define ZZMQ_EXPORT
#  elif defined ZZMQ_BUILD
#    define ZZMQ_EXPORT __declspec(dllexport)
#  else
#    define ZZMQ_EXPORT __declspec(dllimport)
#  endif
#else
#  define ZZMQ_EXPORT __attribute__((visibility("default")))
#endif

/*  For libzmq compatibility                                                  */
#define ZMQ_EXPORT ZZMQ_EXPORT

/******************************************************************************/
/*  Error codes                                                               */
/******************************************************************************/

/*  Native ZZMQ/ZMQ error codes                                               */
#define ZZMQ_HAUSNUMERO 156384712
#define EFSM            (ZZMQ_HAUSNUMERO + 51)
#define ENOCOMPATPROTO  (ZZMQ_HAUSNUMERO + 52)
#define ETERM           (ZZMQ_HAUSNUMERO + 53)
#define EMTHREAD        (ZZMQ_HAUSNUMERO + 54)

ZZMQ_EXPORT int zmq_errno(void);
ZZMQ_EXPORT const char *zmq_strerror(int errnum);
ZZMQ_EXPORT void zmq_version(int *major, int *minor, int *patch);

/******************************************************************************/
/*  Context                                                                   */
/******************************************************************************/

#define ZMQ_IO_THREADS          1
#define ZMQ_MAX_SOCKETS         2
#define ZMQ_SOCKET_LIMIT        3
#define ZMQ_THREAD_PRIORITY     3
#define ZMQ_THREAD_SCHED_POLICY 4
#define ZMQ_MAX_MSGSZ           5
#define ZMQ_MSG_T_SIZE          6

ZZMQ_EXPORT void *zmq_ctx_new(void);
ZZMQ_EXPORT int zmq_ctx_term(void *context);
ZZMQ_EXPORT int zmq_ctx_shutdown(void *context);
ZZMQ_EXPORT int zmq_ctx_set(void *context, int option, int optval);
ZZMQ_EXPORT int zmq_ctx_get(void *context, int option);

/*  Legacy aliases                                                            */
ZZMQ_EXPORT void *zmq_init(int io_threads);
ZZMQ_EXPORT int zmq_term(void *context);
#define zmq_ctx_destroy zmq_ctx_term

/******************************************************************************/
/*  Message                                                                   */
/******************************************************************************/

/*  Message struct - must match libzmq's 64-byte layout                       */
typedef struct zmq_msg_t {
    unsigned char _[64] __attribute__((aligned(sizeof(void *))));
} zmq_msg_t;

typedef void(zmq_free_fn)(void *data, void *hint);

ZZMQ_EXPORT int zmq_msg_init(zmq_msg_t *msg);
ZZMQ_EXPORT int zmq_msg_init_size(zmq_msg_t *msg, size_t size);
ZZMQ_EXPORT int zmq_msg_init_data(zmq_msg_t *msg, void *data, size_t size,
                                   zmq_free_fn *ffn, void *hint);
ZZMQ_EXPORT int zmq_msg_send(zmq_msg_t *msg, void *socket, int flags);
ZZMQ_EXPORT int zmq_msg_recv(zmq_msg_t *msg, void *socket, int flags);
ZZMQ_EXPORT int zmq_msg_close(zmq_msg_t *msg);
ZZMQ_EXPORT int zmq_msg_move(zmq_msg_t *dest, zmq_msg_t *src);
ZZMQ_EXPORT int zmq_msg_copy(zmq_msg_t *dest, zmq_msg_t *src);
ZZMQ_EXPORT void *zmq_msg_data(zmq_msg_t *msg);
ZZMQ_EXPORT size_t zmq_msg_size(const zmq_msg_t *msg);
ZZMQ_EXPORT int zmq_msg_more(const zmq_msg_t *msg);
ZZMQ_EXPORT int zmq_msg_get(const zmq_msg_t *msg, int property);
ZZMQ_EXPORT int zmq_msg_set(zmq_msg_t *msg, int property, int optval);
ZZMQ_EXPORT const char *zmq_msg_gets(const zmq_msg_t *msg, const char *property);

/*  Message properties                                                        */
#define ZMQ_MORE   1
#define ZMQ_SHARED 3

/******************************************************************************/
/*  Socket                                                                    */
/******************************************************************************/

/*  Socket types                                                              */
#define ZMQ_PAIR   0
#define ZMQ_PUB    1
#define ZMQ_SUB    2
#define ZMQ_REQ    3
#define ZMQ_REP    4
#define ZMQ_DEALER 5
#define ZMQ_ROUTER 6
#define ZMQ_PULL   7
#define ZMQ_PUSH   8
#define ZMQ_XPUB   9
#define ZMQ_XSUB   10
#define ZMQ_STREAM 11

/*  Socket options (subset - full list in implementation)                     */
#define ZMQ_SNDHWM          23
#define ZMQ_RCVHWM          24
#define ZMQ_ROUTING_ID      5
#define ZMQ_SUBSCRIBE       6
#define ZMQ_UNSUBSCRIBE     7
#define ZMQ_LINGER          17
#define ZMQ_RECONNECT_IVL   18
#define ZMQ_RCVTIMEO        27
#define ZMQ_SNDTIMEO        28
#define ZMQ_FD              14
#define ZMQ_EVENTS          15

/*  Send/recv flags                                                           */
#define ZMQ_DONTWAIT 1
#define ZMQ_SNDMORE  2

ZZMQ_EXPORT void *zmq_socket(void *context, int type);
ZZMQ_EXPORT int zmq_close(void *socket);
ZZMQ_EXPORT int zmq_setsockopt(void *socket, int option, const void *optval,
                                size_t optvallen);
ZZMQ_EXPORT int zmq_getsockopt(void *socket, int option, void *optval,
                                size_t *optvallen);
ZZMQ_EXPORT int zmq_bind(void *socket, const char *addr);
ZZMQ_EXPORT int zmq_connect(void *socket, const char *addr);
ZZMQ_EXPORT int zmq_unbind(void *socket, const char *addr);
ZZMQ_EXPORT int zmq_disconnect(void *socket, const char *addr);
ZZMQ_EXPORT int zmq_send(void *socket, const void *buf, size_t len, int flags);
ZZMQ_EXPORT int zmq_recv(void *socket, void *buf, size_t len, int flags);
ZZMQ_EXPORT int zmq_socket_monitor(void *socket, const char *addr, int events);

/******************************************************************************/
/*  Polling                                                                   */
/******************************************************************************/

#define ZMQ_POLLIN  1
#define ZMQ_POLLOUT 2
#define ZMQ_POLLERR 4
#define ZMQ_POLLPRI 8

typedef int zmq_fd_t;

typedef struct zmq_pollitem_t {
    void *socket;
    zmq_fd_t fd;
    short events;
    short revents;
} zmq_pollitem_t;

ZZMQ_EXPORT int zmq_poll(zmq_pollitem_t *items, int nitems, long timeout);

/******************************************************************************/
/*  Proxy                                                                     */
/******************************************************************************/

ZZMQ_EXPORT int zmq_proxy(void *frontend, void *backend, void *capture);
ZZMQ_EXPORT int zmq_proxy_steerable(void *frontend, void *backend,
                                     void *capture, void *control);

/******************************************************************************/
/*  Capability probing                                                        */
/******************************************************************************/

ZZMQ_EXPORT int zmq_has(const char *capability);

#ifdef __cplusplus
}
#endif

#endif /* __ZZMQ_H_INCLUDED__ */
```

### Zig FFI Implementation

```zig
// src/c_api.zig - C FFI layer implementation

const std = @import("std");
const zzmq = @import("zzmq.zig");

/// Thread-local errno for C API
threadlocal var c_errno: c_int = 0;

/// Opaque handle wrapper
fn Handle(comptime T: type) type {
    return struct {
        ptr: *T,

        pub fn fromOpaque(opaque: ?*anyopaque) ?@This() {
            const p = opaque orelse return null;
            return .{ .ptr = @ptrCast(@alignCast(p)) };
        }

        pub fn toOpaque(self: @This()) *anyopaque {
            return @ptrCast(self.ptr);
        }
    };
}

const ContextHandle = Handle(zzmq.Context);
const SocketHandle = Handle(zzmq.Socket);

// ============================================================================
// Error handling
// ============================================================================

export fn zmq_errno() c_int {
    return c_errno;
}

export fn zmq_strerror(errnum: c_int) [*:0]const u8 {
    return switch (errnum) {
        @intFromEnum(std.os.E.EFSM) => "Operation cannot be accomplished in current state",
        @intFromEnum(std.os.E.ETERM) => "Context was terminated",
        @intFromEnum(std.os.E.EMTHREAD) => "No thread available",
        @intFromEnum(std.os.E.ENOCOMPATPROTO) => "Protocol not compatible with socket type",
        else => std.os.strerror(@enumFromInt(errnum)),
    };
}

export fn zmq_version(major: *c_int, minor: *c_int, patch: *c_int) void {
    major.* = 4;  // libzmq compatibility
    minor.* = 3;
    patch.* = 6;
}

// ============================================================================
// Context
// ============================================================================

export fn zmq_ctx_new() ?*anyopaque {
    const ctx = zzmq.Context.init(.{}) catch |err| {
        c_errno = errToErrno(err);
        return null;
    };
    return ctx.toOpaque();
}

export fn zmq_ctx_term(context: ?*anyopaque) c_int {
    const ctx = ContextHandle.fromOpaque(context) orelse {
        c_errno = @intFromEnum(std.os.E.EFAULT);
        return -1;
    };
    ctx.ptr.deinit() catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };
    return 0;
}

export fn zmq_ctx_set(context: ?*anyopaque, option: c_int, optval: c_int) c_int {
    const ctx = ContextHandle.fromOpaque(context) orelse {
        c_errno = @intFromEnum(std.os.E.EFAULT);
        return -1;
    };
    ctx.ptr.setOption(@enumFromInt(option), optval) catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };
    return 0;
}

export fn zmq_ctx_get(context: ?*anyopaque, option: c_int) c_int {
    const ctx = ContextHandle.fromOpaque(context) orelse {
        c_errno = @intFromEnum(std.os.E.EFAULT);
        return -1;
    };
    return ctx.ptr.getOption(@enumFromInt(option)) catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };
}

// ============================================================================
// Socket
// ============================================================================

export fn zmq_socket(context: ?*anyopaque, socket_type: c_int) ?*anyopaque {
    const ctx = ContextHandle.fromOpaque(context) orelse {
        c_errno = @intFromEnum(std.os.E.EFAULT);
        return null;
    };

    const sock_type: zzmq.SocketType = @enumFromInt(socket_type);
    const socket = ctx.ptr.socket(sock_type) catch |err| {
        c_errno = errToErrno(err);
        return null;
    };

    return socket.toOpaque();
}

export fn zmq_close(socket: ?*anyopaque) c_int {
    const sock = SocketHandle.fromOpaque(socket) orelse {
        c_errno = @intFromEnum(std.os.E.ENOTSOCK);
        return -1;
    };
    sock.ptr.close() catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };
    return 0;
}

export fn zmq_bind(socket: ?*anyopaque, addr: [*:0]const u8) c_int {
    const sock = SocketHandle.fromOpaque(socket) orelse {
        c_errno = @intFromEnum(std.os.E.ENOTSOCK);
        return -1;
    };
    sock.ptr.bind(std.mem.sliceTo(addr, 0)) catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };
    return 0;
}

export fn zmq_connect(socket: ?*anyopaque, addr: [*:0]const u8) c_int {
    const sock = SocketHandle.fromOpaque(socket) orelse {
        c_errno = @intFromEnum(std.os.E.ENOTSOCK);
        return -1;
    };
    sock.ptr.connect(std.mem.sliceTo(addr, 0)) catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };
    return 0;
}

export fn zmq_send(
    socket: ?*anyopaque,
    buf: [*]const u8,
    len: usize,
    flags: c_int,
) c_int {
    const sock = SocketHandle.fromOpaque(socket) orelse {
        c_errno = @intFromEnum(std.os.E.ENOTSOCK);
        return -1;
    };

    const dontwait = (flags & 1) != 0;
    const sndmore = (flags & 2) != 0;

    var msg = zzmq.Message.initFromSlice(buf[0..len]) catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };

    if (sndmore) msg.setMore(true);

    if (dontwait) {
        if (!sock.ptr.trySend(&msg)) {
            c_errno = @intFromEnum(std.os.E.EAGAIN);
            return -1;
        }
    } else {
        sock.ptr.send(&msg) catch |err| {
            c_errno = errToErrno(err);
            return -1;
        };
    }

    return @intCast(len);
}

export fn zmq_recv(
    socket: ?*anyopaque,
    buf: [*]u8,
    len: usize,
    flags: c_int,
) c_int {
    const sock = SocketHandle.fromOpaque(socket) orelse {
        c_errno = @intFromEnum(std.os.E.ENOTSOCK);
        return -1;
    };

    const dontwait = (flags & 1) != 0;

    var msg: zzmq.Message = undefined;
    if (dontwait) {
        msg = sock.ptr.tryRecv() orelse {
            c_errno = @intFromEnum(std.os.E.EAGAIN);
            return -1;
        };
    } else {
        msg = sock.ptr.recv() catch |err| {
            c_errno = errToErrno(err);
            return -1;
        };
    }
    defer msg.deinit();

    const copy_len = @min(len, msg.size());
    @memcpy(buf[0..copy_len], msg.data()[0..copy_len]);

    return @intCast(msg.size());
}

// ============================================================================
// Message API
// ============================================================================

/// zmq_msg_t layout - must match C header (64 bytes)
const CMessage = extern struct {
    _data: [64]u8 align(@alignOf(*anyopaque)),

    fn fromZig(msg: *zzmq.Message) *CMessage {
        return @ptrCast(msg);
    }

    fn toZig(self: *CMessage) *zzmq.Message {
        return @ptrCast(@alignCast(self));
    }
};

export fn zmq_msg_init(msg: *CMessage) c_int {
    msg.toZig().* = zzmq.Message.init();
    return 0;
}

export fn zmq_msg_init_size(msg: *CMessage, size: usize) c_int {
    msg.toZig().* = zzmq.Message.initSize(size) catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };
    return 0;
}

export fn zmq_msg_init_data(
    msg: *CMessage,
    data: [*]u8,
    size: usize,
    ffn: ?*const fn (*anyopaque, *anyopaque) callconv(.C) void,
    hint: ?*anyopaque,
) c_int {
    msg.toZig().* = zzmq.Message.initExternal(data[0..size], ffn, hint);
    return 0;
}

export fn zmq_msg_data(msg: *CMessage) ?[*]u8 {
    return msg.toZig().data().ptr;
}

export fn zmq_msg_size(msg: *const CMessage) usize {
    return @constCast(msg).toZig().size();
}

export fn zmq_msg_more(msg: *const CMessage) c_int {
    return if (@constCast(msg).toZig().hasMore()) 1 else 0;
}

export fn zmq_msg_close(msg: *CMessage) c_int {
    msg.toZig().deinit();
    return 0;
}

// ============================================================================
// Polling
// ============================================================================

const CPollItem = extern struct {
    socket: ?*anyopaque,
    fd: c_int,
    events: c_short,
    revents: c_short,
};

export fn zmq_poll(items: [*]CPollItem, nitems: c_int, timeout: c_long) c_int {
    if (nitems <= 0) return 0;

    const n: usize = @intCast(nitems);
    var ready: c_int = 0;

    // Convert to ZZMQ poll items and poll
    var zzmq_items: [64]zzmq.PollItem = undefined;
    if (n > zzmq_items.len) {
        c_errno = @intFromEnum(std.os.E.EINVAL);
        return -1;
    }

    for (items[0..n], 0..) |*item, i| {
        zzmq_items[i] = .{
            .socket = if (item.socket) |s| SocketHandle.fromOpaque(s).?.ptr else null,
            .fd = if (item.socket == null) item.fd else -1,
            .events = @bitCast(item.events),
        };
    }

    const timeout_ms: i64 = if (timeout < 0) -1 else timeout;
    const result = zzmq.poll(zzmq_items[0..n], timeout_ms) catch |err| {
        c_errno = errToErrno(err);
        return -1;
    };

    // Copy results back
    for (items[0..n], 0..) |*item, i| {
        item.revents = @bitCast(zzmq_items[i].revents);
        if (item.revents != 0) ready += 1;
    }

    return ready;
}

// ============================================================================
// Helpers
// ============================================================================

fn errToErrno(err: anyerror) c_int {
    return switch (err) {
        error.OutOfMemory => @intFromEnum(std.os.E.ENOMEM),
        error.InvalidEndpoint => @intFromEnum(std.os.E.EINVAL),
        error.AddressInUse => @intFromEnum(std.os.E.EADDRINUSE),
        error.SocketClosed => @intFromEnum(std.os.E.ENOTSOCK),
        error.Timeout => @intFromEnum(std.os.E.ETIMEDOUT),
        error.ContextTerminated => @intFromEnum(std.os.E.ETERM),
        error.InvalidState => @intFromEnum(std.os.E.EFSM),
        error.NoRoute => @intFromEnum(std.os.E.EHOSTUNREACH),
        else => @intFromEnum(std.os.E.EINVAL),
    };
}
```

### FFI Design Decisions

| Decision | Rationale |
|----------|-----------|
| **64-byte zmq_msg_t** | Binary compatibility with existing bindings |
| **Thread-local errno** | C convention, avoids per-call error struct |
| **Opaque void* handles** | Type safety via Zig, flexibility for C |
| **Direct memory layout** | No wrapper allocation for messages |
| **Export with C calling convention** | `export fn` in Zig generates C-compatible symbols |

### Compatibility Testing

```c
// test_compat.c - Verify API compatibility with libzmq
#include <assert.h>
#include <string.h>

// Can be compiled against either libzmq or zzmq
#include <zmq.h>

void test_basic() {
    void *ctx = zmq_ctx_new();
    assert(ctx != NULL);

    void *push = zmq_socket(ctx, ZMQ_PUSH);
    void *pull = zmq_socket(ctx, ZMQ_PULL);
    assert(push && pull);

    assert(zmq_bind(pull, "inproc://test") == 0);
    assert(zmq_connect(push, "inproc://test") == 0);

    const char *msg = "Hello";
    assert(zmq_send(push, msg, strlen(msg), 0) == strlen(msg));

    char buf[256];
    int rc = zmq_recv(pull, buf, sizeof(buf), 0);
    assert(rc == strlen(msg));
    assert(memcmp(buf, msg, rc) == 0);

    zmq_close(push);
    zmq_close(pull);
    zmq_ctx_term(ctx);
}

int main() {
    test_basic();
    return 0;
}
```

### Build Integration

```zig
// build.zig snippet for C library generation
pub fn build(b: *std.Build) void {
    const lib = b.addSharedLibrary(.{
        .name = "zzmq",
        .root_source_file = .{ .path = "src/c_api.zig" },
        .target = target,
        .optimize = optimize,
    });

    // Generate C header
    lib.installHeader("include/zzmq.h", "zmq.h");

    // Install shared library
    b.installArtifact(lib);

    // Also build static library
    const static_lib = b.addStaticLibrary(.{
        .name = "zzmq",
        .root_source_file = .{ .path = "src/c_api.zig" },
        .target = target,
        .optimize = optimize,
    });
    b.installArtifact(static_lib);
}
```

---

## Performance Considerations: Hot Path Deep Dive

This section analyzes the critical hot paths for sending and receiving messages, grounded in libzmq source analysis, and designs ZZMQ's approach using idiomatic ZIO for maximum performance.

### libzmq Hot Path Analysis

**Send Hot Path (socket_base.cpp:1205-1290)**:
```
socket.send(msg)
    │
    ├─ 1. Lock (if thread-safe): scoped_optional_lock_t
    ├─ 2. Context check: unlikely(_ctx_terminated)
    ├─ 3. Message validation: unlikely(!msg_->check())
    ├─ 4. Process commands: process_commands(0, true)  // Non-blocking
    ├─ 5. Set flags: msg_->set_flags(msg_t::more)
    │
    └─ xsend(msg) → Pattern-specific
           │
           └─ lb_t::send(msg) [for PUSH]
                  │
                  └─ pipe_t::write(msg)
                         │
                         ├─ check_hwm()  // Credit check
                         └─ ypipe_t::write(value, incomplete)
                                │
                                ├─ _queue.back() = value  // No atomic!
                                ├─ _queue.push()
                                └─ if (!incomplete) _f = &_queue.back()

    ───────── Flush on complete message ─────────

    pipe_t::flush()
           │
           └─ ypipe_t::flush()
                  │
                  └─ _c.cas(_w, _f)  // SINGLE atomic CAS per batch!
                         │
                         └─ If CAS fails (reader sleeping) → send_activate_read()
```

**Receive Hot Path (socket_base.cpp:1293-1382)**:
```
socket.recv(msg)
    │
    ├─ 1. Lock (if thread-safe)
    ├─ 2. Context check
    ├─ 3. Message validation
    │
    ├─ 4. THROTTLED command check:
    │      if (++_ticks == 100) {    // Only every 100 messages!
    │          process_commands(0);
    │          _ticks = 0;
    │      }
    │
    └─ xrecv(msg) → Pattern-specific
           │
           └─ fq_t::recv(msg) [for PULL]
                  │
                  ├─ Round-robin: _pipes[_current]
                  └─ pipe_t::read(msg)
                         │
                         └─ ypipe_t::read(value)
                                │
                                ├─ check_read()
                                │      └─ _c.cas(&_queue.front(), NULL)  // SINGLE atomic
                                │
                                ├─ *value = _queue.front()  // No atomic!
                                └─ _queue.pop()

    ───────── Credit return on LWM ─────────

    if (_lwm > 0 && _msgs_read % _lwm == 0)
        send_activate_write(_peer, _msgs_read)  // Flow control
```

### libzmq Key Performance Insights

1. **Lock-Free ypipe_t**: The underlying queue uses only ONE atomic CAS operation per flush/read batch. Writers never contend with readers except at the single `_c` pointer.

2. **Batched Flush**: Multiple messages can be written before flush(). The `_f` pointer tracks "flush up to here", so many writes → one atomic.

3. **Throttled Command Check**: `recv()` only checks for commands every 100 messages (`inbound_poll_rate`). This avoids signaler overhead on the hot path.

4. **VSM (Very Small Message)**: Messages ≤~30 bytes stored inline in msg_t (exact size is platform-dependent: `64 - sizeof(metadata_t*) - 3 - 16 - sizeof(uint32_t)` = ~33 bytes on 64-bit). Zero allocation on hot path.

5. **Speculative Write** (stream_engine_base.cpp:393-397): When sending, try to write immediately without waiting for POLLOUT:
   ```cpp
   void restart_output() {
       set_pollout();
       out_event();  // Try writing NOW - low latency!
   }
   ```

6. **Batched Network I/O** (stream_engine_base.cpp:331): Accumulate up to `out_batch_size` (8KB default) before syscall:
   ```cpp
   while (_outsize < _options.out_batch_size) {
       _next_msg(&_tx_msg);  // Pull from pipe
       _encoder->load_msg(&_tx_msg);
       _outsize += _encoder->encode(...);
   }
   write(_outpos, _outsize);  // One syscall for many messages
   ```

7. **Active/Inactive Partitioning**: lb_t and fq_t partition pipes into [0, _active) and [_active, size). Checking readiness only iterates active pipes.

### ZZMQ Hot Path Design

**Design Principle**: ZIO's coroutines eliminate the need for libzmq's complex command/signal machinery. We get equivalent performance through cooperative scheduling.

#### Send Hot Path

```zig
pub fn send(self: *Socket, msg: *Message) !void {
    // 1. Check socket state (no lock needed - single-owner coroutine model)
    if (self.terminated) return error.SocketTerminated;

    // 2. Set flags
    msg.setFlags(.{ .more = flags.sndmore });

    // 3. Pattern-specific send
    try self.pattern.xsend(self, msg);
}

// Load balancer for PUSH
fn xsend(self: *LoadBalancer, msg: *Message) !void {
    while (self.active > 0) {
        const pipe = self.pipes.items[self.current];

        // Try non-blocking write first (hot path)
        if (pipe.tryWrite(msg)) {
            // Success! Handle multipart and round-robin
            self.more = msg.hasMore();
            if (!self.more) {
                pipe.flush();  // Flush on complete message
                self.current = (self.current + 1) % self.active;
            }
            return;
        }

        // Pipe full - deactivate and try next
        self.deactivate(self.current);
    }

    // All pipes full - suspend coroutine (ZIO handles this)
    return error.WouldBlock;
}

// Pipe write - uses ZIO channel
pub fn tryWrite(self: *Pipe, msg: *Message) bool {
    // Non-blocking try_send - returns false if full
    if (self.outbound.trySend(msg.*)) |_| {
        if (!msg.hasMore()) self.msgs_written += 1;
        return true;
    } else {
        return false;
    }
}

pub fn flush(self: *Pipe) void {
    // ZIO channels don't need explicit flush - data is immediately visible
    // But we may need to wake the engine if it was sleeping
    if (self.engine_sleeping) {
        self.engine_wakeup.set();  // Signal engine coroutine
    }
}
```

#### Receive Hot Path

```zig
pub fn recv(self: *Socket) !Message {
    // 1. Check socket state
    if (self.terminated) return error.SocketTerminated;

    // 2. Pattern-specific receive
    return self.pattern.xrecv(self);
}

// Fair queue for PULL
fn xrecv(self: *FairQueue) !Message {
    while (self.active > 0) {
        const pipe = self.pipes.items[self.current];

        // Try non-blocking read first (hot path)
        if (pipe.tryRead()) |msg| {
            self.more = msg.hasMore();
            if (!self.more) {
                self.current = (self.current + 1) % self.active;
            }
            // LWM credit return
            self.maybeReturnCredit(pipe);
            return msg;
        }

        // No message - deactivate and try next
        self.deactivate(self.current);
    }

    // All pipes empty - suspend coroutine (ZIO handles this)
    return error.WouldBlock;
}

// Pipe read - uses ZIO channel
pub fn tryRead(self: *Pipe) ?Message {
    // Non-blocking try_receive
    if (self.inbound.tryReceive()) |msg| {
        if (!msg.hasMore()) self.msgs_read += 1;
        return msg;
    } else {
        return null;
    }
}
```

#### Engine Hot Path

```zig
// Writer coroutine - moves messages from pipe to network
fn writerLoop(self: *Engine) !void {
    var batch_buffer: [8192]u8 = undefined;
    var batch_size: usize = 0;

    while (!self.terminated) {
        // Batch messages up to 8KB (like libzmq)
        while (batch_size < batch_buffer.len) {
            // Non-blocking check first
            if (self.pipe.outbound.tryReceive()) |msg| {
                batch_size += self.codec.encode(&msg, batch_buffer[batch_size..]);
            } else {
                break;  // No more messages ready
            }
        }

        if (batch_size > 0) {
            // Single syscall for entire batch (ZIO stream.sendAll API)
            try self.stream.sendAll(rt, batch_buffer[0..batch_size], .{});
            batch_size = 0;
        } else {
            // No data - yield to let other coroutines run
            // Then wait for either: new message or socket writable
            const msg = self.pipe.outbound.receive(rt);  // Suspends coroutine
            batch_size = self.codec.encode(&msg, &batch_buffer);
        }

        // Speculative write: try immediately (low latency)
        try self.stream.sendAll(rt, batch_buffer[0..batch_size], .{});
        batch_size = 0;
    }
}

// Reader coroutine - moves messages from network to pipe
fn readerLoop(self: *Engine, rt: *zio.Runtime) !void {
    var read_buffer: [65536]u8 = undefined;  // Large buffer for batched reads

    while (!self.terminated) {
        // Read from network (may suspend) - ZIO stream.recv API
        const n = try self.stream.recv(rt, &read_buffer, .{});
        if (n == 0) return error.ConnectionClosed;

        // Decode and push messages
        var decoder = self.codec.decoder();
        var offset: usize = 0;

        while (offset < n) {
            if (try decoder.decode(read_buffer[offset..n])) |msg| {
                // Try non-blocking first
                if (!self.pipe.inbound.trySend(msg)) {
                    // Pipe full (HWM) - must suspend
                    try self.pipe.inbound.send(msg);  // Suspends
                }
            }
            offset = decoder.consumed();
        }
    }
}
```

### Zero-Copy Message Design

```zig
pub const Message = extern struct {
    // 64 bytes total - fits in one cache line (like libzmq msg_t)
    data: Data,
    flags: Flags,
    _padding: [6]u8 = undefined,

    pub const Data = extern union {
        // Inline storage - no allocation for small messages (≤48 bytes)
        inline_data: InlineData,

        // Allocated storage - reference counted
        allocated: AllocatedData,

        // External storage - user-provided buffer with free function
        external: ExternalData,

        // Constant storage - static data, never freed
        constant: ConstantData,
    };

    pub const InlineData = extern struct {
        bytes: [48]u8,  // Max inline size
        len: u8,        // Actual length
        type_tag: u8 = 0,  // Identifies as inline
    };

    pub const AllocatedData = extern struct {
        ptr: [*]u8,
        len: usize,
        capacity: usize,
        refcount: *std.atomic.Value(u32),
        type_tag: u8 = 1,
    };

    pub const ExternalData = extern struct {
        ptr: [*]u8,
        len: usize,
        free_fn: *const fn (*anyopaque, *anyopaque) void,
        hint: *anyopaque,
        type_tag: u8 = 2,
    };

    pub const ConstantData = extern struct {
        ptr: [*]const u8,
        len: usize,
        _unused: [24]u8 = undefined,
        type_tag: u8 = 3,
    };

    pub const Flags = packed struct {
        more: bool = false,
        command: bool = false,
        _reserved: u6 = 0,
    };

    // Hot path: create small message inline (no allocation!)
    pub fn initInline(bytes: []const u8) Message {
        std.debug.assert(bytes.len <= 48);
        var msg = Message{ .data = undefined, .flags = .{} };
        @memcpy(msg.data.inline_data.bytes[0..bytes.len], bytes);
        msg.data.inline_data.len = @intCast(bytes.len);
        msg.data.inline_data.type_tag = 0;
        return msg;
    }

    // Zero-copy: take ownership of external buffer
    pub fn initExternal(
        data: []u8,
        free_fn: *const fn (*anyopaque, *anyopaque) void,
        hint: *anyopaque,
    ) Message {
        return .{
            .data = .{ .external = .{
                .ptr = data.ptr,
                .len = data.len,
                .free_fn = free_fn,
                .hint = hint,
            } },
            .flags = .{},
        };
    }

    // Copy for fan-out: increment refcount (no data copy!)
    pub fn addRef(self: *Message) void {
        switch (self.getTypeTag()) {
            1 => {  // Allocated
                _ = self.data.allocated.refcount.fetchAdd(1, .monotonic);
            },
            else => {},  // Inline/external/constant don't need refcounting
        }
    }
};
```

### ZIO-Specific Optimizations

1. **No Signaler Needed**: ZIO coroutines suspend/resume automatically on channel operations. No need for libzmq's signaler_t/mailbox_t complexity.

2. **No Command Queue**: Direct method calls between components. ZIO's cooperative scheduling ensures single-threaded semantics within a socket.

3. **Channel = Lock-free Queue**: ZIO's bounded channels provide the same semantics as libzmq's ypipe, with coroutine suspension on full/empty.

4. **Speculative Operations**: Try non-blocking operations first, only suspend if they would block:
   ```zig
   // Fast path: non-blocking
   if (channel.trySend(msg)) |_| return;
   // Slow path: suspend and wait
   try channel.send(msg);
   ```

5. **Batched I/O via ZIO**: ZIO's io_uring backend can batch multiple syscalls automatically.

### Performance Comparison

| Aspect | libzmq | ZZMQ (ZIO) |
|--------|--------|------------|
| Thread model | I/O threads + command queues | Coroutines, single-threaded per socket |
| Inter-thread queue | ypipe_t (lock-free, 1 CAS per batch) | ZIO channel (similar, coroutine-aware) |
| Signaling | socketpair/eventfd | Coroutine resume (no syscall) |
| Context switch | Signal → epoll → thread wake | Coroutine switch (~10-20 cycles) |
| Command overhead | Every send/recv processes commands | No commands - direct calls |
| Batching | Manual (out_batch_size) | Automatic (io_uring) + manual batching |

### Latency Hot Path Summary

**Lowest latency send** (ZZMQ):
```
send(msg)
  → tryWrite (1 atomic check)
  → channel.trySend (1 atomic)
  → writerCoroutine resumes (coroutine switch ~20 cycles)
  → encode (memcpy)
  → stream.write (syscall)
```

**Lowest latency recv** (ZZMQ):
```
recv()
  → tryRead (1 atomic check)
  → channel.tryReceive (1 atomic)
  → return message
```

For comparison, libzmq:
```
send(msg)
  → lock (mutex)
  → process_commands (signaler check)
  → xsend
  → pipe.write (ypipe)
  → pipe.flush (CAS)
  → send_activate_read (mailbox + signaler)
  → I/O thread wakes (epoll + thread switch ~1000+ cycles)
  → encode + write
```

ZZMQ's advantage: **No thread switches, no signaler overhead, no command processing on hot path**.

### Memory Layout for Cache Efficiency

```
┌─────────────────────────────────────────────────────────────────┐
│                      Message (64 bytes)                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ InlineData: [48 bytes data] [1 byte len] [1 byte tag]    │   │
│  │     OR                                                    │   │
│  │ AllocatedData: [8 ptr] [8 len] [8 cap] [8 refcount] [tag]│   │
│  └──────────────────────────────────────────────────────────┘   │
│  [1 byte flags] [6 bytes padding]                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
               Fits in single cache line (64 bytes)
               No pointer chasing for small messages
```

---

## Monitoring and Events

Like libzmq's socket monitor, ZZMQ provides visibility into socket lifecycle events.

### Event Types

```zig
pub const SocketEvent = union(enum) {
    // Connection events
    connected: ConnectedEvent,
    connect_delayed: ConnectDelayedEvent,
    connect_retried: ConnectRetriedEvent,

    // Listener events
    listening: ListeningEvent,
    bind_failed: BindFailedEvent,

    // Accept events
    accepted: AcceptedEvent,
    accept_failed: AcceptFailedEvent,

    // Disconnect events
    disconnected: DisconnectedEvent,
    closed: ClosedEvent,
    close_failed: CloseFailedEvent,

    // Handshake events
    handshake_succeeded: HandshakeEvent,
    handshake_failed: HandshakeFailedEvent,

    // Protocol events
    protocol_error: ProtocolErrorEvent,

    pub const ConnectedEvent = struct {
        endpoint: []const u8,
        peer_address: ?std.net.Address,
    };

    pub const DisconnectedEvent = struct {
        endpoint: []const u8,
        peer_address: ?std.net.Address,
        reason: DisconnectReason,
    };

    pub const DisconnectReason = enum {
        peer_closed,
        heartbeat_timeout,
        protocol_error,
        local_close,
    };

    // ... etc
};
```

### Monitor with BroadcastChannel

ZZMQ uses `zio.BroadcastChannel` for monitoring, enabling multiple independent monitors
to observe the same socket's events. This is a natural fit because:

- All monitors want ALL events (no per-subscriber filtering needed)
- Events are small and infrequent
- Multiple monitors are naturally supported
- No complex subscription logic required

**Design Decisions:**

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Channel type** | `zio.BroadcastChannel(SocketEvent)` | Multiple consumers, same events |
| **Filtering** | Receiver-side | Events are small; simple switch statement |
| **Backpressure** | Drop oldest on full | Monitoring shouldn't block I/O |
| **Initialization** | Lazy (on first subscribe) | No overhead if unused |
| **libzmq compat** | C FFI shim available | Encodes to binary, uses inproc PAIR |

```zig
pub const SocketInner = struct {
    // ... other fields ...

    /// Lazy-initialized broadcast channel for monitor events
    monitor_channel: ?*zio.BroadcastChannel(SocketEvent) = null,

    /// Track dropped events (for diagnostics)
    monitor_events_dropped: u64 = 0,

    /// Get or create the monitor broadcast channel
    pub fn monitorChannel(self: *SocketInner) !*zio.BroadcastChannel(SocketEvent) {
        if (self.monitor_channel == null) {
            const channel = try self.ctx.allocator.create(zio.BroadcastChannel(SocketEvent));
            channel.* = zio.BroadcastChannel(SocketEvent).init(self.ctx.allocator, .{
                .buffer_size = 64,  // Per-receiver buffer
            });
            self.monitor_channel = channel;
        }
        return self.monitor_channel.?;
    }

    /// Subscribe to monitor events
    /// Returns a receiver that will get all future events
    pub fn monitorSubscribe(self: *SocketInner) !zio.BroadcastChannel(SocketEvent).Receiver {
        const channel = try self.monitorChannel();
        return channel.subscribe();
    }

    /// Internal: emit event to all monitors (non-blocking)
    fn emitMonitorEvent(self: *SocketInner, event: SocketEvent) void {
        if (self.monitor_channel) |channel| {
            channel.trySend(event) catch |err| switch (err) {
                error.Full => {
                    // Best-effort: monitoring shouldn't block socket operations
                    self.monitor_events_dropped += 1;
                },
                else => {},
            };
        }
    }

    fn deinit(self: *SocketInner) void {
        if (self.monitor_channel) |channel| {
            channel.deinit();
            self.ctx.allocator.destroy(channel);
        }
        // ... other cleanup ...
    }
};
```

### Usage Examples

**Simple single monitor:**

```zig
var socket = try ctx.socket(zzmq.Push);
defer socket.close();

// Subscribe to events
var events = try socket.monitorSubscribe();
defer events.unsubscribe();

// Monitor in separate coroutine
try group.spawn(rt, monitorLoop, .{ &events, rt });

fn monitorLoop(events: *Receiver, rt: *zio.Runtime) void {
    while (events.receive(rt)) |event| {
        switch (event) {
            .connected => |e| log.info("Connected to {s}", .{e.endpoint}),
            .disconnected => |e| log.warn("Disconnected: {s} ({})", .{e.endpoint, e.reason}),
            .handshake_failed => |e| log.err("Handshake failed: {}", .{e.error}),
            else => {},
        }
    } else |err| {
        if (err != error.ChannelClosed) log.err("Monitor error: {}", .{err});
    }
}
```

**Multiple independent monitors (debugging, metrics, alerting):**

```zig
var socket = try ctx.socket(zzmq.Router);

// Each subscriber gets ALL events independently
var debug_rx = try socket.monitorSubscribe();
var metrics_rx = try socket.monitorSubscribe();
var alerts_rx = try socket.monitorSubscribe();

// Debug: log everything
try group.spawn(rt, debugMonitor, .{ &debug_rx, rt });

// Metrics: count events
try group.spawn(rt, metricsMonitor, .{ &metrics_rx, rt });

// Alerts: only care about failures
try group.spawn(rt, alertsMonitor, .{ &alerts_rx, rt });

fn metricsMonitor(rx: *Receiver, rt: *zio.Runtime) void {
    var connected_count: u64 = 0;
    var disconnected_count: u64 = 0;

    while (rx.receive(rt)) |event| {
        switch (event) {
            .connected => connected_count += 1,
            .disconnected => disconnected_count += 1,
            else => {},
        }
    } else |_| {}
}

fn alertsMonitor(rx: *Receiver, rt: *zio.Runtime) void {
    while (rx.receive(rt)) |event| {
        switch (event) {
            .handshake_failed, .bind_failed, .accept_failed => |e| {
                sendAlert("Socket failure", e);
            },
            else => {},  // Ignore non-failure events
        }
    } else |_| {}
}
```

### Integration Points

Events are emitted from:

| Component | Events |
|-----------|--------|
| **Listener** | `listening`, `bind_failed`, `accepted`, `accept_failed` |
| **Connector** | `connected`, `connect_delayed`, `connect_retried` |
| **Engine** | `handshake_succeeded`, `handshake_failed`, `disconnected`, `protocol_error` |
| **Socket** | `closed`, `close_failed`, `monitor_stopped` |

```zig
// Example: Engine emits events
fn performHandshake(self: *Engine, rt: *zio.Runtime) !void {
    // ... handshake logic ...

    if (handshake_error) |err| {
        self.socket.emitMonitorEvent(.{ .handshake_failed = .{
            .endpoint = self.endpoint,
            .error = err,
            .timestamp = rt.now(),
        }});
        return error.HandshakeFailed;
    }

    self.socket.emitMonitorEvent(.{ .handshake_succeeded = .{
        .endpoint = self.endpoint,
        .peer_identity = self.codec.peer_identity,
        .timestamp = rt.now(),
    }});
}

// Example: Connector emits events
fn connectLoop(self: *Connector, rt: *zio.Runtime) void {
    while (self.state != .disconnected) {
        self.socket.emitMonitorEvent(.{ .connect_delayed = .{
            .endpoint = self.endpoint,
            .timestamp = rt.now(),
        }});

        const stream = self.endpoint.connect(rt) catch |err| {
            self.socket.emitMonitorEvent(.{ .connect_retried = .{
                .endpoint = self.endpoint,
                .interval = self.reconnect.interval,
                .attempt = self.reconnect.attempt,
                .timestamp = rt.now(),
            }});
            self.scheduleReconnect(rt);
            continue;
        };

        self.socket.emitMonitorEvent(.{ .connected = .{
            .endpoint = self.endpoint,
            .peer_address = stream.peerAddress(),
            .timestamp = rt.now(),
        }});

        // ... run engine ...
    }
}
```

### C FFI Compatibility

For libzmq compatibility, we provide a shim that bridges to the inproc PAIR pattern:

```zig
/// libzmq-compatible monitor API
/// Creates an internal bridge that encodes events to binary format
pub fn zmq_socket_monitor(
    socket: *anyopaque,
    endpoint: [*:0]const u8,
    events: c_int,
) c_int {
    const sock = @ptrCast(*Socket, @alignCast(@alignOf(Socket), socket));

    // Create internal monitor bridge
    const bridge = MonitorBridge.create(sock, endpoint, events) catch return -1;

    // Bridge subscribes to BroadcastChannel and forwards to inproc PAIR
    sock.ctx.spawnInternal(MonitorBridge.run, .{bridge}) catch return -1;

    return 0;
}

const MonitorBridge = struct {
    receiver: zio.BroadcastChannel(SocketEvent).Receiver,
    pair_socket: *Socket,
    event_mask: u32,

    fn run(self: *MonitorBridge, rt: *zio.Runtime) void {
        defer self.cleanup();

        while (self.receiver.receive(rt)) |event| {
            if (!self.matchesMask(event)) continue;

            // Encode to libzmq binary format (2 frames)
            var event_frame: [6]u8 = undefined;
            var addr_frame: [256]u8 = undefined;
            const encoded = self.encodeEvent(event, &event_frame, &addr_frame);

            // Send as multipart to PAIR socket
            self.pair_socket.sendMultipart(rt, encoded) catch break;
        } else |_| {}
    }

    fn encodeEvent(self: *MonitorBridge, event: SocketEvent, ...) []const []const u8 {
        // libzmq format: [event_id:u16 ++ value:u32, endpoint:string]
        // ...
    }
};
```

---

## libzmq Reference: Critical Behaviors and Edge Cases

This section documents specific libzmq behaviors that ZZMQ must match. These are derived from studying the libzmq source code.

### Credit-Based Flow Control (HWM)

**libzmq implementation** (`src/pipe.cpp:533-538`):
```cpp
bool zmq::pipe_t::check_hwm () const
{
    const bool full =
      _hwm > 0 && _msgs_written - _peers_msgs_read >= uint64_t (_hwm);
    return !full;
}
```

**Key behaviors:**
1. HWM is tracked via credit system, not queue length
2. Writer tracks `_msgs_written`, reader periodically sends `_msgs_read` back
3. Writer is blocked when `written - peer_read >= HWM`

**ZZMQ approach:** ZIO channels handle this implicitly - channel capacity = HWM.

### Low Water Mark (LWM)

**libzmq implementation** (`src/pipe.cpp:452-473`):
```cpp
int zmq::pipe_t::compute_lwm (int hwm_)
{
    // LWM = HWM / 2
    const int result = (hwm_ + 1) / 2;
    return result;
}
```

**Why LWM matters:**
- If LWM = 0: After filling queue, reader must drain ALL messages before writer resumes → poor performance
- If LWM = HWM-1: Lock-step filling, one message at a time → poor performance
- LWM = HWM/2: Good balance between throughput and latency

**libzmq behavior** (`src/pipe.cpp:201-202`):
```cpp
if (_lwm > 0 && _msgs_read % _lwm == 0)
    send_activate_write (_peer, _msgs_read);
```

Reader sends activation signal every LWM messages read.

**ZZMQ approach:** ZIO channels don't expose LWM directly. May need wrapper if fine-grained control needed.

### Message Counting for HWM

**Critical detail** (`src/pipe.cpp:198-199, 227-232`):
```cpp
// Only count complete messages, not MORE frames
if (!(msg_->flags () & msg_t::more) && !msg_->is_routing_id ())
    _msgs_read++;
```

**HWM counts MESSAGES, not frames:**
- A 10-frame multipart message counts as 1 message for HWM
- Routing ID frames don't count
- This is essential for multipart atomicity

**ZZMQ must match:** Count only complete messages against HWM, not individual frames.

### Pipe State Machine

**libzmq pipe states** (`src/pipe.hpp:206-214`):
```cpp
enum {
    active,
    delimiter_received,
    waiting_for_delimiter,
    term_ack_sent,
    term_req_sent1,
    term_req_sent2
} _state;
```

**Why 6 states?**
1. Both ends may terminate simultaneously
2. Delimiter may arrive before or after term command
3. Must handle all timing combinations gracefully

**Key termination behaviors:**
- `delimiter_received`: Delimiter came first, waiting for term command
- `waiting_for_delimiter`: Term came first, draining pending messages
- `term_req_sent1/2`: Handles simultaneous termination from both sides

**ZZMQ approach:** ZIO channel close semantics may simplify this, but we must ensure:
- Pending messages are delivered before close completes (linger behavior)
- Both ends coordinate cleanly regardless of close timing

### Multipart Message Rollback

**libzmq implementation** (`src/pipe.cpp:236-247`):
```cpp
void zmq::pipe_t::rollback () const
{
    msg_t msg;
    if (_out_pipe) {
        while (_out_pipe->unwrite (&msg)) {
            zmq_assert (msg.flags () & msg_t::more);
            const int rc = msg.close ();
            errno_assert (rc == 0);
        }
    }
}
```

**When rollback happens:**
- Connection lost mid-multipart send
- Termination requested mid-multipart
- Send fails mid-multipart

**ZZMQ must support:** Ability to "undo" partial multipart sends. With ZIO channels, may need to track multipart state separately.

### Load Balancer Dropping Mode

**libzmq implementation** (`src/lb.cpp:56-101`):
```cpp
int zmq::lb_t::sendpipe (msg_t *msg_, pipe_t **pipe_)
{
    // Drop the message if required
    if (_dropping) {
        _more = (msg_->flags () & msg_t::more) != 0;
        _dropping = _more;
        // ... drop message ...
        return 0;
    }

    // If send fails mid-multipart
    if (_more) {
        _pipes[_current]->rollback ();
        _dropping = (msg_->flags () & msg_t::more) != 0;
        _more = false;
        errno = EAGAIN;
        return -2;  // Special error code
    }
```

**Critical edge case:**
- If pipe disconnects mid-multipart, enter "dropping mode"
- Continue consuming frames until end of multipart (MORE=false)
- Return success (0) even though messages dropped - for backward compatibility

**ZZMQ must handle:** Track `_more` state in load balancer, handle mid-multipart disconnects gracefully.

### Fair Queue Atomicity Assertion

**libzmq implementation** (`src/fq.cpp:77-80`):
```cpp
//  Check the atomicity of the message.
//  If we've already received the first part of the message
//  we should get the remaining parts without blocking.
zmq_assert (!_more);
```

**Invariant:** If we're mid-multipart recv, next read MUST succeed (pipe shouldn't be empty).

**This assertion catches bugs** - if it fires, multipart atomicity was violated.

### Session-Engine Lifecycle

**Pipe creation timing** (`src/session_base.cpp:394-424`):
```cpp
void zmq::session_base_t::engine_ready ()
{
    // Pipe created AFTER handshake completes
    if (!_pipe && !is_terminating ()) {
        // Create pipe pair...
    }
}
```

**Key insight:** Pipe doesn't exist until handshake succeeds. This means:
- No messages queued during handshake
- Socket only sees connection after successful handshake
- Failed handshakes don't create orphan pipes

### Reconnection and Subscription Resend

**libzmq implementation** (`src/session_base.cpp:576-582`):
```cpp
//  For subscriber sockets we hiccup the inbound pipe, which will cause
//  the socket object to resend all the subscriptions.
if (_pipe
    && (options.type == ZMQ_SUB || options.type == ZMQ_XSUB
        || options.type == ZMQ_DISH))
    _pipe->hiccup ();
```

**Hiccup mechanism:**
1. Creates new inbound pipe
2. Notifies peer to switch to new pipe
3. Triggers socket to resend subscriptions

**Why needed:** After reconnect, the new PUB peer doesn't know our subscriptions.

**ZZMQ must implement:** Track subscriptions in SUB socket, resend on reconnect.

### Linger Timer

**libzmq implementation** (`src/session_base.cpp:497-516`):
```cpp
if (linger_ > 0) {
    zmq_assert (!_has_linger_timer);
    add_timer (linger_, linger_timer_id);
    _has_linger_timer = true;
}
// Pipe termination with delay
_pipe->terminate (linger_ != 0);
```

**Linger semantics:**
- `linger = -1`: Infinite wait for pending messages
- `linger = 0`: Drop all pending messages immediately
- `linger > 0`: Wait up to N ms for pending messages, then drop

**Timer expiry** (`src/session_base.cpp:522-532`):
```cpp
void zmq::session_base_t::timer_event (int id_)
{
    zmq_assert (id_ == linger_timer_id);
    _has_linger_timer = false;
    _pipe->terminate (false);  // Force terminate, drop messages
}
```

**ZZMQ implementation with cancellation shielding:**

In ZIO, we use `rt.beginShield()`/`rt.endShield()` to protect the linger flush from
cancellation. This ensures pending messages get a fair chance to be sent even if
the context is shutting down.

```zig
pub const Session = struct {
    pipe: *Pipe,
    engine: ?*Engine,
    linger_ms: i32,
    linger_complete: zio.Notify = .{},

    /// Called when socket.close() is invoked
    pub fn beginTerminate(self: *Session, rt: *zio.Runtime) void {
        if (self.linger_ms == 0) {
            // Immediate termination - drop all pending
            self.pipe.drop();
            self.terminateEngine();
            return;
        }

        // Shield from cancellation - linger must complete
        rt.beginShield();
        defer rt.endShield();

        if (self.linger_ms < 0) {
            // Infinite linger - flush all pending messages
            self.flushPendingMessages(rt) catch {};
        } else {
            // Timed linger - flush with deadline
            const deadline = rt.now().addDuration(
                Duration.fromMilliseconds(@intCast(self.linger_ms))
            );

            // Try to flush, racing against deadline
            var flush_op = self.asyncFlushPending();
            const result = zio.select(rt, .{
                .flush = &flush_op,
                .timeout = zio.Timeout{ .deadline = deadline },
            }) catch {
                // Interrupted - drop remaining
                self.pipe.drop();
                return;
            };

            switch (result) {
                .flush => {}, // All messages sent
                .timeout => {
                    // Deadline reached - drop remaining
                    self.pipe.drop();
                },
            }
        }

        self.terminateEngine();
        self.linger_complete.set();  // Signal completion
    }

    fn flushPendingMessages(self: *Session, rt: *zio.Runtime) !void {
        while (self.pipe.outbound.tryReceive()) |msg| {
            if (self.engine) |eng| {
                try eng.sendImmediate(rt, msg);
            } else {
                msg.deinit();  // No engine - must drop
            }
        }
    }
};
```

**Key differences from libzmq:**
- libzmq uses timers (poll-based): `add_timer(linger_, linger_timer_id)`
- ZZMQ uses ZIO's `select` with deadline: `Timeout{ .deadline = ... }`
- libzmq callbacks: `timer_event()` fires asynchronously
- ZZMQ shielded block: runs synchronously, protected from cancellation

### Context Shutdown Coordination

**Pending inproc connections** (`src/ctx.cpp:137-145`):
```cpp
// Connect up any pending inproc connections, otherwise we will hang
pending_connections_t copy = _pending_connections;
for (pending_connections_t::iterator p = copy.begin(); p != end; ++p) {
    zmq::socket_base_t *s = create_socket (ZMQ_PAIR);
    s->bind (p->first.c_str ());
    s->close ();
}
```

**Critical edge case:** If socket A calls `connect("inproc://foo")` but nothing ever binds, context shutdown would hang forever waiting for that connection. Solution: create temporary PAIR socket to satisfy pending connects.

**ZZMQ must handle:** Track pending inproc connects, satisfy them on shutdown.

### Active vs Passive Connections

**libzmq session types** (`src/session_base.hpp:98-100`):
```cpp
//  If true, this session (re)connects to the peer.
//  Otherwise, it's a transient session created by the listener.
const bool _active;
```

**Behavior difference:**
- Active (from `connect()`): Will reconnect on disconnect
- Passive (from `accept()`): Destroyed on disconnect, no reconnect

### Flush After Complete Message Only

**libzmq implementation** (`src/lb.cpp:116-124`):
```cpp
_more = (msg_->flags () & msg_t::more) != 0;
if (!_more) {
    _pipes[_current]->flush ();  // Flush only on complete message
    if (++_current >= _active)
        _current = 0;  // Advance round-robin only on complete message
}
```

**Why?**
- Multipart must go to same pipe
- Only advance round-robin after complete message
- Only flush (signal peer) after complete message

### Zero-Copy and Message Ownership

**libzmq pattern** (`src/lb.cpp:126-128`):
```cpp
// Detach the message from the data buffer
const int rc = msg_->init ();  // Resets msg to empty, caller loses data
```

**Ownership transfer:** After successful `send()`, caller's message is emptied. The pipe now owns the data.

**ZZMQ must match:** Move semantics - sender loses ownership on successful send.

### Engine Error Categories

**libzmq error types** (`src/session_base.cpp:450-473`):
```cpp
zmq_assert (reason_ == i_engine::connection_error
            || reason_ == i_engine::timeout_error
            || reason_ == i_engine::protocol_error);

switch (reason_) {
    case i_engine::timeout_error:
    case i_engine::connection_error:
        if (_active) {
            reconnect ();  // Active sessions reconnect
            break;
        }
    case i_engine::protocol_error:
        terminate ();  // Protocol errors always terminate
```

**Error handling rules:**
- Connection/timeout errors on active session → reconnect
- Connection/timeout errors on passive session → terminate
- Protocol errors → always terminate (can't recover)

### Summary: Critical ZZMQ Implementation Requirements

1. **HWM counts complete messages**, not frames
2. **Multipart must be atomic** - rollback partial sends, assert on partial recvs
3. **Load balancer dropping mode** - consume remaining multipart frames silently
4. **Subscriptions must be resent** on reconnect
5. **Linger timer** controls message drain timeout
6. **Pending inproc connects** must be satisfied on shutdown
7. **Flush and round-robin advance** only after complete messages
8. **Move semantics** - sender loses ownership after send
9. **Protocol errors** always terminate, connection errors may reconnect

---

## libzmq Reference: Signaling, Connection, Readiness, Heartbeat, Disconnect

This section documents libzmq's mechanisms for these critical behaviors.

### Signaling (signaler_t, mailbox_t)

libzmq uses a signaler + command queue pattern for cross-thread communication.

**signaler_t** (`src/signaler.cpp`):
```cpp
// Uses socketpair (or eventfd on Linux)
zmq::signaler_t::signaler_t () {
    make_fdpair (&_r, &_w);  // Create read/write fd pair
    unblock_socket (_w);
    unblock_socket (_r);
}

void zmq::signaler_t::send () {
    unsigned char dummy = 0;
    ::send (_w, &dummy, sizeof (dummy), 0);  // Wake up receiver
}

void zmq::signaler_t::recv () {
    unsigned char dummy;
    ::recv (_r, &dummy, sizeof (dummy), 0);  // Consume signal
}
```

**mailbox_t** (`src/mailbox.cpp`) - combines signaler with command queue:
```cpp
void zmq::mailbox_t::send (const command_t &cmd_) {
    _sync.lock ();
    _cpipe.write (cmd_, false);
    const bool ok = _cpipe.flush ();  // Returns false if reader was idle
    _sync.unlock ();
    if (!ok)
        _signaler.send ();  // Only signal if reader was waiting
}

int zmq::mailbox_t::recv (command_t *cmd_, int timeout_) {
    if (_active) {
        if (_cpipe.read (cmd_))
            return 0;  // Fast path: direct read
        _active = false;  // No more commands, go passive
    }

    _signaler.wait (timeout_);  // Wait for signal
    _signaler.recv ();          // Consume signal
    _active = true;             // Now we can read again

    _cpipe.read (cmd_);
    return 0;
}
```

**Key insight**: Signal is only sent when transitioning from idle to active. This minimizes syscalls when commands flow continuously.

**ZZMQ approach**: With ZIO, coroutines suspend/resume without explicit signaling. ZIO channels handle this automatically.

### Connection Lifecycle

**TCP Connect** (`src/stream_connecter_base.cpp`):
```
process_plug()
    |-- delayed_start? -> add_reconnect_timer()
    +-- immediate      -> start_connecting()

start_connecting()
    |-- Create socket
    |-- connect() (non-blocking)
    +-- Register for poll

out_event() [connection complete]
    |-- Check getsockopt(SO_ERROR)
    |-- create_engine() -> zmtp_engine_t
    |-- send_attach() to session
    +-- terminate() connecter (job done)
```

**Reconnect with Exponential Backoff** (`src/stream_connecter_base.cpp:86-114`):
```cpp
int zmq::stream_connecter_base_t::get_new_reconnect_ivl () {
    if (options.reconnect_ivl_max > 0) {
        // Exponential backoff with cap
        if (_current_reconnect_ivl == -1)
            candidate_interval = options.reconnect_ivl;
        else
            candidate_interval = _current_reconnect_ivl * 2;

        if (candidate_interval > options.reconnect_ivl_max)
            _current_reconnect_ivl = options.reconnect_ivl_max;
        else
            _current_reconnect_ivl = candidate_interval;
    } else {
        // Base interval + random jitter
        const int random_jitter = generate_random () % options.reconnect_ivl;
        interval = _current_reconnect_ivl + random_jitter;
    }
}
```

**Inproc Pending Connections** (`src/ctx.cpp:739-775`):
```cpp
// When connect() called before bind()
void zmq::ctx_t::pend_connection (...) {
    if (_endpoints.find (addr_) == _endpoints.end ()) {
        // No bind yet - store for later
        _pending_connections.insert (addr_, pending_connection);
    } else {
        // Bind exists - connect immediately
        connect_inproc_sockets (...);
    }
}

// When bind() called with pending connects
void zmq::ctx_t::connect_pending (...) {
    for (auto& pending : _pending_connections.equal_range (addr_)) {
        connect_inproc_sockets (bind_socket_, pending);
    }
    _pending_connections.erase (addr_);
}
```

**ZZMQ implementation:**

```zig
pub const ConnectionManager = struct {
    /// Active connections by endpoint
    connections: std.StringHashMap(*Connection),

    /// Pending inproc connects (waiting for bind)
    pending_inproc: std.StringHashMap(PendingConnect),

    /// Reconnect backoff state
    reconnect_ivl: i64,
    reconnect_ivl_max: i64,
    current_reconnect_ivl: i64 = -1,

    pub fn connect(self: *ConnectionManager, uri: []const u8, rt: *zio.Runtime) !void {
        const parsed = try parseUri(uri);

        if (parsed.protocol == .inproc) {
            return self.connectInproc(parsed.path, rt);
        }

        // TCP/IPC: Start async connect
        try self.startTcpConnect(parsed, rt);
    }

    fn getNextReconnectInterval(self: *ConnectionManager) i64 {
        if (self.reconnect_ivl_max > 0) {
            // Exponential backoff
            if (self.current_reconnect_ivl == -1) {
                self.current_reconnect_ivl = self.reconnect_ivl;
            } else {
                self.current_reconnect_ivl = @min(
                    self.current_reconnect_ivl * 2,
                    self.reconnect_ivl_max
                );
            }
        } else {
            // Base + jitter
            if (self.current_reconnect_ivl == -1) {
                self.current_reconnect_ivl = self.reconnect_ivl;
            }
            const jitter = std.crypto.random.int(u32) % @intCast(self.reconnect_ivl);
            return self.current_reconnect_ivl + jitter;
        }
        return self.current_reconnect_ivl;
    }
};
```

### Readiness (has_in, has_out)

**Polling model** (`src/socket_base.cpp:459-468`):
```cpp
if (option_ == ZMQ_EVENTS) {
    // Process any pending commands first
    process_commands (0, false);

    // Return bitmask of ready states
    return (has_out () ? ZMQ_POLLOUT : 0)
         | (has_in () ? ZMQ_POLLIN : 0);
}
```

**Fair queue has_in** (`src/fq.cpp:96-118`):
```cpp
bool zmq::fq_t::has_in () {
    // If mid-multipart, more data is available
    if (_more)
        return true;

    // Scan pipes for data, deactivating empty ones
    while (_active > 0) {
        if (_pipes[_current]->check_read ())
            return true;

        // Deactivate empty pipe
        _active--;
        _pipes.swap (_current, _active);
        if (_current == _active)
            _current = 0;
    }
    return false;
}
```

**ZZMQ approach**: Readiness check scans pipes similarly:

```zig
pub fn hasIn(self: *Socket) bool {
    // Check if mid-multipart (must continue reading)
    if (self.pattern_state.receiving_multipart) return true;

    // Check each pipe for readable data
    var iter = self.pipes.iterator();
    while (iter.next()) |pipe| {
        if (!pipe.inbound.isEmpty()) return true;
    }
    return false;
}

pub fn hasOut(self: *Socket) bool {
    // Check if mid-multipart (must continue writing)
    if (self.pattern_state.sending_multipart) return true;

    // Check each pipe for write space
    var iter = self.pipes.iterator();
    while (iter.next()) |pipe| {
        if (!pipe.outbound.isFull()) return true;
    }
    return false;
}
```

### Heartbeat Mechanism

**ZMTP Heartbeat** (`src/stream_engine_base.cpp`, `src/zmtp_engine.cpp`):

```
mechanism_ready()
    +-- if heartbeat_interval > 0
        +-- add_timer(heartbeat_ivl_timer_id)

timer_event(heartbeat_ivl_timer_id)
    |-- _next_msg = produce_ping_message
    |-- out_event()  // Send PING
    +-- add_timer(heartbeat_ivl_timer_id)  // Reschedule

produce_ping_message()
    |-- Create "\4PING" + TTL message
    |-- if heartbeat_timeout > 0
    |   +-- add_timer(heartbeat_timeout_timer_id)
    +-- Return encoded message

On receiving PING:
    |-- Extract remote TTL
    |-- if TTL > 0
    |   +-- add_timer(TTL, heartbeat_ttl_timer_id)
    |-- Prepare PONG response with context
    +-- _next_msg = produce_pong_message

On receiving PONG:
    +-- cancel_timer(heartbeat_timeout_timer_id)

timer_event(heartbeat_timeout_timer_id)
    +-- error(timeout_error)  // No PONG received

timer_event(heartbeat_ttl_timer_id)
    +-- error(timeout_error)  // Peer went silent
```

**ZZMQ implementation:**

```zig
pub const HeartbeatState = struct {
    interval_ms: u32,       // How often to send PING
    timeout_ms: u32,        // How long to wait for PONG
    ttl_ms: u32,            // Tell peer our TTL

    last_ping_sent: i64 = 0,
    awaiting_pong: bool = false,
    peer_ttl_deadline: ?i64 = null,

    pub fn run(self: *HeartbeatState, engine: *Engine, rt: *zio.Runtime) !void {
        while (engine.isConnected()) {
            // Wait for interval
            try zio.time.sleep(rt, self.interval_ms * std.time.ns_per_ms);

            // Send PING with our TTL
            try engine.sendPing(self.ttl_ms);
            self.last_ping_sent = std.time.milliTimestamp();
            self.awaiting_pong = true;

            // Wait for PONG with timeout
            const deadline = self.last_ping_sent + self.timeout_ms;
            while (self.awaiting_pong) {
                const now = std.time.milliTimestamp();
                if (now >= deadline) {
                    return error.HeartbeatTimeout;
                }

                // Check for incoming PONG (via channel)
                if (engine.checkPong()) {
                    self.awaiting_pong = false;
                    break;
                }

                try zio.time.sleep(rt, 10 * std.time.ns_per_ms);
            }
        }
    }

    pub fn onPingReceived(self: *HeartbeatState, peer_ttl: u32) void {
        if (peer_ttl > 0) {
            self.peer_ttl_deadline = std.time.milliTimestamp() + peer_ttl;
        }
    }

    pub fn onAnyMessageReceived(self: *HeartbeatState) void {
        // Any message from peer resets TTL deadline
        self.peer_ttl_deadline = null;
    }
};
```

### Disconnect Detection and Handling

**Error detection** (`src/stream_engine_base.cpp:262-270`):
```cpp
const int rc = read (_inpos, bufsize);
if (rc == -1) {
    if (errno != EAGAIN) {
        error (connection_error);  // Read failed
        return false;
    }
    return true;  // EAGAIN is ok, just no data
}
if (rc == 0) {
    // Connection closed by peer (tcp_read sets errno = EPIPE)
    error (connection_error);
    return false;
}
```

**Error propagation** (`src/stream_engine_base.cpp:667-707`):
```cpp
void zmq::stream_engine_base_t::error (error_reason_t reason_) {
    // For ROUTER with notifications, send disconnect message
    if (options.router_notify & ZMQ_NOTIFY_DISCONNECT) {
        _session->rollback ();
        msg_t disconnect_notification;
        disconnect_notification.init ();
        _session->push_msg (&disconnect_notification);
    }

    // Fire events
    _socket->event_disconnected (_endpoint_uri_pair, _s);

    // Notify session
    _session->engine_error (
        !_handshaking,  // handshaked_
        reason_         // connection_error, timeout_error, protocol_error
    );

    unplug ();
    delete this;
}
```

**Session handling** (`src/session_base.cpp:426-481`):
```cpp
void zmq::session_base_t::engine_error (bool handshaked_, error_reason_t reason_) {
    _engine = NULL;

    // Clean up half-processed messages
    if (_pipe) {
        clean_pipes ();

        // Send disconnect/hiccup messages if configured
        if (!_active && handshaked_ && options.can_recv_disconnect_msg)
            _pipe->send_disconnect_msg ();
        if (_active && handshaked_ && options.can_recv_hiccup_msg)
            _pipe->send_hiccup_msg ();
    }

    // Decide: reconnect or terminate
    switch (reason_) {
        case timeout_error:
        case connection_error:
            if (_active) {
                reconnect ();  // Connector: try again
                break;
            }
            // Passive (from accept): fall through to terminate
        case protocol_error:
            terminate ();
            break;
    }
}
```

**ZZMQ implementation:**

```zig
pub const Engine = struct {
    stream: zio.net.TcpStream,
    pipe: *Pipe,
    session: *Session,
    state: State,

    const State = enum { handshaking, ready, error_ };

    pub fn readerLoop(self: *Engine, rt: *zio.Runtime) void {
        defer self.handleDisconnect();

        while (self.state == .ready) {
            const n = self.stream.recv(rt, &self.read_buf, .{}) catch |err| {
                self.onError(.connection_error, err);
                return;
            };

            if (n == 0) {
                self.onError(.connection_error, error.EndOfStream);
                return;
            }

            self.processIncoming(self.read_buf[0..n]) catch |err| {
                self.onError(.protocol_error, err);
                return;
            };
        }
    }

    fn onError(self: *Engine, reason: ErrorReason, err: anyerror) void {
        self.state = .error_;

        // Fire disconnect event
        if (self.session.socket.monitor) |mon| {
            mon.post(.{ .disconnected = .{
                .endpoint = self.endpoint,
                .reason = reason,
            }});
        }

        // Notify session
        self.session.engineError(!self.handshaking, reason);
    }
};
```

### ZMQ_IMMEDIATE Option

**Effect on pipe creation** (`src/socket_base.cpp:1068`):
```cpp
if (options.immediate != 1 || subscribe_to_all) {
    // Create pipe immediately (can queue messages before connect)
    pipepair (...);
    attach_pipe (...);
}
```

**Effect on hiccup (reconnect)** (`src/socket_base.cpp:1716-1722`):
```cpp
void zmq::socket_base_t::hiccuped (pipe_t *pipe_) {
    if (options.immediate == 1)
        pipe_->terminate (false);  // Drop pipe, create new on reconnect
    else
        xhiccuped (pipe_);         // Keep pipe, resend subscriptions
}
```

**ZZMQ implementation:**

```zig
pub const SocketOptions = struct {
    /// ZMQ_IMMEDIATE - if true, don't create pipe until connected
    immediate: bool = false,
};

pub const Socket = struct {
    pub fn connect(self: *Socket, uri: []const u8, rt: *zio.Runtime) !void {
        if (self.options.immediate) {
            // Don't create pipe yet - wait for connection
            try self.connection_manager.connectDeferred(uri, rt, self);
        } else {
            // Create pipe immediately (can queue before connected)
            const pipe = try self.createPipe();
            try self.attachPipe(pipe);
            try self.connection_manager.connect(uri, rt, pipe);
        }
    }

    fn onHiccup(self: *Socket, pipe: *Pipe) void {
        if (self.options.immediate) {
            pipe.terminate();  // New pipe on reconnect
        } else {
            self.pattern.onHiccup(pipe);  // Resend subs
        }
    }
};
```

### Summary: ZZMQ Implementation Requirements

| libzmq Mechanism | ZZMQ Approach |
|-----------------|---------------|
| signaler_t + mailbox_t | ZIO channels handle wake-up automatically |
| Command queue | Direct method calls + channels |
| Reconnect backoff | Coroutine with exponential delay |
| Pending inproc | HashMap of waiting connects |
| has_in/has_out | Scan pipes for readable/writable |
| Heartbeat PING/PONG | Coroutine with timers |
| TTL monitoring | Deadline tracking per connection |
| Disconnect detection | Read returning 0 or error |
| Error propagation | Session.engineError() callback |
| ZMQ_IMMEDIATE | Deferred pipe creation |

---

## libzmq Reference: Level vs Edge Triggering and Polling

This section analyzes how libzmq handles event polling, the critical distinction between level-triggered and edge-triggered I/O, and the full polling API surface that ZZMQ must provide.

### Level-Triggered vs Edge-Triggered I/O

**Level-Triggered (libzmq's choice)**:
- Event fires repeatedly as long as the condition exists (data available, buffer writable)
- Simpler to use correctly - no risk of missing events
- Requires explicit enable/disable to prevent spurious wakeups

**Edge-Triggered**:
- Event fires once when the condition changes
- More efficient (fewer wakeups) but harder to use correctly
- Must drain all data on each event or risk starvation

**libzmq uses LEVEL-TRIGGERED mode** for all polling backends:

```cpp
// epoll.cpp - Note: NO EPOLLET flag, so level-triggered
void zmq::epoll_t::set_pollin (handle_t handle_)
{
    pe->ev.events |= EPOLLIN;  // Just EPOLLIN, not EPOLLIN | EPOLLET
    epoll_ctl (_epoll_fd, EPOLL_CTL_MOD, pe->fd, &pe->ev);
}

// kqueue.cpp - Note: NO EV_CLEAR flag, so level-triggered
void zmq::kqueue_t::kevent_add (fd_t fd_, short filter_, void *udata_)
{
    EV_SET (&ev, fd_, filter_, EV_ADD, 0, 0, udata_);  // EV_ADD only, not EV_ADD | EV_CLEAR
    kevent (kqueue_fd, &ev, 1, NULL, 0, NULL);
}
```

### The set_pollin/reset_pollin Pattern

To avoid wasteful repeated notifications with level-triggered I/O, libzmq manually enables/disables polling for each event:

```cpp
// stream_engine_base.cpp:307 - Disable read polling when backpressured
if (rc == -1 && errno == EAGAIN) {
    _input_stopped = true;
    reset_pollin (_handle);  // Stop getting in_event() calls
}

// stream_engine_base.cpp:442 - Re-enable when ready
if (!_input_stopped) {
    set_pollin ();  // Resume getting in_event() calls
}

// stream_engine_base.cpp:350-354 - Disable write polling when nothing to send
if (_outsize == 0) {
    _output_stopped = true;
    reset_pollout ();  // Stop getting out_event() calls
}

// stream_engine_base.cpp:388-391 - Re-enable when data available
if (likely (_output_stopped)) {
    set_pollout ();  // Resume getting out_event() calls
    _output_stopped = false;
}
```

This pattern simulates edge-triggered behavior on top of level-triggered I/O, giving libzmq precise control over event delivery.

### Internal Poller Interface (poller_t concept)

libzmq defines an internal poller interface that all backends must implement:

```cpp
// poller_base.hpp - The poller_t concept
class poller_t {
    // Add a file descriptor, returning a handle
    handle_t add_fd(fd_t fd_, i_poll_events *events_);

    // Remove a file descriptor
    void rm_fd(handle_t handle_);

    // Enable/disable input polling
    void set_pollin(handle_t handle_);
    void reset_pollin(handle_t handle_);

    // Enable/disable output polling
    void set_pollout(handle_t handle_);
    void reset_pollout(handle_t handle_);

    // Timer management
    void add_timer(int timeout_, i_poll_events *sink_, int id_);
    void cancel_timer(i_poll_events *sink_, int id_);

    // Lifecycle
    void start(const char *name = NULL);
    void stop();

    // Load tracking
    int get_load() const;
    static int max_fds();
};

// Event handler interface
struct i_poll_events {
    virtual void in_event() = 0;   // Called when readable
    virtual void out_event() = 0;  // Called when writable
    virtual void timer_event(int id_) = 0;  // Called when timer fires
};
```

### User-Facing Polling APIs

libzmq provides two user-facing polling APIs:

#### 1. zmq_poll (Legacy, simpler)

```c
// Poll multiple sockets/fds at once
typedef struct {
    void *socket;      // ZMQ socket (or NULL for raw fd)
    int fd;            // Raw file descriptor (if socket is NULL)
    short events;      // ZMQ_POLLIN | ZMQ_POLLOUT | ZMQ_POLLERR | ZMQ_POLLPRI
    short revents;     // Output: which events occurred
} zmq_pollitem_t;

int zmq_poll(zmq_pollitem_t *items, int nitems, long timeout);
// Returns: number of ready items, or -1 on error
// Timeout: -1 = block forever, 0 = return immediately, >0 = milliseconds

// Example usage:
zmq_pollitem_t items[2] = {
    { socket1, 0, ZMQ_POLLIN, 0 },
    { socket2, 0, ZMQ_POLLIN | ZMQ_POLLOUT, 0 }
};
int rc = zmq_poll(items, 2, 1000);  // Wait up to 1 second
if (items[0].revents & ZMQ_POLLIN) { /* socket1 readable */ }
if (items[1].revents & ZMQ_POLLOUT) { /* socket2 writable */ }
```

#### 2. zmq_poller (Modern, more flexible)

```c
// Create/destroy poller
void *zmq_poller_new(void);
int zmq_poller_destroy(void **poller_p);

// Add/modify/remove sockets
int zmq_poller_add(void *poller, void *socket, void *user_data, short events);
int zmq_poller_modify(void *poller, void *socket, short events);
int zmq_poller_remove(void *poller, void *socket);

// Add/modify/remove raw file descriptors
int zmq_poller_add_fd(void *poller, int fd, void *user_data, short events);
int zmq_poller_modify_fd(void *poller, int fd, short events);
int zmq_poller_remove_fd(void *poller, int fd);

// Wait for events
int zmq_poller_wait(void *poller, zmq_poller_event_t *event, long timeout);
int zmq_poller_wait_all(void *poller, zmq_poller_event_t *events, int n_events, long timeout);

// Get internal fd for external event loops
int zmq_poller_fd(void *poller, int *fd);

// Event structure
typedef struct {
    void *socket;      // The ZMQ socket (or NULL for raw fd)
    int fd;            // The raw fd (if socket is NULL)
    void *user_data;   // User data passed to add
    short events;      // Which events occurred
} zmq_poller_event_t;

// Example usage:
void *poller = zmq_poller_new();
zmq_poller_add(poller, socket1, (void*)"socket1", ZMQ_POLLIN);
zmq_poller_add(poller, socket2, (void*)"socket2", ZMQ_POLLIN | ZMQ_POLLOUT);

zmq_poller_event_t events[10];
int n = zmq_poller_wait_all(poller, events, 10, 1000);
for (int i = 0; i < n; i++) {
    printf("Socket %s ready for %s\n",
           (char*)events[i].user_data,
           events[i].events & ZMQ_POLLIN ? "read" : "write");
}
zmq_poller_destroy(&poller);
```

### ZMQ_FD Socket Option

Each ZMQ socket exposes an internal file descriptor for integration with external event loops:

```c
// Get the socket's signaling fd
int fd;
size_t fd_size = sizeof(fd);
zmq_getsockopt(socket, ZMQ_FD, &fd, &fd_size);

// IMPORTANT: ZMQ_FD becomes readable when ZMQ_EVENTS changes
// You must ALWAYS check ZMQ_EVENTS after ZMQ_FD signals readiness
int events;
size_t events_size = sizeof(events);
zmq_getsockopt(socket, ZMQ_EVENTS, &events, &events_size);
if (events & ZMQ_POLLIN) { /* Actually readable */ }
if (events & ZMQ_POLLOUT) { /* Actually writable */ }
```

This two-step check is required because:
1. ZMQ_FD signals "something changed" not "specific event ready"
2. Multiple sockets may share internal I/O threads
3. The event may have been consumed between signal and check

### ZMQ_EVENTS Socket Option

Returns the current readiness state of a socket:

```c
int events;
size_t events_size = sizeof(events);
zmq_getsockopt(socket, ZMQ_EVENTS, &events, &events_size);

// events is a bitmask:
// ZMQ_POLLIN  - socket has messages ready to receive
// ZMQ_POLLOUT - socket is ready to send (not at HWM)
```

Implementation in socket_base.cpp:

```cpp
int zmq::socket_base_t::getsockopt (int option_, void *optval_, size_t *optvallen_)
{
    if (option_ == ZMQ_EVENTS) {
        *value = 0;
        if (has_in())
            *value |= ZMQ_POLLIN;
        if (has_out())
            *value |= ZMQ_POLLOUT;
        return 0;
    }
}
```

### Thread-Safe Sockets and Signaling

Thread-safe sockets (SERVER, CLIENT, RADIO, DISH, GATHER, SCATTER, DGRAM, PEER, CHANNEL) use a different signaling mechanism:

```cpp
// socket_poller.cpp:95-111
if (is_thread_safe (*socket_)) {
    if (_signaler == NULL) {
        _signaler = new signaler_t();
    }
    socket_->add_signaler (_signaler);  // Socket signals this when events change
}

// check_events then queries each socket directly:
if (it->socket->getsockopt (ZMQ_EVENTS, &events, &events_size) == -1) {
    return -1;
}
```

### ZZMQ Polling Design

For ZZMQ with ZIO, we leverage ZIO's native polling capabilities:

```zig
/// User-facing poll item
pub const PollItem = struct {
    socket: ?*Socket = null,    // ZZMQ socket
    fd: ?std.posix.fd_t = null, // Raw fd (if socket is null)
    events: Events = .{},       // Requested events
    revents: Events = .{},      // Returned events
    user_data: ?*anyopaque = null,

    pub const Events = packed struct {
        pollin: bool = false,
        pollout: bool = false,
        pollerr: bool = false,
        pollpri: bool = false,
    };
};

/// zmq_poll equivalent - poll multiple sockets/fds
pub fn poll(items: []PollItem, timeout_ms: ?i64) !usize {
    // For ZMQ sockets: check readiness directly
    // For raw fds: use ZIO's I/O abstraction

    var ready_count: usize = 0;

    // First pass: check if any sockets are immediately ready
    for (items) |*item| {
        item.revents = .{};
        if (item.socket) |socket| {
            const events = socket.getEvents();
            if (item.events.pollin and events.pollin) {
                item.revents.pollin = true;
                ready_count += 1;
            }
            if (item.events.pollout and events.pollout) {
                item.revents.pollout = true;
                ready_count += 1;
            }
        }
    }

    if (ready_count > 0 or timeout_ms == 0) {
        return ready_count;
    }

    // Need to wait - use ZIO's select mechanism
    const deadline = if (timeout_ms) |t|
        zio.time.Instant.now().add(.{ .ms = t })
    else
        null;

    return zzmq_poll_wait(items, deadline);
}

/// Modern zmq_poller equivalent
pub const Poller = struct {
    items: std.ArrayList(PollItem),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Poller {
        return .{
            .items = std.ArrayList(PollItem).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Poller) void {
        self.items.deinit();
    }

    pub fn add(self: *Poller, socket: *Socket, user_data: ?*anyopaque, events: PollItem.Events) !void {
        try self.items.append(.{
            .socket = socket,
            .user_data = user_data,
            .events = events,
        });
    }

    pub fn addFd(self: *Poller, fd: std.posix.fd_t, user_data: ?*anyopaque, events: PollItem.Events) !void {
        try self.items.append(.{
            .fd = fd,
            .user_data = user_data,
            .events = events,
        });
    }

    pub fn modify(self: *Poller, socket: *Socket, events: PollItem.Events) !void {
        for (self.items.items) |*item| {
            if (item.socket == socket) {
                item.events = events;
                return;
            }
        }
        return error.NotFound;
    }

    pub fn remove(self: *Poller, socket: *Socket) !void {
        for (self.items.items, 0..) |item, i| {
            if (item.socket == socket) {
                _ = self.items.orderedRemove(i);
                return;
            }
        }
        return error.NotFound;
    }

    pub fn wait(self: *Poller, events: []PollItem, timeout_ms: ?i64) !usize {
        return poll(self.items.items, timeout_ms);
    }
};

/// Socket.getEvents() - equivalent to ZMQ_EVENTS getsockopt
pub fn getEvents(self: *Socket) PollItem.Events {
    return .{
        .pollin = self.hasIn(),
        .pollout = self.hasOut(),
    };
}

/// Socket.getFd() - equivalent to ZMQ_FD getsockopt
/// Returns fd that signals when socket events change
pub fn getFd(self: *Socket) !std.posix.fd_t {
    // Return the read end of an internal signaling pipe/eventfd
    return self.event_signaler.getReadFd();
}
```

### ZIO's Native Polling Advantage

ZIO already handles I/O readiness internally. For ZZMQ:

```zig
/// ZIO-native way to wait for socket readiness
pub fn waitReadable(self: *Socket) !void {
    // ZIO handles this through channel operations
    // When receiving, the coroutine suspends until data is available
    // No explicit polling needed for single-socket operations
}

/// For multi-socket operations, use select
pub fn selectReadable(sockets: []*Socket, timeout: ?zio.time.Duration) !?*Socket {
    // Build list of channels to wait on
    var channels: [sockets.len]anyframe = undefined;
    for (sockets, 0..) |s, i| {
        channels[i] = s.getReadFrame();
    }

    // ZIO select - returns which channel became ready
    const ready_idx = zio.select(&channels, timeout) orelse return null;
    return sockets[ready_idx];
}
```

### Internal vs External Event Loop Integration

**Scenario 1: ZZMQ as the event loop (typical)**
```zig
// Simple blocking API - ZIO handles scheduling
const msg = try socket.recv();  // Suspends coroutine until ready
try socket.send(response);      // Suspends if would block
```

**Scenario 2: External event loop integration**
```zig
// Get signaling fd for external loop (e.g., libuv, Qt)
const fd = try socket.getFd();

// External loop:
// when fd becomes readable {
    const events = socket.getEvents();
    if (events.pollin) {
        // Non-blocking recv since we know it's ready
        const msg = try socket.recvNoWait();
    }
// }
```

### Summary: ZZMQ Polling Requirements

| libzmq Feature | ZZMQ Approach |
|---------------|---------------|
| Level-triggered epoll/kqueue | ZIO handles backend selection automatically |
| set_pollin/reset_pollin | ZIO channels manage readiness internally |
| zmq_poll() | `zzmq.poll()` function with PollItem array |
| zmq_poller_* API | `zzmq.Poller` struct |
| ZMQ_FD | `socket.getFd()` for external loop integration |
| ZMQ_EVENTS | `socket.getEvents()` returns current readiness |
| Thread-safe signaling | Event signaler per socket |
| Timer integration | ZIO timer integration in poll/select |

---

## libzmq Deep Dives: Protocol, Patterns, and Internals

This section provides comprehensive analysis of critical libzmq subsystems, grounded in source code study.

### ZMTP Wire Protocol (Encoder/Decoder)

**Frame Format** (v2_protocol.hpp, v2_encoder.cpp, v2_decoder.cpp):

```
┌─────────────────────────────────────────────────────────────┐
│  Flags (1 byte)  │  Size (1 or 8 bytes)  │  Body (N bytes)  │
└─────────────────────────────────────────────────────────────┘

Flags byte:
  bit 0 (0x01): MORE     - More frames follow in this message
  bit 1 (0x02): LARGE    - Size is 8 bytes (not 1)
  bit 2 (0x04): COMMAND  - This is a command frame, not data

Size encoding:
  - If LARGE flag clear: 1 byte, max 255
  - If LARGE flag set:   8 bytes, network byte order (big-endian)
```

**Encoder State Machine** (v2_encoder.cpp):
```
State: message_ready
  1. Encode flags byte (MORE | LARGE | COMMAND)
  2. Encode size (1 or 8 bytes)
  3. If subscribe/cancel: add 1-byte prefix (1=sub, 0=cancel)
  4. Transition to: size_ready

State: size_ready
  1. Write message body directly from msg_t data
  2. Transition to: message_ready (next message)
```

**Decoder State Machine** (v2_decoder.cpp):
```
State: flags_ready (read 1 byte)
  1. Parse MORE, COMMAND flags
  2. If LARGE: transition to eight_byte_size_ready
  3. Else: transition to one_byte_size_ready

State: one_byte_size_ready / eight_byte_size_ready
  1. Parse size
  2. Validate against max_msg_size
  3. Allocate message (zero-copy if possible)
  4. Transition to: message_ready

State: message_ready
  1. Body has been read into message
  2. Return 1 to signal complete message
  3. Transition to: flags_ready (next frame)
```

**ZMTP Greeting** (zmtp_engine.cpp):
```
Bytes 0-9:   Signature (0xFF, 8 bytes padding, 0x7F)
Byte 10:     Revision (3 for ZMTP 3.x)
Byte 11:     Minor version (1 for ZMTP 3.1)
Bytes 12-31: Mechanism name (NULL-padded, e.g., "NULL", "PLAIN", "CURVE")
Bytes 32-63: Filler (zeros)
```

**ZZMQ Implementation**:
```zig
pub const Frame = struct {
    flags: Flags,
    data: []const u8,

    pub const Flags = packed struct {
        more: bool = false,
        large: bool = false,  // Computed from size
        command: bool = false,
        _reserved: u5 = 0,
    };
};

pub const Encoder = struct {
    pub fn encodeFrame(frame: Frame, writer: anytype) !void {
        var flags: u8 = 0;
        if (frame.flags.more) flags |= 0x01;
        if (frame.data.len > 255) flags |= 0x02;
        if (frame.flags.command) flags |= 0x04;

        try writer.writeByte(flags);
        if (frame.data.len > 255) {
            try writer.writeInt(u64, frame.data.len, .big);
        } else {
            try writer.writeByte(@intCast(frame.data.len));
        }
        try writer.writeAll(frame.data);
    }
};
```

---

### Socket Pattern State Machines

**REQ Socket** (req.cpp) - Strict request-reply:
```
States:
  _receiving_reply = false  → Can send, cannot receive
  _receiving_reply = true   → Cannot send, can receive

Send flow:
  1. If _receiving_reply && _strict: return EFSM
  2. If _message_begins:
     a. Send delimiter frame (empty, MORE flag)
     b. Drain any old replies (prevent stale reply matching)
  3. Send user frames via DEALER
  4. On final frame (!MORE): set _receiving_reply = true

Receive flow:
  1. If !_receiving_reply: return EFSM
  2. Skip frames until delimiter found (empty frame with MORE)
  3. Return subsequent frames to user
  4. On final frame (!MORE): set _receiving_reply = false

Options:
  ZMQ_REQ_CORRELATE: Add request_id frame for matching
  ZMQ_REQ_RELAXED:   Allow send without receiving reply
```

**REP Socket** (rep.cpp) - Mirrors REQ:
```
States:
  _sending_reply = false  → Can receive, cannot send
  _sending_reply = true   → Can send, cannot receive

Receive flow:
  1. If _sending_reply: return EFSM
  2. Copy routing frames to reply pipe (identity, delimiter)
  3. Return content frames to user
  4. On final frame: set _sending_reply = true

Send flow:
  1. If !_sending_reply: return EFSM
  2. Send frames via ROUTER (prepends copied routing)
  3. On final frame: set _sending_reply = false
```

**ROUTER Socket** (router.cpp) - Routing by identity:
```
Identity assignment:
  1. If peer sends identity frame: use it
  2. Else: generate random 5-byte identity (0x00 prefix + 4 random bytes)
  3. Store in _outpipes map: identity → pipe

Send flow:
  1. First frame MUST be routing identity
  2. Lookup pipe by identity in _outpipes
  3. If not found && _mandatory: return EHOSTUNREACH
  4. If not found && !_mandatory: silently drop message
  5. Send remaining frames to that pipe

Receive flow:
  1. Fair-queue from all pipes
  2. Prepend identity frame (so user knows sender)
  3. Return frames to user

Options:
  ZMQ_ROUTER_MANDATORY: Error if identity not found
  ZMQ_ROUTER_HANDOVER:  Take over on duplicate identity
```

**PUB/XPUB Socket** (pub.cpp, xpub.cpp):
```
_subscriptions: mtrie_t storing prefix → set<pipe*>

On subscriber connect:
  1. Parse subscription: [0x01 | 0x00] + prefix
  2. 0x01 = subscribe, 0x00 = unsubscribe
  3. _subscriptions.add(prefix, pipe) or .rm(prefix, pipe)

On publish:
  1. Get message topic (first frame)
  2. _subscriptions.match(topic, callback)
  3. Callback writes message copy to each matching pipe
```

---

### Subscription Matching (MTrie)

**Data Structure** (generic_mtrie.hpp):
```
Multi-trie: Prefix tree where each node has set of pipes

struct mtrie_node {
    pipes_t* _pipes;      // Pipes subscribed at this prefix
    unsigned char _min;   // Minimum character in children
    unsigned short _count; // Number of child slots
    union {
        mtrie_node* node;   // Single child (optimization)
        mtrie_node** table; // Array of children
    } _next;
};

Operations:
  add(prefix, pipe)  → O(prefix_length)
  rm(prefix, pipe)   → O(prefix_length)
  match(topic, cb)   → O(topic_length), calls cb for each matching pipe
```

**Match Algorithm**:
```
match(topic):
  node = root
  callback(root.pipes)  // Empty prefix matches all

  for each char c in topic:
    if c not in node.children: break
    node = node.children[c]
    callback(node.pipes)
```

---

### Multipart Message Atomicity

**Rollback Mechanism** (pipe.cpp:236):
```cpp
void pipe_t::rollback() {
    // Remove incomplete message from outbound pipe
    msg_t msg;
    while (_out_pipe->unwrite(&msg)) {
        zmq_assert(msg.flags() & msg_t::more);  // Only MORE frames
        msg.close();
    }
}
```

**How It Works**:
1. `ypipe::unwrite()` removes last unflushed item
2. `flush()` only called on complete message (no MORE flag)
3. Incomplete messages can always be rolled back

**ZZMQ Approach**:
```zig
pub const Pipe = struct {
    pending_multipart: std.ArrayList(Message),

    pub fn write(self: *Pipe, msg: Message) !void {
        self.pending_multipart.append(msg);
        if (!msg.hasMore()) {
            // Flush all pending frames atomically
            for (self.pending_multipart.items) |frame| {
                try self.outbound.send(self.rt, frame);
            }
            self.pending_multipart.clearRetainingCapacity();
        }
    }

    pub fn rollback(self: *Pipe) void {
        for (self.pending_multipart.items) |*frame| frame.deinit();
        self.pending_multipart.clearRetainingCapacity();
    }
};
```

---

### Inproc Transport

**Mechanism** (ctx.cpp):

```
On connect("inproc://name") before bind:
  → Store in _pending_connections[name]

On bind("inproc://name"):
  → Register endpoint
  → Connect all pending connections
  → connect_inproc_sockets():
      - Set HWM = connector_sndhwm + binder_rcvhwm
      - Create bidirectional pipe pair
      - Attach to both sockets

Key insight: No network I/O, no encoder/decoder, just direct pipe
```

---

### PLAIN Security Mechanism

**Handshake Flow**:
```
Client                              Server
   │                                   │
   │ ─── HELLO (user+pass) ────────►  │
   │                                   │  [ZAP auth]
   │ ◄─── WELCOME ──────────────────  │
   │                                   │
   │ ─── INITIATE (metadata) ──────►  │
   │                                   │
   │ ◄─── READY (metadata) ─────────  │
   │                                   │
```

**Command Formats**:
```
HELLO:    "\x05HELLO" + len(1) + username + len(1) + password
WELCOME:  "\x07WELCOME"
INITIATE: "\x08INITIATE" + metadata
READY:    "\x05READY" + metadata
ERROR:    "\x05ERROR" + len(1) + reason

Metadata: [len(1) + key + len(4,big) + value]*
  Standard keys: "Socket-Type", "Identity"
```

---

### Linger and Graceful Shutdown

**Linger Values**:
- `0`: Immediately discard pending messages
- `>0`: Wait up to N milliseconds for delivery
- `-1`: Wait forever

**Shutdown Sequence**:
```
1. zmq_close(socket)
   └─► send REAP to reaper thread

2. Reaper: process_term(linger)
   └─► For each pipe: terminate(linger > 0)

3. If linger > 0:
   └─► Start linger timer
   └─► Allow messages to drain

4. On timer expiry OR all messages sent:
   └─► Force terminate
   └─► send_term_ack() up the ownership tree

5. Context waits for all sockets reaped
```

---

### Error Recovery

**Error Types**:
```
connection_error → Reconnect (network failure)
protocol_error   → Terminate (ZMTP violation)
timeout_error    → Reconnect (heartbeat timeout)
```

**Reconnection Backoff** (stream_connecter_base.cpp):
```
delay = current_interval
current_interval = min(current_interval * 2, max_interval)
delay += random_jitter(±25%)
schedule_reconnect(delay)
```

**On Reconnect (SUB socket)**:
```
session->reconnect()
  └─► pipe->hiccup()
      └─► Socket resends all subscriptions
```

---

## Testing Strategy and Coverage

This section defines the comprehensive test suite required to validate ZZMQ against libzmq semantics. Tests are organized by category with specific test cases that verify correctness, edge cases, and semantic equivalence.

### Message System Tests

**Basic Message Operations:**
```zig
test "message init with data" {
    var msg = try Message.init(allocator, "hello");
    defer msg.deinit();
    try testing.expectEqualStrings("hello", msg.data());
}

test "message move transfers ownership" {
    var msg1 = try Message.init(allocator, "data");
    var msg2 = msg1.move();
    defer msg2.deinit();
    try testing.expect(msg1.size() == 0);
    try testing.expectEqualStrings("data", msg2.data());
}

test "message copy creates shared reference" {
    var msg1 = try Message.init(allocator, "shared");
    defer msg1.deinit();
    var msg2 = try msg1.copy();
    defer msg2.deinit();
    try testing.expectEqual(msg1.data().ptr, msg2.data().ptr);
}
```

**Storage Class Tests:**
| Test Case | Verification |
|-----------|--------------|
| VSM (≤48 bytes) | Message stored inline, no heap allocation |
| Small heap (25-255 bytes) | Single allocation, owned storage |
| Large message (>255 bytes) | Owned storage with proper alignment |
| External buffer | Zero-copy wrapping, proper lifecycle |
| Refcounted sharing | Copy increments refcount, deinit decrements |
| Refcount to zero | Memory freed when last reference released |

**Multipart Message Tests:**
```zig
test "multipart message MORE flag" {
    var frame1 = try Message.init(allocator, "part1");
    frame1.setMore(true);
    var frame2 = try Message.init(allocator, "part2");

    try testing.expect(frame1.hasMore());
    try testing.expect(!frame2.hasMore());
}

test "multipart atomicity on send" {
    // 3-part message should be buffered until complete
    try socket.send(part1);  // MORE=1, buffered
    try socket.send(part2);  // MORE=1, buffered
    try socket.send(part3);  // MORE=0, all 3 flushed atomically
}

test "multipart atomicity on receive" {
    // Partial multipart not visible to receiver
    // All parts available only after final frame arrives
}
```

### HWM and Flow Control Tests

**High Water Mark Semantics:**
```zig
test "HWM blocks sender when full" {
    socket.setOption(.sndhwm, 10);

    // Fill to HWM
    for (0..10) |_| {
        try socket.send(msg);  // Should succeed
    }

    // 11th message should block (suspend coroutine)
    // Verify with timeout or concurrent receiver
}

test "HWM zero means unlimited" {
    socket.setOption(.sndhwm, 0);
    // Should never block due to HWM (only memory limits)
}

test "HWM applies per-pipe" {
    // Each connected peer has independent HWM tracking
}
```

**Drop Behavior by Socket Type:**
| Socket Type | On HWM | Test Verification |
|-------------|--------|-------------------|
| PUSH | Block | Sender suspends until space available |
| PUB | Drop | Messages silently dropped, sender continues |
| DEALER | Block | Sender suspends |
| ROUTER | Drop | Messages to specific peer dropped |
| REQ/REP | Block | Strict alternation maintained |

**Backpressure Tests:**
```zig
test "slow consumer causes sender backpressure" {
    // Fast sender, slow receiver
    // Verify sender blocks at HWM, resumes when space available
}

test "pipe backpressure propagates to socket" {
    // Multiple peers, one slow
    // Verify correct peer-specific backpressure
}
```

### Socket Pattern Tests

#### PUSH/PULL Tests
```zig
test "PUSH round-robins across connected PULLs" {
    var push = try ctx.socket(.push);
    var pull1 = try ctx.socket(.pull);
    var pull2 = try ctx.socket(.pull);

    try push.connect("inproc://rr");
    try pull1.bind("inproc://rr");
    try pull2.bind("inproc://rr");

    // Send 4 messages
    for (0..4) |i| try push.send(msg(i));

    // Each PULL should receive 2 messages
    try testing.expectEqual(@as(u32, 0), pull1.recv().asInt());
    try testing.expectEqual(@as(u32, 2), pull1.recv().asInt());
    try testing.expectEqual(@as(u32, 1), pull2.recv().asInt());
    try testing.expectEqual(@as(u32, 3), pull2.recv().asInt());
}

test "PUSH blocks when all PULLs at HWM" {
    // All peers at HWM, send should block
}

test "PULL fair-queues from multiple PUSHs" {
    // Multiple senders, verify interleaved reception
}
```

#### REQ/REP Tests
```zig
test "REQ/REP strict alternation" {
    // REQ: send then recv
    try req.send(request);
    const reply = try req.recv();

    // REP: recv then send
    const request = try rep.recv();
    try rep.send(reply);
}

test "REQ double send fails with EFSM" {
    try req.send(msg1);
    const result = req.send(msg2);
    try testing.expectError(error.EFSM, result);
}

test "REP double recv fails with EFSM" {
    const msg = try rep.recv();
    const result = rep.recv();
    try testing.expectError(error.EFSM, result);
}

test "REQ/REP preserves envelope on reply" {
    // Multipart: [identity][empty][payload]
    // REP must return reply to correct REQ
}

test "REQ retry on disconnect" {
    // REQ_RELAXED: can send new request after disconnect
    // Default: must receive reply before next send
}
```

#### DEALER/ROUTER Tests
```zig
test "ROUTER prepends identity frame" {
    var dealer = try ctx.socket(.dealer);
    dealer.setOption(.identity, "client-1");

    var router = try ctx.socket(.router);
    // ... connect ...

    try dealer.send(payload);

    const id_frame = try router.recv();
    try testing.expectEqualStrings("client-1", id_frame.data());
    try testing.expect(id_frame.hasMore());

    const payload_frame = try router.recv();
    // ...
}

test "ROUTER routes by identity" {
    // Send to specific peer by prepending identity frame
}

test "ROUTER drops message for unknown identity" {
    // ZMQ_ROUTER_MANDATORY = 0: silent drop
    // ZMQ_ROUTER_MANDATORY = 1: return EHOSTUNREACH
}

test "DEALER round-robins sends" {
    // Like PUSH but for request-reply patterns
}

test "DEALER fair-queues receives" {
    // Like PULL
}
```

#### PUB/SUB Tests
```zig
test "SUB receives only matching subscriptions" {
    try sub.setOption(.subscribe, "weather.");

    try pub.send(msg("weather.nyc sunny"));   // Received
    try pub.send(msg("weather.la cloudy"));   // Received
    try pub.send(msg("stocks.aapl 150"));     // NOT received

    try testing.expectEqualStrings("weather.nyc sunny", (try sub.recv()).data());
    try testing.expectEqualStrings("weather.la cloudy", (try sub.recv()).data());
    try testing.expect(sub.hasIn() == false);
}

test "SUB empty subscription receives all" {
    try sub.setOption(.subscribe, "");
    // All messages received
}

test "SUB multiple subscriptions" {
    try sub.setOption(.subscribe, "A");
    try sub.setOption(.subscribe, "B");
    // Messages starting with A or B received
}

test "SUB unsubscribe" {
    try sub.setOption(.subscribe, "X");
    try sub.setOption(.unsubscribe, "X");
    // Messages starting with X no longer received
}

test "PUB drops when SUB at HWM" {
    // No backpressure to publisher
    // Slow subscriber loses messages
}

test "XSUB/XPUB subscription forwarding" {
    // XSUB sends subscription messages upstream
    // XPUB receives and processes subscriptions
}
```

### Connection Lifecycle Tests

**Bind/Connect Semantics:**
```zig
test "bind before connect" {
    try server.bind("tcp://127.0.0.1:5555");
    try client.connect("tcp://127.0.0.1:5555");
    // Connection established
}

test "connect before bind (late bind)" {
    try client.connect("tcp://127.0.0.1:5556");
    // Client queues messages or blocks
    try server.bind("tcp://127.0.0.1:5556");
    // Messages delivered after bind
}

test "multiple binds same socket" {
    try socket.bind("tcp://127.0.0.1:5555");
    try socket.bind("tcp://127.0.0.1:5556");
    // Socket accepts on both endpoints
}

test "multiple connects same socket" {
    try socket.connect("tcp://host1:5555");
    try socket.connect("tcp://host2:5555");
    // Socket connected to both peers
}

test "bind to wildcard port" {
    try socket.bind("tcp://127.0.0.1:*");
    const endpoint = socket.lastEndpoint();
    // Returns actual bound port
}
```

**Disconnect and Unbind:**
```zig
test "disconnect removes peer" {
    try socket.connect("tcp://127.0.0.1:5555");
    try socket.disconnect("tcp://127.0.0.1:5555");
    // Peer removed, no reconnection attempts
}

test "unbind stops accepting" {
    try socket.bind("tcp://127.0.0.1:5555");
    try socket.unbind("tcp://127.0.0.1:5555");
    // Port released, new connections rejected
}
```

### Reconnection and Error Recovery Tests

```zig
test "automatic reconnection on disconnect" {
    try client.connect("tcp://127.0.0.1:5555");
    // Server crashes or disconnects
    // Client automatically attempts reconnection
}

test "reconnection with exponential backoff" {
    socket.setOption(.reconnect_ivl, 100);      // 100ms initial
    socket.setOption(.reconnect_ivl_max, 5000); // 5s max

    // Verify backoff: 100, 200, 400, 800, 1600, 3200, 5000, 5000...
}

test "reconnection preserves subscriptions" {
    try sub.setOption(.subscribe, "topic");
    // Disconnect and reconnect
    // Subscriptions automatically resent to new peer
}

test "no reconnection after explicit disconnect" {
    try socket.disconnect("tcp://...");
    // Should not attempt to reconnect
}

test "connection timeout" {
    socket.setOption(.connect_timeout, 1000);  // 1 second
    try socket.connect("tcp://unreachable:5555");
    // Should timeout and trigger reconnect cycle
}
```

### Inproc Transport Tests

```zig
test "inproc connect before bind" {
    // Unlike TCP, inproc can handle connect-before-bind
    try client.connect("inproc://test");
    try server.bind("inproc://test");
    // Connection established
}

test "inproc zero-copy transfer" {
    // Messages passed by reference, not copied
    var msg = try Message.init(allocator, large_data);
    const ptr_before = msg.data().ptr;
    try push.send(msg);
    const received = try pull.recv();
    try testing.expectEqual(ptr_before, received.data().ptr);
}

test "inproc between sockets in same context only" {
    var ctx1 = try Context.init(...);
    var ctx2 = try Context.init(...);
    var s1 = try ctx1.socket(.push);
    var s2 = try ctx2.socket(.pull);

    try s1.bind("inproc://test");
    const result = s2.connect("inproc://test");
    try testing.expectError(error.ENOENT, result);
}

test "inproc endpoint names are context-scoped" {
    // Same name in different contexts = different endpoints
}
```

### ZMTP Protocol Compatibility Tests

**Greeting and Handshake:**
```zig
test "ZMTP greeting exchange" {
    // Verify: signature (0xFF, 8 bytes, 0x7F)
    // Version: 3.1
    // Mechanism: NULL/PLAIN
    // as-server flag
}

test "ZMTP NULL mechanism handshake" {
    // READY command exchange
    // Socket-Type property
    // Identity property (if set)
}

test "ZMTP version negotiation" {
    // Connect to ZMTP 3.0 peer
    // Should negotiate to common version
}
```

**Frame Encoding:**
```zig
test "short frame encoding (size < 255)" {
    // flags:1 + size:1 + body:N
    const wire = encodeFrame(flags, data);
    try testing.expectEqual(@as(u8, 0), wire[0] & 0x02);  // Not LARGE
}

test "long frame encoding (size >= 255)" {
    // flags:1 + size:8 (network byte order) + body:N
    const wire = encodeFrame(flags, large_data);
    try testing.expect(wire[0] & 0x02 != 0);  // LARGE flag
}

test "MORE flag in multipart" {
    // First frame: MORE=1
    // Last frame: MORE=0
}

test "COMMAND flag for control messages" {
    // SUBSCRIBE, CANCEL, PING, PONG
}
```

**Wire Compatibility:**
```zig
test "interop with libzmq" {
    // ZZMQ server, libzmq client
    // libzmq server, ZZMQ client
    // Verify messages decoded correctly
}
```

### PLAIN Security Tests

```zig
test "PLAIN authentication success" {
    server.setOption(.plain_server, true);
    server.setOption(.plain_username, "admin");
    server.setOption(.plain_password, "secret");

    client.setOption(.plain_username, "admin");
    client.setOption(.plain_password, "secret");

    // Connection established, messages flow
}

test "PLAIN authentication failure" {
    // Wrong password
    client.setOption(.plain_password, "wrong");
    // Connection rejected with 400 error
}

test "PLAIN mechanism in greeting" {
    // Verify greeting contains "PLAIN" mechanism
}

test "PLAIN command sequence" {
    // Client: HELLO (username, password)
    // Server: WELCOME or ERROR
    // Client: INITIATE (metadata)
    // Server: READY (metadata)
}
```

### Linger and Shutdown Tests

```zig
test "linger=0 immediate close" {
    socket.setOption(.linger, 0);
    try socket.send(msg);  // Queued
    socket.close();        // Immediate, message may be lost
}

test "linger>0 waits to drain" {
    socket.setOption(.linger, 1000);  // 1 second
    try socket.send(msg);
    socket.close();  // Waits up to 1s for message delivery
}

test "linger=-1 waits forever" {
    socket.setOption(.linger, -1);
    // Close blocks until all messages delivered
    // (or peer disconnects)
}

test "context termination waits for sockets" {
    var socket = try ctx.socket(.push);
    socket.setOption(.linger, 5000);
    try socket.send(msg);

    // In another coroutine:
    ctx.term();  // Blocks until socket closes and lingers complete
}

test "graceful shutdown sequence" {
    // 1. Stop accepting new connections
    // 2. Drain pending messages (per linger)
    // 3. Close pipes
    // 4. Release resources
}
```

### Subscription Matching Tests

**Prefix Matching:**
```zig
test "exact prefix match" {
    try sub.subscribe("foo");
    try testing.expect(matches("foo", "foobar"));
    try testing.expect(matches("foo", "foo"));
    try testing.expect(!matches("foo", "fo"));
    try testing.expect(!matches("foo", "bar"));
}

test "empty subscription matches all" {
    try sub.subscribe("");
    try testing.expect(matches("", "anything"));
}

test "binary prefix matching" {
    // Subscriptions are byte sequences, not strings
    try sub.subscribe(&[_]u8{0x00, 0x01});
    // Matches messages starting with those bytes
}
```

**MTrie Subscription Tests:**
```zig
test "overlapping subscriptions" {
    try sub.subscribe("foo");
    try sub.subscribe("foobar");
    // Message "foobarbaz" matches both, delivered once
}

test "subscription add/remove" {
    try sub.subscribe("A");
    try sub.subscribe("B");
    try sub.unsubscribe("A");
    // Only "B" matches now
}

test "multiple subscribers same prefix" {
    try sub1.subscribe("X");
    try sub2.subscribe("X");
    // Both receive messages starting with "X"
}

test "subscription reference counting" {
    // Internal: ensure mtrie tracks subscription counts
    // Unsubscribe removes only when count reaches zero
}
```

### Polling and Readiness Tests

```zig
test "poll single socket readable" {
    try push.send(msg);

    var items = [_]PollItem{
        .{ .socket = pull, .events = .{ .pollin = true } },
    };

    const ready = try poll(&items, 1000);
    try testing.expectEqual(@as(usize, 1), ready);
    try testing.expect(items[0].revents.pollin);
}

test "poll multiple sockets" {
    var items = [_]PollItem{
        .{ .socket = socket1, .events = .{ .pollin = true } },
        .{ .socket = socket2, .events = .{ .pollin = true } },
        .{ .socket = socket3, .events = .{ .pollout = true } },
    };

    const ready = try poll(&items, 100);
    // Check which sockets are ready
}

test "poll timeout" {
    var items = [_]PollItem{...};
    const ready = try poll(&items, 100);  // 100ms timeout
    try testing.expectEqual(@as(usize, 0), ready);  // Nothing ready
}

test "poll immediate (timeout=0)" {
    const ready = try poll(&items, 0);
    // Returns immediately with current state
}

test "poll indefinite (timeout=-1)" {
    // Blocks until at least one socket ready
}

test "hasIn and hasOut" {
    try push.send(msg);
    try testing.expect(pull.hasIn());
    try testing.expect(push.hasOut());  // Not at HWM
}

test "getFd for external event loop" {
    const fd = socket.getFd();
    // Add to epoll/kqueue
    // When signaled, call socket.getEvents() or hasIn()/hasOut()
}
```

### Identity and Routing Tests

```zig
test "socket identity" {
    socket.setOption(.identity, "my-id");
    const id = socket.getOption(.identity);
    try testing.expectEqualStrings("my-id", id);
}

test "generated identity for anonymous sockets" {
    // ROUTER generates UUID-like identity for connecting sockets
}

test "identity uniqueness" {
    // ROUTER rejects duplicate identities (ZMQ_ROUTER_HANDOVER = 0)
    // Or takes over connection (ZMQ_ROUTER_HANDOVER = 1)
}

test "routing table maintenance" {
    // Identities removed when peer disconnects
    // Identities added when peer connects
}
```

### Edge Cases and Error Handling

```zig
test "send on closed socket" {
    socket.close();
    const result = socket.send(msg);
    try testing.expectError(error.ENOTSOCK, result);
}

test "recv on closed socket" {
    socket.close();
    const result = socket.recv();
    try testing.expectError(error.ENOTSOCK, result);
}

test "operations on terminated context" {
    ctx.term();
    const result = socket.send(msg);
    try testing.expectError(error.ETERM, result);
}

test "invalid endpoint format" {
    const result = socket.connect("invalid://endpoint");
    try testing.expectError(error.EINVAL, result);
}

test "bind to already-bound port" {
    try socket1.bind("tcp://127.0.0.1:5555");
    const result = socket2.bind("tcp://127.0.0.1:5555");
    try testing.expectError(error.EADDRINUSE, result);
}

test "message too large" {
    socket.setOption(.maxmsgsize, 1024);
    const result = socket.recv();  // Peer sends >1024 bytes
    try testing.expectError(error.EMSGSIZE, result);
}
```

### Concurrency and Thread Safety Tests

```zig
test "context shared across coroutines" {
    // Multiple coroutines creating sockets from same context
    const ctx = try Context.init(...);

    var group = try Group.init(allocator);
    try group.spawn(worker1, .{ctx});
    try group.spawn(worker2, .{ctx});
    try group.await();
}

test "socket not shared across coroutines" {
    // Sockets must not be used from multiple coroutines
    // (unless explicitly synchronized)
}

test "message ownership transfer" {
    // After send, message belongs to socket
    // Caller must not access message data
}
```

### Performance and Stress Tests

```zig
test "throughput benchmark" {
    const msg_count = 1_000_000;
    const msg_size = 100;

    // Measure messages per second
    // Compare against libzmq baseline
}

test "latency benchmark" {
    // REQ/REP round-trip
    // Measure p50, p99, p99.9 latencies
}

test "many connections" {
    // 1000+ concurrent connections
    // Verify no resource exhaustion
}

test "large message handling" {
    // 1GB message
    // Verify memory efficiency (streaming, not full copy)
}

test "long-running stability" {
    // Hours of continuous operation
    // Verify no memory leaks, handle exhaustion
}
```

### Test Matrix Summary

| Category | Test Count | Priority |
|----------|------------|----------|
| Message System | 12 | P0 - Critical |
| HWM/Flow Control | 8 | P0 - Critical |
| PUSH/PULL Pattern | 6 | P0 - Critical |
| REQ/REP Pattern | 8 | P0 - Critical |
| DEALER/ROUTER Pattern | 8 | P1 - High |
| PUB/SUB Pattern | 10 | P0 - Critical |
| Connection Lifecycle | 10 | P0 - Critical |
| Reconnection | 8 | P1 - High |
| Inproc Transport | 6 | P1 - High |
| ZMTP Protocol | 10 | P0 - Critical |
| PLAIN Security | 6 | P2 - Medium |
| Linger/Shutdown | 8 | P1 - High |
| Subscription Matching | 8 | P0 - Critical |
| Polling/Readiness | 10 | P0 - Critical |
| Identity/Routing | 6 | P1 - High |
| Edge Cases | 10 | P1 - High |
| Concurrency | 6 | P1 - High |
| Performance | 8 | P2 - Medium |

**Total: ~140 test cases**

### Compatibility Testing with libzmq

For semantic equivalence, run parallel tests against both implementations:

```zig
// Test harness that runs same test against ZZMQ and libzmq
fn compatTest(comptime testFn: fn (*Socket, *Socket) anyerror!void) !void {
    // Run with ZZMQ
    var zzmq_push = try zzmq.Context.socket(.push);
    var zzmq_pull = try zzmq.Context.socket(.pull);
    try testFn(&zzmq_push, &zzmq_pull);

    // Run with libzmq (via C bindings)
    var zmq_push = c_zmq_socket(ctx, ZMQ_PUSH);
    var zmq_pull = c_zmq_socket(ctx, ZMQ_PULL);
    try testFn(&zmq_push, &zmq_pull);

    // Both should behave identically
}
```

### Continuous Integration

- Run full test suite on every PR
- Performance regression tests (flag if >5% slower)
- Memory leak detection (valgrind/sanitizers)
- Cross-platform testing (Linux, macOS, Windows)
- Interoperability tests with libzmq binaries

---

## Implementation Roadmap

### Phase 1: Core Infrastructure
- [ ] Message type with inline/owned/shared/external storage
- [ ] Context and socket lifecycle
- [ ] Pipe with zio.Channel
- [ ] Basic PUSH/PULL patterns
- [ ] Polling API (`poll()` and `Poller`)
- [ ] Socket readiness (`hasIn()`, `hasOut()`, `getEvents()`)

### Phase 2: Network Transport
- [ ] TCP transport (connect/listen)
- [ ] Engine with reader/writer coroutines
- [ ] ZMTP codec (minimal: greeting, message frames)
- [ ] Reconnection logic
- [ ] `getFd()` for external event loop integration

### Phase 3: More Patterns
- [ ] PUB/SUB with subscriptions
- [ ] REQ/REP with state machine
- [ ] DEALER/ROUTER with routing

### Phase 4: Full ZMTP
- [ ] Security mechanisms (NULL, PLAIN)
- [ ] Heartbeat
- [ ] Commands (READY, SUBSCRIBE, etc.)

### Phase 5: Additional Features
- [ ] IPC transport
- [ ] Inproc transport
- [ ] Socket monitoring
- [ ] CURVE security (optional)
- [ ] Thread-safe socket signaling

---

## Summary

ZZMQ provides ZeroMQ semantics on Zig/ZIO by:

1. **Frame-based pipes with growable queues**: Pipes store frames (not complete messages), with truly unlimited capacity when HWM=0. HWM enforced via message counters, not queue size.
2. **Coroutines for connections**: Each connection is an Engine with reader/writer coroutine pair managed by ZIO Groups.
3. **Pattern-specific logic**: Comptime traits for PUSH/PULL/PUB/SUB/REQ/REP/DEALER/ROUTER with pattern-appropriate send/recv semantics.
4. **Explicit copy semantics**: Messages use copy-on-send with inline small-message optimization. No hidden sharing or reference counting complexity.
5. **Blocking API backed by async**: Natural synchronous code, efficient cooperative scheduling via ZIO runtime.
6. **Level-triggered polling via ZIO**: Compatible `poll()`/`Poller` APIs with `getFd()` for external event loop integration.
7. **Two-tier architecture**: Comptime-optimized layer for Zig users (zero-cost abstractions), runtime-flexible layer for C FFI compatibility.

The design prioritizes:
- **Simplicity**: ZIO handles async I/O, cancellation, and structured concurrency
- **Performance**: Minimal copies, cache-friendly layouts, io_uring integration
- **Compatibility**: libzmq semantics, ZMTP 3.1 wire protocol, familiar polling APIs
- **Correctness**: Explicit ownership, validated configuration, graceful error handling

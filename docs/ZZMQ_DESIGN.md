# ZZMQ: ZeroMQ Semantics on Zig with ZIO

A high-performance messaging library implementing ZeroMQ semantics, built idiomatically on Zig and the ZIO async I/O framework.

## Table of Contents

1. [Design Goals](#design-goals)
2. [Why Zig + ZIO](#why-zig--zio)
3. [Architecture Overview](#architecture-overview)
4. [Core Types](#core-types)
5. [Message System](#message-system)
6. [Socket Architecture](#socket-architecture)
7. [Pipe System](#pipe-system)
8. [Select and Multiplexing](#select-and-multiplexing)
9. [Connection Management](#connection-management)
10. [Transport Layer](#transport-layer)
11. [ZMTP Codec](#zmtp-codec)
12. [Socket Patterns](#socket-patterns)
13. [Options and Configuration](#options-and-configuration)
14. [Error Handling](#error-handling)
15. [API Design](#api-design)
16. [Performance Considerations](#performance-considerations)
17. [Monitoring and Events](#monitoring-and-events)
18. [libzmq Reference: Critical Behaviors](#libzmq-reference-critical-behaviors-and-edge-cases)
19. [libzmq Reference: Signaling, Connection, Heartbeat](#libzmq-reference-signaling-connection-readiness-heartbeat-disconnect)
20. [libzmq Reference: Level vs Edge Triggering and Polling](#libzmq-reference-level-vs-edge-triggering-and-polling)
21. [Implementation Roadmap](#implementation-roadmap)
22. [Summary](#summary)

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

4. **Inline small messages**: Messages ≤48 bytes are stored inline in the Message struct. No allocation, no indirection.

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
    → stream.write(buf)         // ZIO async write

WARM PATH - Engine reader (per-message, but async):
  stream.read(buf)              // ZIO async read
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

## Core Types

### Context

The context owns shared resources and provides the ZIO runtime for all sockets.

```zig
pub const Context = struct {
    /// The ZIO runtime - ALL I/O goes through this
    runtime: *zio.Runtime,

    /// Allocator for dynamic allocations
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

    pub fn init(runtime: *zio.Runtime, allocator: std.mem.Allocator, options: ContextOptions) !*Context;
    pub fn deinit(self: *Context) void;

    /// Create a socket of the specified type
    pub fn socket(self: *Context, comptime Pattern: type) !*Socket(Pattern);

    /// Shutdown: stop accepting new operations, start draining
    pub fn shutdown(self: *Context) void;

    /// Terminate: block until all sockets closed and drained
    pub fn terminate(self: *Context) void;
};

pub const ContextOptions = struct {
    max_sockets: u32 = 1024,
    max_message_size: usize = 0,  // 0 = no limit
    io_threads: u32 = 1,          // Hint (ZIO manages actual threading)
};
```

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
/// Engine writer: send multipart atomically
fn writeMultipart(self: *Engine, rt: *zio.Runtime) !void {
    var multipart_frames = std.ArrayList([]u8).init(self.allocator);
    defer multipart_frames.deinit();

    // Collect all frames of multipart
    while (true) {
        const msg = try self.pipe.outbound.receive(rt);
        defer msg.deinit();

        const encoded = try self.codec.encodeMessage(&self.write_buf, &msg);
        try multipart_frames.append(try self.allocator.dupe(u8, encoded));

        if (!msg.flags.more) break;
    }

    // Write all frames in single operation (or as close as possible)
    // TCP may split across packets, but ZMQ peers handle reassembly
    for (multipart_frames.items) |frame| {
        try self.stream.writeAll(rt, frame);
    }
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

A pipe represents the bidirectional connection between a socket and a peer (either remote via network, or local via inproc).

### Pipe Structure

```zig
pub const Pipe = struct {
    id: PipeId,

    /// Channel: socket → engine (outbound messages)
    /// Capacity = send HWM
    outbound: *zio.Channel(Message),

    /// Channel: engine → socket (inbound messages)
    /// Capacity = recv HWM
    inbound: *zio.Channel(Message),

    /// Peer identity (assigned during handshake or by ROUTER)
    routing_id: ?RoutingId,

    /// Associated engine (null for inproc)
    engine: ?*Engine,

    /// State
    state: State,

    /// Statistics
    stats: Stats,

    /// Options snapshot (from socket at creation time)
    options: PipeOptions,

    const State = enum {
        active,
        draining_outbound,  // Closing, flushing sends
        draining_inbound,   // Peer closing, receiving remaining
        closed,
    };

    const Stats = struct {
        messages_sent: u64 = 0,
        messages_recv: u64 = 0,
        bytes_sent: u64 = 0,
        bytes_recv: u64 = 0,
    };

    const PipeOptions = struct {
        send_hwm: u32,
        recv_hwm: u32,
        conflate: bool,
    };

    /// Write message to outbound channel (socket → network)
    /// Suspends if channel full (HWM backpressure)
    pub fn write(self: *Pipe, rt: *zio.Runtime, msg: Message) !void {
        if (self.state != .active) return error.PipeClosed;

        if (self.options.conflate) {
            // Conflate mode: replace any pending message
            _ = self.outbound.tryReceive() catch {};
        }

        try self.outbound.send(rt, msg);
        self.stats.messages_sent += 1;
        self.stats.bytes_sent += msg.len();
    }

    /// Try to write without blocking
    pub fn tryWrite(self: *Pipe, msg: Message) !bool {
        if (self.state != .active) return error.PipeClosed;

        if (self.options.conflate) {
            _ = self.outbound.tryReceive() catch {};
        }

        self.outbound.trySend(msg) catch |err| switch (err) {
            error.ChannelFull => return false,
            else => return err,
        };

        self.stats.messages_sent += 1;
        self.stats.bytes_sent += msg.len();
        return true;
    }

    /// Read message from inbound channel (network → socket)
    /// Suspends if channel empty
    pub fn read(self: *Pipe, rt: *zio.Runtime) !Message {
        const msg = try self.inbound.receive(rt);
        self.stats.messages_recv += 1;
        self.stats.bytes_recv += msg.len();
        return msg;
    }

    /// Try to read without blocking
    pub fn tryRead(self: *Pipe) !?Message {
        const msg = self.inbound.tryReceive() catch |err| switch (err) {
            error.ChannelEmpty => return null,
            error.ChannelClosed => return error.PipeClosed,
            else => return err,
        };
        self.stats.messages_recv += 1;
        self.stats.bytes_recv += msg.len();
        return msg;
    }

    /// Check if pipe has data to read
    pub fn hasData(self: *Pipe) bool {
        return !self.inbound.isEmpty();
    }

    /// Check if pipe can accept writes
    pub fn canWrite(self: *Pipe) bool {
        return self.state == .active and !self.outbound.isFull();
    }

    /// Begin graceful close
    pub fn close(self: *Pipe, rt: *zio.Runtime) void {
        if (self.state != .active) return;

        self.state = .draining_outbound;
        self.outbound.close(.graceful);

        // Engine will detect channel close and finish sending
    }
};

pub const PipeId = u32;
```

### High Water Mark (HWM) - Comprehensive Design

HWM is one of the most critical ZMQ semantics. This section details how ZZMQ implements HWM to match libzmq behavior.

#### libzmq HWM Reference

From `src/pipe.cpp:533-538`:
```cpp
bool zmq::pipe_t::check_hwm () const
{
    const bool full =
      _hwm > 0 && _msgs_written - _peers_msgs_read >= uint64_t (_hwm);
    return !full;
}
```

Key libzmq behaviors:
- Default HWM = 1000 (`src/options.cpp:168`)
- HWM = 0 means **unlimited** (never blocks)
- Inproc: HWM = sender's sndhwm + receiver's rcvhwm
- TCP/IPC: Each side uses its own HWMs independently
- HWM counts **complete messages**, not frames

#### HWM Type Definition

```zig
/// High water mark value
/// null = unlimited (0 in libzmq)
/// value = bounded capacity
pub const Hwm = ?u32;

pub const HwmDefaults = struct {
    pub const send: Hwm = 1000;
    pub const recv: Hwm = 1000;
};

/// Convert libzmq-style HWM (0 = unlimited) to ZZMQ
pub fn fromLibzmq(value: i32) Hwm {
    return if (value <= 0) null else @intCast(value);
}

/// Convert ZZMQ HWM to libzmq-style
pub fn toLibzmq(hwm: Hwm) i32 {
    return hwm orelse 0;
}
```

#### Channel Capacity from HWM

```zig
/// Calculate effective channel capacity
/// For unlimited HWM, we use a large but finite buffer
fn hwmToCapacity(hwm: Hwm) usize {
    return hwm orelse std.math.maxInt(u32);  // ~4 billion for "unlimited"
}

/// Alternative: truly unbounded using growable buffer
/// (but this loses backpressure guarantees)
```

**Design decision**: Even "unlimited" HWM uses a large finite buffer. This prevents:
- Memory exhaustion from runaway producers
- Provides eventual backpressure at extreme scales
- Matches practical libzmq behavior (memory is finite)

#### Inproc HWM Calculation

From `src/socket_base.cpp:785-794`:
```cpp
// The total HWM for an inproc connection should be the sum of
// the binder's HWM and the connector's HWM.
const int sndhwm = peer.socket == NULL ? options.sndhwm
                   : options.sndhwm != 0 && peer.options.rcvhwm != 0
                     ? options.sndhwm + peer.options.rcvhwm
                     : 0;
```

**ZZMQ implementation:**

```zig
/// Calculate effective HWM for inproc connection
/// libzmq sums both sides; if either is unlimited, total is unlimited
fn calculateInprocHwm(
    connector_send: Hwm,
    binder_recv: Hwm,
) Hwm {
    // If either side is unlimited, total is unlimited
    if (connector_send == null or binder_recv == null) {
        return null;  // unlimited
    }
    // Sum both sides
    return connector_send.? + binder_recv.?;
}

/// Create pipe pair for inproc connection
fn createInprocPipePair(
    allocator: std.mem.Allocator,
    connector_opts: *const SocketOptions,
    binder_opts: *const SocketOptions,
) !struct { connector_pipe: *Pipe, binder_pipe: *Pipe } {
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

    // Create channels with calculated capacities
    const c2b_cap = hwmToCapacity(c2b_hwm);
    const b2c_cap = hwmToCapacity(b2c_hwm);

    const c2b_buf = try allocator.alloc(Message, c2b_cap);
    const b2c_buf = try allocator.alloc(Message, b2c_cap);

    // Connector's pipe: outbound=c2b, inbound=b2c
    const connector_pipe = try allocator.create(Pipe);
    connector_pipe.* = .{
        .outbound = zio.Channel(Message).init(c2b_buf),
        .inbound = zio.Channel(Message).init(b2c_buf),
        .effective_send_hwm = c2b_hwm,
        .effective_recv_hwm = b2c_hwm,
        // Track peer's HWM for dynamic updates
        .peer_send_hwm = binder_opts.send_hwm,
        .peer_recv_hwm = binder_opts.recv_hwm,
    };

    // Binder's pipe: shares same channels, reversed direction
    const binder_pipe = try allocator.create(Pipe);
    binder_pipe.* = .{
        .outbound = zio.Channel(Message).init(b2c_buf),
        .inbound = zio.Channel(Message).init(c2b_buf),
        .effective_send_hwm = b2c_hwm,
        .effective_recv_hwm = c2b_hwm,
        .peer_send_hwm = connector_opts.send_hwm,
        .peer_recv_hwm = connector_opts.recv_hwm,
    };

    return .{ .connector_pipe = connector_pipe, .binder_pipe = binder_pipe };
}
```

#### TCP/IPC HWM (Non-Inproc)

For TCP connections, each side uses its own HWM independently:

```zig
/// Create pipe for TCP/IPC connection
/// Each side uses its own HWM (no summing)
fn createTcpPipe(
    allocator: std.mem.Allocator,
    options: *const SocketOptions,
) !*Pipe {
    const send_cap = hwmToCapacity(options.send_hwm);
    const recv_cap = hwmToCapacity(options.recv_hwm);

    const send_buf = try allocator.alloc(Message, send_cap);
    const recv_buf = try allocator.alloc(Message, recv_cap);

    const pipe = try allocator.create(Pipe);
    pipe.* = .{
        .outbound = zio.Channel(Message).init(send_buf),
        .inbound = zio.Channel(Message).init(recv_buf),
        .effective_send_hwm = options.send_hwm,
        .effective_recv_hwm = options.recv_hwm,
        .peer_send_hwm = null,  // Unknown for TCP
        .peer_recv_hwm = null,
    };

    return pipe;
}
```

#### HWM Counts Complete Messages, Not Frames

**Critical semantic**: A 10-frame multipart message counts as ONE message for HWM.

From libzmq `src/pipe.cpp:198-199`:
```cpp
if (!(msg_->flags () & msg_t::more) && !msg_->is_routing_id ())
    _msgs_read++;
```

**ZZMQ implementation:**

```zig
pub const Pipe = struct {
    outbound: *zio.Channel(Message),
    inbound: *zio.Channel(Message),

    /// Track messages written (for HWM, not frames)
    msgs_written: u64 = 0,

    /// Messages in current multipart (not counted until complete)
    pending_multipart_count: u32 = 0,

    /// Write message to pipe, respecting multipart HWM semantics
    pub fn write(self: *Pipe, msg: Message, rt: *zio.Runtime) !void {
        // Always write to channel (channel enforces backpressure)
        try self.outbound.send(rt, msg);

        // Track for HWM: only count complete messages
        if (msg.flags.more) {
            // Part of multipart - don't count yet
            self.pending_multipart_count += 1;
        } else {
            // End of message (single or multipart)
            // This counts as ONE message for HWM
            self.msgs_written += 1;
            self.pending_multipart_count = 0;
        }
    }

    /// Check if HWM allows writing
    /// Note: This is for patterns that need to check before writing
    pub fn canWrite(self: *Pipe) bool {
        // If unlimited HWM, always writable
        if (self.effective_send_hwm == null) return true;

        // Check channel capacity
        return !self.outbound.isFull();
    }
};
```

**Important**: The channel itself provides frame-level backpressure. The `msgs_written` counter is for statistics and compatibility, but the actual HWM enforcement happens at the channel level. Since multipart messages are written atomically (all frames or none), this works correctly.

#### Dynamic HWM Updates

libzmq allows changing HWM via `setsockopt` after connections exist:

From `src/socket_base.cpp:1586-1588`:
```cpp
for (pipes_t::size_type i = 0; i != size; ++i) {
    _pipes[i]->set_hwms (options.rcvhwm, options.sndhwm);
    _pipes[i]->send_hwms_to_peer (options.sndhwm, options.rcvhwm);
}
```

**ZZMQ challenge**: ZIO channels have fixed capacity at creation. Options:

1. **Recreate channel** (disruptive, loses messages)
2. **Track logical HWM separately** (channel may be larger than HWM)
3. **Disallow dynamic HWM changes** (deviation from libzmq)

**ZZMQ approach**: Track logical HWM separately from channel capacity:

```zig
pub const Pipe = struct {
    outbound: *zio.Channel(Message),

    /// Physical channel capacity (immutable)
    channel_capacity: usize,

    /// Logical HWM (can be changed dynamically)
    effective_send_hwm: Hwm,

    /// For inproc: peer's HWM for boost calculation
    peer_recv_hwm: Hwm,

    /// Check if logical HWM allows writing
    pub fn checkLogicalHwm(self: *Pipe) bool {
        const hwm = self.effective_send_hwm orelse return true;

        // Count messages in channel (not frames)
        // This requires tracking or counting
        return self.outstandingMessages() < hwm;
    }

    /// Update HWM dynamically (for setsockopt)
    pub fn setHwm(self: *Pipe, new_hwm: Hwm) void {
        self.effective_send_hwm = new_hwm;
        // Note: If new HWM > channel_capacity, we're limited by channel
        // If new HWM < channel_capacity, logical HWM takes effect
    }

    /// Update peer's HWM (received via command from peer)
    pub fn setPeerHwm(self: *Pipe, peer_send: Hwm, peer_recv: Hwm) void {
        self.peer_recv_hwm = peer_recv;
        // Recalculate effective HWM for inproc
        self.effective_send_hwm = calculateInprocHwm(
            self.local_send_hwm,
            peer_recv,
        );
    }
};
```

#### Conflate Mode (ZMQ_CONFLATE)

libzmq's conflate mode keeps only the latest message, dropping older ones:

From `src/pipe.cpp:26-27`:
```cpp
if (conflate_[0])
    upipe1 = new (std::nothrow) ypipe_conflate_t ();
```

**ZZMQ implementation:**

```zig
/// Conflating channel - keeps only latest message
pub fn ConflatingChannel(comptime T: type) type {
    return struct {
        latest: ?T = null,
        mutex: std.Thread.Mutex = .{},
        has_value: std.Thread.Condition = .{},
        closed: bool = false,

        const Self = @This();

        /// Send overwrites any existing value
        pub fn send(self: *Self, rt: *zio.Runtime, value: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) return error.ChannelClosed;

            // Drop old value if exists
            if (self.latest) |*old| {
                old.deinit();
            }

            self.latest = value;
            self.has_value.signal();
        }

        /// Receive gets latest value (waits if none)
        pub fn receive(self: *Self, rt: *zio.Runtime) !T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.latest == null and !self.closed) {
                // Suspend via ZIO
                self.has_value.wait(&self.mutex);
            }

            if (self.latest) |value| {
                self.latest = null;
                return value;
            }

            return error.ChannelClosed;
        }
    };
}

/// Create pipe with conflate support
fn createPipe(
    allocator: std.mem.Allocator,
    options: *const SocketOptions,
) !*Pipe {
    if (options.conflate) {
        // Conflating channels (capacity 1, overwrites)
        return createConflatePipe(allocator);
    } else {
        // Normal bounded channels
        return createNormalPipe(allocator, options);
    }
}
```

#### HWM Behavior Summary

| Scenario | libzmq Behavior | ZZMQ Implementation |
|----------|-----------------|---------------------|
| Default HWM | 1000 | `HwmDefaults.send = 1000` |
| HWM = 0 | Unlimited | `Hwm = null` |
| Inproc HWM | Sum of both sides | `calculateInprocHwm()` |
| TCP HWM | Each side independent | Use socket's own HWM |
| HWM counting | Complete messages only | Track `msgs_written` on !more |
| Dynamic HWM | Update via command | `setHwm()` + command to peer |
| Conflate mode | Keep only latest | `ConflatingChannel` |
| Either side unlimited | Total unlimited | `null` propagates |

#### Backpressure Flow

```
Socket.send()
    │
    ▼
Pattern.send() ─── check canWrite() for pattern-specific logic
    │
    ▼
Pipe.write()
    │
    ├── Channel has space? ─── Yes ──► Immediate write
    │           │
    │          No (HWM)
    │           │
    │           ▼
    │   Coroutine suspends
    │           │
    │   Engine drains channel
    │           │
    │   Channel signals space
    │           │
    │   Coroutine resumes
    │           │
    └───────────┴──────────► Write completes
```

This matches libzmq's HWM semantics using ZIO's cooperative scheduling instead of lock-free queues + signaling.

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

### Performance Characteristics

| Scenario | Behavior |
|----------|----------|
| One pipe ready | O(1) - immediate return on first check |
| All pipes empty | O(n) setup + suspend until any ready |
| HWM backpressure | Coroutine suspends, zero CPU spin |
| Many pipes (>8) | Falls back to polling or batched select |

The key insight: ZIO's cooperative scheduling means "waiting" is just coroutine suspension. No busy-waiting, no kernel threads blocked.

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
        reconnecting,
    };

    const ReconnectState = struct {
        attempt: u32 = 0,
        interval: u64,       // Current interval (ms)
        interval_min: u64,   // ZMQ_RECONNECT_IVL
        interval_max: u64,   // ZMQ_RECONNECT_IVL_MAX

        fn nextDelay(self: *ReconnectState) u64 {
            const delay = self.interval;

            // Exponential backoff
            self.interval = @min(self.interval * 2, self.interval_max);
            self.attempt += 1;

            // Add jitter (±25%)
            const jitter = delay / 4;
            const rand = std.crypto.random.int(u64) % (jitter * 2);
            return delay - jitter + rand;
        }

        fn reset(self: *ReconnectState) void {
            self.attempt = 0;
            self.interval = self.interval_min;
        }
    };

    /// Connection loop coroutine
    pub fn run(self: *Connector, rt: *zio.Runtime) void {
        while (self.state != .disconnected) {
            // Attempt connection
            self.state = .connecting;

            const stream = self.endpoint.connect(rt) catch |err| {
                self.handleConnectFailure(rt, err);
                continue;
            };

            // Connection succeeded
            self.reconnect.reset();
            self.state = .connected;

            const engine = Engine.create(self.socket, stream, .client) catch |err| {
                stream.close(rt);
                self.handleConnectFailure(rt, err);
                continue;
            };

            self.engine = engine;

            // Run engine (blocks until disconnect)
            engine.run(rt);

            // Engine finished (disconnect)
            self.engine = null;
            engine.destroy();

            if (self.state == .disconnected) break;

            // Reconnect with backoff
            self.state = .reconnecting;
            const delay = self.reconnect.nextDelay();
            zio.time.sleep(rt, .fromMilliseconds(delay)) catch break;
        }
    }

    fn handleConnectFailure(self: *Connector, rt: *zio.Runtime, err: anyerror) void {
        // Log, notify monitor, schedule retry
        const delay = self.reconnect.nextDelay();
        zio.time.sleep(rt, .fromMilliseconds(delay)) catch {};
    }

    pub fn disconnect(self: *Connector) void {
        self.state = .disconnected;
        if (self.engine) |e| e.stop();
    }
};
```

### Engine (ZMTP protocol handler)

```zig
pub const Engine = struct {
    /// The pipe this engine serves
    pipe: *Pipe,

    /// Network stream
    stream: zio.net.Stream,

    /// ZMTP codec state
    codec: ZmtpCodec,

    /// Role (affects handshake)
    role: Role,

    /// State
    state: State,

    /// Heartbeat state
    heartbeat: HeartbeatState,

    /// Group for reader/writer coroutines
    group: zio.Group,

    const Role = enum { client, server };

    const State = enum {
        handshaking,
        ready,
        closing,
        closed,
    };

    const HeartbeatState = struct {
        interval: ?u64,       // ZMQ_HEARTBEAT_IVL (ms)
        timeout: u64,         // ZMQ_HEARTBEAT_TIMEOUT (ms)
        ttl: u64,             // ZMQ_HEARTBEAT_TTL (ms)
        last_recv: i64,       // Timestamp of last recv
        last_send: i64,       // Timestamp of last send
    };

    pub fn run(self: *Engine, rt: *zio.Runtime) void {
        defer self.cleanup(rt);

        // Perform ZMTP handshake
        self.performHandshake(rt) catch |err| {
            self.handleError(err);
            return;
        };

        self.state = .ready;

        // Spawn reader and writer coroutines
        self.group.spawn(rt, Engine.readerLoop, .{ self, rt }) catch return;
        self.group.spawn(rt, Engine.writerLoop, .{ self, rt }) catch return;

        if (self.heartbeat.interval) |_| {
            self.group.spawn(rt, Engine.heartbeatLoop, .{ self, rt }) catch return;
        }

        // Wait for all to finish
        self.group.wait(rt);
    }

    /// Reader: network → inbound channel
    fn readerLoop(self: *Engine, rt: *zio.Runtime) void {
        var read_buf: [65536]u8 = undefined;

        while (self.state == .ready) {
            // Read from network
            const n = self.stream.read(rt, &read_buf, self.readTimeout()) catch |err| {
                self.handleReadError(err);
                break;
            };

            if (n == 0) {
                // EOF - peer closed
                break;
            }

            self.heartbeat.last_recv = zio.time.now();

            // Decode ZMTP frames
            var offset: usize = 0;
            while (offset < n) {
                const frame = self.codec.decode(read_buf[offset..n]) catch |err| {
                    self.handleProtocolError(err);
                    break;
                } orelse break;  // Need more data

                offset += frame.consumed;

                switch (frame.type) {
                    .message => {
                        // Write to inbound channel
                        self.pipe.inbound.send(rt, frame.message) catch |err| {
                            self.handleChannelError(err);
                            break;
                        };
                    },
                    .command => {
                        self.handleCommand(frame.command);
                    },
                }
            }
        }
    }

    /// Writer: outbound channel → network
    fn writerLoop(self: *Engine, rt: *zio.Runtime) void {
        var write_buf: [65536]u8 = undefined;

        while (self.state == .ready) {
            // Read from outbound channel
            const msg = self.pipe.outbound.receive(rt) catch |err| switch (err) {
                error.ChannelClosed => break,  // Pipe closing
                else => {
                    self.handleChannelError(err);
                    break;
                },
            };
            defer msg.deinit();

            // Encode to ZMTP
            const encoded = self.codec.encode(&write_buf, msg) catch |err| {
                self.handleProtocolError(err);
                break;
            };

            // Write to network
            self.stream.writeAll(rt, encoded, self.writeTimeout()) catch |err| {
                self.handleWriteError(err);
                break;
            };

            self.heartbeat.last_send = zio.time.now();
        }
    }

    /// Heartbeat: periodic PING/PONG
    fn heartbeatLoop(self: *Engine, rt: *zio.Runtime) void {
        const interval = self.heartbeat.interval orelse return;

        while (self.state == .ready) {
            zio.time.sleep(rt, .fromMilliseconds(interval)) catch break;

            // Check for timeout
            const now = zio.time.now();
            if (now - self.heartbeat.last_recv > self.heartbeat.timeout) {
                self.handleTimeout();
                break;
            }

            // Send PING if needed
            if (now - self.heartbeat.last_send > interval) {
                self.sendCommand(.ping) catch break;
            }
        }
    }

    fn stop(self: *Engine) void {
        self.state = .closing;
        self.group.cancel();
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
        // Read more data from network
        const n = self.stream.read(rt, read_buf[decode_offset..]) catch |err| {
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

---

## Options and Configuration

### Socket Options (libzmq compatible)

```zig
pub const SocketOption = enum {
    // High water marks
    send_hwm,           // ZMQ_SNDHWM - default 1000
    recv_hwm,           // ZMQ_RCVHWM - default 1000

    // Timeouts
    send_timeout,       // ZMQ_SNDTIMEO - default -1 (infinite)
    recv_timeout,       // ZMQ_RCVTIMEO - default -1 (infinite)

    // Connection behavior
    linger,             // ZMQ_LINGER - default -1 (infinite)
    reconnect_ivl,      // ZMQ_RECONNECT_IVL - default 100ms
    reconnect_ivl_max,  // ZMQ_RECONNECT_IVL_MAX - default 0 (no max)
    connect_timeout,    // ZMQ_CONNECT_TIMEOUT - default 0 (no timeout)

    // Identity
    routing_id,         // ZMQ_ROUTING_ID

    // Heartbeat
    heartbeat_ivl,      // ZMQ_HEARTBEAT_IVL - default 0 (disabled)
    heartbeat_timeout,  // ZMQ_HEARTBEAT_TIMEOUT
    heartbeat_ttl,      // ZMQ_HEARTBEAT_TTL

    // Pattern-specific
    subscribe,          // ZMQ_SUBSCRIBE (SUB only)
    unsubscribe,        // ZMQ_UNSUBSCRIBE (SUB only)
    req_relaxed,        // ZMQ_REQ_RELAXED
    req_correlate,      // ZMQ_REQ_CORRELATE
    router_mandatory,   // ZMQ_ROUTER_MANDATORY

    // Behavior
    conflate,           // ZMQ_CONFLATE - keep only last message
    immediate,          // ZMQ_IMMEDIATE

    // TCP options
    tcp_keepalive,      // ZMQ_TCP_KEEPALIVE
    tcp_keepalive_cnt,
    tcp_keepalive_idle,
    tcp_keepalive_intvl,

    pub fn Type(comptime self: SocketOption) type {
        return switch (self) {
            .send_hwm, .recv_hwm => u32,
            .send_timeout, .recv_timeout => i32,  // -1 = infinite
            .linger => i32,
            .reconnect_ivl, .reconnect_ivl_max => u32,
            .connect_timeout => u32,
            .routing_id => []const u8,
            .heartbeat_ivl, .heartbeat_timeout, .heartbeat_ttl => u32,
            .subscribe, .unsubscribe => []const u8,
            .req_relaxed, .req_correlate, .router_mandatory => bool,
            .conflate, .immediate => bool,
            .tcp_keepalive => i32,
            .tcp_keepalive_cnt, .tcp_keepalive_idle, .tcp_keepalive_intvl => i32,
        };
    }
};

pub const SocketOptions = struct {
    send_hwm: u32 = 1000,
    recv_hwm: u32 = 1000,
    send_timeout: i32 = -1,
    recv_timeout: i32 = -1,
    linger: i32 = -1,
    reconnect_ivl: u32 = 100,
    reconnect_ivl_max: u32 = 0,
    connect_timeout: u32 = 0,
    routing_id: ?RoutingId = null,
    heartbeat_ivl: u32 = 0,
    heartbeat_timeout: u32 = 0,
    heartbeat_ttl: u32 = 0,
    conflate: bool = false,
    immediate: bool = false,

    // TCP
    tcp_keepalive: i32 = -1,
    tcp_keepalive_cnt: i32 = -1,
    tcp_keepalive_idle: i32 = -1,
    tcp_keepalive_intvl: i32 = -1,

    // Pattern-specific (set by pattern)
    req_relaxed: bool = false,
    req_correlate: bool = false,
    router_mandatory: bool = false,
};
```

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

4. **VSM (Very Small Message)**: Messages ≤24 bytes stored inline in msg_t - zero allocation on hot path.

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
            // Single syscall for entire batch
            try self.stream.write(batch_buffer[0..batch_size]);
            batch_size = 0;
        } else {
            // No data - yield to let other coroutines run
            // Then wait for either: new message or socket writable
            const msg = self.pipe.outbound.receive();  // Suspends coroutine
            batch_size = self.codec.encode(&msg, &batch_buffer);
        }

        // Speculative write: try immediately (low latency)
        try self.stream.write(batch_buffer[0..batch_size]);
        batch_size = 0;
    }
}

// Reader coroutine - moves messages from network to pipe
fn readerLoop(self: *Engine) !void {
    var read_buffer: [65536]u8 = undefined;  // Large buffer for batched reads

    while (!self.terminated) {
        // Read from network (may suspend)
        const n = try self.stream.read(&read_buffer);
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

### Monitor Channel

```zig
pub const Monitor = struct {
    /// Event channel (bounded to prevent buildup)
    events: zio.Channel(SocketEvent),

    /// Which events to capture (bitmask)
    event_mask: EventMask,

    pub const EventMask = packed struct {
        connected: bool = true,
        disconnected: bool = true,
        bind_failed: bool = true,
        accept_failed: bool = true,
        handshake_failed: bool = true,
        protocol_error: bool = true,
        // ... etc
    };

    /// Create a monitor for a socket
    pub fn init(allocator: std.mem.Allocator, capacity: usize, mask: EventMask) !Monitor {
        var buf = try allocator.alloc(SocketEvent, capacity);
        return .{
            .events = zio.Channel(SocketEvent).init(buf),
            .event_mask = mask,
        };
    }

    /// Receive next event (blocking)
    pub fn recv(self: *Monitor, rt: *zio.Runtime) !SocketEvent {
        return self.events.receive(rt);
    }

    /// Try to receive event (non-blocking)
    pub fn tryRecv(self: *Monitor) ?SocketEvent {
        return self.events.tryReceive() catch null;
    }

    /// Post event (internal use by socket)
    pub fn post(self: *Monitor, event: SocketEvent) void {
        // Non-blocking: drop if channel full
        _ = self.events.trySend(event) catch {};
    }
};

// Usage
pub fn monitorExample(ctx: *zzmq.Context, rt: *zio.Runtime) !void {
    var socket = try ctx.socket(zzmq.Push);
    defer socket.close();

    // Attach monitor
    var monitor = try Monitor.init(ctx.allocator, 100, .{});
    defer monitor.deinit();
    try socket.attachMonitor(&monitor);

    // Connect (will generate events)
    try socket.connect("tcp://127.0.0.1:5555");

    // Monitor events in separate coroutine
    var group: zio.Group = .init;
    defer group.cancel(rt);

    try group.spawn(rt, struct {
        fn run(mon: *Monitor, runtime: *zio.Runtime) !void {
            while (true) {
                const event = mon.recv(runtime) catch break;
                switch (event) {
                    .connected => |e| {
                        std.log.info("Connected to {s}", .{e.endpoint});
                    },
                    .disconnected => |e| {
                        std.log.info("Disconnected from {s}: {}", .{e.endpoint, e.reason});
                    },
                    .handshake_failed => |e| {
                        std.log.err("Handshake failed: {}", .{e.error});
                    },
                    else => {},
                }
            }
        }
    }.run, .{ &monitor, rt });
}
```

### Integration Points

Events are posted from:
- **Listener**: `listening`, `bind_failed`, `accepted`, `accept_failed`
- **Connector**: `connected`, `connect_delayed`, `connect_retried`
- **Engine**: `handshake_succeeded`, `handshake_failed`, `disconnected`, `protocol_error`
- **Socket**: `closed`, `close_failed`

```zig
// Example: Engine posts events
fn performHandshake(self: *Engine, rt: *zio.Runtime) !void {
    // ... handshake logic ...

    if (handshake_error) |err| {
        self.postEvent(.{ .handshake_failed = .{
            .endpoint = self.endpoint,
            .error = err,
        }});
        return error.HandshakeFailed;
    }

    self.postEvent(.{ .handshake_succeeded = .{
        .endpoint = self.endpoint,
        .peer_identity = self.codec.properties.peer_identity,
    }});
}

fn postEvent(self: *Engine, event: SocketEvent) void {
    if (self.monitor) |mon| {
        mon.post(event);
    }
}
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
            const n = self.stream.read(rt, &self.read_buf) catch |err| {
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

1. **Using zio.Channel as pipes**: Bounded channels with backpressure = HWM
2. **Coroutines for connections**: Each connection is Engine coroutine pair
3. **Pattern-specific logic**: Traits for PUSH/PULL/PUB/SUB/etc.
4. **Reference-counted messages**: Zero-copy fan-out
5. **Blocking API backed by async**: Natural code, efficient execution
6. **Level-triggered polling via ZIO**: Compatible poll/poller APIs with external event loop integration

The design prioritizes:
- Simplicity (ZIO does the hard work)
- Performance (minimal copies, efficient scheduling)
- Compatibility (libzmq semantics, ZMTP wire protocol, polling APIs)

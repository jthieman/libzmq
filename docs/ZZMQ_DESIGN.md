# ZZMQ: ZeroMQ Semantics on Zig with ZIO

A comprehensive design for a ZeroMQ-compatible messaging library built on Zig and the ZIO async I/O framework.

## Table of Contents

1. [Overview](#overview)
2. [Design Principles](#design-principles)
3. [Architecture](#architecture)
4. [Core Types](#core-types)
5. [Message System](#message-system)
6. [Socket Architecture](#socket-architecture)
7. [Pipe System](#pipe-system)
8. [Connection Management](#connection-management)
9. [Transport Layer](#transport-layer)
10. [Socket Patterns](#socket-patterns)
11. [Options and Configuration](#options-and-configuration)
12. [Error Handling](#error-handling)
13. [API Design](#api-design)
14. [Performance Considerations](#performance-considerations)
15. [Implementation Roadmap](#implementation-roadmap)

---

## Overview

ZZMQ is a messaging library that implements ZeroMQ semantics using Zig and the ZIO coroutine runtime. It provides familiar ZMQ patterns (PUSH/PULL, PUB/SUB, REQ/REP, etc.) with ZMTP wire compatibility, while leveraging ZIO's stackful coroutines for clean, efficient async I/O.

### Why ZIO?

ZIO provides:
- **Stackful coroutines**: Write blocking-style code that's actually async
- **Multi-backend I/O**: io_uring, epoll, kqueue, IOCP, poll
- **Bounded channels with backpressure**: Maps directly to HWM semantics
- **Structured concurrency**: Groups for managing connection lifecycles
- **Select**: Multiplexing across multiple operations

### Key Insight

ZIO's `Channel(T)` with bounded capacity IS our pipe mechanism. A channel with capacity N naturally implements HWM=N with proper backpressure. This eliminates the need for custom lock-free queues (ypipe) - the ZIO runtime handles the coordination.

---

## Design Principles

1. **ZIO-native**: Embrace coroutines, not fight them. Blocking APIs backed by async I/O.

2. **libzmq semantics**: Same behavior for HWM, linger, reconnection, patterns, etc.

3. **ZMTP compatibility**: Wire-compatible with libzmq for interoperability.

4. **Zero-copy where possible**: Reference-counted messages, avoid copies on hot paths.

5. **Explicit resource management**: Zig-style `defer` cleanup, no hidden allocations.

6. **Single runtime per context**: All sockets share one ZIO runtime (user-provided).

---

## Architecture

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

### Channel Backpressure = HWM

The key insight: `zio.Channel(Message)` with bounded capacity naturally implements HWM:

```zig
// Create pipe with HWM
fn createPipe(allocator: std.mem.Allocator, options: PipeOptions) !*Pipe {
    // Allocate channel buffers
    const outbound_buf = try allocator.alloc(Message, options.send_hwm);
    const inbound_buf = try allocator.alloc(Message, options.recv_hwm);

    const pipe = try allocator.create(Pipe);
    pipe.* = .{
        .id = generatePipeId(),
        .outbound = zio.Channel(Message).init(outbound_buf),
        .inbound = zio.Channel(Message).init(inbound_buf),
        // ...
    };

    return pipe;
}
```

When a socket calls `pipe.write()`:
1. If channel has space → immediate write, coroutine continues
2. If channel full (HWM reached) → coroutine suspends until engine drains
3. If `trySend` used → returns false immediately when full

This matches libzmq's HWM semantics exactly, but using ZIO's cooperative scheduling instead of lock-free queues + signaling.

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

## Performance Considerations

### Hot Path Analysis

**Send path (PUSH)**:
1. `socket.send(msg)` → Pattern selects pipe (O(1) round-robin)
2. `pipe.write(msg)` → `channel.send()` (may suspend if HWM)
3. Engine coroutine wakes → `channel.receive()`
4. ZMTP encode (in-place, no copy)
5. `stream.write()` → kernel

**Receive path (PULL)**:
1. Engine `stream.read()` → kernel
2. ZMTP decode → allocate message
3. `channel.send()` to inbound (may suspend if HWM)
4. Socket coroutine wakes → `channel.receive()`
5. `socket.recv()` returns

### Memory Layout

```zig
// Message: 64 bytes (1 cache line)
pub const Message = struct {
    data: Data,           // 56 bytes (union)
    flags: Flags,         // 1 byte
    routing_id: ?RoutingId, // Pointer (8 bytes) - stored separately
    // ...
};

// Inline data: up to 48 bytes without allocation
pub const InlineData = struct {
    len: u8,
    bytes: [48]u8,        // Fits most control messages
};
```

### Channel Performance

`zio.Channel` uses:
- Mutex for synchronization (fast uncontended)
- Ring buffer storage
- Wait queues for blocking

For our use case (SPSC within socket/engine pair), this is efficient. The coroutine suspend/resume is the primary cost, which ZIO optimizes.

### Avoiding Copies

| Operation | Copy? |
|-----------|-------|
| Small message create | Copy into inline |
| Large message create | Allocate, no copy |
| Send (single pipe) | Move, no copy |
| Send (PUB fan-out) | Refcount, no copy |
| Pipe → Engine | Move through channel |
| ZMTP encode | Read-only, no copy |
| ZMTP decode | Allocate new |
| Engine → Pipe | Move through channel |
| Recv | Move to user |

### Backpressure Behavior

When HWM is reached:
1. `pipe.write()` calls `channel.send()`
2. Channel is full → coroutine suspends
3. Engine drains channel → channel has space
4. Original coroutine resumes

This is cooperative backpressure - no busy waiting, no signals, just coroutine scheduling.

---

## Implementation Roadmap

### Phase 1: Core Infrastructure
- [ ] Message type with inline/owned/shared/external storage
- [ ] Context and socket lifecycle
- [ ] Pipe with zio.Channel
- [ ] Basic PUSH/PULL patterns

### Phase 2: Network Transport
- [ ] TCP transport (connect/listen)
- [ ] Engine with reader/writer coroutines
- [ ] ZMTP codec (minimal: greeting, message frames)
- [ ] Reconnection logic

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

---

## Summary

ZZMQ provides ZeroMQ semantics on Zig/ZIO by:

1. **Using zio.Channel as pipes**: Bounded channels with backpressure = HWM
2. **Coroutines for connections**: Each connection is Engine coroutine pair
3. **Pattern-specific logic**: Traits for PUSH/PULL/PUB/SUB/etc.
4. **Reference-counted messages**: Zero-copy fan-out
5. **Blocking API backed by async**: Natural code, efficient execution

The design prioritizes:
- Simplicity (ZIO does the hard work)
- Performance (minimal copies, efficient scheduling)
- Compatibility (libzmq semantics, ZMTP wire protocol)

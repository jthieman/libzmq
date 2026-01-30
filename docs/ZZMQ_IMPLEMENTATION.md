# ZZMQ Implementation Guide

This document provides a practical roadmap for implementing ZZMQ, a ZeroMQ-compatible messaging library built on Zig and ZIO. It translates the design decisions from `ZZMQ_DESIGN.md` into concrete implementation tasks.

## Table of Contents

1. [Project Structure](#project-structure)
2. [Build Configuration](#build-configuration)
3. [Dependencies](#dependencies)
4. [Implementation Phases](#implementation-phases)
5. [Phase 1: Core Infrastructure](#phase-1-core-infrastructure)
6. [Phase 2: Pipe System](#phase-2-pipe-system)
7. [Phase 3: ZMTP Protocol](#phase-3-zmtp-protocol)
8. [Phase 4: Socket Patterns](#phase-4-socket-patterns)
9. [Phase 5: Transports](#phase-5-transports)
10. [Phase 6: Advanced Features](#phase-6-advanced-features)
11. [Testing Strategy](#testing-strategy)
12. [Development Workflow](#development-workflow)
13. [Milestones](#milestones)

---

## Project Structure

```
zzmq/
├── build.zig                 # Build configuration
├── build.zig.zon             # Package dependencies
├── src/
│   ├── zzmq.zig              # Public API (root file)
│   ├── context.zig           # Context management
│   ├── socket.zig            # Socket types and operations
│   ├── message.zig           # Message type
│   ├── frame.zig             # Frame type (internal)
│   ├── pipe.zig              # Pipe system
│   ├── pipe/
│   │   ├── frame_queue.zig   # Growable frame queue
│   │   ├── hwm.zig           # HWM tracking
│   │   └── pipe_set.zig      # Pipe collection
│   ├── patterns/
│   │   ├── pattern.zig       # Pattern interface
│   │   ├── push.zig          # PUSH pattern
│   │   ├── pull.zig          # PULL pattern
│   │   ├── pub.zig           # PUB pattern
│   │   ├── sub.zig           # SUB pattern
│   │   ├── xpub.zig          # XPUB pattern
│   │   ├── xsub.zig          # XSUB pattern
│   │   ├── req.zig           # REQ pattern
│   │   ├── rep.zig           # REP pattern
│   │   ├── dealer.zig        # DEALER pattern
│   │   ├── router.zig        # ROUTER pattern
│   │   └── pair.zig          # PAIR pattern
│   ├── transport/
│   │   ├── transport.zig     # Transport interface
│   │   ├── tcp.zig           # TCP transport
│   │   ├── ipc.zig           # IPC transport (Unix sockets)
│   │   └── inproc.zig        # Inproc transport
│   ├── protocol/
│   │   ├── zmtp.zig          # ZMTP codec
│   │   ├── greeting.zig      # ZMTP greeting
│   │   ├── commands.zig      # ZMTP commands
│   │   └── engine.zig        # Protocol engine
│   ├── subscription/
│   │   ├── trie.zig          # Subscription trie
│   │   └── cache.zig         # Subscription cache (XSUB)
│   ├── util/
│   │   ├── fair_queue.zig    # Fair queue
│   │   ├── load_balancer.zig # Load balancer
│   │   ├── distributor.zig   # Message distributor
│   │   └── routing_id.zig    # Routing ID utilities
│   ├── poll.zig              # Polling API
│   ├── options.zig           # Socket options
│   ├── errors.zig            # Error types
│   └── monitor.zig           # Socket monitoring
├── ffi/
│   ├── zzmq.h                # C header
│   └── ffi.zig               # C FFI implementation
├── tests/
│   ├── test_message.zig      # Message tests
│   ├── test_pipe.zig         # Pipe tests
│   ├── test_patterns.zig     # Pattern tests
│   ├── test_zmtp.zig         # Protocol tests
│   ├── test_transport.zig    # Transport tests
│   ├── test_integration.zig  # Integration tests
│   └── test_interop.zig      # libzmq interop tests
└── examples/
    ├── hello_world.zig       # Basic REQ/REP
    ├── pub_sub.zig           # PUB/SUB example
    ├── push_pull.zig         # PUSH/PULL pipeline
    ├── router_dealer.zig     # Async request/reply
    └── proxy.zig             # Proxy example
```

---

## Build Configuration

### build.zig

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ZIO dependency
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });

    // Main library module
    const zzmq_mod = b.addModule("zzmq", .{
        .root_source_file = b.path("src/zzmq.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zio", .module = zio_dep.module("zio") },
        },
    });

    // Static library for C FFI
    const lib = b.addStaticLibrary(.{
        .name = "zzmq",
        .root_source_file = b.path("ffi/ffi.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib.root_module.addImport("zzmq", zzmq_mod);
    lib.root_module.addImport("zio", zio_dep.module("zio"));
    b.installArtifact(lib);

    // Install C header
    b.installFile("ffi/zzmq.h", "include/zzmq.h");

    // Unit tests
    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/zzmq.zig"),
        .target = target,
        .optimize = optimize,
    });
    unit_tests.root_module.addImport("zio", zio_dep.module("zio"));

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    // Integration tests
    const integration_tests = b.addTest(.{
        .root_source_file = b.path("tests/test_integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    integration_tests.root_module.addImport("zzmq", zzmq_mod);
    integration_tests.root_module.addImport("zio", zio_dep.module("zio"));

    const run_integration_tests = b.addRunArtifact(integration_tests);
    const integration_step = b.step("test-integration", "Run integration tests");
    integration_step.dependOn(&run_integration_tests.step);

    // Examples
    const examples = [_][]const u8{
        "hello_world",
        "pub_sub",
        "push_pull",
        "router_dealer",
        "proxy",
    };

    for (examples) |example| {
        const exe = b.addExecutable(.{
            .name = example,
            .root_source_file = b.path(b.fmt("examples/{s}.zig", .{example})),
            .target = target,
            .optimize = optimize,
        });
        exe.root_module.addImport("zzmq", zzmq_mod);
        exe.root_module.addImport("zio", zio_dep.module("zio"));
        b.installArtifact(exe);
    }
}
```

### build.zig.zon

```zig
.{
    .name = "zzmq",
    .version = "0.1.0",
    .dependencies = .{
        .zio = .{
            .url = "https://github.com/user/zio/archive/refs/tags/v0.1.0.tar.gz",
            .hash = "...",
        },
    },
    .paths = .{
        "build.zig",
        "build.zig.zon",
        "src",
        "ffi",
    },
}
```

---

## Dependencies

| Dependency | Purpose | Required |
|------------|---------|----------|
| **ZIO** | Async I/O runtime | Yes |
| **Zig std** | Standard library | Yes |
| **libzmq** | Interop testing only | Test only |

---

## Implementation Phases

```
Phase 1: Core Infrastructure     [Foundation]
    │
    ▼
Phase 2: Pipe System             [Internal messaging]
    │
    ▼
Phase 3: ZMTP Protocol           [Wire protocol]
    │
    ▼
Phase 4: Socket Patterns         [PUSH/PULL, PUB/SUB, etc.]
    │
    ▼
Phase 5: Transports              [TCP, IPC, Inproc]
    │
    ▼
Phase 6: Advanced Features       [Monitoring, Security, C FFI]
```

Each phase builds on the previous. Complete each phase before moving to the next.

---

## Phase 1: Core Infrastructure

**Goal:** Establish foundational types and basic socket lifecycle.

### 1.1 Message Type (`src/message.zig`)

```zig
//! Message type with inline small-message optimization.
//!
//! Design decisions:
//! - Inline storage for messages ≤ 64 bytes (no allocation)
//! - Owned storage for larger messages
//! - Explicit copy semantics (no hidden reference counting)

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Message = struct {
    pub const INLINE_SIZE = 64;

    storage: Storage,
    flags: Flags = .{},

    const Storage = union(enum) {
        inline_data: InlineData,
        owned: OwnedData,
        empty: void,
    };

    const InlineData = struct {
        data: [INLINE_SIZE]u8,
        len: u8,
    };

    const OwnedData = struct {
        ptr: [*]u8,
        len: usize,
        cap: usize,
        allocator: Allocator,
    };

    pub const Flags = packed struct {
        more: bool = false,
        command: bool = false,
        _padding: u6 = 0,
    };

    /// Create empty message
    pub fn init() Message {
        return .{ .storage = .{ .empty = {} } };
    }

    /// Create message with given size
    pub fn initSize(allocator: Allocator, size: usize) !Message {
        if (size <= INLINE_SIZE) {
            return .{
                .storage = .{ .inline_data = .{
                    .data = undefined,
                    .len = @intCast(size),
                } },
            };
        }

        const ptr = try allocator.alloc(u8, size);
        return .{
            .storage = .{ .owned = .{
                .ptr = ptr.ptr,
                .len = size,
                .cap = size,
                .allocator = allocator,
            } },
        };
    }

    /// Create message from existing data (copies)
    pub fn initCopy(allocator: Allocator, data: []const u8) !Message {
        var msg = try initSize(allocator, data.len);
        @memcpy(msg.data(), data);
        return msg;
    }

    pub fn deinit(self: *Message) void {
        switch (self.storage) {
            .owned => |owned| {
                owned.allocator.free(owned.ptr[0..owned.cap]);
            },
            else => {},
        }
        self.* = init();
    }

    pub fn data(self: *Message) []u8 {
        return switch (self.storage) {
            .inline_data => |*d| d.data[0..d.len],
            .owned => |o| o.ptr[0..o.len],
            .empty => &[_]u8{},
        };
    }

    pub fn constData(self: *const Message) []const u8 {
        return switch (self.storage) {
            .inline_data => |d| d.data[0..d.len],
            .owned => |o| o.ptr[0..o.len],
            .empty => &[_]u8{},
        };
    }

    pub fn size(self: *const Message) usize {
        return switch (self.storage) {
            .inline_data => |d| d.len,
            .owned => |o| o.len,
            .empty => 0,
        };
    }

    pub fn hasMore(self: *const Message) bool {
        return self.flags.more;
    }

    /// Create a copy of this message
    pub fn copy(self: *const Message, allocator: Allocator) !Message {
        var new_msg = try initSize(allocator, self.size());
        @memcpy(new_msg.data(), self.constData());
        new_msg.flags = self.flags;
        return new_msg;
    }
};

test "Message inline storage" {
    const msg = try Message.initCopy(std.testing.allocator, "hello");
    defer msg.deinit();

    try std.testing.expectEqualStrings("hello", msg.constData());
    try std.testing.expectEqual(.inline_data, std.meta.activeTag(msg.storage));
}

test "Message owned storage" {
    const large = "x" ** 100;
    const msg = try Message.initCopy(std.testing.allocator, large);
    defer msg.deinit();

    try std.testing.expectEqual(@as(usize, 100), msg.size());
    try std.testing.expectEqual(.owned, std.meta.activeTag(msg.storage));
}
```

**Tasks:**
- [ ] Implement `Message` struct with inline/owned storage
- [ ] Implement `Flags` for `more`, `command` bits
- [ ] Implement `initSize`, `initCopy`, `deinit`
- [ ] Implement `copy` for explicit copying
- [ ] Add unit tests for inline vs owned threshold
- [ ] Add unit tests for copy semantics

### 1.2 Frame Type (`src/frame.zig`)

```zig
//! Internal frame type for pipe communication.
//! Frames are the unit of pipe transfer; messages may span multiple frames.

pub const Frame = struct {
    data: []u8,
    flags: Flags,
    allocator: Allocator,

    pub const Flags = packed struct {
        more: bool = false,
        command: bool = false,
        _padding: u6 = 0,
    };

    pub fn init(allocator: Allocator, size: usize) !Frame {
        const data = try allocator.alloc(u8, size);
        return .{
            .data = data,
            .flags = .{},
            .allocator = allocator,
        };
    }

    pub fn initCopy(allocator: Allocator, src: []const u8) !Frame {
        var frame = try init(allocator, src.len);
        @memcpy(frame.data, src);
        return frame;
    }

    pub fn deinit(self: *Frame) void {
        self.allocator.free(self.data);
        self.* = undefined;
    }

    pub fn isMore(self: *const Frame) bool {
        return self.flags.more;
    }

    pub fn isCommand(self: *const Frame) bool {
        return self.flags.command;
    }
};
```

**Tasks:**
- [ ] Implement `Frame` struct
- [ ] Add flags for `more`, `command`
- [ ] Add unit tests

### 1.3 Error Types (`src/errors.zig`)

```zig
//! ZZMQ error types with libzmq-compatible error codes.

pub const Error = error{
    // Socket errors
    InvalidSocket,
    SocketClosed,
    InvalidState,

    // Message errors
    MessageTooLarge,
    InvalidMessage,

    // Connection errors
    ConnectionRefused,
    ConnectionReset,
    HostUnreachable,
    NetworkUnreachable,

    // Protocol errors
    ProtocolError,
    InvalidHandshake,
    VersionMismatch,

    // Resource errors
    OutOfMemory,
    TooManyOpenFiles,

    // Operation errors
    WouldBlock,
    TimedOut,
    Interrupted,

    // Configuration errors
    InvalidEndpoint,
    InvalidOption,
    AddressInUse,
    AddressNotAvailable,

    // Pattern-specific
    NoRoutingId,
    MandatoryRouting,
};

/// Convert to errno-style integer for C FFI
pub fn toErrno(err: Error) c_int {
    return switch (err) {
        .WouldBlock => 11,      // EAGAIN
        .InvalidSocket => 88,   // ENOTSOCK
        .ConnectionRefused => 111, // ECONNREFUSED
        .TimedOut => 110,       // ETIMEDOUT
        // ... etc
    };
}
```

**Tasks:**
- [ ] Define error enum covering all error cases
- [ ] Map to errno values for C FFI
- [ ] Add descriptive error messages

### 1.4 Context (`src/context.zig`)

```zig
//! Context manages socket lifecycle and shared resources.

const std = @import("std");
const zio = @import("zio");
const Allocator = std.mem.Allocator;

pub const Context = struct {
    allocator: Allocator,

    /// Active sockets
    sockets: std.ArrayList(*Socket),

    /// Inproc registry (shared across sockets)
    inproc_registry: InprocRegistry,

    /// Shutdown state
    shutting_down: bool = false,

    pub fn init(allocator: Allocator) !*Context {
        const ctx = try allocator.create(Context);
        ctx.* = .{
            .allocator = allocator,
            .sockets = std.ArrayList(*Socket).init(allocator),
            .inproc_registry = InprocRegistry.init(allocator),
        };
        return ctx;
    }

    pub fn deinit(self: *Context) void {
        // Close all sockets
        for (self.sockets.items) |socket| {
            socket.close();
        }
        self.sockets.deinit();
        self.inproc_registry.deinit();
        self.allocator.destroy(self);
    }

    pub fn socket(self: *Context, comptime socket_type: SocketType) !*Socket(socket_type) {
        if (self.shutting_down) return error.ContextTerminated;

        const sock = try Socket(socket_type).init(self);
        try self.sockets.append(sock.base());
        return sock;
    }

    pub fn shutdown(self: *Context) void {
        self.shutting_down = true;
        for (self.sockets.items) |socket| {
            socket.initiateShutdown();
        }
    }
};
```

**Tasks:**
- [ ] Implement `Context` struct
- [ ] Socket creation and tracking
- [ ] Inproc registry ownership
- [ ] Graceful shutdown coordination
- [ ] Add unit tests

### 1.5 Socket Base (`src/socket.zig`)

```zig
//! Type-safe socket implementation with comptime pattern selection.

const std = @import("std");
const zio = @import("zio");

pub const SocketType = enum {
    PUSH,
    PULL,
    PUB,
    SUB,
    XPUB,
    XSUB,
    REQ,
    REP,
    DEALER,
    ROUTER,
    PAIR,
};

/// Type-safe socket with comptime pattern selection
pub fn Socket(comptime socket_type: SocketType) type {
    return struct {
        const Self = @This();
        const Pattern = PatternFor(socket_type);

        // Capabilities (comptime)
        pub const can_send = Pattern.can_send;
        pub const can_recv = Pattern.can_recv;

        context: *Context,
        pattern: Pattern,
        options: Options,
        pipes: PipeSet,
        state: State,

        const State = enum {
            ready,
            bound,
            connected,
            closing,
            closed,
        };

        pub fn init(context: *Context) !*Self {
            const self = try context.allocator.create(Self);
            self.* = .{
                .context = context,
                .pattern = Pattern.init(context.allocator),
                .options = Options.default(),
                .pipes = PipeSet.init(context.allocator),
                .state = .ready,
            };
            return self;
        }

        pub fn close(self: *Self) void {
            self.state = .closing;
            // Linger logic, pipe cleanup, etc.
            self.state = .closed;
        }

        // Comptime-checked send
        pub fn send(self: *Self, msg: *Message, rt: *zio.Runtime) !void {
            comptime if (!can_send) {
                @compileError("Cannot send on " ++ @tagName(socket_type) ++ " socket");
            };

            if (self.state != .connected and self.state != .bound) {
                return error.InvalidState;
            }

            return self.pattern.send(&self.pipes, msg, rt);
        }

        // Comptime-checked recv
        pub fn recv(self: *Self, rt: *zio.Runtime) !Message {
            comptime if (!can_recv) {
                @compileError("Cannot recv on " ++ @tagName(socket_type) ++ " socket");
            };

            return self.pattern.recv(&self.pipes, rt);
        }

        pub fn bind(self: *Self, endpoint: []const u8) !void {
            const ep = try Endpoint.parse(endpoint);
            // Create listener...
            self.state = .bound;
        }

        pub fn connect(self: *Self, endpoint: []const u8) !void {
            const ep = try Endpoint.parse(endpoint);
            // Create connector...
            self.state = .connected;
        }

        // Pattern-specific methods exposed via comptime
        pub usingnamespace if (socket_type == .SUB or socket_type == .XSUB)
            struct {
                pub fn subscribe(self: *Self, prefix: []const u8) !void {
                    return self.pattern.subscribe(prefix);
                }
                pub fn unsubscribe(self: *Self, prefix: []const u8) !void {
                    return self.pattern.unsubscribe(prefix);
                }
            }
        else
            struct {};
    };
}

fn PatternFor(comptime socket_type: SocketType) type {
    return switch (socket_type) {
        .PUSH => @import("patterns/push.zig").Push,
        .PULL => @import("patterns/pull.zig").Pull,
        .PUB => @import("patterns/pub.zig").Pub,
        .SUB => @import("patterns/sub.zig").Sub,
        // ... etc
    };
}
```

**Tasks:**
- [ ] Define `SocketType` enum
- [ ] Implement comptime `Socket(type)` generic
- [ ] Comptime send/recv capability checks
- [ ] State machine (ready/bound/connected/closing/closed)
- [ ] `bind()` and `connect()` methods
- [ ] Pattern-specific method exposure via `usingnamespace`
- [ ] Add unit tests for comptime checks

---

## Phase 2: Pipe System

**Goal:** Implement the internal messaging system between sockets.

### 2.1 Frame Queue (`src/pipe/frame_queue.zig`)

```zig
//! Growable frame queue with chunk-based allocation.
//!
//! Design:
//! - Chunks of 256 frames (like libzmq's message_pipe_granularity)
//! - Grows as needed for unlimited HWM
//! - Signal-on-flush for batching

const std = @import("std");
const zio = @import("zio");
const Frame = @import("../frame.zig").Frame;
const Allocator = std.mem.Allocator;

pub const FrameQueue = struct {
    chunks: std.ArrayList(*Chunk),
    allocator: Allocator,

    /// Read position
    head_chunk: usize = 0,
    head_index: usize = 0,

    /// Write position
    tail_chunk: usize = 0,
    tail_index: usize = 0,

    /// Signaling
    readable: zio.Notify = .{},
    has_unflushed: bool = false,
    closed: bool = false,

    pub const CHUNK_SIZE = 256;

    const Chunk = struct {
        frames: [CHUNK_SIZE]?Frame,

        fn init() Chunk {
            return .{ .frames = [_]?Frame{null} ** CHUNK_SIZE };
        }
    };

    pub fn init(allocator: Allocator) FrameQueue {
        return .{
            .chunks = std.ArrayList(*Chunk).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FrameQueue) void {
        // Free all frames
        while (self.tryDequeue()) |frame| {
            var f = frame;
            f.deinit();
        }
        // Free chunks
        for (self.chunks.items) |chunk| {
            self.allocator.destroy(chunk);
        }
        self.chunks.deinit();
    }

    pub fn enqueue(self: *FrameQueue, frame: Frame) !void {
        // Ensure we have space
        try self.ensureCapacity();

        const chunk = self.chunks.items[self.tail_chunk];
        chunk.frames[self.tail_index] = frame;

        self.tail_index += 1;
        if (self.tail_index >= CHUNK_SIZE) {
            self.tail_index = 0;
            self.tail_chunk += 1;
        }

        self.has_unflushed = true;
    }

    pub fn flush(self: *FrameQueue) void {
        if (self.has_unflushed) {
            self.readable.notify();
            self.has_unflushed = false;
        }
    }

    pub fn dequeue(self: *FrameQueue, rt: *zio.Runtime) !Frame {
        while (self.isEmpty()) {
            if (self.closed) return error.Closed;
            try self.readable.wait(rt);
        }
        return self.tryDequeue().?;
    }

    pub fn tryDequeue(self: *FrameQueue) ?Frame {
        if (self.isEmpty()) return null;

        const chunk = self.chunks.items[self.head_chunk];
        const frame = chunk.frames[self.head_index].?;
        chunk.frames[self.head_index] = null;

        self.head_index += 1;
        if (self.head_index >= CHUNK_SIZE) {
            self.head_index = 0;
            self.head_chunk += 1;
            // Could free old chunks here for memory efficiency
        }

        return frame;
    }

    pub fn isEmpty(self: *const FrameQueue) bool {
        return self.head_chunk == self.tail_chunk and
               self.head_index == self.tail_index;
    }

    pub fn count(self: *const FrameQueue) usize {
        return (self.tail_chunk - self.head_chunk) * CHUNK_SIZE +
               self.tail_index - self.head_index;
    }

    fn ensureCapacity(self: *FrameQueue) !void {
        if (self.tail_chunk >= self.chunks.items.len) {
            const chunk = try self.allocator.create(Chunk);
            chunk.* = Chunk.init();
            try self.chunks.append(chunk);
        }
    }
};
```

**Tasks:**
- [ ] Implement chunk-based storage
- [ ] Implement `enqueue`, `dequeue`, `tryDequeue`
- [ ] Implement `flush` with `zio.Notify`
- [ ] Implement `close` for shutdown
- [ ] Add memory limit option
- [ ] Add unit tests

### 2.2 Pipe (`src/pipe.zig`)

```zig
//! Bidirectional pipe connecting two endpoints.
//!
//! Design:
//! - Two frame queues (outbound, inbound)
//! - HWM tracking via message counters
//! - Multipart message state tracking

const std = @import("std");
const zio = @import("zio");
const FrameQueue = @import("pipe/frame_queue.zig").FrameQueue;
const Frame = @import("frame.zig").Frame;
const RoutingId = @import("util/routing_id.zig").RoutingId;
const Allocator = std.mem.Allocator;

pub const Pipe = struct {
    outbound: FrameQueue,
    inbound: FrameQueue,
    allocator: Allocator,

    // HWM tracking (message-level, not frame-level)
    msgs_written: u64 = 0,
    msgs_read: u64 = 0,
    peers_msgs_read: u64 = 0,

    send_hwm: ?u32 = 1000,
    recv_hwm: ?u32 = 1000,

    // Multipart state
    sending_multipart: bool = false,
    receiving_multipart: bool = false,

    // Identity (for ROUTER)
    routing_id: ?RoutingId = null,

    // State
    state: State = .active,

    const State = enum {
        active,
        delimiter_sent,    // We sent delimiter, waiting for peer
        delimiter_received, // Peer sent delimiter
        closing,
        closed,
    };

    pub fn init(allocator: Allocator, send_hwm: ?u32, recv_hwm: ?u32) !*Pipe {
        const pipe = try allocator.create(Pipe);
        pipe.* = .{
            .outbound = FrameQueue.init(allocator),
            .inbound = FrameQueue.init(allocator),
            .allocator = allocator,
            .send_hwm = send_hwm,
            .recv_hwm = recv_hwm,
        };
        return pipe;
    }

    pub fn deinit(self: *Pipe) void {
        self.outbound.deinit();
        self.inbound.deinit();
        self.allocator.destroy(self);
    }

    /// Check if we can start a new message (HWM check)
    pub fn canSendMessage(self: *const Pipe) bool {
        // Always allow continuing multipart
        if (self.sending_multipart) return true;

        // No HWM = unlimited
        const hwm = self.send_hwm orelse return true;

        // Check message count against HWM
        return (self.msgs_written - self.peers_msgs_read) < hwm;
    }

    /// Write a frame to the pipe
    pub fn writeFrame(self: *Pipe, frame: Frame) !void {
        try self.outbound.enqueue(frame);

        if (!frame.isMore()) {
            // Complete message
            self.msgs_written += 1;
            self.sending_multipart = false;
        } else {
            self.sending_multipart = true;
        }
    }

    /// Flush written frames (signal reader)
    pub fn flush(self: *Pipe) void {
        self.outbound.flush();
    }

    /// Read a frame from the pipe
    pub fn readFrame(self: *Pipe, rt: *zio.Runtime) !Frame {
        const frame = try self.inbound.dequeue(rt);

        if (!frame.isMore()) {
            self.msgs_read += 1;
            self.receiving_multipart = false;
            // TODO: Send credit back to peer
        } else {
            self.receiving_multipart = true;
        }

        return frame;
    }

    /// Try to read without blocking
    pub fn tryReadFrame(self: *Pipe) ?Frame {
        const frame = self.inbound.tryDequeue() orelse return null;

        if (!frame.isMore()) {
            self.msgs_read += 1;
            self.receiving_multipart = false;
        } else {
            self.receiving_multipart = true;
        }

        return frame;
    }

    /// Check if readable (for select/poll)
    pub fn hasIn(self: *const Pipe) bool {
        return !self.inbound.isEmpty();
    }

    /// Check if writable (HWM not exceeded)
    pub fn hasOut(self: *const Pipe) bool {
        return self.canSendMessage();
    }

    /// Rollback incomplete multipart message
    pub fn rollback(self: *Pipe) void {
        if (!self.sending_multipart) return;

        // Remove frames until we hit a non-MORE frame or empty
        while (true) {
            // Need to implement unwrite for this
            // For now, this is a placeholder
            break;
        }
        self.sending_multipart = false;
    }

    /// Initiate graceful shutdown
    pub fn terminate(self: *Pipe) void {
        self.state = .closing;
        self.outbound.close();
        self.inbound.close();
    }
};
```

**Tasks:**
- [ ] Implement `Pipe` struct with two `FrameQueue`s
- [ ] Implement HWM tracking (message counters)
- [ ] Implement `canSendMessage` check
- [ ] Implement multipart state tracking
- [ ] Implement `rollback` for incomplete multipart
- [ ] Implement graceful shutdown (`terminate`)
- [ ] Add routing ID for ROUTER pattern
- [ ] Add unit tests

### 2.3 Pipe Set (`src/pipe/pipe_set.zig`)

```zig
//! Collection of pipes with routing ID lookup.

pub const PipeSet = struct {
    pipes: std.ArrayList(*Pipe),
    by_routing_id: std.HashMap(RoutingIdKey, *Pipe, RoutingIdContext, 80),
    allocator: Allocator,

    pub fn init(allocator: Allocator) PipeSet {
        return .{
            .pipes = std.ArrayList(*Pipe).init(allocator),
            .by_routing_id = std.HashMap(RoutingIdKey, *Pipe, RoutingIdContext, 80).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn add(self: *PipeSet, pipe: *Pipe) !void {
        try self.pipes.append(pipe);
        if (pipe.routing_id) |id| {
            try self.by_routing_id.put(id.toKey(), pipe);
        }
    }

    pub fn remove(self: *PipeSet, pipe: *Pipe) void {
        // Remove from array
        for (self.pipes.items, 0..) |p, i| {
            if (p == pipe) {
                _ = self.pipes.swapRemove(i);
                break;
            }
        }
        // Remove from routing ID map
        if (pipe.routing_id) |id| {
            _ = self.by_routing_id.remove(id.toKey());
        }
    }

    pub fn getByRoutingId(self: *PipeSet, id: []const u8) ?*Pipe {
        const key = RoutingId.fromSlice(id) catch return null;
        return self.by_routing_id.get(key.toKey());
    }

    pub fn count(self: *const PipeSet) usize {
        return self.pipes.items.len;
    }
};
```

**Tasks:**
- [ ] Implement `PipeSet`
- [ ] Add routing ID lookup
- [ ] Add iterator support
- [ ] Add unit tests

---

## Phase 3: ZMTP Protocol

**Goal:** Implement ZMTP 3.1 wire protocol for network communication.

### 3.1 Greeting (`src/protocol/greeting.zig`)

```zig
//! ZMTP greeting exchange.
//!
//! Greeting format (64 bytes):
//!   [0xFF][8-byte size][0x7F]  - Signature (10 bytes)
//!   [major][minor]             - Version (2 bytes)
//!   [mechanism...]             - Security mechanism (20 bytes)
//!   [as-server]                - Role (1 byte)
//!   [filler...]                - Padding (31 bytes)

pub const Greeting = struct {
    pub const SIZE = 64;
    pub const SIGNATURE = [10]u8{ 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F };

    major: u8 = 3,
    minor: u8 = 1,
    mechanism: [20]u8 = padMechanism("NULL"),
    as_server: bool = false,

    pub fn encode(self: *const Greeting) [SIZE]u8 {
        var buf: [SIZE]u8 = undefined;

        // Signature
        @memcpy(buf[0..10], &SIGNATURE);
        // Version
        buf[10] = self.major;
        buf[11] = self.minor;
        // Mechanism
        @memcpy(buf[12..32], &self.mechanism);
        // As-server
        buf[32] = if (self.as_server) 1 else 0;
        // Filler
        @memset(buf[33..64], 0);

        return buf;
    }

    pub fn decode(buf: *const [SIZE]u8) !Greeting {
        // Validate signature
        if (!std.mem.eql(u8, buf[0..10], &SIGNATURE)) {
            return error.InvalidSignature;
        }

        return .{
            .major = buf[10],
            .minor = buf[11],
            .mechanism = buf[12..32].*,
            .as_server = buf[32] != 0,
        };
    }

    fn padMechanism(name: []const u8) [20]u8 {
        var buf: [20]u8 = [_]u8{0} ** 20;
        @memcpy(buf[0..name.len], name);
        return buf;
    }
};
```

**Tasks:**
- [ ] Implement greeting encode/decode
- [ ] Validate signature
- [ ] Handle version negotiation
- [ ] Add unit tests

### 3.2 Commands (`src/protocol/commands.zig`)

```zig
//! ZMTP commands (READY, SUBSCRIBE, CANCEL, PING, PONG).

pub const Command = union(enum) {
    ready: ReadyCommand,
    subscribe: SubscribeCommand,
    cancel: CancelCommand,
    ping: PingCommand,
    pong: PongCommand,
    error_cmd: ErrorCommand,

    pub const ReadyCommand = struct {
        metadata: std.StringHashMap([]const u8),

        pub fn encode(self: *const ReadyCommand, allocator: Allocator) ![]u8 {
            // Command name + metadata
            var buf = std.ArrayList(u8).init(allocator);
            try buf.appendSlice("\x05READY");

            var iter = self.metadata.iterator();
            while (iter.next()) |entry| {
                // Key length (1 byte) + key + value length (4 bytes) + value
                try buf.append(@intCast(entry.key_ptr.len));
                try buf.appendSlice(entry.key_ptr.*);
                try buf.writer().writeInt(u32, @intCast(entry.value_ptr.len), .big);
                try buf.appendSlice(entry.value_ptr.*);
            }

            return buf.toOwnedSlice();
        }
    };

    pub const SubscribeCommand = struct {
        prefix: []const u8,

        pub fn encode(self: *const SubscribeCommand, allocator: Allocator) ![]u8 {
            var buf = try allocator.alloc(u8, 10 + self.prefix.len);
            @memcpy(buf[0..10], "\x09SUBSCRIBE");
            @memcpy(buf[10..], self.prefix);
            return buf;
        }
    };

    pub const CancelCommand = struct {
        prefix: []const u8,
    };

    pub const PingCommand = struct {
        ttl: u16,
        context: []const u8,
    };

    pub const PongCommand = struct {
        context: []const u8,
    };

    pub const ErrorCommand = struct {
        reason: []const u8,
    };
};
```

**Tasks:**
- [ ] Implement READY command with metadata
- [ ] Implement SUBSCRIBE/CANCEL commands
- [ ] Implement PING/PONG for heartbeat
- [ ] Implement ERROR command
- [ ] Add encode/decode for all commands
- [ ] Add unit tests

### 3.3 Codec (`src/protocol/zmtp.zig`)

```zig
//! ZMTP frame encoder/decoder.

pub const Codec = struct {
    state: State = .greeting,
    allocator: Allocator,

    // Decoding buffer
    decode_buf: std.ArrayList(u8),

    // Limits
    max_frame_size: usize = 1024 * 1024 * 1024,

    const State = enum {
        greeting,
        handshake,
        traffic,
    };

    pub fn init(allocator: Allocator) Codec {
        return .{
            .allocator = allocator,
            .decode_buf = std.ArrayList(u8).init(allocator),
        };
    }

    /// Encode a message frame
    pub fn encodeFrame(self: *Codec, frame: Frame) ![]u8 {
        const size = frame.data.len;
        const flags: u8 = (if (frame.flags.more) @as(u8, 0x01) else 0) |
                         (if (frame.flags.command) @as(u8, 0x04) else 0);

        if (size <= 255) {
            // Short frame: [flags][1-byte size][data]
            var buf = try self.allocator.alloc(u8, 2 + size);
            buf[0] = flags;
            buf[1] = @intCast(size);
            @memcpy(buf[2..], frame.data);
            return buf;
        } else {
            // Long frame: [flags|0x02][8-byte size][data]
            var buf = try self.allocator.alloc(u8, 9 + size);
            buf[0] = flags | 0x02;
            std.mem.writeInt(u64, buf[1..9], size, .big);
            @memcpy(buf[9..], frame.data);
            return buf;
        }
    }

    /// Decode frame from stream
    pub fn decodeFrame(self: *Codec, reader: anytype) !Frame {
        const flags = try reader.readByte();

        const is_long = (flags & 0x02) != 0;
        const size: usize = if (is_long)
            try reader.readInt(u64, .big)
        else
            try reader.readByte();

        if (size > self.max_frame_size) {
            return error.FrameTooLarge;
        }

        var frame = try Frame.init(self.allocator, size);
        try reader.readNoEof(frame.data);

        frame.flags.more = (flags & 0x01) != 0;
        frame.flags.command = (flags & 0x04) != 0;

        return frame;
    }
};
```

**Tasks:**
- [ ] Implement frame encoding (short/long format)
- [ ] Implement frame decoding
- [ ] Add max frame size protection
- [ ] Implement greeting exchange
- [ ] Implement handshake (READY command exchange)
- [ ] Add unit tests

### 3.4 Engine (`src/protocol/engine.zig`)

```zig
//! Protocol engine: manages ZMTP connection lifecycle.
//!
//! Spawns reader and writer coroutines for each connection.

pub const Engine = struct {
    pipe: *Pipe,
    codec: Codec,
    stream: zio.net.Stream,
    group: zio.Group,
    state: State = .connecting,
    allocator: Allocator,

    const State = enum {
        connecting,
        greeting,
        handshake,
        traffic,
        closing,
        closed,
    };

    pub fn init(allocator: Allocator, stream: zio.net.Stream, pipe: *Pipe) !*Engine {
        const engine = try allocator.create(Engine);
        engine.* = .{
            .pipe = pipe,
            .codec = Codec.init(allocator),
            .stream = stream,
            .group = zio.Group.init(),
            .allocator = allocator,
        };
        return engine;
    }

    pub fn start(self: *Engine, rt: *zio.Runtime) void {
        // Spawn reader and writer coroutines
        self.group.spawnDetached(rt, readerLoop, .{ self, rt });
        self.group.spawnDetached(rt, writerLoop, .{ self, rt });
    }

    fn readerLoop(self: *Engine, rt: *zio.Runtime) void {
        defer self.onReaderDone();

        // Greeting exchange
        self.doGreeting(rt) catch return;

        // Handshake
        self.doHandshake(rt) catch return;

        self.state = .traffic;

        // Main traffic loop
        while (self.state == .traffic) {
            const frame = self.codec.decodeFrame(self.stream.reader()) catch |err| {
                self.handleError(err);
                return;
            };

            self.pipe.inbound.enqueue(frame) catch return;

            if (!frame.isMore()) {
                self.pipe.inbound.flush();
            }
        }
    }

    fn writerLoop(self: *Engine, rt: *zio.Runtime) void {
        defer self.onWriterDone();

        // Wait for handshake to complete
        while (self.state != .traffic and self.state != .closing) {
            rt.yield();
        }

        // Main traffic loop
        while (self.state == .traffic) {
            const frame = self.pipe.outbound.dequeue(rt) catch return;

            const encoded = self.codec.encodeFrame(frame) catch return;
            defer self.allocator.free(encoded);

            self.stream.writeAll(encoded) catch |err| {
                self.handleError(err);
                return;
            };
        }
    }

    fn doGreeting(self: *Engine, rt: *zio.Runtime) !void {
        // Send our greeting
        const our_greeting = Greeting{};
        try self.stream.writeAll(&our_greeting.encode());

        // Receive peer greeting
        var buf: [Greeting.SIZE]u8 = undefined;
        try self.stream.readAll(&buf);

        const peer_greeting = try Greeting.decode(&buf);

        // Version check
        if (peer_greeting.major != 3) {
            return error.VersionMismatch;
        }
    }

    fn doHandshake(self: *Engine, rt: *zio.Runtime) !void {
        // Send READY
        var metadata = std.StringHashMap([]const u8).init(self.allocator);
        try metadata.put("Socket-Type", @tagName(self.pipe.socket_type));

        const ready = Command{ .ready = .{ .metadata = metadata } };
        const encoded = try ready.ready.encode(self.allocator);
        defer self.allocator.free(encoded);

        // Encode as command frame
        var frame = try Frame.initCopy(self.allocator, encoded);
        frame.flags.command = true;

        const wire = try self.codec.encodeFrame(frame);
        defer self.allocator.free(wire);

        try self.stream.writeAll(wire);

        // Receive peer READY
        const peer_frame = try self.codec.decodeFrame(self.stream.reader());
        defer peer_frame.deinit();

        if (!peer_frame.flags.command) {
            return error.ProtocolError;
        }

        // Parse READY command
        // ...
    }

    pub fn stop(self: *Engine, rt: *zio.Runtime) void {
        self.state = .closing;
        self.group.cancel(rt);
        self.group.wait(rt);
        self.state = .closed;
    }
};
```

**Tasks:**
- [ ] Implement engine state machine
- [ ] Implement reader/writer coroutines
- [ ] Implement greeting exchange
- [ ] Implement handshake (READY)
- [ ] Handle connection errors
- [ ] Implement graceful shutdown
- [ ] Add heartbeat support (PING/PONG)
- [ ] Add unit tests

---

## Phase 4: Socket Patterns

**Goal:** Implement all socket patterns with correct semantics.

### 4.1 Pattern Interface (`src/patterns/pattern.zig`)

```zig
//! Pattern interface that all socket patterns implement.

pub const PatternVTable = struct {
    send: ?*const fn (*anyopaque, *PipeSet, *Message, *zio.Runtime) SendError!void,
    recv: ?*const fn (*anyopaque, *PipeSet, *zio.Runtime) RecvError!Message,
    has_in: *const fn (*anyopaque, *PipeSet) bool,
    has_out: *const fn (*anyopaque, *PipeSet) bool,
    attach_pipe: *const fn (*anyopaque, *Pipe) void,
    detach_pipe: *const fn (*anyopaque, *Pipe) void,
};

pub const SendError = error{
    WouldBlock,
    InvalidState,
    HostUnreachable,
    OutOfMemory,
};

pub const RecvError = error{
    WouldBlock,
    InvalidState,
    OutOfMemory,
    Closed,
};
```

### 4.2 PUSH/PULL (`src/patterns/push.zig`, `src/patterns/pull.zig`)

```zig
// push.zig
pub const Push = struct {
    lb: LoadBalancer,

    pub const can_send = true;
    pub const can_recv = false;

    pub fn init(allocator: Allocator) Push {
        return .{ .lb = LoadBalancer.init(allocator) };
    }

    pub fn send(self: *Push, pipes: *PipeSet, msg: *Message, rt: *zio.Runtime) !void {
        const pipe = try self.lb.selectForSend(pipes, rt);
        try self.writeMessage(pipe, msg);
        pipe.flush();
    }

    pub fn attachPipe(self: *Push, pipe: *Pipe) void {
        self.lb.attach(pipe);
    }

    pub fn detachPipe(self: *Push, pipe: *Pipe) void {
        self.lb.detach(pipe);
    }

    pub fn hasOut(self: *Push, pipes: *PipeSet) bool {
        return self.lb.hasWritable(pipes);
    }
};

// pull.zig
pub const Pull = struct {
    fq: FairQueue,

    pub const can_send = false;
    pub const can_recv = true;

    pub fn init(allocator: Allocator) Pull {
        return .{ .fq = FairQueue.init(allocator) };
    }

    pub fn recv(self: *Pull, pipes: *PipeSet, rt: *zio.Runtime) !Message {
        return self.fq.recv(pipes, rt);
    }

    pub fn attachPipe(self: *Pull, pipe: *Pipe) void {
        self.fq.attach(pipe);
    }

    pub fn hasIn(self: *Pull, pipes: *PipeSet) bool {
        return self.fq.hasReadable(pipes);
    }
};
```

**Tasks:**
- [ ] Implement PUSH with load balancer
- [ ] Implement PULL with fair queue
- [ ] Handle multipart atomicity
- [ ] Add unit tests

### 4.3 PUB/SUB (`src/patterns/pub.zig`, `src/patterns/sub.zig`)

```zig
// pub.zig
pub const Pub = struct {
    subscriptions: SubscriptionTrie,
    match_buffer: PipeSet,

    pub const can_send = true;
    pub const can_recv = false;

    pub fn send(self: *Pub, pipes: *PipeSet, msg: *Message, rt: *zio.Runtime) !void {
        // Match first frame against subscriptions
        self.match_buffer.clear();
        self.subscriptions.matchUnique(msg.constData(), &self.match_buffer);

        // Send to all matching pipes
        for (self.match_buffer.pipes.items) |pipe| {
            var copy = try msg.copy(self.allocator);
            try self.writeMessage(pipe, &copy);
            pipe.flush();
        }
    }

    pub fn attachPipe(self: *Pub, pipe: *Pipe) void {
        // New pipe, no subscriptions yet
        // Process incoming subscription messages
    }

    pub fn handleSubscription(self: *Pub, pipe: *Pipe, subscribe: bool, prefix: []const u8) !void {
        if (subscribe) {
            _ = try self.subscriptions.subscribe(prefix, pipe);
        } else {
            _ = self.subscriptions.unsubscribe(prefix, pipe);
        }
    }
};

// sub.zig
pub const Sub = struct {
    inner: XSub,

    pub const can_send = false;
    pub const can_recv = true;

    pub fn init(allocator: Allocator) Sub {
        var sub = Sub{ .inner = XSub.init(allocator) };
        sub.inner.filter = true;
        return sub;
    }

    pub fn subscribe(self: *Sub, prefix: []const u8) !void {
        try self.inner.sendSubscription(true, prefix);
    }

    pub fn unsubscribe(self: *Sub, prefix: []const u8) !void {
        try self.inner.sendSubscription(false, prefix);
    }

    pub fn recv(self: *Sub, pipes: *PipeSet, rt: *zio.Runtime) !Message {
        return self.inner.recv(pipes, rt);
    }
};
```

**Tasks:**
- [ ] Implement PUB with subscription trie
- [ ] Implement SUB as XSUB wrapper
- [ ] Subscription message handling
- [ ] Message filtering
- [ ] Add unit tests

### 4.4 XPUB/XSUB (`src/patterns/xpub.zig`, `src/patterns/xsub.zig`)

**Tasks:**
- [ ] Implement XPUB with all options
- [ ] Implement XSUB with subscription cache
- [ ] Manual mode support
- [ ] Subscription notifications
- [ ] Add unit tests

### 4.5 REQ/REP (`src/patterns/req.zig`, `src/patterns/rep.zig`)

```zig
// req.zig
pub const Req = struct {
    state: State = .ready,
    lb: LoadBalancer,
    current_pipe: ?*Pipe = null,

    const State = enum { ready, waiting_reply };

    pub const can_send = true;
    pub const can_recv = true;

    pub fn send(self: *Req, pipes: *PipeSet, msg: *Message, rt: *zio.Runtime) !void {
        if (self.state != .ready) return error.InvalidState;

        const pipe = try self.lb.selectForSend(pipes, rt);

        // Add empty delimiter frame
        var delimiter = try Frame.init(self.allocator, 0);
        delimiter.flags.more = true;
        try pipe.writeFrame(delimiter);

        // Send message
        try self.writeMessage(pipe, msg);
        pipe.flush();

        self.current_pipe = pipe;
        self.state = .waiting_reply;
    }

    pub fn recv(self: *Req, pipes: *PipeSet, rt: *zio.Runtime) !Message {
        if (self.state != .waiting_reply) return error.InvalidState;

        // Read from same pipe we sent to
        const pipe = self.current_pipe.?;

        // Skip delimiter
        var delimiter = try pipe.readFrame(rt);
        delimiter.deinit();

        // Read message
        const msg = try self.readMessage(pipe, rt);

        self.state = .ready;
        self.current_pipe = null;

        return msg;
    }
};

// rep.zig
pub const Rep = struct {
    state: State = .ready,
    fq: FairQueue,
    current_pipe: ?*Pipe = null,
    routing_frames: std.ArrayList(Frame),

    const State = enum { ready, waiting_send };

    pub fn recv(self: *Rep, pipes: *PipeSet, rt: *zio.Runtime) !Message {
        if (self.state != .ready) return error.InvalidState;

        const pipe = try self.fq.selectReadable(pipes, rt);

        // Collect routing frames until delimiter
        self.routing_frames.clearRetainingCapacity();
        while (true) {
            var frame = try pipe.readFrame(rt);
            if (frame.data.len == 0 and !frame.flags.more) {
                frame.deinit();
                break;  // Delimiter
            }
            try self.routing_frames.append(frame);
        }

        // Read actual message
        const msg = try self.readMessage(pipe, rt);

        self.current_pipe = pipe;
        self.state = .waiting_send;

        return msg;
    }

    pub fn send(self: *Rep, pipes: *PipeSet, msg: *Message, rt: *zio.Runtime) !void {
        if (self.state != .waiting_send) return error.InvalidState;

        const pipe = self.current_pipe.?;

        // Send back routing frames
        for (self.routing_frames.items) |frame| {
            try pipe.writeFrame(frame);
        }

        // Send delimiter
        var delimiter = try Frame.init(self.allocator, 0);
        try pipe.writeFrame(delimiter);

        // Send message
        try self.writeMessage(pipe, msg);
        pipe.flush();

        self.state = .ready;
        self.current_pipe = null;
        self.routing_frames.clearRetainingCapacity();
    }
};
```

**Tasks:**
- [ ] Implement REQ state machine (send → recv → send)
- [ ] Implement REP state machine (recv → send → recv)
- [ ] Empty delimiter handling
- [ ] Add unit tests

### 4.6 DEALER/ROUTER (`src/patterns/dealer.zig`, `src/patterns/router.zig`)

**Tasks:**
- [ ] Implement DEALER (async REQ)
- [ ] Implement ROUTER with routing ID management
- [ ] Identity lifecycle (explicit, ZMTP, auto-generated)
- [ ] `ROUTER_MANDATORY` and `ROUTER_HANDOVER` options
- [ ] Add unit tests

### 4.7 PAIR (`src/patterns/pair.zig`)

**Tasks:**
- [ ] Implement PAIR (single pipe, bidirectional)
- [ ] Enforce single peer constraint
- [ ] Add unit tests

---

## Phase 5: Transports

**Goal:** Implement TCP, IPC, and Inproc transports.

### 5.1 Transport Interface (`src/transport/transport.zig`)

```zig
pub const Transport = union(enum) {
    tcp: TcpTransport,
    ipc: IpcTransport,
    inproc: InprocTransport,
};

pub const Listener = struct {
    transport: Transport,
    endpoint: Endpoint,
    // ...

    pub fn accept(self: *Listener, rt: *zio.Runtime) !Connection {
        return switch (self.transport) {
            .tcp => |*t| t.accept(rt),
            .ipc => |*t| t.accept(rt),
            .inproc => |*t| t.accept(rt),
        };
    }
};

pub const Connector = struct {
    transport: Transport,
    endpoint: Endpoint,
    reconnect: ReconnectConfig,
    // ...
};
```

### 5.2 TCP Transport (`src/transport/tcp.zig`)

**Tasks:**
- [ ] Implement TCP listener (bind)
- [ ] Implement TCP connector (connect)
- [ ] Reconnection with exponential backoff
- [ ] TCP keepalive options
- [ ] IPv4 and IPv6 support
- [ ] Add integration tests

### 5.3 IPC Transport (`src/transport/ipc.zig`)

**Tasks:**
- [ ] Implement Unix domain socket transport
- [ ] File permission handling
- [ ] Cleanup on close
- [ ] Windows named pipe support (optional)
- [ ] Add integration tests

### 5.4 Inproc Transport (`src/transport/inproc.zig`)

```zig
pub const InprocRegistry = struct {
    bound: std.StringHashMap(BoundEndpoint),
    pending: std.StringHashMap(std.ArrayList(PendingConnect)),
    mutex: std.Thread.Mutex,

    pub fn bind(self: *InprocRegistry, name: []const u8, socket: *anyopaque) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.bound.contains(name)) return error.AddressInUse;

        try self.bound.put(name, .{ .socket = socket });

        // Connect any pending
        if (self.pending.get(name)) |pending| {
            for (pending.items) |p| {
                self.connectPair(socket, p.socket, p.pipe);
            }
            pending.clearAndFree();
        }
    }

    pub fn connect(self: *InprocRegistry, name: []const u8, socket: *anyopaque) !*Pipe {
        self.mutex.lock();
        defer self.mutex.unlock();

        const pipe = try Pipe.init(self.allocator, null, null);

        if (self.bound.get(name)) |bound| {
            self.connectPair(bound.socket, socket, pipe);
        } else {
            // Queue for later
            var pending = self.pending.get(name) orelse blk: {
                const list = std.ArrayList(PendingConnect).init(self.allocator);
                try self.pending.put(name, list);
                break :blk self.pending.get(name).?;
            };
            try pending.append(.{ .socket = socket, .pipe = pipe });
        }

        return pipe;
    }
};
```

**Tasks:**
- [ ] Implement inproc registry
- [ ] Direct pipe connection (no network)
- [ ] Pending connect queue (connect before bind)
- [ ] Thread-safe access
- [ ] Add unit tests

---

## Phase 6: Advanced Features

### 6.1 Polling API (`src/poll.zig`)

**Tasks:**
- [ ] Implement `zmq_poll()` equivalent
- [ ] Implement `Poller` class
- [ ] `getFd()` for external event loop integration
- [ ] `getEvents()` for readiness checking
- [ ] Add integration tests

### 6.2 Socket Monitoring (`src/monitor.zig`)

**Tasks:**
- [ ] Implement monitor socket (inproc pair)
- [ ] Event types (connected, disconnected, etc.)
- [ ] Event encoding
- [ ] Add unit tests

### 6.3 C FFI (`ffi/ffi.zig`, `ffi/zzmq.h`)

**Tasks:**
- [ ] C header with libzmq-compatible API
- [ ] Zig FFI implementation
- [ ] Error code mapping
- [ ] Context and socket handle management
- [ ] Build as static/dynamic library
- [ ] Add interop tests

### 6.4 Security (Future)

**Tasks:**
- [ ] NULL mechanism (no security)
- [ ] PLAIN mechanism (username/password)
- [ ] CURVE mechanism (optional, requires crypto)
- [ ] ZAP authentication protocol

---

## Testing Strategy

### Unit Tests

Each module has embedded unit tests:

```zig
test "Message inline vs owned" {
    // Test inline storage for small messages
    // Test owned storage for large messages
}
```

Run with: `zig build test`

### Integration Tests

End-to-end tests in `tests/`:

```zig
test "PUSH/PULL pipeline" {
    // Create context
    // Create PUSH and PULL sockets
    // Connect via inproc
    // Send messages
    // Verify receipt
}
```

Run with: `zig build test-integration`

### Interop Tests

Test against libzmq:

```zig
test "interop with libzmq REQ/REP" {
    // Start libzmq REP server (external process)
    // Connect ZZMQ REQ client
    // Exchange messages
    // Verify compatibility
}
```

### Performance Tests

```zig
test "throughput benchmark" {
    // Measure messages per second
    // Compare with libzmq baseline
}
```

---

## Development Workflow

### 1. Implement Feature

```bash
# Create/edit source file
vim src/patterns/push.zig

# Run tests
zig build test

# Check for compile errors
zig build
```

### 2. Test Integration

```bash
# Run integration tests
zig build test-integration

# Run specific example
zig build && ./zig-out/bin/hello_world
```

### 3. Test Interop

```bash
# Start libzmq server
./tests/libzmq_server &

# Run ZZMQ client
zig build && ./zig-out/bin/test_interop
```

### 4. Commit

Follow conventional commits:
- `feat: add PUSH pattern`
- `fix: correct HWM message counting`
- `test: add PUSH/PULL integration test`
- `docs: update implementation guide`

---

## Milestones

### M1: Core Foundation ✓ when complete
- [ ] Message type with inline optimization
- [ ] Frame type
- [ ] Error types
- [ ] Context and basic socket lifecycle
- [ ] Build system

### M2: Internal Messaging
- [ ] Frame queue
- [ ] Pipe with HWM
- [ ] Pipe set

### M3: Wire Protocol
- [ ] ZMTP greeting
- [ ] ZMTP commands
- [ ] Codec (encode/decode)
- [ ] Engine (reader/writer loops)

### M4: Basic Patterns
- [ ] PUSH/PULL
- [ ] Inproc transport
- [ ] Integration test: push/pull over inproc

### M5: Network
- [ ] TCP transport
- [ ] Reconnection logic
- [ ] Integration test: push/pull over TCP

### M6: PUB/SUB
- [ ] Subscription trie
- [ ] PUB pattern
- [ ] SUB pattern
- [ ] XPUB/XSUB patterns

### M7: Request/Reply
- [ ] REQ/REP patterns
- [ ] DEALER/ROUTER patterns
- [ ] Routing ID management

### M8: Production Ready
- [ ] Polling API
- [ ] Socket monitoring
- [ ] IPC transport
- [ ] C FFI
- [ ] Documentation
- [ ] Performance benchmarks
- [ ] libzmq interop tests

---

## Appendix: Quick Reference

### Key Files to Start

1. `src/message.zig` - Start here
2. `src/frame.zig` - Internal frame type
3. `src/pipe/frame_queue.zig` - Queue implementation
4. `src/pipe.zig` - Pipe with HWM
5. `src/protocol/zmtp.zig` - Wire protocol

### Common Patterns

**Reading from pipe with timeout:**
```zig
const result = zio.select(.{
    pipe.inbound.readable.pollable(),
    zio.timeout(Duration.fromMilliseconds(1000)),
}, rt);

if (result[0]) {
    const frame = pipe.tryReadFrame().?;
}
```

**Writing with HWM check:**
```zig
if (!pipe.canSendMessage()) {
    return error.WouldBlock;
}
try pipe.writeFrame(frame);
pipe.flush();
```

### ZIO Primitives Used

| ZIO Type | ZZMQ Usage |
|----------|------------|
| `zio.Notify` | Signal readable/writable |
| `zio.select` | Wait on multiple sources |
| `zio.Group` | Structured concurrency |
| `zio.net.Stream` | TCP/IPC connections |
| `zio.net.Server` | TCP/IPC listeners |

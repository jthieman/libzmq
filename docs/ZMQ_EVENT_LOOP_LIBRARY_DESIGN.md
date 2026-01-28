# ZMQ Event-Loop Library Design

A ground-up design for a ZeroMQ-semantics messaging library built on event-driven I/O.

## Table of Contents

1. [Design Philosophy](#design-philosophy)
2. [Core Concepts](#core-concepts)
3. [Architecture Overview](#architecture-overview)
4. [The Event Loop Integration](#the-event-loop-integration)
5. [Socket Architecture](#socket-architecture)
6. [Pipe System](#pipe-system)
7. [Flow Control (HWM/LWM)](#flow-control-hwmlwm)
8. [Message System](#message-system)
9. [Connection Management](#connection-management)
10. [Transport Layer](#transport-layer)
11. [Socket Types Implementation](#socket-types-implementation)
12. [Thread Safety Model](#thread-safety-model)
13. [API Design](#api-design)
14. [Error Handling](#error-handling)

---

## Design Philosophy

### Principles

1. **Event-loop native**: All I/O is non-blocking, driven by callbacks/async
2. **Zero hidden threads**: User controls all threading via event loop placement
3. **Explicit over implicit**: Resource ownership and lifetimes are clear
4. **Familiar semantics**: libzmq users should feel at home
5. **Composable**: Works with any event loop abstraction (libuv-style)
6. **Memory efficient**: Minimize allocations, support zero-copy paths

### Non-Goals

- Drop-in API compatibility with libzmq
- Blocking send/recv as primary interface (available as convenience wrapper)
- Hidden background threads or work stealing

---

## Core Concepts

### Glossary (libzmq Compatibility)

| Concept | Description | libzmq Equivalent |
|---------|-------------|-------------------|
| Context | Container for sockets, owns shared resources | `zmq_ctx_t` |
| Socket | Messaging endpoint with pattern semantics | `zmq_socket_t` |
| Pipe | Internal queue connecting socket to peer | `pipe_t` |
| Message | Data unit, possibly multipart | `zmq_msg_t` |
| Endpoint | Address to bind/connect | URI string |
| HWM | High water mark - max queue depth | `ZMQ_SNDHWM`/`ZMQ_RCVHWM` |
| Linger | Time to flush on close | `ZMQ_LINGER` |

### What's Different

| libzmq | This Design |
|--------|-------------|
| I/O threads in context | User provides event loop |
| Blocking send/recv | Callback-based with try_send/try_recv |
| Implicit reconnection threads | Reconnection via event loop timers |
| Mailbox signaling | Direct event loop wakeup |
| Thread per socket model | Sockets belong to event loop |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           User Application                               │
│                                                                          │
│   socket.send(msg, |result| { ... });                                   │
│   socket.on_readable(|| { let msg = socket.recv(); });                  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Socket Layer                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Socket<T: SocketType>                        │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │   │
│  │  │    State     │  │    Pipes     │  │   Options    │          │   │
│  │  │  (pattern-   │  │  (to peers)  │  │  (hwm,etc)   │          │   │
│  │  │   specific)  │  │              │  │              │          │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Pipe Layer                                  │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │                          Pipe                                      │ │
│  │  ┌─────────────┐              ┌─────────────┐                     │ │
│  │  │  Outbound   │──────────────│  Inbound    │                     │ │
│  │  │   Queue     │   (to peer)  │   Queue     │   (from peer)       │ │
│  │  │  (write)    │              │  (read)     │                     │ │
│  │  └─────────────┘              └─────────────┘                     │ │
│  │       │                             │                              │ │
│  │       │ HWM check                   │ LWM signal                   │ │
│  │       ▼                             ▼                              │ │
│  │  [flow control state]         [readiness callbacks]               │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Connection Layer                               │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │                      Connection                                    │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐               │ │
│  │  │   Engine    │  │   Codec     │  │  Transport  │               │ │
│  │  │  (framing)  │  │  (ZMTP)     │  │  (TCP/IPC)  │               │ │
│  │  └─────────────┘  └─────────────┘  └─────────────┘               │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │                      Listener                                      │ │
│  │           (accepts connections, creates Connection)               │ │
│  └───────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌───────────────────────────────────────────────────────────────────┐ │
│  │                      Connector                                     │ │
│  │           (initiates connections, handles reconnect)              │ │
│  └───────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Event Loop (libuv-style)                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │   Poll     │  │   Timers   │  │   Idle     │  │   Async    │       │
│  │  Handles   │  │            │  │  Handles   │  │  Handles   │       │
│  └────────────┘  └────────────┘  └────────────┘  └────────────┘       │
│                                                                          │
│                     io_uring / kqueue / epoll                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## The Event Loop Integration

### Event Loop Trait/Interface

We assume a libuv-style event loop with these primitives:

```rust
// Abstract event loop interface (what we expect from libuv-like library)
trait EventLoop {
    // Poll handle for file descriptors
    fn create_poll(&self, fd: RawFd) -> PollHandle;

    // Timer for delayed/periodic execution
    fn create_timer(&self) -> TimerHandle;

    // Async handle for cross-thread wakeup
    fn create_async(&self, callback: fn()) -> AsyncHandle;

    // Idle handle for deferred work
    fn create_idle(&self) -> IdleHandle;

    // Run the loop
    fn run(&self, mode: RunMode);

    // Stop the loop
    fn stop(&self);
}

trait PollHandle {
    fn start(&self, events: PollEvents, callback: fn(status: i32, events: PollEvents));
    fn stop(&self);
}

trait TimerHandle {
    fn start(&self, timeout_ms: u64, repeat_ms: u64, callback: fn());
    fn stop(&self);
}

trait AsyncHandle {
    fn send(&self);  // Thread-safe wakeup
}
```

### Context Design

The Context holds shared resources but delegates I/O to user-provided event loop:

```rust
struct Context {
    // Configuration
    max_sockets: usize,
    max_message_size: usize,

    // Shared state for inproc transport
    inproc_endpoints: RwLock<HashMap<String, InprocEndpoint>>,

    // Socket ID generator
    next_socket_id: AtomicU64,

    // Optional: shared buffer pools
    buffer_pool: Option<BufferPool>,
}

impl Context {
    fn new() -> Self { ... }

    fn with_options(options: ContextOptions) -> Self { ... }

    // Create socket attached to an event loop
    fn socket<T: SocketType>(&self, event_loop: &EventLoop) -> Socket<T> {
        Socket::new(self, event_loop)
    }
}
```

### Socket-Loop Binding

Each socket is bound to exactly one event loop at creation:

```rust
struct Socket<T: SocketType> {
    // Identity
    id: SocketId,
    ctx: Arc<Context>,

    // Event loop binding
    loop_handle: LoopHandle,

    // Poll handle for readiness notification
    readiness_handle: AsyncHandle,

    // Pattern-specific state
    state: T::State,

    // Common socket state
    common: SocketCommon,
}

struct SocketCommon {
    // Options
    options: SocketOptions,

    // All pipes (connections to peers)
    pipes: PipeSet,

    // Bound endpoints (listeners)
    listeners: Vec<Listener>,

    // Connected endpoints (connectors + active connections)
    connectors: Vec<Connector>,

    // Readiness state
    readable: bool,
    writable: bool,

    // User callbacks
    on_readable: Option<Box<dyn FnMut()>>,
    on_writable: Option<Box<dyn FnMut()>>,
    on_error: Option<Box<dyn FnMut(Error)>>,
}
```

---

## Socket Architecture

### Socket Type Trait

Each socket pattern implements this trait:

```rust
trait SocketType {
    /// Pattern-specific state
    type State: Default;

    /// Called when a new pipe is attached
    fn pipe_attached(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId);

    /// Called when a pipe is detached
    fn pipe_detached(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId);

    /// Send a message - returns which pipe(s) to write to
    fn send(
        state: &mut Self::State,
        common: &mut SocketCommon,
        msg: Message,
    ) -> SendResult;

    /// Receive a message - selects which pipe to read from
    fn recv(
        state: &mut Self::State,
        common: &mut SocketCommon,
    ) -> RecvResult;

    /// Check if socket is sendable
    fn can_send(state: &Self::State, common: &SocketCommon) -> bool;

    /// Check if socket is receivable
    fn can_recv(state: &Self::State, common: &SocketCommon) -> bool;

    /// Called when a pipe becomes readable
    fn pipe_readable(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId);

    /// Called when a pipe becomes writable
    fn pipe_writable(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId);
}

enum SendResult {
    /// Message queued successfully
    Queued,
    /// Would block - no pipe available/ready
    WouldBlock,
    /// Error
    Error(Error),
}

enum RecvResult {
    /// Message received
    Message(Message),
    /// No message available
    WouldBlock,
    /// Error
    Error(Error),
}
```

### Socket Options (libzmq Compatible)

```rust
struct SocketOptions {
    // Identity
    routing_id: Option<Vec<u8>>,       // ZMQ_ROUTING_ID

    // High water marks
    send_hwm: usize,                    // ZMQ_SNDHWM (default: 1000)
    recv_hwm: usize,                    // ZMQ_RCVHWM (default: 1000)

    // Timeouts (for blocking wrappers)
    send_timeout: Duration,             // ZMQ_SNDTIMEO
    recv_timeout: Duration,             // ZMQ_RCVTIMEO

    // Connection behavior
    linger: Duration,                   // ZMQ_LINGER (default: -1 = infinite)
    reconnect_interval: Duration,       // ZMQ_RECONNECT_IVL (default: 100ms)
    reconnect_interval_max: Duration,   // ZMQ_RECONNECT_IVL_MAX (default: 0 = none)
    connect_timeout: Duration,          // ZMQ_CONNECT_TIMEOUT

    // TCP options
    tcp_keepalive: TcpKeepalive,       // ZMQ_TCP_KEEPALIVE_*
    tcp_nodelay: bool,                  // Nagle's algorithm

    // Buffer sizes
    send_buffer_size: usize,            // ZMQ_SNDBUF
    recv_buffer_size: usize,            // ZMQ_RCVBUF

    // Multicast
    multicast_hops: u8,                 // ZMQ_MULTICAST_HOPS
    multicast_rate: u32,                // ZMQ_RATE

    // Misc
    immediate: bool,                    // ZMQ_IMMEDIATE
    ipv6: bool,                         // ZMQ_IPV6

    // Pattern-specific (set by socket type)
    conflate: bool,                     // ZMQ_CONFLATE
    probe_router: bool,                 // ZMQ_PROBE_ROUTER
    router_mandatory: bool,             // ZMQ_ROUTER_MANDATORY
    req_relaxed: bool,                  // ZMQ_REQ_RELAXED
    req_correlate: bool,                // ZMQ_REQ_CORRELATE
}
```

### Readiness Model

Sockets expose readiness for the event loop to drive:

```rust
impl<T: SocketType> Socket<T> {
    /// Check if send would succeed without blocking
    pub fn is_writable(&self) -> bool {
        T::can_send(&self.state, &self.common)
    }

    /// Check if recv would succeed without blocking
    pub fn is_readable(&self) -> bool {
        T::can_recv(&self.state, &self.common)
    }

    /// Register callback for when socket becomes readable
    pub fn on_readable<F: FnMut() + 'static>(&mut self, callback: F) {
        self.common.on_readable = Some(Box::new(callback));
        // Immediately fire if already readable
        if self.is_readable() {
            self.fire_readable();
        }
    }

    /// Register callback for when socket becomes writable
    pub fn on_writable<F: FnMut() + 'static>(&mut self, callback: F) {
        self.common.on_writable = Some(Box::new(callback));
        if self.is_writable() {
            self.fire_writable();
        }
    }

    // Internal: called when readiness may have changed
    fn update_readiness(&mut self) {
        let was_readable = self.common.readable;
        let was_writable = self.common.writable;

        self.common.readable = self.is_readable();
        self.common.writable = self.is_writable();

        // Fire callbacks on transitions
        if !was_readable && self.common.readable {
            self.fire_readable();
        }
        if !was_writable && self.common.writable {
            self.fire_writable();
        }
    }

    fn fire_readable(&mut self) {
        if let Some(ref mut cb) = self.common.on_readable {
            cb();
        }
    }

    fn fire_writable(&mut self) {
        if let Some(ref mut cb) = self.common.on_writable {
            cb();
        }
    }
}
```

---

## Pipe System

Pipes connect sockets to peers. Each connection results in a pipe.

### Pipe Structure

```rust
/// Unique identifier for a pipe within a socket
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
struct PipeId(u32);

/// A pipe represents one connection to a peer
struct Pipe {
    id: PipeId,

    // Queues
    outbound: MessageQueue,    // Messages to send to peer
    inbound: MessageQueue,     // Messages received from peer

    // Flow control state
    hwm: HighWaterMark,

    // Peer identity (for ROUTER)
    routing_id: Option<RoutingId>,

    // Connection this pipe uses (None for inproc)
    connection: Option<ConnectionHandle>,

    // State
    state: PipeState,

    // Statistics
    msgs_sent: u64,
    msgs_recv: u64,
    bytes_sent: u64,
    bytes_recv: u64,
}

enum PipeState {
    Active,
    DrainingSend,     // Closing, but flushing outbound
    DrainingRecv,     // Peer closing, receiving remaining
    Closed,
}

/// Collection of pipes with pattern-specific indexing
struct PipeSet {
    pipes: HashMap<PipeId, Pipe>,
    next_id: u32,

    // Indices for efficient lookup
    by_routing_id: HashMap<RoutingId, PipeId>,
    readable_pipes: VecDeque<PipeId>,   // Pipes with data to read
    writable_pipes: VecDeque<PipeId>,   // Pipes that can accept writes
}
```

### Message Queue

Lock-free single-producer single-consumer queue:

```rust
/// Efficient message queue for pipe
struct MessageQueue {
    // Ring buffer for small queues, linked list for large
    storage: QueueStorage,

    // Counters for flow control
    enqueued: AtomicU64,
    dequeued: AtomicU64,

    // Capacity (HWM)
    capacity: usize,

    // Low water mark for flow control signaling
    lwm: usize,
}

enum QueueStorage {
    // For queues up to ~64 messages, use ring buffer
    Ring(RingBuffer<Message>),
    // For larger queues, use chunked linked list
    Chunked(ChunkedQueue<Message>),
}

impl MessageQueue {
    fn new(hwm: usize) -> Self {
        let lwm = (hwm + 1) / 2;  // Same as libzmq
        let storage = if hwm <= 64 {
            QueueStorage::Ring(RingBuffer::new(hwm))
        } else {
            QueueStorage::Chunked(ChunkedQueue::new())
        };

        Self {
            storage,
            enqueued: AtomicU64::new(0),
            dequeued: AtomicU64::new(0),
            capacity: hwm,
            lwm,
        }
    }

    /// Try to enqueue a message
    fn try_push(&mut self, msg: Message) -> Result<(), Message> {
        if self.len() >= self.capacity {
            return Err(msg);
        }

        self.storage.push(msg);
        self.enqueued.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Try to dequeue a message
    fn try_pop(&mut self) -> Option<Message> {
        let msg = self.storage.pop()?;
        self.dequeued.fetch_add(1, Ordering::Release);
        Some(msg)
    }

    /// Current queue length
    fn len(&self) -> usize {
        let enq = self.enqueued.load(Ordering::Acquire);
        let deq = self.dequeued.load(Ordering::Acquire);
        (enq - deq) as usize
    }

    /// Check if below low water mark (peer should resume sending)
    fn below_lwm(&self) -> bool {
        self.len() < self.lwm
    }

    /// Check if at capacity
    fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }
}
```

### Pipe Operations

```rust
impl Pipe {
    /// Attempt to write a message to this pipe
    fn write(&mut self, msg: Message) -> WriteResult {
        if self.state != PipeState::Active {
            return WriteResult::Closed;
        }

        match self.outbound.try_push(msg) {
            Ok(()) => {
                self.msgs_sent += 1;
                self.bytes_sent += msg.size() as u64;

                // Notify connection that data is available
                if let Some(ref conn) = self.connection {
                    conn.notify_data_available();
                }

                WriteResult::Ok
            }
            Err(msg) => WriteResult::WouldBlock(msg),
        }
    }

    /// Attempt to read a message from this pipe
    fn read(&mut self) -> Option<Message> {
        let msg = self.inbound.try_pop()?;
        self.msgs_recv += 1;
        self.bytes_recv += msg.size() as u64;

        // Check if we crossed low water mark - signal peer to resume
        if self.inbound.below_lwm() {
            self.signal_resume_peer();
        }

        Some(msg)
    }

    /// Check if this pipe has messages to read
    fn has_data(&self) -> bool {
        !self.inbound.is_empty()
    }

    /// Check if this pipe can accept writes
    fn can_write(&self) -> bool {
        self.state == PipeState::Active && !self.outbound.is_full()
    }

    /// Called when inbound data arrives from connection
    fn on_inbound_message(&mut self, msg: Message) -> InboundResult {
        match self.inbound.try_push(msg) {
            Ok(()) => InboundResult::Ok,
            Err(msg) => {
                // Inbound queue full - apply back-pressure
                InboundResult::Backpressure(msg)
            }
        }
    }

    /// Signal peer that it can resume sending (crossed LWM)
    fn signal_resume_peer(&self) {
        if let Some(ref conn) = self.connection {
            conn.send_credit();
        }
    }
}

enum WriteResult {
    Ok,
    WouldBlock(Message),  // Returns message back
    Closed,
}

enum InboundResult {
    Ok,
    Backpressure(Message),
}
```

---

## Flow Control (HWM/LWM)

### High Water Mark Semantics

```
HWM = 1000 (example)
LWM = 500

Queue State:
┌─────────────────────────────────────────────────────────────┐
│░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░│
│◄────────── Messages in queue ─────────────►                 │
│                                                             │
│         0         LWM(500)              HWM(1000)           │
│         │            │                      │               │
│         ▼            ▼                      ▼               │
│         ├────────────┼──────────────────────┤               │
│         │   ▲        │         ▲            │               │
│         │   │        │         │            │               │
│         │  Resume    │       Block          │               │
│         │  sending   │       sending        │               │
│         │            │                      │               │
└─────────────────────────────────────────────────────────────┘

State Machine:

     ┌─────────────────────────────────────┐
     │                                     │
     ▼                                     │
  ┌──────┐    len >= HWM    ┌──────────┐   │
  │ Open │─────────────────►│ Blocked  │   │
  └──────┘                  └──────────┘   │
     ▲                           │         │
     │       len < LWM           │         │
     └───────────────────────────┘         │
                                           │
  (peer reads messages, queue drains)──────┘
```

### Credit-Based Flow Control

For network connections, we use credit-based flow control:

```rust
struct FlowControl {
    // Local state
    send_credits: u32,        // How many messages we can send
    recv_window: u32,         // How many messages peer can send us

    // Configuration
    hwm: u32,
    lwm: u32,
}

impl FlowControl {
    fn new(hwm: u32) -> Self {
        let lwm = (hwm + 1) / 2;
        Self {
            send_credits: hwm,    // Start with full credits
            recv_window: hwm,
            hwm,
            lwm,
        }
    }

    /// Called when we want to send a message
    fn can_send(&self) -> bool {
        self.send_credits > 0
    }

    /// Called after sending a message
    fn message_sent(&mut self) {
        debug_assert!(self.send_credits > 0);
        self.send_credits -= 1;
    }

    /// Called when peer sends us credit
    fn receive_credit(&mut self, amount: u32) {
        self.send_credits = self.send_credits.saturating_add(amount);
    }

    /// Called when we read a message from inbound queue
    fn message_read(&mut self) -> Option<u32> {
        self.recv_window -= 1;

        // When we cross below LWM, send credit to peer
        if self.recv_window == self.lwm {
            let credit = self.hwm - self.lwm;
            self.recv_window = self.hwm;
            return Some(credit);
        }
        None
    }
}
```

### HWM Behavior by Socket Type

| Socket Type | Send HWM Behavior | Recv HWM Behavior |
|-------------|-------------------|-------------------|
| PUSH | Block/drop on full | N/A |
| PULL | N/A | Apply backpressure |
| PUB | Drop messages | N/A |
| SUB | N/A | Apply backpressure |
| REQ | Block on full | Apply backpressure |
| REP | Block on full | Apply backpressure |
| DEALER | Block/round-robin skip | Apply backpressure |
| ROUTER | Drop (unless mandatory) | Apply backpressure |

---

## Message System

### Message Structure

```rust
/// A ZMQ message - can be single-part or multi-part
struct Message {
    frames: SmallVec<[Frame; 1]>,  // Optimize for single-frame common case
}

/// A single frame of data
struct Frame {
    data: FrameData,
    flags: FrameFlags,
}

enum FrameData {
    // Small message - inline storage (up to ~64 bytes)
    Inline(InlineData),

    // Heap-allocated owned data
    Owned(Box<[u8]>),

    // Reference-counted shared data
    Shared(Arc<[u8]>),

    // Zero-copy: user-provided buffer with custom deallocation
    External {
        ptr: *const u8,
        len: usize,
        hint: *mut c_void,
        free_fn: unsafe extern "C" fn(*mut c_void, *mut c_void),
    },
}

bitflags! {
    struct FrameFlags: u8 {
        const MORE = 0x01;       // More frames follow
        const COMMAND = 0x02;    // This is a command frame
        const IDENTITY = 0x04;   // This is an identity frame
    }
}

// Inline storage for small messages
#[repr(C)]
struct InlineData {
    len: u8,
    data: [u8; 63],
}
```

### Message API

```rust
impl Message {
    /// Create empty message
    pub fn new() -> Self {
        Self { frames: SmallVec::new() }
    }

    /// Create single-frame message from bytes
    pub fn from_bytes(data: &[u8]) -> Self {
        let frame = Frame::from_bytes(data);
        Self { frames: smallvec![frame] }
    }

    /// Create message with zero-copy buffer
    pub fn from_external<F>(ptr: *const u8, len: usize, hint: *mut c_void, free_fn: F) -> Self
    where
        F: FnOnce(*mut c_void)
    {
        // ... zero-copy construction
    }

    /// Add a frame to this message
    pub fn push_frame(&mut self, frame: Frame) {
        if let Some(last) = self.frames.last_mut() {
            last.flags.insert(FrameFlags::MORE);
        }
        self.frames.push(frame);
    }

    /// Iterate over frames
    pub fn frames(&self) -> impl Iterator<Item = &Frame> {
        self.frames.iter()
    }

    /// Get single-frame message data (panics if multipart)
    pub fn data(&self) -> &[u8] {
        assert!(self.frames.len() == 1, "Use frames() for multipart");
        self.frames[0].as_bytes()
    }

    /// Total size across all frames
    pub fn size(&self) -> usize {
        self.frames.iter().map(|f| f.len()).sum()
    }

    /// Check if this is a multipart message
    pub fn is_multipart(&self) -> bool {
        self.frames.len() > 1
    }
}

impl Frame {
    pub fn from_bytes(data: &[u8]) -> Self {
        let data = if data.len() <= 63 {
            FrameData::Inline(InlineData::from_slice(data))
        } else {
            FrameData::Owned(data.into())
        };

        Self {
            data,
            flags: FrameFlags::empty(),
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match &self.data {
            FrameData::Inline(i) => &i.data[..i.len as usize],
            FrameData::Owned(b) => b,
            FrameData::Shared(a) => a,
            FrameData::External { ptr, len, .. } => {
                unsafe { std::slice::from_raw_parts(*ptr, *len) }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }
}
```

---

## Connection Management

### Listener (bind)

```rust
/// Accepts incoming connections on a bound address
struct Listener {
    endpoint: Endpoint,
    transport: TransportListener,
    socket_id: SocketId,
}

impl Listener {
    fn new(
        endpoint: Endpoint,
        event_loop: &EventLoop,
        socket_id: SocketId,
        options: &SocketOptions,
    ) -> Result<Self> {
        let transport = TransportListener::bind(&endpoint, event_loop)?;

        Ok(Self {
            endpoint,
            transport,
            socket_id,
        })
    }

    /// Start accepting connections
    fn start(&mut self, on_connection: impl FnMut(Connection)) {
        self.transport.on_accept(move |stream, peer_addr| {
            // Create connection from accepted stream
            let conn = Connection::from_accepted(stream, peer_addr);
            on_connection(conn);
        });
    }
}
```

### Connector (connect)

```rust
/// Manages outgoing connection with automatic reconnection
struct Connector {
    endpoint: Endpoint,
    socket_id: SocketId,
    options: ConnectorOptions,
    state: ConnectorState,

    // Event loop handles
    reconnect_timer: Option<TimerHandle>,
}

enum ConnectorState {
    Disconnected,
    Connecting(PendingConnection),
    Connected(Connection),
    Reconnecting { attempt: u32 },
}

struct ConnectorOptions {
    reconnect_interval: Duration,
    reconnect_interval_max: Duration,
    connect_timeout: Duration,
    immediate: bool,
}

impl Connector {
    fn new(endpoint: Endpoint, options: ConnectorOptions) -> Self {
        Self {
            endpoint,
            socket_id: SocketId(0),
            options,
            state: ConnectorState::Disconnected,
            reconnect_timer: None,
        }
    }

    /// Start connection attempt
    fn connect(&mut self, event_loop: &EventLoop, on_connected: impl FnMut(Connection)) {
        self.state = ConnectorState::Connecting(
            PendingConnection::new(&self.endpoint, event_loop, self.options.connect_timeout)
        );

        // Set up connection result handling
        self.handle_connection_result(event_loop, on_connected);
    }

    /// Handle connection success
    fn on_connected(&mut self, conn: Connection, on_connected: &mut impl FnMut(Connection)) {
        self.state = ConnectorState::Connected(conn.clone());
        on_connected(conn);
    }

    /// Handle connection failure or disconnect
    fn on_disconnected(&mut self, event_loop: &EventLoop) {
        let delay = self.calculate_reconnect_delay();

        self.state = ConnectorState::Reconnecting {
            attempt: match &self.state {
                ConnectorState::Reconnecting { attempt } => attempt + 1,
                _ => 1,
            }
        };

        // Schedule reconnection
        self.reconnect_timer = Some(
            event_loop.create_timer()
        );
        self.reconnect_timer.as_ref().unwrap().start(
            delay.as_millis() as u64,
            0,
            || self.reconnect_attempt(event_loop),
        );
    }

    fn calculate_reconnect_delay(&self) -> Duration {
        let base = self.options.reconnect_interval;
        let max = self.options.reconnect_interval_max;

        if max == Duration::ZERO {
            return base;
        }

        let attempt = match &self.state {
            ConnectorState::Reconnecting { attempt } => *attempt,
            _ => 0,
        };

        // Exponential backoff with jitter
        let delay = base * (1 << attempt.min(10));
        let delay = delay.min(max);

        // Add jitter (±25%)
        let jitter_range = delay / 4;
        let jitter = Duration::from_millis(
            rand::random::<u64>() % (jitter_range.as_millis() as u64 * 2)
        );

        delay - jitter_range + jitter
    }
}
```

### Connection

```rust
/// Active network connection
struct Connection {
    id: ConnectionId,
    transport: TransportStream,
    codec: ZmtpCodec,

    // Associated pipe (set after handshake)
    pipe_id: Option<PipeId>,

    // Peer identity (from ZMTP handshake)
    peer_identity: Option<Vec<u8>>,

    // State
    state: ConnectionState,

    // Flow control
    flow: FlowControl,

    // Send buffer (partially sent message)
    send_buffer: Option<EncodedMessage>,
}

enum ConnectionState {
    Handshaking,
    Ready,
    Closing,
    Closed,
}

impl Connection {
    /// Called when transport is readable
    fn on_readable(&mut self) -> Vec<ConnectionEvent> {
        let mut events = Vec::new();

        loop {
            // Read from transport
            let data = match self.transport.read() {
                Ok(data) if data.is_empty() => break,
                Ok(data) => data,
                Err(WouldBlock) => break,
                Err(e) => {
                    events.push(ConnectionEvent::Error(e));
                    break;
                }
            };

            // Decode ZMTP frames
            match self.codec.decode(&data) {
                Ok(frames) => {
                    for frame in frames {
                        events.push(self.handle_frame(frame));
                    }
                }
                Err(e) => {
                    events.push(ConnectionEvent::Error(e));
                    break;
                }
            }
        }

        events
    }

    /// Called when transport is writable
    fn on_writable(&mut self) -> bool {
        // Flush any pending send buffer
        if let Some(ref mut buf) = self.send_buffer {
            match self.transport.write(buf.remaining()) {
                Ok(n) => {
                    buf.advance(n);
                    if buf.is_empty() {
                        self.send_buffer = None;
                    }
                }
                Err(WouldBlock) => return false,
                Err(_) => return false,
            }
        }

        true  // Can accept more data
    }

    /// Try to send a message
    fn send(&mut self, msg: &Message) -> Result<(), SendError> {
        if self.send_buffer.is_some() {
            return Err(SendError::WouldBlock);
        }

        // Encode message to ZMTP
        let encoded = self.codec.encode(msg)?;

        // Try immediate send
        match self.transport.write(&encoded) {
            Ok(n) if n == encoded.len() => Ok(()),
            Ok(n) => {
                // Partial write - buffer remainder
                let mut buf = EncodedMessage::from(encoded);
                buf.advance(n);
                self.send_buffer = Some(buf);
                Ok(())
            }
            Err(WouldBlock) => {
                self.send_buffer = Some(EncodedMessage::from(encoded));
                Ok(())
            }
            Err(e) => Err(SendError::Io(e)),
        }
    }

    fn handle_frame(&mut self, frame: ZmtpFrame) -> ConnectionEvent {
        match frame {
            ZmtpFrame::Message(msg) => {
                ConnectionEvent::Message(msg)
            }
            ZmtpFrame::Command(cmd) => {
                self.handle_command(cmd)
            }
        }
    }

    fn handle_command(&mut self, cmd: ZmtpCommand) -> ConnectionEvent {
        match cmd {
            ZmtpCommand::Ready { identity, .. } => {
                self.peer_identity = identity;
                self.state = ConnectionState::Ready;
                ConnectionEvent::Ready
            }
            ZmtpCommand::Credit(amount) => {
                self.flow.receive_credit(amount);
                ConnectionEvent::CreditReceived
            }
            // ... other commands
        }
    }
}

enum ConnectionEvent {
    Ready,
    Message(Message),
    CreditReceived,
    Error(Error),
    Closed,
}
```

---

## Transport Layer

### Transport Abstraction

```rust
trait Transport {
    type Listener: TransportListener;
    type Stream: TransportStream;

    fn scheme() -> &'static str;
}

trait TransportListener {
    fn bind(addr: &str, event_loop: &EventLoop) -> Result<Self>;
    fn on_accept(&mut self, callback: impl FnMut(Box<dyn TransportStream>, SocketAddr));
    fn local_addr(&self) -> Result<SocketAddr>;
    fn close(&mut self);
}

trait TransportStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn poll_handle(&self) -> &PollHandle;
    fn shutdown(&mut self, how: Shutdown);
    fn close(&mut self);

    // Optional optimizations
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut]) -> Result<usize> {
        // Default: single buffer read
        self.read(&mut bufs[0])
    }

    fn write_vectored(&mut self, bufs: &[IoSlice]) -> Result<usize> {
        // Default: single buffer write
        self.write(&bufs[0])
    }
}
```

### TCP Transport

```rust
struct TcpTransport;

impl Transport for TcpTransport {
    type Listener = TcpListener;
    type Stream = TcpStream;

    fn scheme() -> &'static str { "tcp" }
}

struct TcpListener {
    socket: Socket,
    poll_handle: PollHandle,
    accept_callback: Option<Box<dyn FnMut(TcpStream, SocketAddr)>>,
}

impl TransportListener for TcpListener {
    fn bind(addr: &str, event_loop: &EventLoop) -> Result<Self> {
        let addr: SocketAddr = addr.parse()?;
        let socket = Socket::new(Domain::ipv4(), Type::stream(), None)?;

        socket.set_reuse_addr(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(128)?;

        let poll_handle = event_loop.create_poll(socket.as_raw_fd());

        Ok(Self {
            socket,
            poll_handle,
            accept_callback: None,
        })
    }

    fn on_accept(&mut self, callback: impl FnMut(TcpStream, SocketAddr) + 'static) {
        self.accept_callback = Some(Box::new(callback));

        self.poll_handle.start(PollEvents::READABLE, |status, events| {
            if status < 0 || !events.contains(PollEvents::READABLE) {
                return;
            }

            // Accept all pending connections
            loop {
                match self.socket.accept() {
                    Ok((stream, addr)) => {
                        stream.set_nonblocking(true).ok();
                        let tcp_stream = TcpStream::from_socket(stream, &self.event_loop);
                        if let Some(ref mut cb) = self.accept_callback {
                            cb(tcp_stream, addr);
                        }
                    }
                    Err(ref e) if e.kind() == WouldBlock => break,
                    Err(e) => {
                        // Log error, continue
                        break;
                    }
                }
            }
        });
    }
}

struct TcpStream {
    socket: Socket,
    poll_handle: PollHandle,
}

impl TransportStream for TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Use recv() for TCP
        match self.socket.recv(buf, 0) {
            Ok(0) => Err(Error::new(ErrorKind::UnexpectedEof, "connection closed")),
            Ok(n) => Ok(n),
            Err(e) if e.kind() == WouldBlock => Err(Error::from(WouldBlock)),
            Err(e) => Err(e),
        }
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.socket.send(buf, 0)
    }
}
```

### IPC Transport (Unix Domain Sockets)

```rust
struct IpcTransport;

impl Transport for IpcTransport {
    type Listener = IpcListener;
    type Stream = IpcStream;

    fn scheme() -> &'static str { "ipc" }
}

// Similar structure to TCP but with Unix domain sockets
// Path-based addressing instead of host:port
```

### Inproc Transport

Special case: in-process communication without network:

```rust
struct InprocTransport;

struct InprocEndpoint {
    // Pending connections waiting for bind
    pending_connectors: Vec<InprocConnector>,
    // Bound socket
    bound_socket: Option<WeakSocketRef>,
}

impl InprocTransport {
    fn bind(ctx: &Context, path: &str, socket: &Socket) -> Result<()> {
        let mut endpoints = ctx.inproc_endpoints.write();

        if endpoints.contains_key(path) {
            return Err(Error::AddressInUse);
        }

        let endpoint = InprocEndpoint {
            pending_connectors: Vec::new(),
            bound_socket: Some(socket.weak_ref()),
        };

        endpoints.insert(path.to_string(), endpoint);

        // Connect any pending connectors
        // ...

        Ok(())
    }

    fn connect(ctx: &Context, path: &str, socket: &Socket) -> Result<Pipe> {
        let mut endpoints = ctx.inproc_endpoints.write();

        match endpoints.get_mut(path) {
            Some(endpoint) if endpoint.bound_socket.is_some() => {
                // Direct connection - create pipe pair
                let (pipe_a, pipe_b) = create_pipe_pair(socket.options.hwm);

                // Attach pipes to both sockets
                // ...

                Ok(pipe_a)
            }
            Some(endpoint) => {
                // Bound socket not ready yet, queue connector
                endpoint.pending_connectors.push(InprocConnector::new(socket));
                Err(Error::WouldBlock)
            }
            None => {
                // No endpoint - queue for later
                let endpoint = InprocEndpoint {
                    pending_connectors: vec![InprocConnector::new(socket)],
                    bound_socket: None,
                };
                endpoints.insert(path.to_string(), endpoint);
                Err(Error::WouldBlock)
            }
        }
    }
}
```

---

## Socket Types Implementation

### PUSH Socket

```rust
struct PushType;

struct PushState {
    // Load balancer for round-robin distribution
    lb: LoadBalancer,
}

impl SocketType for PushType {
    type State = PushState;

    fn pipe_attached(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId) {
        state.lb.add_pipe(pipe);
        common.pipes.add_writable(pipe);
    }

    fn pipe_detached(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId) {
        state.lb.remove_pipe(pipe);
    }

    fn send(state: &mut Self::State, common: &mut SocketCommon, msg: Message) -> SendResult {
        // Round-robin to next available pipe
        loop {
            let pipe_id = match state.lb.next() {
                Some(id) => id,
                None => return SendResult::WouldBlock,
            };

            let pipe = common.pipes.get_mut(pipe_id);
            match pipe.write(msg) {
                WriteResult::Ok => {
                    pipe.flush();
                    return SendResult::Queued;
                }
                WriteResult::WouldBlock(msg_back) => {
                    // This pipe is full, try next
                    state.lb.mark_inactive(pipe_id);
                    msg = msg_back;
                    continue;
                }
                WriteResult::Closed => {
                    state.lb.remove_pipe(pipe_id);
                    continue;
                }
            }
        }
    }

    fn recv(_state: &mut Self::State, _common: &mut SocketCommon) -> RecvResult {
        RecvResult::Error(Error::NotSupported)
    }

    fn can_send(state: &Self::State, common: &SocketCommon) -> bool {
        state.lb.has_active_pipe()
    }

    fn can_recv(_state: &Self::State, _common: &SocketCommon) -> bool {
        false
    }

    fn pipe_writable(state: &mut Self::State, _common: &mut SocketCommon, pipe: PipeId) {
        state.lb.mark_active(pipe);
    }
}
```

### PULL Socket

```rust
struct PullType;

struct PullState {
    // Fair queue for reading from multiple pipes
    fq: FairQueue,
}

impl SocketType for PullType {
    type State = PullState;

    fn pipe_attached(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId) {
        state.fq.add_pipe(pipe);
    }

    fn pipe_detached(state: &mut Self::State, _common: &mut SocketCommon, pipe: PipeId) {
        state.fq.remove_pipe(pipe);
    }

    fn send(_state: &mut Self::State, _common: &mut SocketCommon, _msg: Message) -> SendResult {
        SendResult::Error(Error::NotSupported)
    }

    fn recv(state: &mut Self::State, common: &mut SocketCommon) -> RecvResult {
        // Fair-queue read from pipes
        let mut attempts = 0;
        let pipe_count = state.fq.pipe_count();

        while attempts < pipe_count {
            let pipe_id = match state.fq.next() {
                Some(id) => id,
                None => return RecvResult::WouldBlock,
            };

            let pipe = common.pipes.get_mut(pipe_id);
            if let Some(msg) = pipe.read() {
                return RecvResult::Message(msg);
            }

            // This pipe is empty, try next
            state.fq.mark_empty(pipe_id);
            attempts += 1;
        }

        RecvResult::WouldBlock
    }

    fn can_send(_state: &Self::State, _common: &SocketCommon) -> bool {
        false
    }

    fn can_recv(state: &Self::State, common: &SocketCommon) -> bool {
        state.fq.has_readable_pipe()
    }

    fn pipe_readable(state: &mut Self::State, _common: &mut SocketCommon, pipe: PipeId) {
        state.fq.mark_readable(pipe);
    }
}
```

### PUB Socket

```rust
struct PubType;

struct PubState {
    // All subscriber pipes
    subscribers: Vec<PipeId>,
}

impl SocketType for PubType {
    type State = PubState;

    fn pipe_attached(state: &mut Self::State, _common: &mut SocketCommon, pipe: PipeId) {
        state.subscribers.push(pipe);
    }

    fn pipe_detached(state: &mut Self::State, _common: &mut SocketCommon, pipe: PipeId) {
        state.subscribers.retain(|&id| id != pipe);
    }

    fn send(state: &mut Self::State, common: &mut SocketCommon, msg: Message) -> SendResult {
        // Send to all subscribers (fan-out)
        // Clone message for each subscriber
        for &pipe_id in &state.subscribers {
            let pipe = common.pipes.get_mut(pipe_id);

            // Clone message (copy-on-write if shared)
            let msg_clone = msg.clone();

            match pipe.write(msg_clone) {
                WriteResult::Ok => {
                    pipe.flush();
                }
                WriteResult::WouldBlock(_) => {
                    // PUB drops messages when subscriber is slow
                    // (no backpressure to other subscribers)
                }
                WriteResult::Closed => {
                    // Will be cleaned up in pipe_detached
                }
            }
        }

        SendResult::Queued
    }

    fn recv(_state: &mut Self::State, _common: &mut SocketCommon) -> RecvResult {
        RecvResult::Error(Error::NotSupported)
    }

    fn can_send(state: &Self::State, _common: &SocketCommon) -> bool {
        !state.subscribers.is_empty()
    }

    fn can_recv(_state: &Self::State, _common: &SocketCommon) -> bool {
        false
    }
}
```

### SUB Socket

```rust
struct SubType;

struct SubState {
    // Subscriptions (prefix matching)
    subscriptions: SubscriptionTrie,
    // Fair queue for reading
    fq: FairQueue,
}

impl SocketType for SubType {
    type State = SubState;

    fn recv(state: &mut Self::State, common: &mut SocketCommon) -> RecvResult {
        loop {
            let pipe_id = match state.fq.next() {
                Some(id) => id,
                None => return RecvResult::WouldBlock,
            };

            let pipe = common.pipes.get_mut(pipe_id);
            let msg = match pipe.read() {
                Some(m) => m,
                None => {
                    state.fq.mark_empty(pipe_id);
                    continue;
                }
            };

            // Check subscription filter
            if state.subscriptions.matches(msg.topic()) {
                return RecvResult::Message(msg);
            }

            // Message doesn't match subscriptions, drop it
            // (This shouldn't happen if PUB-side filtering is working)
        }
    }

    // ... rest similar to PULL
}

impl SubState {
    fn subscribe(&mut self, prefix: &[u8]) {
        self.subscriptions.insert(prefix);
        // Send subscription to all connected PUBs
    }

    fn unsubscribe(&mut self, prefix: &[u8]) {
        self.subscriptions.remove(prefix);
        // Send unsubscription to all connected PUBs
    }
}
```

### ROUTER Socket

```rust
struct RouterType;

struct RouterState {
    // Map routing ID to pipe
    routes: HashMap<RoutingId, PipeId>,
    // Fair queue for incoming
    fq: FairQueue,
    // Options
    mandatory: bool,
}

impl SocketType for RouterType {
    type State = RouterState;

    fn pipe_attached(state: &mut Self::State, common: &mut SocketCommon, pipe: PipeId) {
        // Routing ID is assigned during ZMTP handshake
        // or can be set explicitly
        let pipe_obj = common.pipes.get(pipe);
        if let Some(routing_id) = &pipe_obj.routing_id {
            state.routes.insert(routing_id.clone(), pipe);
        }
        state.fq.add_pipe(pipe);
    }

    fn send(state: &mut Self::State, common: &mut SocketCommon, msg: Message) -> SendResult {
        // First frame must be routing ID
        let frames = msg.frames();
        if frames.is_empty() {
            return SendResult::Error(Error::InvalidMessage);
        }

        let routing_id = RoutingId::from_bytes(frames[0].as_bytes());

        let pipe_id = match state.routes.get(&routing_id) {
            Some(&id) => id,
            None => {
                if state.mandatory {
                    return SendResult::Error(Error::HostUnreachable);
                } else {
                    // Silently drop
                    return SendResult::Queued;
                }
            }
        };

        // Strip routing ID frame, send rest
        let msg_without_id = msg.without_first_frame();

        let pipe = common.pipes.get_mut(pipe_id);
        match pipe.write(msg_without_id) {
            WriteResult::Ok => {
                pipe.flush();
                SendResult::Queued
            }
            WriteResult::WouldBlock(msg) => {
                if state.mandatory {
                    SendResult::WouldBlock
                } else {
                    SendResult::Queued  // Drop
                }
            }
            WriteResult::Closed => {
                state.routes.remove(&routing_id);
                SendResult::Error(Error::HostUnreachable)
            }
        }
    }

    fn recv(state: &mut Self::State, common: &mut SocketCommon) -> RecvResult {
        let pipe_id = match state.fq.next() {
            Some(id) => id,
            None => return RecvResult::WouldBlock,
        };

        let pipe = common.pipes.get_mut(pipe_id);
        let msg = match pipe.read() {
            Some(m) => m,
            None => {
                state.fq.mark_empty(pipe_id);
                return RecvResult::WouldBlock;
            }
        };

        // Prepend routing ID to message
        let routing_id = pipe.routing_id.as_ref().expect("pipe must have routing ID");
        let msg_with_id = msg.prepend_frame(Frame::from_bytes(routing_id.as_bytes()));

        RecvResult::Message(msg_with_id)
    }
}
```

### REQ Socket

```rust
struct ReqType;

struct ReqState {
    // Load balancer for sending requests
    lb: LoadBalancer,
    // Currently expecting reply from this pipe
    expecting_reply_from: Option<PipeId>,
    // Request ID for correlation
    next_request_id: u32,
    // Options
    relaxed: bool,     // ZMQ_REQ_RELAXED
    correlate: bool,   // ZMQ_REQ_CORRELATE
}

impl SocketType for ReqType {
    type State = ReqState;

    fn send(state: &mut Self::State, common: &mut SocketCommon, msg: Message) -> SendResult {
        // REQ enforces strict send/recv alternation (unless relaxed)
        if !state.relaxed && state.expecting_reply_from.is_some() {
            return SendResult::Error(Error::InvalidState);
        }

        // Find a pipe to send to
        let pipe_id = match state.lb.next() {
            Some(id) => id,
            None => return SendResult::WouldBlock,
        };

        // Optionally add request ID frame for correlation
        let msg = if state.correlate {
            let request_id = state.next_request_id;
            state.next_request_id = state.next_request_id.wrapping_add(1);
            msg.prepend_frame(Frame::from_bytes(&request_id.to_be_bytes()))
        } else {
            msg
        };

        // Add empty delimiter frame (REQ envelope)
        let msg = msg.prepend_frame(Frame::empty());

        let pipe = common.pipes.get_mut(pipe_id);
        match pipe.write(msg) {
            WriteResult::Ok => {
                pipe.flush();
                state.expecting_reply_from = Some(pipe_id);
                SendResult::Queued
            }
            WriteResult::WouldBlock(msg) => SendResult::WouldBlock,
            WriteResult::Closed => {
                state.lb.remove_pipe(pipe_id);
                SendResult::WouldBlock
            }
        }
    }

    fn recv(state: &mut Self::State, common: &mut SocketCommon) -> RecvResult {
        // Must have sent a request first (unless relaxed)
        let pipe_id = match state.expecting_reply_from {
            Some(id) => id,
            None if state.relaxed => {
                // In relaxed mode, can recv from any pipe
                match state.fq_recv(common) {
                    Some(msg) => return RecvResult::Message(msg),
                    None => return RecvResult::WouldBlock,
                }
            }
            None => return RecvResult::Error(Error::InvalidState),
        };

        let pipe = common.pipes.get_mut(pipe_id);
        let msg = match pipe.read() {
            Some(m) => m,
            None => return RecvResult::WouldBlock,
        };

        // Strip envelope (empty delimiter frame)
        let msg = msg.without_first_frame();

        // Verify correlation if enabled
        if state.correlate {
            // Verify and strip request ID frame
            // ...
        }

        state.expecting_reply_from = None;
        RecvResult::Message(msg)
    }

    fn can_send(state: &Self::State, _common: &SocketCommon) -> bool {
        (state.relaxed || state.expecting_reply_from.is_none())
            && state.lb.has_active_pipe()
    }

    fn can_recv(state: &Self::State, common: &SocketCommon) -> bool {
        match state.expecting_reply_from {
            Some(pipe_id) => {
                common.pipes.get(pipe_id).has_data()
            }
            None if state.relaxed => {
                // Any pipe readable
                common.pipes.any_readable()
            }
            None => false,
        }
    }
}
```

---

## Thread Safety Model

### Ownership Rules

1. **Sockets belong to one event loop**: Created with event loop reference, all operations from that loop's thread
2. **Context is thread-safe**: Can create sockets from any thread
3. **Messages are transferable**: Can be created on one thread, sent to another via socket

### Cross-Thread Communication

For applications needing cross-thread message passing:

```rust
/// Thread-safe submission handle for a socket
struct SocketSubmitter {
    // Async handle to wake event loop
    async_handle: AsyncHandle,
    // Lock-free queue for submissions
    queue: MpscQueue<Submission>,
}

impl SocketSubmitter {
    /// Send a message (from any thread)
    fn send(&self, msg: Message) -> PendingSend {
        let (tx, rx) = oneshot::channel();

        self.queue.push(Submission::Send {
            msg,
            completion: tx,
        });

        // Wake event loop
        self.async_handle.send();

        PendingSend { rx }
    }
}

// Usage:
// let submitter = socket.submitter();
// std::thread::spawn(move || {
//     submitter.send(msg).block();
// });
```

### Processing Submissions

```rust
impl<T: SocketType> Socket<T> {
    /// Called from event loop when async handle fires
    fn process_submissions(&mut self) {
        while let Some(sub) = self.submissions.try_pop() {
            match sub {
                Submission::Send { msg, completion } => {
                    let result = self.try_send(msg);
                    let _ = completion.send(result);
                }
                Submission::Recv { completion } => {
                    let result = self.try_recv();
                    let _ = completion.send(result);
                }
                // ...
            }
        }
    }
}
```

---

## API Design

### Core API

```rust
// Context
let ctx = Context::new();

// Create socket on event loop
let push = ctx.socket::<Push>(&event_loop);
let pull = ctx.socket::<Pull>(&event_loop);

// Bind/Connect
push.bind("tcp://127.0.0.1:5555")?;
pull.connect("tcp://127.0.0.1:5555")?;

// Options
push.set_option(SendHwm(1000))?;
push.set_option(Linger(Duration::from_secs(1)))?;

// Callbacks
pull.on_readable(|| {
    while let Some(msg) = pull.try_recv() {
        process(msg);
    }
});

push.on_writable(|| {
    while let Some(msg) = pending_messages.pop() {
        if push.try_send(msg).is_err() {
            pending_messages.push_front(msg);
            break;
        }
    }
});

// Run event loop
event_loop.run(RunMode::Default);
```

### Blocking Wrapper

For simpler use cases:

```rust
struct BlockingSocket<T: SocketType> {
    inner: Socket<T>,
    event_loop: EventLoop,
}

impl<T: SocketType> BlockingSocket<T> {
    /// Blocking send
    fn send(&mut self, msg: Message) -> Result<()> {
        self.send_timeout(msg, None)
    }

    fn send_timeout(&mut self, msg: Message, timeout: Option<Duration>) -> Result<()> {
        loop {
            match self.inner.try_send(msg) {
                SendResult::Queued => return Ok(()),
                SendResult::WouldBlock => {
                    // Run event loop until writable
                    self.wait_writable(timeout)?;
                }
                SendResult::Error(e) => return Err(e),
            }
        }
    }

    /// Blocking receive
    fn recv(&mut self) -> Result<Message> {
        self.recv_timeout(None)
    }

    fn recv_timeout(&mut self, timeout: Option<Duration>) -> Result<Message> {
        loop {
            match self.inner.try_recv() {
                RecvResult::Message(m) => return Ok(m),
                RecvResult::WouldBlock => {
                    self.wait_readable(timeout)?;
                }
                RecvResult::Error(e) => return Err(e),
            }
        }
    }

    fn wait_writable(&mut self, timeout: Option<Duration>) -> Result<()> {
        // Set up one-shot callback
        let ready = Rc::new(Cell::new(false));
        let ready_clone = ready.clone();

        self.inner.on_writable(move || {
            ready_clone.set(true);
        });

        // Run event loop
        let deadline = timeout.map(|t| Instant::now() + t);
        while !ready.get() {
            let loop_timeout = deadline.map(|d| d - Instant::now());
            self.event_loop.run_once(loop_timeout)?;

            if deadline.map(|d| Instant::now() >= d).unwrap_or(false) {
                return Err(Error::Timeout);
            }
        }

        Ok(())
    }
}
```

### Async/Await Integration

```rust
// For async runtimes
impl<T: SocketType> Socket<T> {
    async fn send_async(&self, msg: Message) -> Result<()> {
        poll_fn(|cx| {
            match self.try_send(msg.clone()) {
                SendResult::Queued => Poll::Ready(Ok(())),
                SendResult::WouldBlock => {
                    self.register_send_waker(cx.waker().clone());
                    Poll::Pending
                }
                SendResult::Error(e) => Poll::Ready(Err(e)),
            }
        }).await
    }

    async fn recv_async(&self) -> Result<Message> {
        poll_fn(|cx| {
            match self.try_recv() {
                RecvResult::Message(m) => Poll::Ready(Ok(m)),
                RecvResult::WouldBlock => {
                    self.register_recv_waker(cx.waker().clone());
                    Poll::Pending
                }
                RecvResult::Error(e) => Poll::Ready(Err(e)),
            }
        }).await
    }
}
```

---

## Error Handling

### Error Types

```rust
#[derive(Debug)]
enum Error {
    // Socket errors
    InvalidState,          // Operation invalid in current state
    NotSupported,          // Operation not supported by socket type
    TerminatingContext,    // Context is shutting down

    // Address errors
    InvalidEndpoint,       // Malformed endpoint string
    AddressInUse,         // Endpoint already bound
    AddressNotAvailable,  // Cannot bind to address

    // Connection errors
    ConnectionRefused,
    ConnectionReset,
    HostUnreachable,
    NetworkUnreachable,
    Timeout,

    // Message errors
    InvalidMessage,        // Message format invalid for socket type
    MessageTooLarge,      // Exceeds max message size

    // I/O errors
    Io(std::io::Error),

    // Resource errors
    TooManyOpenFiles,
    OutOfMemory,
}

impl Error {
    /// libzmq-compatible error code
    fn to_errno(&self) -> i32 {
        match self {
            Error::InvalidState => libc::EFSM,
            Error::NotSupported => libc::ENOTSUP,
            Error::AddressInUse => libc::EADDRINUSE,
            Error::ConnectionRefused => libc::ECONNREFUSED,
            Error::Timeout => libc::ETIMEDOUT,
            Error::HostUnreachable => libc::EHOSTUNREACH,
            // ...
        }
    }
}
```

---

## Summary

This design provides:

1. **Event-loop native**: No hidden threads, user controls concurrency
2. **Familiar semantics**: Same socket types, HWM, reconnection, etc.
3. **High performance**: Minimal allocations, efficient queues, batching
4. **Flexible integration**: Works with any libuv-style event loop
5. **Clear ownership**: Sockets bound to loops, thread-safe context

Key differences from libzmq:
- Callback-based instead of blocking by default
- No internal I/O threads
- Explicit event loop integration
- Simpler internal architecture (no mailbox/command system)

# Performance Analysis & Optimizations

Addendum to the ZMQ Event-Loop Library Design addressing performance concerns.

## Table of Contents

1. [Performance Gaps in Simplified Design](#performance-gaps-in-simplified-design)
2. [Critical libzmq Optimizations We Must Preserve](#critical-libzmq-optimizations-we-must-preserve)
3. [Detailed Queue Implementation](#detailed-queue-implementation)
4. [Event Loop Wakeup Optimization](#event-loop-wakeup-optimization)
5. [Memory Management](#memory-management)
6. [Cache Optimization](#cache-optimization)
7. [Batching Strategies](#batching-strategies)
8. [Zero-Copy Message Handling](#zero-copy-message-handling)
9. [Benchmarking Targets](#benchmarking-targets)

---

## Performance Gaps in Simplified Design

The initial design oversimplified several critical areas:

### 1. Queue Wakeup Inefficiency

**Problem**: The simple design wakes the event loop on every `flush()`:
```rust
// WRONG - wakes loop on every message
fn write(&mut self, msg: Message) {
    self.queue.push(msg);
    self.event_loop.wake();  // syscall every time!
}
```

**libzmq's solution**: The `ypipe_t::flush()` returns `false` only when the reader is sleeping (cursor was NULL), avoiding unnecessary signals.

### 2. Naive Queue Implementation

**Problem**: Standard ring buffers or linked lists have issues:
- Ring buffer: Fixed size, requires resize or blocking
- Linked list: Allocation per message, poor cache locality

**libzmq's solution**: `yqueue_t` uses chunked allocation (N items per chunk) with spare chunk caching.

### 3. Missing Prefetch Optimization

**Problem**: Simple queues check emptiness on every read:
```rust
// WRONG - atomic load on every check
fn try_pop(&self) -> Option<T> {
    if self.is_empty() { return None; }  // atomic read
    // ...
}
```

**libzmq's solution**: The `_r` pointer caches the prefetched range, avoiding atomic operations until the local cache is exhausted.

### 4. False Sharing

**Problem**: Reader and writer state on same cache line causes cache thrashing:
```rust
// WRONG - reader and writer state adjacent
struct Queue {
    write_idx: AtomicUsize,  // Writer touches this
    read_idx: AtomicUsize,   // Reader touches this (CACHE LINE CONFLICT!)
}
```

### 5. PUB Fan-out Copying

**Problem**: Naive clone for each subscriber:
```rust
// WRONG - full copy for each subscriber
for sub in subscribers {
    sub.send(msg.clone());  // memcpy for each!
}
```

### 6. Event Loop Integration Overhead

**Problem**: Generic event loop callbacks may have overhead:
```rust
// Potential overhead from dynamic dispatch
event_loop.on_readable(fd, Box::new(|| { ... }));
```

---

## Critical libzmq Optimizations We Must Preserve

### The ypipe Algorithm

This is the crown jewel of libzmq's performance. Key insight:

```
Writer Thread                         Reader Thread
─────────────────                     ─────────────────
_w: first unflushed                   _r: first unprefetched
_f: flush point
         │                                    │
         ▼                                    ▼
┌────────────────────────────────────────────────────┐
│ [written] [written] [written] │ [prefetched] [read]│
└────────────────────────────────────────────────────┘
                                │
                                ▼
                    _c: cursor (SINGLE ATOMIC)
                    - Points past last flushed
                    - NULL = reader is sleeping

flush() algorithm:
1. If _w == _f: nothing to flush, return true
2. CAS(_c, _w, _f):
   - Success: reader was awake (c == _w), atomically publish
   - Failure: reader sleeping (c was NULL), set _c = _f non-atomically
3. Return whether reader was awake (determines if signal needed)

check_read() algorithm:
1. If front != _r && _r != NULL: have prefetched data, return true
2. CAS(_c, front, NULL): try to grab cursor, mark as sleeping
3. If CAS returned front (nothing new) or NULL (error): no data
4. Otherwise: _r = returned value, data available
```

**Why this is brilliant**:
- Single atomic variable for synchronization
- Writer can detect sleeping reader without extra synchronization
- Reader batches prefetches (grab everything up to cursor)
- No spurious wakeups

### The yqueue Chunked Allocation

```
Chunk 0          Chunk 1          Chunk 2
┌─────────┐     ┌─────────┐     ┌─────────┐
│ [0..N-1]│────►│ [N..2N-1│────►│[2N..3N-1│
│  prev ◄─┼─────┤  prev ◄─┼─────┤  prev   │
└─────────┘     └─────────┘     └─────────┘
     ▲                               ▲
  _begin                           _end

Spare chunk: Most recently freed chunk kept for reuse
- Producer/consumer at similar rates = no allocation in steady state
- Cache-aligned chunks prevent false sharing between chunks
```

---

## Detailed Queue Implementation

### Complete Lock-Free Pipe Queue

```rust
use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr::{null_mut, NonNull};
use std::alloc::{alloc, dealloc, Layout};
use std::mem::MaybeUninit;

/// Cache line size for alignment
const CACHE_LINE: usize = 64;

/// Number of items per chunk (tuned for message size)
const CHUNK_SIZE: usize = 256;

/// Lock-free SPSC queue with libzmq's ypipe algorithm
#[repr(C)]
pub struct YPipe<T> {
    // === Writer-only state (cache line 1) ===
    writer: CachePadded<WriterState<T>>,

    // === Reader-only state (cache line 2) ===
    reader: CachePadded<ReaderState<T>>,

    // === Shared synchronization point (cache line 3) ===
    cursor: CachePadded<AtomicPtr<Slot<T>>>,

    // === Spare chunk for allocation reuse (cache line 4) ===
    spare: CachePadded<AtomicPtr<Chunk<T>>>,
}

#[repr(C, align(64))]
struct CachePadded<T>(T);

struct WriterState<T> {
    /// Current write chunk
    chunk: NonNull<Chunk<T>>,
    /// Position in chunk
    pos: usize,
    /// First unflushed slot
    w: *mut Slot<T>,
    /// Flush-up-to pointer
    f: *mut Slot<T>,
}

struct ReaderState<T> {
    /// Current read chunk
    chunk: NonNull<Chunk<T>>,
    /// Position in chunk
    pos: usize,
    /// Prefetch end pointer
    r: *mut Slot<T>,
}

/// A chunk of slots
#[repr(C, align(64))]  // Cache-line aligned
struct Chunk<T> {
    slots: [Slot<T>; CHUNK_SIZE],
    next: AtomicPtr<Chunk<T>>,
    prev: *mut Chunk<T>,
}

/// A slot in the queue
struct Slot<T> {
    value: MaybeUninit<T>,
}

impl<T> YPipe<T> {
    pub fn new() -> Self {
        let chunk = Self::alloc_chunk();
        let first_slot = unsafe { (*chunk.as_ptr()).slots.as_mut_ptr() };

        Self {
            writer: CachePadded(WriterState {
                chunk,
                pos: 0,
                w: first_slot,
                f: first_slot,
            }),
            reader: CachePadded(ReaderState {
                chunk,
                pos: 0,
                r: null_mut(),
            }),
            cursor: CachePadded(AtomicPtr::new(first_slot)),
            spare: CachePadded(AtomicPtr::new(null_mut())),
        }
    }

    /// Write an item without flushing
    /// `incomplete` = true for multipart messages (don't advance flush point)
    #[inline]
    pub fn write(&mut self, value: T, incomplete: bool) {
        let writer = &mut self.writer.0;

        // Write value to current slot
        unsafe {
            let slot = &mut (*writer.chunk.as_ptr()).slots[writer.pos];
            slot.value.write(value);
        }

        // Advance to next slot
        writer.pos += 1;
        if writer.pos == CHUNK_SIZE {
            self.advance_write_chunk();
        }

        // Update flush point if message is complete
        if !incomplete {
            writer.f = self.write_slot_ptr();
        }
    }

    /// Flush written items to reader
    /// Returns `false` if reader was sleeping (caller must signal)
    #[inline]
    pub fn flush(&mut self) -> bool {
        let writer = &mut self.writer.0;

        // Nothing to flush?
        if writer.w == writer.f {
            return true;
        }

        // Try to atomically publish: CAS cursor from w to f
        let result = self.cursor.0.compare_exchange(
            writer.w,
            writer.f,
            Ordering::Release,  // Publish writes before cursor update
            Ordering::Relaxed,
        );

        match result {
            Ok(_) => {
                // CAS succeeded - reader was awake
                writer.w = writer.f;
                true
            }
            Err(_) => {
                // CAS failed - cursor was NULL, reader is sleeping
                // Safe to write non-atomically since reader won't touch it
                self.cursor.0.store(writer.f, Ordering::Release);
                writer.w = writer.f;
                false  // Caller must signal reader
            }
        }
    }

    /// Check if data is available for reading
    #[inline]
    pub fn check_read(&mut self) -> bool {
        let reader = &mut self.reader.0;

        // Have prefetched data?
        let front = self.read_slot_ptr();
        if front != reader.r && !reader.r.is_null() {
            return true;
        }

        // Try to prefetch: CAS cursor to NULL (mark as sleeping)
        let result = self.cursor.0.compare_exchange(
            front,
            null_mut(),
            Ordering::Acquire,  // Acquire published writes
            Ordering::Relaxed,
        );

        match result {
            Ok(_) | Err(p) if p.is_null() => {
                // No new data (cursor was front) or error (cursor was null)
                reader.r = front;
                false
            }
            Err(new_cursor) => {
                // Got new cursor position - data available up to there
                reader.r = new_cursor;
                true
            }
        }
    }

    /// Read an item from the queue
    #[inline]
    pub fn read(&mut self) -> Option<T> {
        if !self.check_read() {
            return None;
        }

        let reader = &mut self.reader.0;

        // Read value from current slot
        let value = unsafe {
            let slot = &(*reader.chunk.as_ptr()).slots[reader.pos];
            slot.value.assume_init_read()
        };

        // Advance to next slot
        reader.pos += 1;
        if reader.pos == CHUNK_SIZE {
            self.advance_read_chunk();
        }

        Some(value)
    }

    /// Rollback incomplete writes (for multipart message abort)
    pub fn unwrite(&mut self) -> Option<T> {
        let writer = &mut self.writer.0;

        // Can only unwrite unflushed items
        if writer.f == self.write_slot_ptr() {
            return None;
        }

        // Move back one position
        if writer.pos == 0 {
            writer.pos = CHUNK_SIZE - 1;
            // Move to previous chunk...
            unsafe {
                let prev = (*writer.chunk.as_ptr()).prev;
                writer.chunk = NonNull::new_unchecked(prev);
            }
        } else {
            writer.pos -= 1;
        }

        // Read back the value
        let value = unsafe {
            let slot = &(*writer.chunk.as_ptr()).slots[writer.pos];
            slot.value.assume_init_read()
        };

        Some(value)
    }

    // === Private helpers ===

    fn write_slot_ptr(&self) -> *mut Slot<T> {
        unsafe {
            (*self.writer.0.chunk.as_ptr())
                .slots
                .as_mut_ptr()
                .add(self.writer.0.pos)
        }
    }

    fn read_slot_ptr(&self) -> *mut Slot<T> {
        unsafe {
            (*self.reader.0.chunk.as_ptr())
                .slots
                .as_mut_ptr()
                .add(self.reader.0.pos)
        }
    }

    fn advance_write_chunk(&mut self) {
        let writer = &mut self.writer.0;

        // Try to reuse spare chunk
        let spare = self.spare.0.swap(null_mut(), Ordering::Relaxed);
        let next_chunk = if !spare.is_null() {
            unsafe { NonNull::new_unchecked(spare) }
        } else {
            Self::alloc_chunk()
        };

        // Link chunks
        unsafe {
            let current = writer.chunk.as_ptr();
            let next = next_chunk.as_ptr();
            (*current).next.store(next, Ordering::Release);
            (*next).prev = current;
        }

        writer.chunk = next_chunk;
        writer.pos = 0;
    }

    fn advance_read_chunk(&mut self) {
        let reader = &mut self.reader.0;

        unsafe {
            let old_chunk = reader.chunk.as_ptr();
            let next = (*old_chunk).next.load(Ordering::Acquire);
            reader.chunk = NonNull::new_unchecked(next);
            (*next).prev = null_mut();

            // Try to save as spare (swap so most recent is kept)
            let old_spare = self.spare.0.swap(old_chunk, Ordering::Relaxed);
            if !old_spare.is_null() {
                Self::dealloc_chunk(NonNull::new_unchecked(old_spare));
            }
        }

        reader.pos = 0;
    }

    fn alloc_chunk() -> NonNull<Chunk<T>> {
        let layout = Layout::new::<Chunk<T>>();
        unsafe {
            let ptr = alloc(layout) as *mut Chunk<T>;
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            // Initialize next/prev
            (*ptr).next = AtomicPtr::new(null_mut());
            (*ptr).prev = null_mut();
            NonNull::new_unchecked(ptr)
        }
    }

    fn dealloc_chunk(chunk: NonNull<Chunk<T>>) {
        let layout = Layout::new::<Chunk<T>>();
        unsafe {
            dealloc(chunk.as_ptr() as *mut u8, layout);
        }
    }
}

// Safety: YPipe is safe to send between threads as long as
// writer and reader are on separate threads
unsafe impl<T: Send> Send for YPipe<T> {}
```

---

## Event Loop Wakeup Optimization

### The Problem

Every `flush()` that returns `false` needs to signal the reader. But:
1. The reader might be polling on multiple sockets
2. We can't call into the event loop from arbitrary contexts
3. Multiple flushes before the reader runs = wasted signals

### Solution: Coalesced Wakeup with Pending Flag

```rust
/// Efficiently signals an event loop
pub struct LoopSignaler {
    /// Async handle for cross-thread wakeup
    async_handle: AsyncHandle,

    /// Pending signal flag (avoids syscall if already pending)
    pending: AtomicBool,

    /// Associated event loop (for same-thread optimization)
    loop_thread_id: ThreadId,
}

impl LoopSignaler {
    /// Signal the event loop (safe from any thread)
    #[inline]
    pub fn signal(&self) {
        // Fast path: already pending, skip syscall
        if self.pending.swap(true, Ordering::AcqRel) {
            return;  // Already pending, someone else will wake it
        }

        // Same thread? No need to signal.
        if std::thread::current().id() == self.loop_thread_id {
            return;
        }

        // Actually send the signal
        self.async_handle.send();
    }

    /// Called by event loop when processing signals
    pub fn consume(&self) {
        self.pending.store(false, Ordering::Release);
    }
}
```

### Integration with Pipe

```rust
impl Pipe {
    pub fn flush(&mut self) -> FlushResult {
        let reader_awake = self.queue.flush();

        if reader_awake {
            FlushResult::ReaderAwake
        } else {
            // Reader was sleeping - signal needed
            self.signaler.signal();
            FlushResult::ReaderSignaled
        }
    }
}
```

### Deferred Signal Batching

For high-throughput scenarios, batch signals:

```rust
/// Accumulates signals, sends once
pub struct BatchedSignaler {
    signalers: Vec<&LoopSignaler>,
}

impl BatchedSignaler {
    pub fn add(&mut self, signaler: &LoopSignaler) {
        // Only add if not already pending
        if !signaler.pending.load(Ordering::Relaxed) {
            self.signalers.push(signaler);
        }
    }

    pub fn flush(self) {
        // Deduplicate by loop
        let mut seen_loops: HashSet<*const AsyncHandle> = HashSet::new();

        for sig in self.signalers {
            let handle_ptr = &sig.async_handle as *const _;
            if seen_loops.insert(handle_ptr) {
                sig.signal();
            }
        }
    }
}

// Usage in PUB socket fan-out:
pub fn send_to_subscribers(&mut self, msg: Message) {
    let mut batch = BatchedSignaler::new();

    for sub in &mut self.subscribers {
        sub.pipe.write(msg.share());
        if !sub.pipe.flush_no_signal() {
            batch.add(&sub.signaler);
        }
    }

    batch.flush();  // Single signal even for 1000 subscribers
}
```

---

## Memory Management

### Message Buffer Pooling

```rust
/// Pool of pre-allocated message buffers
pub struct BufferPool {
    /// Small buffers (≤256 bytes)
    small: SegmentedPool<256>,
    /// Medium buffers (≤4KB)
    medium: SegmentedPool<4096>,
    /// Large buffers (≤64KB)
    large: SegmentedPool<65536>,

    /// Statistics
    stats: PoolStats,
}

/// Lock-free buffer pool using thread-local caches
struct SegmentedPool<const SIZE: usize> {
    /// Global pool (lock-free stack)
    global: AtomicStack<Buffer<SIZE>>,
    /// Per-thread cache
    local: ThreadLocal<LocalCache<SIZE>>,
}

struct LocalCache<const SIZE: usize> {
    /// Local free list (no synchronization needed)
    buffers: Vec<Buffer<SIZE>>,
    /// Max local cache size
    max_cached: usize,
}

impl<const SIZE: usize> SegmentedPool<SIZE> {
    pub fn acquire(&self) -> Buffer<SIZE> {
        // Try local cache first
        if let Some(cache) = self.local.get() {
            if let Some(buf) = cache.buffers.pop() {
                return buf;
            }
        }

        // Try global pool
        if let Some(buf) = self.global.pop() {
            return buf;
        }

        // Allocate new
        Buffer::new()
    }

    pub fn release(&self, buf: Buffer<SIZE>) {
        let cache = self.local.get_or_default();

        if cache.buffers.len() < cache.max_cached {
            // Keep in local cache
            cache.buffers.push(buf);
        } else {
            // Return to global pool
            self.global.push(buf);
        }
    }
}
```

### Zero-Copy Reference Counting

```rust
/// Reference-counted message data with zero-copy support
pub struct MsgData {
    inner: NonNull<MsgDataInner>,
}

#[repr(C)]
struct MsgDataInner {
    /// Reference count
    refcount: AtomicU32,

    /// Data length
    len: u32,

    /// Capacity (for owned buffers)
    capacity: u32,

    /// How to free this buffer
    free_fn: FreeFn,

    /// Free function hint
    hint: *mut c_void,

    /// Inline data (for small messages)
    /// OR pointer to external data
    data: DataStorage,
}

enum DataStorage {
    /// Small message: data inline
    Inline([u8; 48]),  // Total MsgDataInner = 64 bytes = 1 cache line

    /// Large message: pointer to data
    External(*mut u8),
}

type FreeFn = unsafe extern "C" fn(data: *mut c_void, hint: *mut c_void);

impl MsgData {
    /// Create with zero-copy buffer
    pub fn from_external(
        data: *mut u8,
        len: usize,
        free_fn: FreeFn,
        hint: *mut c_void,
    ) -> Self {
        // Allocate header only, data stays external
        let inner = Box::new(MsgDataInner {
            refcount: AtomicU32::new(1),
            len: len as u32,
            capacity: 0,  // External
            free_fn,
            hint,
            data: DataStorage::External(data),
        });

        Self {
            inner: NonNull::from(Box::leak(inner)),
        }
    }

    /// Clone with copy-on-write semantics
    pub fn share(&self) -> Self {
        unsafe {
            (*self.inner.as_ptr())
                .refcount
                .fetch_add(1, Ordering::Relaxed);
        }
        Self { inner: self.inner }
    }

    /// Get mutable access, cloning if shared
    pub fn make_mut(&mut self) -> &mut [u8] {
        let inner = unsafe { self.inner.as_ref() };

        if inner.refcount.load(Ordering::Relaxed) == 1 {
            // Sole owner, safe to mutate
            return self.data_mut();
        }

        // Shared - need to copy
        let new_data = self.data().to_vec();
        let new_inner = Box::new(MsgDataInner {
            refcount: AtomicU32::new(1),
            len: new_data.len() as u32,
            capacity: new_data.capacity() as u32,
            free_fn: default_free,
            hint: null_mut(),
            data: DataStorage::External(Box::into_raw(new_data.into_boxed_slice()) as *mut u8),
        });

        // Decrement old refcount
        self.dec_ref();

        self.inner = NonNull::from(Box::leak(new_inner));
        self.data_mut()
    }
}

impl Clone for MsgData {
    fn clone(&self) -> Self {
        self.share()  // Cheap reference count increment
    }
}

impl Drop for MsgData {
    fn drop(&mut self) {
        self.dec_ref();
    }
}
```

---

## Cache Optimization

### Structure Layout

```rust
/// Socket state with cache-optimized layout
#[repr(C)]
pub struct SocketState {
    // === Hot path (first cache line) ===
    /// Flags checked on every operation
    flags: SocketFlags,
    /// Cached readability
    readable: bool,
    /// Cached writability
    writable: bool,
    _pad1: [u8; 5],

    /// Inline pipe for single-connection case
    inline_pipe: Option<NonNull<Pipe>>,

    /// Quick check: any pipes available?
    pipe_count: u32,
    _pad2: u32,

    // === Second cache line: options ===
    options: SocketOptions,

    // === Third cache line: callbacks (rarely accessed in hot path) ===
    callbacks: SocketCallbacks,

    // === Rest: pipe storage, endpoints, etc. ===
    pipes: PipeSet,
    listeners: Vec<Listener>,
    connectors: Vec<Connector>,
}

bitflags! {
    struct SocketFlags: u8 {
        const CLOSED = 0x01;
        const TERMINATING = 0x02;
        const BOUND = 0x04;
        const CONNECTED = 0x08;
    }
}
```

### Pipe Set Optimization

```rust
/// Optimized pipe collection for common patterns
pub struct PipeSet {
    /// Single pipe (common case for point-to-point)
    inline: Option<PipeId>,

    /// Multiple pipes (allocated only when needed)
    extended: Option<Box<ExtendedPipeSet>>,
}

struct ExtendedPipeSet {
    /// All pipes by ID
    pipes: HashMap<PipeId, Pipe>,

    /// Pipes with data to read (maintained as we go)
    readable: IndexSet<PipeId>,

    /// Pipes that can accept writes
    writable: IndexSet<PipeId>,

    /// Routing table (for ROUTER socket)
    routing: Option<HashMap<RoutingId, PipeId>>,
}

impl PipeSet {
    /// Fast path: single pipe
    #[inline]
    pub fn get_single(&mut self) -> Option<&mut Pipe> {
        self.inline.as_ref().and_then(|id| {
            self.extended
                .as_mut()
                .and_then(|e| e.pipes.get_mut(id))
        })
    }

    /// Get next readable pipe (fair queue)
    #[inline]
    pub fn next_readable(&mut self) -> Option<&mut Pipe> {
        // Single pipe fast path
        if let Some(id) = self.inline {
            if let Some(ext) = &mut self.extended {
                if let Some(pipe) = ext.pipes.get_mut(&id) {
                    if pipe.has_data() {
                        return Some(pipe);
                    }
                }
            }
            return None;
        }

        // Multiple pipes: round-robin through readable set
        let ext = self.extended.as_mut()?;
        let id = ext.readable.pop()?;
        let pipe = ext.pipes.get_mut(&id)?;

        // If still readable, add back to end
        if pipe.has_data() {
            ext.readable.insert(id);
        }

        Some(pipe)
    }
}
```

---

## Batching Strategies

### Read Batching

```rust
impl Socket {
    /// Read multiple messages efficiently
    pub fn recv_batch(&mut self, out: &mut Vec<Message>, max: usize) -> usize {
        let mut count = 0;

        while count < max {
            // Try each readable pipe
            match self.recv_one() {
                Some(msg) => {
                    out.push(msg);
                    count += 1;
                }
                None => break,
            }
        }

        count
    }

    /// Process all pending in single event callback
    pub fn drain_readable(&mut self) -> impl Iterator<Item = Message> + '_ {
        std::iter::from_fn(move || self.recv_one())
    }
}

// Usage in event loop:
socket.on_readable(|| {
    for msg in socket.drain_readable().take(1000) {
        process(msg);
    }
});
```

### Write Batching

```rust
impl Socket {
    /// Write multiple messages, single flush
    pub fn send_batch(&mut self, msgs: impl IntoIterator<Item = Message>) -> SendBatchResult {
        let mut sent = 0;
        let mut failed = Vec::new();

        for msg in msgs {
            match self.write_no_flush(msg) {
                Ok(()) => sent += 1,
                Err(msg) => failed.push(msg),
            }
        }

        // Single flush at end
        self.flush_all();

        SendBatchResult { sent, failed }
    }

    fn write_no_flush(&mut self, msg: Message) -> Result<(), Message> {
        // Pattern-specific write without flushing
        self.state.write_no_flush(&mut self.common, msg)
    }

    fn flush_all(&mut self) {
        // Flush all pipes that have pending data
        let mut signaler = BatchedSignaler::new();

        for pipe in self.common.pipes.iter_mut() {
            if !pipe.flush_no_signal() {
                signaler.add(&pipe.signaler);
            }
        }

        signaler.flush();
    }
}
```

### Connection I/O Batching

```rust
impl Connection {
    /// Process all available reads
    pub fn drain_reads(&mut self) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut buffer = [0u8; 65536];  // Reusable read buffer

        loop {
            match self.transport.read(&mut buffer) {
                Ok(0) => break,  // EOF
                Ok(n) => {
                    // Decode all complete messages
                    self.codec.extend(&buffer[..n]);
                    while let Some(msg) = self.codec.decode_one() {
                        messages.push(msg);
                    }
                }
                Err(ref e) if e.kind() == WouldBlock => break,
                Err(_) => break,
            }
        }

        messages
    }

    /// Write with vectored I/O
    pub fn flush_writes(&mut self) -> Result<(), Error> {
        let mut iovecs = Vec::with_capacity(16);

        // Gather pending writes into iovec
        while let Some(chunk) = self.send_queue.peek_chunk() {
            iovecs.push(IoSlice::new(chunk));
            if iovecs.len() >= 16 {
                break;
            }
        }

        if iovecs.is_empty() {
            return Ok(());
        }

        // Single writev syscall for all chunks
        let written = self.transport.write_vectored(&iovecs)?;
        self.send_queue.consume(written);

        Ok(())
    }
}
```

---

## Zero-Copy Message Handling

### PUB Fan-out Without Copying

```rust
impl PubState {
    pub fn send(&mut self, common: &mut SocketCommon, msg: Message) {
        // Share the message data (just increment refcount)
        // Don't clone until necessary
        let shared_data = msg.share_data();

        for &pipe_id in &self.subscribers {
            let pipe = common.pipes.get_mut(pipe_id);

            // Create message with shared data
            let sub_msg = Message::from_shared(shared_data.share());

            match pipe.write_no_flush(sub_msg) {
                Ok(()) => {}
                Err(_) => {
                    // PUB drops on HWM - that's fine
                }
            }
        }

        // Batch flush all pipes
        common.pipes.flush_all();
    }
}
```

### User-Provided Buffer Support

```rust
/// Send with zero-copy from user buffer
pub fn send_external(
    &mut self,
    data: &[u8],
    free_fn: extern "C" fn(*mut c_void, *mut c_void),
    hint: *mut c_void,
) -> Result<(), Error> {
    let msg = Message::from_external(
        data.as_ptr() as *mut u8,
        data.len(),
        free_fn,
        hint,
    );

    self.send(msg)
}

// Example: send from mmap'd file
let data = mmap_file("data.bin");
socket.send_external(
    &data,
    |ptr, _| munmap(ptr),
    std::ptr::null_mut(),
)?;
```

---

## Benchmarking Targets

### Latency Targets

| Operation | Target | libzmq Baseline |
|-----------|--------|-----------------|
| Inproc send/recv (single thread) | < 200 ns | ~500 ns |
| Inproc send/recv (cross-thread) | < 1 μs | ~2-3 μs |
| TCP loopback RTT | < 10 μs | ~15-20 μs |
| Event loop wakeup | < 500 ns | N/A |

### Throughput Targets

| Scenario | Target | libzmq Baseline |
|----------|--------|-----------------|
| Inproc 1:1 | > 30M msg/s | ~15M msg/s |
| TCP 1:1 (small msgs) | > 5M msg/s | ~2-3M msg/s |
| TCP 1:1 (64KB msgs) | > 10 GB/s | ~5 GB/s |
| PUB:SUB 1:1000 | > 1M msg/s broadcast | ~500K msg/s |

### Memory Targets

| Metric | Target |
|--------|--------|
| Per-socket overhead | < 2 KB |
| Per-connection overhead | < 4 KB |
| Per-message overhead (small) | 0 (inline) |
| Per-message overhead (large) | 64 bytes header |

### Key Benchmark Scenarios

1. **Ping-pong latency**: Single message RTT
2. **Throughput**: Sustained message rate
3. **Fan-out**: PUB to N subscribers
4. **Fan-in**: N producers to single consumer
5. **Mixed workload**: Varied message sizes
6. **Connection churn**: Rapid connect/disconnect
7. **Memory stability**: Long-running with allocation tracking

---

## Summary: What We Must Get Right

1. **YPipe algorithm**: Single CAS, sleeping reader detection, prefetch batching
2. **Chunked allocation**: Amortize allocation, spare chunk reuse
3. **Cache alignment**: Separate reader/writer state, 64-byte alignment
4. **Signal coalescing**: Pending flag to avoid redundant syscalls
5. **Reference-counted messages**: Zero-copy fan-out and external buffers
6. **Batched I/O**: Vectored writes, drain reads
7. **Thread-local caching**: Buffer pools with local caches

The simplified design is a good starting point, but these optimizations are non-negotiable for competitive performance.

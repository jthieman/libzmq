# Modern Alternatives to ypipe

Rethinking inter-component communication for event-loop architectures.

## What Does ypipe Actually Solve?

Let's decompose ypipe's responsibilities:

| Responsibility | ypipe's Solution | Is It Still Needed? |
|----------------|------------------|---------------------|
| Lock-free SPSC queue | Chunked linked list with single CAS | Maybe - depends on architecture |
| "Is reader sleeping?" detection | Cursor NULL trick | No - event loops solve this differently |
| Batch visibility (multipart) | Flush pointer | Yes, but can be simpler |
| Memory efficiency | Spare chunk reuse | Yes, but allocators are better now |
| Cross-thread signaling | Returns bool to trigger signaler | Yes, but better primitives exist |

## Key Insight: Most Communication is Same-Loop

In libzmq's architecture, **everything** goes through pipes:
```
App Thread → pipe → I/O Thread → pipe → Session → Engine → Network
```

In an event-loop architecture, most of this is **same-thread**:
```
Event Loop:
  Socket.send() → Engine.write() → TCP buffer  (all same thread!)
  TCP readable → Engine.decode() → Socket.on_message()  (all same thread!)
```

The only cross-thread communication is:
1. **Cross-loop sockets**: App using sockets from another thread (uncommon)
2. **Thread pool dispatch**: If we offload work (optional)
3. **Context shutdown**: Coordination signal (rare)

## Alternative Architecture: Direct Delivery + Selective Queuing

### Principle: Queue Only When Necessary

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Event Loop                                    │
│                                                                      │
│  ┌──────────┐     direct call      ┌──────────┐                    │
│  │  Socket  │─────────────────────►│  Engine  │                    │
│  │  (PUSH)  │◄─────────────────────│  (TCP)   │                    │
│  └──────────┘     direct call      └──────────┘                    │
│       │                                  │                          │
│       │                                  │                          │
│       ▼                                  ▼                          │
│  No queue needed!              Kernel handles buffering             │
│  Same thread = direct call     (TCP send buffer, io_uring SQ)      │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

Cross-loop (rare):
┌──────────────┐                    ┌──────────────┐
│  Event Loop  │    Simple SPSC    │  Event Loop  │
│      A       │═══════════════════│      B       │
│              │  + futex signal   │              │
└──────────────┘                    └──────────────┘
```

### Same-Loop "Pipe" - Zero Overhead

```rust
/// Connection between components on the same event loop
/// No queue, no atomics, no signaling - just function calls
struct LocalPipe {
    /// Direct reference to peer (Rc because same thread)
    peer: Rc<RefCell<PipeEndpoint>>,

    /// Flow control state (no atomics needed - single threaded)
    credits: u32,
    hwm: u32,
}

impl LocalPipe {
    #[inline]
    fn write(&mut self, msg: Message) -> WriteResult {
        if self.credits == 0 {
            return WriteResult::WouldBlock(msg);
        }

        self.credits -= 1;

        // Direct delivery - no queue!
        self.peer.borrow_mut().on_message(msg);

        WriteResult::Ok
    }

    #[inline]
    fn grant_credits(&mut self, n: u32) {
        self.credits = (self.credits + n).min(self.hwm);
    }
}
```

**Performance**: Zero overhead. Just a function call + refcount check.

### Cross-Loop Communication - When Actually Needed

For the rare case of cross-thread sockets, we need a queue. But it can be simpler:

```rust
/// Cross-loop pipe using modern primitives
struct RemotePipe {
    /// Simple bounded SPSC queue (not ypipe complexity)
    queue: SpscQueue<Message>,

    /// Modern signaling
    signaler: FutexSignaler,

    /// Flow control
    credits: AtomicU32,
}
```

## Modern Signaling Alternatives

### Option 1: Futex (Linux) - Simplest

```rust
/// Futex-based signaler - simpler than ypipe's cursor trick
pub struct FutexSignaler {
    /// 0 = no data, 1 = has data, 2 = waiting
    state: AtomicU32,
}

impl FutexSignaler {
    /// Producer: signal that data is available
    #[inline]
    pub fn notify(&self) {
        let old = self.state.swap(1, Ordering::Release);
        if old == 2 {
            // Consumer was waiting - wake it
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    &self.state as *const _ as *const u32,
                    libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                    1,  // Wake one waiter
                );
            }
        }
        // If old was 0 or 1, consumer will see it on next check
    }

    /// Consumer: wait for data (with timeout)
    pub fn wait(&self, timeout: Option<Duration>) -> bool {
        loop {
            // Try to transition to waiting state
            match self.state.compare_exchange(
                0, 2,
                Ordering::Acquire,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Successfully marked as waiting, now actually wait
                    let ts = timeout.map(|d| libc::timespec {
                        tv_sec: d.as_secs() as i64,
                        tv_nsec: d.subsec_nanos() as i64,
                    });

                    unsafe {
                        libc::syscall(
                            libc::SYS_futex,
                            &self.state as *const _ as *const u32,
                            libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                            2,  // Expected value
                            ts.as_ref().map(|t| t as *const _).unwrap_or(std::ptr::null()),
                        );
                    }
                    // Spurious wakeup possible, loop will recheck
                }
                Err(1) => {
                    // Has data! Consume it.
                    self.state.store(0, Ordering::Release);
                    return true;
                }
                Err(_) => {
                    // Unexpected state, retry
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Consumer: check without blocking
    #[inline]
    pub fn try_consume(&self) -> bool {
        self.state.compare_exchange(
            1, 0,
            Ordering::Acquire,
            Ordering::Relaxed,
        ).is_ok()
    }
}
```

**Comparison to ypipe's signaling**:
- ypipe: Cursor NULL trick + separate signaler_t with eventfd/pipe
- Futex: Single atomic + kernel-assisted wait
- **Futex wins**: Fewer moving parts, well-optimized in kernel

### Option 2: eventfd with Coalescing

```rust
/// Eventfd signaler with coalesced wakeups
pub struct EventfdSignaler {
    fd: RawFd,
    /// Tracks if signal is pending (avoids redundant writes)
    pending: AtomicBool,
}

impl EventfdSignaler {
    pub fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            pending: AtomicBool::new(false),
        })
    }

    /// Signal (coalesced - multiple signals = one syscall)
    #[inline]
    pub fn signal(&self) {
        // Fast path: already signaled, skip syscall
        if self.pending.swap(true, Ordering::Release) {
            return;
        }

        let val: u64 = 1;
        unsafe {
            libc::write(self.fd, &val as *const _ as *const libc::c_void, 8);
        }
    }

    /// Consume signal (call after poll returns)
    pub fn consume(&self) {
        let mut val: u64 = 0;
        unsafe {
            libc::read(self.fd, &mut val as *mut _ as *mut libc::c_void, 8);
        }
        self.pending.store(false, Ordering::Release);
    }

    /// Get fd for polling
    pub fn fd(&self) -> RawFd {
        self.fd
    }
}
```

### Option 3: io_uring MSG_RING (Kernel 5.18+)

```rust
/// io_uring based cross-ring signaling - ZERO SYSCALLS with SQPOLL
pub struct IoUringSignaler {
    target_ring_fd: RawFd,
    pending: AtomicBool,
}

impl IoUringSignaler {
    /// Signal another io_uring instance
    pub fn signal(&self, source_ring: &mut IoUring) {
        if self.pending.swap(true, Ordering::Release) {
            return;
        }

        // Queue a MSG_RING operation
        let sqe = source_ring.get_sqe().unwrap();
        sqe.prep_msg_ring(
            self.target_ring_fd,
            0,  // len (unused)
            SIGNAL_USER_DATA,  // user_data for target CQE
            0,  // flags
        );

        // With SQPOLL, this doesn't even need a syscall!
    }
}
```

**This is the most performant option on modern Linux**: With SQPOLL mode, cross-loop signaling requires **zero syscalls**.

### Option 4: The "Batch Check" Model

If latency tolerance allows (microseconds, not nanoseconds):

```rust
/// No explicit signaling - just periodic checking
pub struct BatchedEventLoop {
    /// Queues from other loops
    inbound_queues: Vec<RemoteQueue>,

    /// Check interval
    batch_interval: Duration,
}

impl BatchedEventLoop {
    pub fn run(&mut self) {
        loop {
            // 1. Check all inbound queues (no signaling needed)
            for queue in &mut self.inbound_queues {
                while let Some(msg) = queue.try_recv() {
                    self.process(msg);
                }
            }

            // 2. Do normal event loop work
            let events = self.poller.wait(self.batch_interval);
            self.process_events(events);
        }
    }
}
```

**Tradeoff**: Adds up to `batch_interval` latency for cross-loop messages, but zero signaling overhead.

## Modern SPSC Queue Alternatives

### Option 1: Simple Ring Buffer

For bounded queues (which we want for HWM anyway):

```rust
/// Cache-line padded ring buffer
#[repr(C)]
pub struct RingQueue<T, const N: usize> {
    // Writer cache line
    write_pos: CachePadded<AtomicUsize>,

    // Reader cache line
    read_pos: CachePadded<AtomicUsize>,

    // Data
    buffer: Box<[UnsafeCell<MaybeUninit<T>>; N]>,
}

impl<T, const N: usize> RingQueue<T, N> {
    #[inline]
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let write = self.write_pos.load(Ordering::Relaxed);
        let read = self.read_pos.load(Ordering::Acquire);

        if write.wrapping_sub(read) >= N {
            return Err(value);  // Full
        }

        unsafe {
            (*self.buffer[write % N].get()).write(value);
        }

        self.write_pos.store(write.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    #[inline]
    pub fn try_pop(&self) -> Option<T> {
        let read = self.read_pos.load(Ordering::Relaxed);
        let write = self.write_pos.load(Ordering::Acquire);

        if read == write {
            return None;  // Empty
        }

        let value = unsafe {
            (*self.buffer[read % N].get()).assume_init_read()
        };

        self.read_pos.store(read.wrapping_add(1), Ordering::Release);
        Some(value)
    }
}
```

**Why this might be fine**:
- Modern CPUs have excellent branch prediction
- L1 cache is fast enough that the extra atomic loads don't matter much
- Simpler = fewer bugs

### Option 2: Crossbeam's ArrayQueue

Just use a well-tested implementation:

```rust
use crossbeam_queue::ArrayQueue;

struct Pipe {
    queue: ArrayQueue<Message>,
    signaler: FutexSignaler,
}
```

Crossbeam's queues are extremely well-optimized and battle-tested.

### Option 3: Chunked Queue (When Unbounded Needed)

If we need unbounded (or very large HWM):

```rust
/// Simplified chunked queue - no spare chunk complexity
pub struct ChunkedQueue<T> {
    head: AtomicPtr<Chunk<T>>,
    tail: UnsafeCell<*mut Chunk<T>>,

    // Use a thread-local allocator for chunks
    allocator: ChunkAllocator<T>,
}

struct Chunk<T> {
    data: [MaybeUninit<T>; 64],
    next: AtomicPtr<Chunk<T>>,
    write_pos: AtomicUsize,
    read_pos: UnsafeCell<usize>,
}
```

**Key simplification vs ypipe**: Use a proper allocator (jemalloc, mimalloc) instead of manual spare chunk management. Modern allocators have thread-local caches that achieve similar performance with less complexity.

## The "No Queue" Path: Network I/O

For network sockets, we might not need queues at all:

```rust
impl TcpEngine {
    /// Send a message - goes directly to kernel buffer
    pub fn send(&mut self, msg: &Message) -> io::Result<SendStatus> {
        // With io_uring: just queue the write operation
        // The kernel's send buffer IS our queue

        if self.ring.sq_space_left() == 0 {
            return Ok(SendStatus::WouldBlock);
        }

        let sqe = self.ring.get_sqe().unwrap();
        sqe.prep_write_fixed(self.fd, msg.as_bytes(), self.buffer_idx);

        Ok(SendStatus::Queued)
    }
}
```

**Insight**: io_uring's submission queue + kernel send buffer replaces our need for userspace queuing on the network path.

## Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SAME EVENT LOOP                                │
│                                                                          │
│   ┌────────┐         direct call         ┌────────┐                     │
│   │ Socket │ ──────────────────────────► │ Engine │                     │
│   │        │ ◄────────────────────────── │        │                     │
│   └────────┘         direct call         └────────┘                     │
│        │                                      │                          │
│        │ Rc<RefCell> for inproc peers        │ io_uring SQ              │
│        ▼                                      ▼                          │
│   Zero queuing overhead               Kernel handles buffering          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                          CROSS EVENT LOOP (rare)                         │
│                                                                          │
│   ┌────────────────┐                      ┌────────────────┐            │
│   │  Event Loop A  │                      │  Event Loop B  │            │
│   │                │                      │                │            │
│   │  ┌──────────┐  │   ArrayQueue<Msg>   │  ┌──────────┐  │            │
│   │  │  Socket  │══╬═════════════════════╬══│  Socket  │  │            │
│   │  └──────────┘  │  + futex/eventfd    │  └──────────┘  │            │
│   │                │    signaling        │                │            │
│   └────────────────┘                      └────────────────┘            │
│                                                                          │
│   Simple bounded queue + modern signaler                                │
│   (Not ypipe - unnecessary complexity for rare path)                    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Summary: What We Actually Need

| Component | ypipe Approach | Modern Approach |
|-----------|---------------|-----------------|
| Same-loop messaging | N/A (didn't exist) | Direct function calls |
| Cross-loop queue | Complex ypipe | Simple ArrayQueue |
| Cross-loop signaling | Cursor NULL + signaler | Futex or io_uring MSG_RING |
| Memory allocation | Manual spare chunk | Modern allocator (jemalloc) |
| Network buffering | Userspace queue | io_uring SQ + kernel buffers |
| Multipart batching | Flush pointer | Can keep, but simpler impl |

## Performance Comparison

| Path | ypipe-based | Modern Approach |
|------|-------------|-----------------|
| Same-loop inproc | ~500ns (queue overhead) | ~10ns (direct call) |
| Cross-loop inproc | ~2μs | ~1-2μs (similar) |
| Network send | ~500ns to queue | ~100ns (direct to io_uring) |
| Signal overhead | eventfd write | 0 with io_uring MSG_RING |

## Conclusion

**Don't port ypipe**. Instead:

1. **Same-loop paths**: Direct `Rc<RefCell>` references, zero queueing
2. **Cross-loop paths**: `crossbeam::ArrayQueue` + `FutexSignaler`
3. **Network I/O**: Let io_uring handle the buffering
4. **Memory**: Trust modern allocators, don't hand-roll spare chunks

The result is simpler code that's actually **faster** because:
- Most paths avoid queues entirely (same-loop)
- Modern primitives (futex, io_uring) are better than decade-old workarounds
- Less code = fewer cache misses, better branch prediction

ypipe was brilliant for its time. For a modern event-loop library, it's unnecessary complexity.

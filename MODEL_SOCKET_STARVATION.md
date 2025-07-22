# Model Socket Starvation: Client Timeout Behavior

## Problem Statement

Current lading UDS generator (lading/src/generator/unix_datagram.rs:277-289) tracks failed sends but doesn't accurately model client timeout behavior. The generator uses async `socket.send()` which blocks until the socket is ready, rather than failing quickly when the socket cannot accept data within a timeout window.

Real clients (e.g., datadog-go) use a 100ms write timeout ([source](https://github.com/DataDog/datadog-go/blob/a6b9cdf87a42985c32e5abfc1e6e10e349107ee0/statsd/options.go#L19)). When Agent cannot keep up, clients timeout and drop metrics. Lading needs to model this behavior to accurately measure performance under CPU starvation conditions.

## Current Implementation Analysis

### What Works Today
- Failed sends are tracked via `request_failure` metric
- Throttle mechanism controls send rate via `throttle.wait_for(total_bytes)`
- Connection failures are tracked and retried
- "Peek then consume" pattern ensures throttle accuracy

### What's Missing
- No timeout modeling - sends block indefinitely
- No explicit queue to track pending writes
- Cannot simulate client-side timeouts and dropped metrics
- Backpressure isn't accurately measured

### Key Implementation Insights

After analyzing the codebase:
- **Throttle has no capacity return mechanism** - this is by design and actually helps our use case
- **Throttle measures attempted load**, not successful sends - this is the correct behavior
- **Current implementation uses datagram semantics** - no partial writes, no retries

## Technical Design: Timeout-Aware Send Layer

### Core Architecture

Introduce a thin timeout modeling layer between throttle and socket:

```rust
// Current flow:
throttle.wait_for(bytes) → socket.send(block) → track_metrics

// Proposed flow:
throttle.wait_for(bytes) → timeout_aware_send(block) → track_metrics
                                     ↓
                          internal_queue_management
```

### Implementation Design

```rust
struct TimeoutAwareSender {
    socket: UnixDatagram,
    timeout: Duration,
    pending_queue: VecDeque<(Instant, Vec<u8>)>,
    max_queue_size: usize,
}

impl TimeoutAwareSender {
    async fn send(&mut self, data: &[u8]) -> SendResult {
        // 1. Expire old entries first (deterministic)
        self.expire_old_entries();

        // 2. Try non-blocking send
        match self.socket.try_send(data) {
            Ok(bytes) => SendResult::Success(bytes),
            Err(e) if e.kind() == WouldBlock => {
                // 3. Add to queue if under limit
                if self.pending_queue.len() < self.max_queue_size {
                    self.pending_queue.push_back((Instant::now(), data.to_vec()));
                    SendResult::Queued
                } else {
                    SendResult::Dropped(DropReason::QueueFull)
                }
            }
            Err(e) => SendResult::Failed(e),
        }
    }

    fn expire_old_entries(&mut self) {
        let cutoff = Instant::now() - self.timeout;
        while let Some((timestamp, _)) = self.pending_queue.front() {
            if *timestamp < cutoff {
                self.pending_queue.pop_front();
                // Track expiration metric
            } else {
                break;
            }
        }
    }
}
```

### Key Design Decisions

1. **Maintain Throttle Semantics**: The throttle continues to control the rate of send attempts, not successful sends. This accurately models real client behavior where clients attempt to send at a configured rate regardless of success.

2. **Bounded Queue**: Use a fixed-size queue to:
   - Prevent unbounded memory growth
   - Maintain determinism
   - Model realistic client behavior (clients have finite buffers)

3. **Deterministic Expiration**: Check for expired entries on each send attempt, not via background timers. This ensures:
   - Predictable behavior
   - No additional threads/tasks
   - Consistent measurement

4. **Clear Result Types**: Distinguish between:
   - Success: Data sent immediately
   - Queued: Added to pending queue
   - Dropped: Queue full or other failure
   - Expired: Timed out in queue

### Integration Points

The implementation requires minimal changes to existing code:

1. **In Child::spin()**: Replace direct socket.send() with TimeoutAwareSender
2. **Configuration**: Add optional timeout parameters
3. **Metrics**: Add new counters for queue behavior
4. **No throttle changes**: Throttle remains unchanged

### Coordinated Omission Avoidance

This design avoids coordinated omission by:
- Attempting sends on every throttle tick (as required by blt's comment)
- Never blocking on socket readiness
- Tracking all attempted operations, not just successful ones
- Maintaining the expected send rate regardless of socket congestion

## Implementation Steps

1. **Create TimeoutAwareSender struct**
   - Implement non-blocking send logic
   - Add queue management with expiration
   - Define clear result types

2. **Integrate into unix_datagram generator**
   - Replace socket.send() calls
   - Maintain existing metrics
   - Add new timeout-specific metrics

3. **Add Configuration**
   ```yaml
   generator:
     unix_datagram:
       path: /var/run/datadog/dsd.socket
       bytes_per_second: 10_000_000
       client_behavior:
         timeout_ms: 100        # Optional, defaults to no timeout
         max_queue_depth: 1000  # Optional, defaults to reasonable limit
   ```

4. **Testing Strategy**
   - Unit tests for TimeoutAwareSender in isolation
   - Property tests for queue bounds and determinism
   - Integration tests with real sockets
   - Verification that throttle rate matches attempted sends

## Metrics to Track

Existing metrics remain unchanged:
- `bytes_written`: Successfully sent bytes
- `packets_sent`: Successfully sent packets
- `request_failure`: Failed send attempts

New metrics for timeout behavior:
- `writes_queued`: Writes added to pending queue
- `writes_expired`: Writes that exceeded timeout window
- `writes_dropped`: Writes dropped due to queue limits
- `queue_depth`: Current number of pending writes
- `send_attempt_result{result="success|queued|dropped|expired"}`: Result distribution

## Benefits of This Approach

1. **Accurate Client Modeling**: Matches real client behavior under load
2. **Throttle Accuracy**: Maintains precise control over attempted load
3. **Deterministic**: No background tasks or non-deterministic draining
4. **Testable**: Timeout layer can be tested in isolation
5. **Backward Compatible**: Opt-in via configuration
6. **Performance**: Minimal overhead, no additional threads

## References

- Current implementation: lading/src/generator/unix_datagram.rs
- Throttle mechanism: lading_throttle/
- datadog-go timeout: 100ms default

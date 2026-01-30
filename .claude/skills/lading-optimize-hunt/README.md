# Rust Optimization Techniques for lading

This document explains the reasoning behind each optimization pattern in the lading-optimize-hunt skill. These patterns are based on well-established Rust performance best practices and have been validated through benchmarks and community research.

## Table of Contents

- [Preallocation Patterns](#preallocation-patterns)
- [Hash Function Optimization](#hash-function-optimization)
- [String Building Optimization](#string-building-optimization)
- [Parameter Type Optimization](#parameter-type-optimization)
- [Allocation Patterns](#allocation-patterns)
- [Inlining Optimization](#inlining-optimization)
- [Iterator Optimization](#iterator-optimization)
- [Reference Patterns](#reference-patterns)

---

## Preallocation Patterns

### `Vec::with_capacity(n)` / `String::with_capacity(n)` / `HashMap::with_capacity(n)`

**Pattern**: `Vec::new()` + repeated push operations

**Why it matters**: When you know (or can estimate) the final size of a collection, preallocating avoids multiple reallocations as the collection grows.

**How it works**:
- `Vec` starts with capacity 0 and doubles when full (0 → 4 → 8 → 16 → 32...)
- Each reallocation requires:
  1. Allocating new memory
  2. Copying all existing elements
  3. Freeing old memory
- For 1000 elements: ~10 reallocations without preallocation vs 0 with preallocation

**Example**:
```rust
// Slow: multiple reallocations
let mut v = Vec::new();
for i in 0..1000 {
    v.push(i);  // May trigger reallocation
}

// Fast: single allocation
let mut v = Vec::with_capacity(1000);
for i in 0..1000 {
    v.push(i);  // Never reallocates
}
```

**Bug risk**: None - purely additive optimization

**References**:
- Rust std documentation on Vec::with_capacity

---

## Hash Function Optimization

### `FxHashMap` from rustc-hash

**Pattern**: Using default `HashMap` with integer, pointer, or short string keys

**Why it matters**: The default HashMap uses SipHash, a cryptographically secure hash function designed to prevent DoS attacks. For internal data structures where DoS isn't a concern, faster non-cryptographic hashes significantly improve performance.

**How it works**:
- SipHash: Secure but slow (~13 CPU cycles per byte)
- FxHash: Fast but not DoS-resistant (~0.5 CPU cycles per byte)
- FxHash works on 8 bytes at a time, making it much faster for common key types

**Performance impact**:
- rustc benchmarks showed 4-84% slowdowns when switching FROM FxHash back to default HashMap
- Consistently outperforms FNV hash in rustc itself
- Best for: integer keys, pointer keys, small fixed-size keys

**Example**:
```rust
// Slow for internal data structures
use std::collections::HashMap;
let mut map: HashMap<u64, Value> = HashMap::new();

// Fast for internal data structures
use rustc_hash::FxHashMap;
let mut map: FxHashMap<u64, Value> = FxHashMap::default();
```

**Bug risk**: Hash collision DoS attacks (only relevant if keys come from untrusted sources)

**When NOT to use**: User-facing APIs, networked services where keys come from untrusted sources

**References**:
- [Hashing - The Rust Performance Book](https://nnethercote.github.io/perf-book/hashing.html)
- [rustc-hash GitHub](https://github.com/rust-lang/rustc-hash)
- [Using rustc-hash in Rust: A Guide](https://dlcoder.medium.com/using-rustc-hash-in-rust-a-guide-to-faster-and-safer-hashing-1f3dacfbf6de)

---

## String Building Optimization

### `write!()` to reused buffer instead of `format!()`

**Pattern**: Building strings in loops using `format!()` or repeated `format!() + push_str()`

**Why it matters**: `format!()` always allocates a new String, even if you immediately append it to another String. Using `write!()` to a reused buffer eliminates intermediate allocations.

**Performance impact**:
- One developer documented a **75% performance increase** switching from `format!()` to `write!()`
- Benefits compound in hot loops

**How it works**:
```rust
// Slow: allocates a new String for each format!()
let mut result = String::new();
for i in 0..1000 {
    let formatted = format!("Item {}: {}", i, value);  // Allocates!
    result.push_str(&formatted);
}

// Fast: writes directly to the buffer
use std::fmt::Write;
let mut result = String::with_capacity(estimated_size);
for i in 0..1000 {
    write!(result, "Item {}: {}", i, value).unwrap();  // No allocation
}
```

**Additional benefits**:
- Reduces memory fragmentation
- Better cache locality
- Lower allocator pressure

**Bug risk**: Format errors (usually caught at compile time), capacity estimation errors (causes reallocation but still correct)

**References**:
- [write! vs format! when constructing Rust strings](https://medium.com/@nic-obert/write-vs-format-when-constructing-rust-strings-10eb49e4dbf6)
- [Rust Performance Pitfalls — Llogiq on stuff](https://llogiq.github.io/2017/06/01/perf-pitfalls.html)
- [I/O - The Rust Performance Book](https://nnethercote.github.io/perf-book/io.html)

---

## Parameter Type Optimization

### `&[T]` instead of `&Vec<T>`, `&str` instead of `&String`

**Pattern**: Function parameters using `&Vec<T>` or `&String`

**Why it matters**: Using slice types (`&[T]`, `&str`) instead of reference-to-owned types provides better performance and more flexibility.

**Performance impact**:
- `&Vec<T>` requires two pointer dereferences, `&[T]` requires one
- Real-world example: converting `&mut Vec<T>` to `&mut [T]` sped up the fastblur crate by **15%**
- Enables better bounds-check elimination by LLVM

**How it works**:
```rust
// Slower: requires extra indirection
fn process(data: &Vec<u8>) {
    for byte in data {
        // Two dereferences: &Vec -> Vec -> data
    }
}

// Faster: direct access
fn process(data: &[u8]) {
    for byte in data {
        // One dereference: &[u8] -> data
    }
}
```

**Additional benefits**:
- More general: works with any slice, not just Vec-owned data
- Works with arrays: `&[1, 2, 3]`
- Works with partial slices: `&vec[10..20]`
- Clearer intent: signals you won't change the length

**Bug risk**: None - this is a strict improvement

**Clippy lint**: `ptr_arg` warns about this pattern

**References**:
- [Clippy Issue #10269](https://github.com/rust-lang/rust-clippy/issues/10269)
- [Clippy Issue #6406](https://github.com/rust-lang/rust-clippy/issues/6406)

---

## Allocation Patterns

### Moving allocations outside loops

**Pattern**: Allocating inside a hot loop when the same buffer could be reused

**Why it matters**: Allocation is expensive. Reusing allocations amortizes the cost across iterations.

**Example**:
```rust
// Slow: allocates 1000 times
for item in items {
    let mut buffer = Vec::new();
    process_into(&mut buffer, item);
    send(buffer);
}

// Fast: allocates once, reuses buffer
let mut buffer = Vec::new();
for item in items {
    buffer.clear();  // Retains capacity
    process_into(&mut buffer, item);
    send(&buffer);
}
```

**Bug risk**: Lifetime issues (buffer must outlive loop), state leakage between iterations (remember to clear!)

### Object pooling / Buffer reuse

**Pattern**: Repeatedly allocating and deallocating expensive objects (Vec, String, complex structs)

**Why it matters**: Object pools amortize allocation costs across many operations and reduce allocator pressure.

**Performance impact**:
- Lifeguard library benchmarks show **~9x faster** allocation for pooled objects
- Critical for high-throughput systems like load generators
- Reduces memory fragmentation

**How it works**:
```rust
// Without pooling
for _ in 0..1_000_000 {
    let buffer = String::with_capacity(4096);  // Allocates
    use_buffer(buffer);
    // Deallocates
}

// With pooling
use lifeguard::Pool;
let pool = Pool::with_size(100);
for _ in 0..1_000_000 {
    let buffer = pool.detach();  // Reuses from pool
    use_buffer(buffer);
    // Returns to pool automatically
}
```

**Especially valuable for**:
- Load generators
- Network servers
- Hot loops with temporary buffers
- Async tasks that create/destroy similar objects repeatedly

**Trade-offs**:
- Memory remains allocated in pool
- Need to choose appropriate pool size
- Must ensure objects are properly reset between uses

**Bug risk**: State leakage between pool uses, lifecycle management complexity

**References**:
- [GitHub - zslayton/lifeguard](https://github.com/zslayton/lifeguard)
- [object_pool - Rust docs](https://docs.rs/object-pool)
- [A different memory pool in Rust](https://cbarrete.com/pool.html)

### Bounded buffers for unbounded growth

**Pattern**: Collections that grow indefinitely without limits (unbounded `Vec`, `String`, `HashMap`)

**Why it matters**: Unbounded growth can lead to memory exhaustion, unpredictable performance, and OOM crashes. In long-running services or load generators, this is a critical reliability concern.

**How it works**:
Replace unbounded collections with bounded alternatives:
- Ring buffers (circular buffers) - fixed-size, overwrites oldest data
- Bounded queues - reject or block when full
- LRU caches - evict least-recently-used when at capacity
- Sampling - keep only a subset of data

**Example**:
```rust
// Unbounded: memory grows forever
let mut events = Vec::new();
loop {
    events.push(receive_event());  // Never clears, grows forever
}

// Bounded with ring buffer
use ringbuf::RingBuffer;
let mut events = RingBuffer::new(1000);  // Fixed size
loop {
    events.push_overwrite(receive_event());  // Overwrites oldest
}

// Bounded with capacity limit
let mut events = Vec::with_capacity(1000);
loop {
    if events.len() >= 1000 {
        events.clear();  // Or drain oldest half
    }
    events.push(receive_event());
}
```

**Performance impact**:
- Eliminates unbounded memory growth
- Makes worst-case memory usage predictable
- Can improve cache locality (smaller working set)
- Bounded queues may add coordination overhead

**Trade-offs**:
- **Semantic change**: Data is lost (oldest evicted or new rejected)
- Must choose appropriate bound size
- May need different eviction strategy per use case
- Requires careful consideration of what data can be safely discarded

**Bug risk**: **HIGH** - This changes program semantics. Data loss may be unacceptable for some use cases. Requires explicit decision about:
- What happens when full (reject, overwrite, drain)?
- Is data loss acceptable?
- What's the right capacity?

**When to use**:
- Metrics/telemetry buffers (sampling is acceptable)
- Event logs where recent data matters most
- Caches (by definition should have bounds)
- Load generators (don't need infinite history)

**When NOT to use**:
- Persistent storage requirements
- Exact historical replay needed
- Data loss is unacceptable

**References**:
- [ringbuf - Rust docs](https://docs.rs/ringbuf)
- [bounded-vec-deque - Rust docs](https://docs.rs/bounded-vec-deque)
- [Circular buffer - Wikipedia](https://en.wikipedia.org/wiki/Circular_buffer)

---

## Inlining Optimization

### `#[inline]` attribute for hot cross-crate functions

**Pattern**: Small, frequently-called functions in library crates that aren't being inlined by caller crates

**Why it matters**: Rust compiles each crate separately. Without `#[inline]`, even trivial functions cannot be inlined across crate boundaries. This prevents:
- Eliminating function call overhead
- Further optimizations across the call boundary
- Dead code elimination

**How it works**:
- Within a crate: the compiler can inline freely
- Across crates: the compiler needs `#[inline]` to include the function body in the crate metadata
- Generic functions are implicitly `#[inline]` (their bodies must be available for monomorphization)

**Example**:
```rust
// In lading_payload crate - without #[inline]
pub fn is_empty(&self) -> bool {
    self.len == 0
}

// In lading crate - cannot inline across crate boundary
if payload.is_empty() {  // Function call overhead
    return;
}

// With #[inline]
#[inline]
pub fn is_empty(&self) -> bool {
    self.len == 0
}

// In lading crate - can inline
if payload.is_empty() {  // Inlined, zero overhead
    return;
}
```

**When to use**:
- Small functions (getters, simple checks, wrappers)
- Functions called in hot loops
- Public API functions that are performance-critical

**Trade-offs**:
- Increases compile time (function compiled in each crate that uses it)
- Increases binary size (code duplication)
- Don't use on large functions - code bloat negates benefits

**Special cases**:
- `#[inline(always)]`: Forces inlining even when compiler thinks it's not beneficial (use sparingly)
- Generic functions: Already implicitly inline
- LTO (Link-Time Optimization): Makes all functions inlinable but drastically increases compile time

**Bug risk**: Binary size bloat if overused

**References**:
- [Inlining - The Rust Performance Book](https://nnethercote.github.io/perf-book/inlining.html)
- [Inline In Rust](https://matklad.github.io/2021/07/09/inline-in-rust.html)
- [Inlining and Performance Optimization with #[inline]](https://www.slingacademy.com/article/inlining-and-performance-optimization-with-inline/)

---

## Iterator Optimization

### Iterator chains without intermediate `.collect()`

**Pattern**: Calling `.collect()` between iterator operations to create intermediate collections

**Why it matters**: Iterator chains are zero-cost abstractions - they don't allocate until you consume them. Intermediate `.collect()` calls force allocation and evaluation, negating this benefit.

**Example**:
```rust
// Slow: allocates three times
let data = vec![1, 2, 3, 4, 5];
let filtered: Vec<_> = data.iter()
    .filter(|&&x| x > 2)
    .collect();  // Allocation 1
let mapped: Vec<_> = filtered.iter()
    .map(|&&x| x * 2)
    .collect();  // Allocation 2
let summed: Vec<_> = mapped.iter()
    .take(2)
    .collect();  // Allocation 3

// Fast: zero intermediate allocations
let summed: Vec<_> = data.iter()
    .filter(|&&x| x > 2)
    .map(|&x| x * 2)
    .take(2)
    .collect();  // Only one allocation
```

**How it works**:
- Iterators are lazy - they don't execute until consumed
- Chaining operations creates nested iterator types
- Compiler can often optimize entire chains into tight loops
- Only the final `.collect()` allocates

**Special considerations**:
- `.chain()` can be slower in some cases due to complexity
- Consider using `.for_each()` instead of `.collect()` if you don't need the collection
- Known issue: `chain()` + `collect()` may not optimize well (use `for_each()` instead)

**Bug risk**: Logic errors when refactoring (order of operations matters), off-by-one errors

**References**:
- [Iterators - The Rust Performance Book](https://nnethercote.github.io/perf-book/iterators.html)
- [Rust's iterators optimize nicely—and contain a footgun](https://ntietz.com/blog/rusts-iterators-optimize-footgun/)
- [Rust Performance Pitfalls — Llogiq on stuff](https://llogiq.github.io/2017/06/01/perf-pitfalls.html)

---

## Reference Patterns

### Using references instead of clones

**Pattern**: Cloning data when borrowing would suffice

**Why it matters**: Cloning allocates memory and copies data. References are zero-cost.

**Example**:
```rust
// Slow: unnecessary clone
fn process(data: String) {
    println!("{}", data);
}

let s = String::from("hello");
process(s.clone());  // Allocates and copies

// Fast: borrow instead
fn process(data: &str) {
    println!("{}", data);
}

let s = String::from("hello");
process(&s);  // Zero cost
```

**Trade-offs**:
- Borrowing adds lifetime complexity
- Sometimes cloning is necessary for ownership reasons
- Profile before optimizing - cloning small data is cheap

**Bug risk**: Lifetime complexity, borrow checker errors

### Large structs by reference or Box

**Pattern**: Passing or returning large structs by value

**Why it matters**: Passing by value requires copying the entire struct. References and `Box` avoid this.

**Example**:
```rust
struct LargeData {
    buffer: [u8; 4096],
    // ... more fields
}

// Slow: copies 4KB+ on every call
fn process(data: LargeData) { ... }

// Fast: zero-copy
fn process(data: &LargeData) { ... }

// Or use Box for ownership transfer
fn process(data: Box<LargeData>) { ... }
```

**Bug risk**: Nil pointer dereferences with Box, lifetime complexity with references

---

## Summary

These optimization patterns represent well-established Rust best practices validated by:
- The Rust Performance Book (maintained by rustc contributor Nicholas Nethercote)
- Real-world production codebases (rustc, Servo, Firefox)
- Community benchmarks and research
- Clippy lints (official Rust linter)

When applied to lading's hot paths (payload generation, throttling, serialization), these techniques can yield significant performance improvements while maintaining correctness and determinism.

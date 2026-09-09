# Revisit

Notes on things worth a second look, deferred out of the PR that raised them.

## `set_write_timeout` on the control handshake write

`lading/src/neper/rr.rs`, in `wait_for_generator`:

```rust
// accept(2) on Linux returns a blocking socket regardless of
// the listener's O_NONBLOCK; a small write_timeout guards
// against a generator that connects but never reads.
conn.set_write_timeout(Some(HANDSHAKE_TIMEOUT))
    .expect("set_write_timeout on accepted TcpStream must succeed");
conn.write_all(&flows_bytes)?;
```

The comment's justification does not hold. `write_all` on a blocking socket
returns once the bytes are copied into the kernel send buffer; it does not
wait for the peer to read. The send buffer only backs up when the peer's
receive window closes, which needs enough unacked data in flight to fill it.
This write is `HANDSHAKE_LEN` = 2 bytes and the minimum `tcp_wmem` is 4096,
so a 2-byte write into a freshly accepted, empty send buffer always completes
immediately, whether or not the generator ever calls `read`. The timeout
cannot fire in the scenario it names.

The comment's first clause is correct: `conn` really is blocking, since Linux
`accept(2)` does not inherit `O_NONBLOCK` from the listener. And
`wait_for_generator` is `async`, so `write_all` is a blocking syscall on a
tokio worker thread - the timeout caps that at 5s rather than forever. But
since the write cannot block in the first place, it is insurance against
nothing.

Origin: it mirrors the client side in `run_client`, where the timeout *is*
load-bearing. There, `read_exact` blocks until the blackhole actually writes,
so a blackhole that accepts and then stalls would hang the generator
indefinitely without it. The write side was symmetrized from that, but the
asymmetry of TCP means the reasoning does not carry over.

Options:

1. Drop the `set_write_timeout` call and the comment. Fewest moving parts,
   and it removes one `expect`. The failure it was reaching for - a peer that
   connects and vanishes - surfaces as `EPIPE`/`ECONNRESET` from `write_all`,
   which already propagates via `?`.
2. Keep it and rewrite the comment to say what it actually does: bound a
   blocking syscall issued from an async task.

Preference is (1).

Caveat: if `HANDSHAKE_LEN` ever grows past the send buffer this reasoning
changes, but that would be a protocol change.

Related: `run_client` does blocking `connect` / `read_exact` / `thread::sleep`
directly on the async runtime thread. Out of scope here, but the same
blocking-in-async concern applies there and is more real.

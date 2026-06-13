# System Design Implementations

Idiomatic Go implementations of classic system-design building blocks, each
backed by a thorough test suite. The focus is on correct behaviour, clear code,
and measurable properties rather than production packaging.

A recurring theme across the packages is **testability through injected time** —
clocks and tickers are interfaces, so time-dependent logic is driven
deterministically in tests instead of with `time.Sleep`.

## Components

| Component | Package | Summary |
| --- | --- | --- |
| [Consistent Hashing](consistent-hashing/) | `consistenthashing` | Hash ring over a red-black tree with virtual nodes and affected-range tracking |
| [Token Bucket](ratelimiter/token_bucket.go) | `ratelimiter` | Per-key rate limiter allowing bursts up to a capacity |
| [Leaky Bucket](ratelimiter/leaky_bucket.go) | `ratelimiter` | Per-key queue that leaks work to a consumer at a fixed rate |

`cmd/server` is a placeholder entrypoint (wired up via the `Makefile`); the
substance of the repo lives in the packages above.

## Running

```sh
make test                        # go test ./...   — run the full suite
make run                         # go run ./cmd/server/
make build                       # build to ./bin/server
make clean                       # remove ./bin

# Per package:
make test-ratelimiter            # go test ./ratelimiter/
make test-consistent-hashing     # go test ./consistent-hashing/
make bench-consistent-hashing    # benchmarks (ns/op, allocs)
make dist-consistent-hashing     # print the load-distribution table

# Useful directly:
go test ./... -race              # race detector
```

---

## Consistent Hashing

A consistent hash ring that maps keys to servers so that adding or removing a
server only relocates keys in the affected arc of the ring (~`K/n`), instead of
remapping nearly everything as plain `hash(key) % n` would.

### API

```go
ch := consistenthashing.New(virtualNodes)

ch.AddServer("server-a")        // returns the []Range now owned by the new server
ch.RemoveServer("server-a")     // returns the []Range freed up for its neighbours
server, err := ch.GetKey("user-42")
```

### Design

- **Red-black tree ring** ([`emirpasic/gods`](https://github.com/emirpasic/gods)) keeps node positions sorted, so a lookup is a single `Ceiling` query in **O(log n)**.
- **Virtual nodes** — each server is placed at `virtualNodes` positions on the ring. More positions ⇒ smoother load (see below).
- **Affected-range tracking** — `AddServer`/`RemoveServer` return the exact ring ranges that change hands, so a caller can drive key migration.
- **Concurrency-safe** — guarded by a `sync.RWMutex`; lookups take the read lock, membership changes the write lock. Verified under `go test -race`.
- **Hashing** — SHA-256 truncated to the high 64 bits, giving uniform positions on a `uint64` ring.

### Load distribution

Virtual nodes are what make the ring balanced. Routing **100,000 keys across 10
servers** and measuring the per-server load, the spread tightens sharply as the
virtual-node count grows. The **coefficient of variation** (stddev / mean) is the
headline metric — lower is more even — and it shrinks roughly as `1/√vnodes`:

| vnodes | stddev | cv | max/min |
| ---: | ---: | ---: | ---: |
| 1 | 10156.4 | 1.0156 | 228.45× |
| 10 | 2816.0 | 0.2816 | 2.56× |
| 50 | 905.1 | 0.0905 | 1.39× |
| 100 | 927.0 | 0.0927 | 1.31× |
| 250 | 551.8 | 0.0552 | 1.19× |
| 500 | 446.7 | 0.0447 | 1.16× |

With a single position per server the busiest node holds **228×** the load of the
quietest; at 500 positions that gap collapses to **1.16×**. Reproduce with:

```sh
make dist-consistent-hashing     # go test ./consistent-hashing/ -run TestLoadDistribution -v
```

### Benchmarks

`go test ./consistent-hashing/ -bench . -benchmem` (Apple M4 Pro):

```
BenchmarkGetKey/servers=10        87.7 ns/op     8 B/op   1 allocs/op
BenchmarkGetKey/servers=100      125.9 ns/op     8 B/op   1 allocs/op
BenchmarkGetKey/servers=1000     172.9 ns/op     8 B/op   1 allocs/op
BenchmarkAddServer/vnodes=1        1.0 µs/op    144 B/op   7 allocs/op
BenchmarkAddServer/vnodes=100     91.3 µs/op  14602 B/op  410 allocs/op
BenchmarkAddServer/vnodes=500    628.1 µs/op  69779 B/op 2412 allocs/op
```

Each server here registers 100 virtual nodes, so `servers=1000` is a **100,000-node
ring**. Lookup time grows only ~2× while the ring grows 100× — the logarithmic
cost of the tree `Ceiling`. `AddServer` scales linearly with the virtual-node
count, since each join inserts one node per virtual node.

---

## Rate Limiters

Two complementary algorithms, both keyed per client (e.g. per user or IP) so a
single instance limits many independent callers. Each takes its time source as
an interface, so the tests advance time deterministically rather than sleeping.

### Token Bucket

Each key gets a bucket that refills continuously at `rate` tokens/second up to
`capacity`. A request spends one token; if none are available it is rejected.
Because a full bucket holds `capacity` tokens, it permits short **bursts** up to
that size while bounding the long-run average rate.

```go
tb := ratelimiter.NewTokenBucket(capacity, rate) // tokens, tokens/sec
if tb.Allow("user-42") {
    // serve the request
}
```

- Lazy refill — tokens are recomputed from the elapsed time on each `Allow`, so there is no background goroutine or timer.
- `Clock` is injectable (`newTokenBucketWithClock`), making refill behaviour testable to the microsecond.

### Leaky Bucket

Models a fixed-rate queue. Each key has a buffered channel of capacity `size`;
`Push` enqueues an item, and a per-key worker goroutine leaks up to `rate`
items per tick to a shared output channel — smoothing bursty input into a steady
output stream.

```go
lb := ratelimiter.NewLeakyBucket[Request](size, rate) // queue depth, items/sec
lb.Push("user-42", req)
```

- **Generic** over the queued item type `T`.
- The leak interval is driven by a `Ticker` interface (`newLeakyBucketWithClock`), so tests inject a fake ticker and step the clock instead of waiting on a real one-second tick.

### Testing approach

Both limiters are covered by subtest suites (`t.Run`) that exercise capacity
limits and refill/leak over simulated time; the token bucket additionally checks
per-key isolation and concurrent access. Swapping the real clock/ticker for
fakes keeps the suite fast and free of timing flakiness.

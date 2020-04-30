# flightlimit
[![codecov](https://codecov.io/gh/mollerdaniel/flightlimit/branch/master/graph/badge.svg)](https://codecov.io/gh/mollerdaniel/flightlimit)
[![GoDoc](https://godoc.org/github.com/mollerdaniel/flightlimit?status.svg)](https://godoc.org/github.com/mollerdaniel/flightlimit)

flightlimit is an in-flight (concurrency) limiter using redis inspired by go-redis/redis_rate

[Docs](https://godoc.org/github.com/mollerdaniel/flightlimit) for usage

## Basics

The basics consists of:
1. an atomic redis roundtrip on takeoff `Inc()`
2. check result if limit was hit or not
3. your processing
4. land using a second atomic redis roundtrip `Dec()`

## Flusher

The optional Flusher takes care of eventual keys in incorrect state due to network or redis outtage. One example is if redis crashes after 1, leaving the state of the counter in a stale state.

## Example

```go
// Miniredis server
mr, _ := miniredis.Run()

// Setup Redis
ring := redis.NewRing(&redis.RingOptions{
    Addrs: map[string]string{"server0": mr.Addr()},
})

// Control max allowed time for flightlimit to block
ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
defer cancel()

// Create a new flighlimiter
l := flightlimit.NewLimiter(ring, true)
defer l.Close()

// Allow a maximum of 10 in-flight, with expected processing time of <= 30 Seconds
limit := flightlimit.NewLimit(10, 30*time.Second)

// Mark as in-flight
r, _ := l.Inc(ctx, "foo:123", limit)

if !r.Allowed {
    //
    // Here a 429 could be returned
    //
    return
}

// Processing
fmt.Println(r.Remaining)

// Land
l.Decr(ctx, r)
```

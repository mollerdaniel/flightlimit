# flightlimit
[![codecov](https://codecov.io/gh/mollerdaniel/flightlimit/branch/master/graph/badge.svg)](https://codecov.io/gh/mollerdaniel/flightlimit)
[![GoDoc](https://godoc.org/github.com/mollerdaniel/flightlimit?status.svg)](https://godoc.org/github.com/mollerdaniel/flightlimit)
[![Go Report Card](https://goreportcard.com/badge/github.com/mollerdaniel/flightlimit)](https://goreportcard.com/report/github.com/mollerdaniel/flightlimit)

flightlimit is an in-flight (concurrency) limiter using redis inspired by go-redis/redis_rate

[GoDoc](https://godoc.org/github.com/mollerdaniel/flightlimit) for usage

## Basics

The normal workflow:
1. an atomic redis roundtrip on takeoff `Inc()`
2. check if result limit was hit or not, take action or go to #3
3. normal processing
4. land using a second atomic redis roundtrip `Dec()`

In case of errors the principle is "innocent before proven guilty", we Allow instead of block.

## Flusher

The optional Flusher takes care of keys in incorrect state due to network or redis outtage. One example is if redis crashes after step #1, leaving the state of the counter in a stale state.

## Example

```go
// Miniredis server for testing
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

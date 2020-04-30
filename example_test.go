package flightlimit_test

import (
	"context"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/mollerdaniel/flightlimit"
)

func ExampleNewLimiter() {
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
	// Output: 9
}

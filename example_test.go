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
	// Setup Redis
	mr, _ := miniredis.Run()
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"server0": mr.Addr()},
	})

	ctx := context.Background()

	l := flightlimit.NewLimiter(ring, nil, true)

	limit := flightlimit.NewLimit(10, time.Minute*10)

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

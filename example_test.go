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
	mr, _ := miniredis.Run()
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"server0": mr.Addr()},
	})
	ctx := context.Background()

	l := flightlimit.NewLimiter(ring, nil)
	r, _ := l.Inc(ctx, "foo:123", flightlimit.NewLimit(10, time.Minute*10))

	if !r.Allowed {
		return
	}
	fmt.Println(r.Remaining)

	l.Decr(ctx, r)
	// Output: 9
}

package flightlimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func BenchmarkIncDecr(b *testing.B) {
	l, _ := flightlimit(true)
	defer l.Close()
	limit := NewLimit(10, time.Hour)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := l.Inc(context.TODO(), "foo", limit)
			if err != nil {
				b.Fatal(err)
			}
			err = l.Decr(context.TODO(), r)
			if err != nil {
				b.Fatal(err)
			}

		}
	})
}

func flightlimit(flusher bool) (*Limiter, *miniredis.Miniredis) {
	mr, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{"server0": mr.Addr()},
	})
	if err := ring.FlushDB(context.TODO()).Err(); err != nil {
		panic(err)
	}
	return NewLimiter(ring, flusher), mr
}

func TestInc(t *testing.T) {
	l, _ := flightlimit(true)
	defer l.Close()
	limit := NewLimit(10, time.Hour)

	// 1 takeoff
	res, err := l.Inc(context.TODO(), "test_id", limit)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(9), res.Remaining)

	// 2 more takeoff (total of 3)
	res, err = l.IncN(context.TODO(), "test_id", limit, 2)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(7), res.Remaining)

	// 1000 won't fit, should fail and keep 3
	res, err = l.IncN(context.TODO(), "test_id", limit, 1000)
	assert.Nil(t, err)
	assert.False(t, res.Allowed)
	assert.Equal(t, int64(7), res.Remaining)

	// 7 more takeoff (total of 10)
	res, err = l.IncN(context.TODO(), "test_id", limit, 7)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(0), res.Remaining)

	// 1 more takeoff (total of 11)
	res, err = l.IncN(context.TODO(), "test_id", limit, 1)
	assert.Nil(t, err)
	assert.False(t, res.Allowed)
	assert.Equal(t, int64(0), res.Remaining)
}

func TestIncDynamic(t *testing.T) {
	l, _ := flightlimit(true)
	defer l.Close()
	limit := NewLimit(10, time.Hour)

	// 1 takeoff
	res, err := l.IncDynamic(context.TODO(), "test_id", limit, 10)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(9), res.Remaining)

	// 2 more takeoff (total of 3)
	res, err = l.IncNDynamic(context.TODO(), "test_id", limit, 2, 10)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(7), res.Remaining)

	// 2 more takeoff should fail because we just lowered the limit to 4
	res, err = l.IncNDynamic(context.TODO(), "test_id", limit, 2, 4)
	assert.Nil(t, err)
	assert.False(t, res.Allowed)
	assert.Equal(t, int64(1), res.Remaining)

	// 2 more takeoff should work because we increased the limit to 5
	res, err = l.IncNDynamic(context.TODO(), "test_id", limit, 2, 5)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(0), res.Remaining)
}

func TestIncNAndDecrCount(t *testing.T) {
	l, s := flightlimit(true)
	defer l.Close()
	limit := NewLimit(20, time.Hour)

	// Start with 10 in flight
	s.Set(redisPrefix+"test_id", "10")

	// Incr by 3
	res, err := l.IncN(context.TODO(), "test_id", limit, 3)

	// No errors
	assert.NoError(t, err)

	// Should have 13
	s.CheckGet(t, redisPrefix+"test_id", "13")

	// Decr
	err = l.Decr(context.TODO(), res)

	// No errors
	assert.NoError(t, err)

	// Should have 10
	s.CheckGet(t, redisPrefix+"test_id", "10")
}

func TestKeyConflict(t *testing.T) {
	l, _ := flightlimit(true)
	defer l.Close()
	limit := NewLimit(2, time.Hour)

	// Unique Key 1
	res, err := l.Inc(context.TODO(), "k1", limit)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, res.Remaining, int64(1))

	// Unique Key 2
	res, err = l.Inc(context.TODO(), "k2", limit)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, res.Remaining, int64(1))
}

func TestKeyExpiry(t *testing.T) {
	l, s := flightlimit(true)
	defer l.Close()
	limit := NewLimit(10, time.Hour)

	res, err := l.Inc(context.TODO(), "test_id", limit)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, res.Remaining, int64(9))

	// Run 5 min
	s.FastForward(time.Minute * 5)

	// Key should be on the server
	assert.True(t, s.Exists(redisPrefix+"test_id"))

	// wait 56 min more
	s.FastForward(time.Minute * 56)

	// key should not exist after a total of 61 min
	assert.False(t, s.Exists(redisPrefix+"test_id"))

	// Start a new
	res, _ = l.Inc(context.TODO(), "test_id", limit)

	// Key should be on the server
	assert.True(t, s.Exists(redisPrefix+"test_id"))

	// wait 30 mins
	s.FastForward(time.Minute * 30)

	// add another
	res, _ = l.Inc(context.TODO(), "test_id", limit)

	// wait 35 mins
	s.FastForward(time.Minute * 30)

	// close "first" one
	err = l.Decr(context.TODO(), res)

	// no error
	assert.NoError(t, err)

	// wait 59 mins
	s.FastForward(time.Minute * 59)

	// Key should be on the server
	assert.True(t, s.Exists(redisPrefix+"test_id"))

	// wait 2 mins
	s.FastForward(time.Minute * 2)

	// key should not exist
	assert.False(t, s.Exists(redisPrefix+"test_id"))
}

func TestKeyError(t *testing.T) {
	l, s := flightlimit(true)
	limit := NewLimit(10, time.Hour)
	k := "test_id"

	// Inc by 1
	_, err := l.Inc(context.TODO(), k, limit)

	// key should exists with value 1
	s.CheckGet(t, redisPrefix+k, "1")

	// no error
	assert.NoError(t, err)

	// Close redis to simulate failure
	s.Close()

	// Inc
	res, err := l.Inc(context.TODO(), k, limit)

	// Should return error
	assert.Error(t, err)

	// And the result should still be valid
	// N should be 0 so if we run Decr on failed Result the final value will be correct
	assert.True(t, res.Allowed)
	assert.Equal(t, res.n, 0)
	assert.Equal(t, res.Limit, limit)
	assert.Equal(t, res.key, k)
	assert.Equal(t, res.Remaining, int64(10))

	// Start redis to let the flusher remove the key
	s.Start()

	// key should exists with value 1
	s.CheckGet(t, redisPrefix+k, "1")

	// Let the flusher finish
	l.Close()

	// And the key should have been reset due to earlier failures
	assert.False(t, s.Exists(redisPrefix+k))
}

func TestDecr(t *testing.T) {
	l, s := flightlimit(true)
	defer l.Close()
	limit := NewLimit(10, time.Hour)

	// Inc to make the key exist
	realres, err := l.Inc(context.TODO(), "test_id", limit)

	// No error
	assert.NoError(t, err)

	// Simulate another case where result on ground (n == 0)
	r := &Result{
		key: "foo",
	}

	// "Land" the result
	err = l.Decr(context.Background(), r)

	// Should not result in error
	assert.NoError(t, err)

	// and the key should still be 1, not 0
	s.CheckGet(t, redisPrefix+"test_id", "1")

	// but if we later land the real result
	err = l.Decr(context.Background(), realres)

	// Should not result in error
	assert.NoError(t, err)

	// and the key should be 0
	s.CheckGet(t, redisPrefix+"test_id", "0")
}

func TestAsyncFlush(t *testing.T) {
	k := "test_id"
	l, s := flightlimit(true)
	defer s.Close()
	limit := NewLimit(10, time.Hour)
	_, err := l.Inc(context.TODO(), k, limit)
	assert.NoError(t, err)
	assert.True(t, s.Exists(redisPrefix+k))
	l.wgin.Add(1)
	// Add Task to queue
	l.addKeyToFlushQueue(k)

	// Wait for tasks to finish
	l.Close()

	// Key should have been deleted by the taskrunner
	assert.False(t, s.Exists(redisPrefix+k))
}

func TestAsyncFlushExpRetry(t *testing.T) {
	const flushRetries = 1
	k := "test_id"
	l, s := flightlimit(true)
	limit := NewLimit(10, time.Hour)
	l.wgin.Add(1)
	// Add a key
	_, err := l.Inc(context.TODO(), k, limit)
	assert.NoError(t, err)

	// it should exist
	assert.True(t, s.Exists(redisPrefix+k))

	// Close the redis server
	s.Close()

	// A Decr failed, and a key is added to the queue
	l.addKeyToFlushQueue(k)

	// Wait a bit
	time.Sleep(1 * time.Millisecond)

	// Revive redis
	s.Start()

	// signal Close
	l.Close()

	// Since we restarted redis it should be gone now
	assert.False(t, s.Exists(redisPrefix+k))
}

func TestAsyncFlushMaxRetries(t *testing.T) {
	const flushRetries = 1
	k := "test_id"
	l, s := flightlimit(true)
	limit := NewLimit(10, time.Hour)
	l.wgin.Add(1)

	// Add a key
	_, err := l.Inc(context.TODO(), k, limit)
	assert.NoError(t, err)

	// it should exist
	assert.True(t, s.Exists(redisPrefix+k))

	// Close the redis server
	s.Close()

	// A Decr failed, and a key is added to the queue
	l.addKeyToFlushQueue(redisPrefix + k)

	// signal Close
	l.Close()

	// now start redis
	s.Start()

	// Since we didn't restart redis before expretry fuse went off the key should still exist
	assert.True(t, s.Exists(redisPrefix+k))
}

func TestFlusherEnabled(t *testing.T) {
	// Enabled
	assert.Equal(t, NewLimiter(nil, true).flusherEnabled(), true)

	// Disabled
	assert.Equal(t, NewLimiter(nil, false).flusherEnabled(), false)
}

func TestInvalidFlightStateWithFlusher(t *testing.T) {
	l, s := flightlimit(true)
	limit := NewLimit(10, time.Hour)

	// Create a scenario with negative count that shouldn't exist without crashes
	s.Set(redisPrefix+"test_id", "-5")

	// Liftoff
	res, err := l.Inc(context.TODO(), "test_id", limit)

	// In this scenario we flush instead of decr so n should be 0
	assert.Equal(t, res.n, 0)

	// No error
	assert.NoError(t, err)

	// Wait for Flusher to exit
	l.Close()

	// Check that the key is reset and does not exist
	if s.Exists(redisPrefix + "test_id") {
		t.Fatal(redisPrefix + "test_id should not have existed anymore")
	}
}

func TestNonBlockingTaskQueue(t *testing.T) {
	l, _ := flightlimit(false)
	defer l.Close()
	// Injecting to queue should never block but instead drop excessive tasks
	for i := 0; i <= flushBufferLength*2; i++ {
		l.addTaskToQueue(flushTask{Key: "foo"})
	}
}

func TestFullFlushOfQueue(t *testing.T) {
	l, s := flightlimit(true)

	// A lot injected, and spilled outside the bucket
	for i := 0; i <= flushBufferLength*2; i++ {
		l.addTaskToQueue(flushTask{Key: "foo" + fmt.Sprint(1)})
	}

	// Wait for Flusher to finish
	l.Close()

	// All should be gone
	for i := 0; i <= flushBufferLength; i++ {
		assert.False(t, s.Exists(redisPrefix+"foo"+fmt.Sprint(i)))
	}
}

func TestGetTimeoutSecond(t *testing.T) {
	assert.Equal(t, 30, NewLimit(0, 30*time.Second).getTimeoutSecond())
	assert.Equal(t, 1, NewLimit(0, 50*time.Millisecond).getTimeoutSecond())
	assert.Equal(t, 120, NewLimit(0, 2*time.Minute).getTimeoutSecond())
	assert.Equal(t, 3600, NewLimit(0, time.Hour).getTimeoutSecond())
	assert.Equal(t, 1, NewLimit(0, 1500*time.Millisecond).getTimeoutSecond())
	assert.Equal(t, 2, NewLimit(0, 2100*time.Millisecond).getTimeoutSecond())
}

func TestNilRediser(t *testing.T) {
	var r *redis.Ring

	l := NewLimiter(r, true)

	defer l.Close()
	limit := NewLimit(10, time.Hour)

	// 1 takeoff
	res, err := l.Inc(context.TODO(), "test_id", limit)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(10), res.Remaining)
	err = l.Decr(context.TODO(), res)
	assert.Nil(t, err)
}

func TestNilRediserRealNilValue(t *testing.T) {
	l := NewLimiter(nil, true)

	defer l.Close()
	limit := NewLimit(10, time.Hour)

	// 1 takeoff
	res, err := l.Inc(context.TODO(), "test_id", limit)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(10), res.Remaining)
	err = l.Decr(context.TODO(), res)
	assert.Nil(t, err)

	// edgecase where rediser is nil, when it wasn't nil on Inc (how? :D)
	res.n = 1
	err = l.Decr(context.TODO(), res)
	assert.Nil(t, err)
}

func TestIsNil(t *testing.T) {
	assert.Equal(t, false, isNil(string("hello")))
}

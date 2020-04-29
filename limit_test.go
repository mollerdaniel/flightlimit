package flightlimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func BenchmarkIncDecr(b *testing.B) {
	l, _ := flightlimit()
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

func flightlimit() (*Limiter, *miniredis.Miniredis) {
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
	return NewLimiter(ring, nil, true), mr
}

func TestInc(t *testing.T) {
	l, _ := flightlimit()
	defer l.Close()
	limit := NewLimit(10, time.Hour)

	res, err := l.Inc(context.TODO(), "test_id", limit)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, res.Remaining, int64(9))

	res, err = l.IncN(context.TODO(), "test_id", limit, 2)
	assert.Nil(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, res.Remaining, int64(7))

	res, err = l.IncN(context.TODO(), "test_id", limit, 1000)
	assert.Nil(t, err)
	assert.False(t, res.Allowed)
	assert.Equal(t, res.Remaining, int64(0))
}

func TestKeyConflict(t *testing.T) {
	l, _ := flightlimit()
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
	l, s := flightlimit()
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
	l, s := flightlimit()
	defer l.Close()
	limit := NewLimit(10, time.Hour)

	// Close redis
	s.Close()

	// Inc
	_, err := l.Inc(context.TODO(), "test_id", limit)

	// Should return error
	assert.Error(t, err)
}

func TestAsyncFlush(t *testing.T) {
	k := "test_id"
	l, s := flightlimit()
	defer s.Close()
	limit := NewLimit(10, time.Hour)
	_, err := l.Inc(context.TODO(), k, limit)
	assert.NoError(t, err)
	assert.True(t, s.Exists(redisPrefix+k))

	// Add Task to queue
	l.addKeyToFlushQueue(redisPrefix + k)

	// Wait for tasks to finish
	l.Close()

	// Key should have been deleted by the taskrunner
	assert.False(t, s.Exists(redisPrefix+k))
}

func TestAsyncFlushExpRetry(t *testing.T) {
	const flushRetries = 1
	k := "test_id"
	l, s := flightlimit()
	limit := NewLimit(10, time.Hour)

	// Add a key
	_, err := l.Inc(context.TODO(), k, limit)
	assert.NoError(t, err)

	// it should exist
	assert.True(t, s.Exists(redisPrefix+k))

	// Close the redis server
	s.Close()

	// A Decr failed, and a key is added to the queue
	l.addKeyToFlushQueue(redisPrefix + k)

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

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
	l, s := flightlimit()
	limit := NewLimit(10, time.Hour)

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

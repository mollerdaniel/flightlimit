package flightlimit

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const flushRetries = 4
const flushWaitTime = time.Second * 1
const flushBufferLength = 10240
const redisPrefix = "inflight:"

type rediser interface {
	TxPipeline() redis.Pipeliner
}

// Limit instructions.
type Limit struct {
	// InFlight is the max number of InFlight before rate
	// limit hits and requests starts failing.
	InFlight int64
	// Timeout should be above the expected max runtime for a request in flight.
	Timeout time.Duration
}

// NewLimit creates a new Limit.
func NewLimit(inflight int64, timeout time.Duration) *Limit {
	return &Limit{
		InFlight: inflight,
		Timeout:  timeout,
	}
}

// Limiter controls how frequently events are allowed to happen.
type Limiter struct {
	rdb          rediser
	errFlushChan chan FlushTask
	wg           *sync.WaitGroup
	flusher      bool
}

// NewLimiter returns a new Limiter.
func NewLimiter(rdb rediser, wg *sync.WaitGroup, enableflusher bool) *Limiter {
	if wg == nil {
		wg = &sync.WaitGroup{}
	}
	wg.Add(1)
	l := &Limiter{
		rdb:          rdb,
		errFlushChan: make(chan FlushTask, flushBufferLength),
		wg:           wg,
		flusher:      enableflusher,
	}
	go l.Flusher()
	return l
}

// Close the Limiter.
func (l *Limiter) Close() {
	for {
		if len(l.errFlushChan) == 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	close(l.errFlushChan)
	l.wg.Wait()
}

// Flusher runs as a routine to flush keys struck by redis communication
// issues, to avoid incorrect limits caused by redis outtage.
func (l *Limiter) Flusher() {
	for {
		ftask, ok := <-l.errFlushChan
		if !ok {
			break
		}
		l.RunTask(&ftask)
	}
	l.wg.Done()
}

// RunTask is a recursive function for exp backoff.
func (l *Limiter) RunTask(ftask *FlushTask) {
	if ftask.Expired() {
		return
	}
	ftask.BlockWait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	err := l.delete(ctx, ftask.Key)
	cancel()
	if err != nil {
		l.RunTask(ftask)
	}
}

// Inc is shorthand for IncN(key, 1).
func (l *Limiter) Inc(ctx context.Context, key string, limit *Limit) (*Result, error) {
	return l.IncN(ctx, key, limit, 1)
}

// addTaskToQueue adds a FlushTask to the flushbuffer.
func (l *Limiter) addTaskToQueue(f FlushTask) {
	if len(l.errFlushChan) >= flushBufferLength-1 {
		return
	}
	l.errFlushChan <- f
}

// addKeyToFlushQueue adds a key to the backlog flushQueue if it's not full.
func (l *Limiter) addKeyToFlushQueue(key string) {
	f := FlushTask{
		Key: key,
		try: 0,
	}
	l.addTaskToQueue(f)
}

// flusherEnabled returns if flusher is enabled.
func (l *Limiter) flusherEnabled() bool {
	return l.flusher
}

// IncN reports whether n events may happen at time now.
func (l *Limiter) IncN(ctx context.Context, key string, limit *Limit, n int) (*Result, error) {
	nkey := redisPrefix + key
	// Execute using one rdb-server roundtrip.
	pipe := l.rdb.TxPipeline()

	// INCRBY + EX
	incr := pipe.IncrBy(ctx, nkey, int64(n))
	pipe.Expire(ctx, nkey, limit.Timeout)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return &Result{}, err
	}

	// Result
	cur := maxZero(incr.Val())
	res := &Result{
		Limit:     limit,
		Allowed:   cur <= limit.InFlight,
		Remaining: maxZero(limit.InFlight - cur),
		key:       nkey,
	}

	// In a not Allowed scenario, we can waste an inline roundtrip to ensure performance when Allowed.
	// This allows us to decr async if context is canceled.
	// To avoid blocking, we send of async failed Decr(s) to a queue.
	if !res.Allowed {
		err = l.Decr(ctx, res)
		if err != nil && l.flusherEnabled() {
			go l.addKeyToFlushQueue(key)
		}
	}
	return res, nil
}

func (l *Limiter) delete(ctx context.Context, key string) error {
	pipe := l.rdb.TxPipeline()
	pipe.Del(ctx, key)
	_, err := pipe.Exec(ctx)
	return err
}

// Decr is a shorthand for DecrN(ctx, r, 1).
func (l *Limiter) Decr(ctx context.Context, r *Result) error {
	return l.DecrN(ctx, r, 1)
}

// DecrN decreases inflight value by N.
func (l *Limiter) DecrN(ctx context.Context, r *Result, n int) error {
	pipe := l.rdb.TxPipeline()

	// DECRBY + EX
	pipe.DecrBy(ctx, r.key, int64(n))
	pipe.Expire(ctx, r.key, r.Limit.Timeout)

	_, err := pipe.Exec(ctx)
	return err
}

// FlushTask is instruction to flush an key due to failures.
type FlushTask struct {
	Key string
	try int
}

// Expired explains if the task has exceeded it's retries.
func (f *FlushTask) Expired() bool {
	return f.try >= flushRetries
}

// BlockWait adds blocking timeout for retry logic to slow down the thread,
// and avoid bursts to a broken redis.
func (f *FlushTask) BlockWait() {
	if f.try > 0 {
		stime := int(math.Pow(2, float64(f.try)-1))
		time.Sleep(time.Duration(stime) * time.Second)
	}
	f.try++
}

// Result is the obj we containing the result.
type Result struct {
	// Limit is the limit that was used to obtain this result.
	Limit *Limit

	// Allowed reports whether event may happen at time now.
	Allowed bool

	// Remaining is the maximum number of requests that could be
	// permitted instantaneously for this key given the current
	// state. For example, if a rate limiter allows 10 requests in
	// flight and has 6 mid air, Remaining would be 4.
	Remaining int64

	// Internal key for close()
	key string
}

// maxZero returns the larger of x or 0.
func maxZero(x int64) int64 {
	if x < 0 {
		return 0
	}
	return x
}

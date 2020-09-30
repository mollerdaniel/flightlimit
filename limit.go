package flightlimit

import (
	"context"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const flushRetries = 4
const flushWaitTime = time.Second * 1
const flushBufferLength = 10240
const flushBufferLengthTimingMargin = 100
const redisPrefix = "inflight:"
const hitlimitDecrTimeout = 2 * time.Second

type rediser interface {
	TxPipeline() redis.Pipeliner
	Eval(context context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(context context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(context context.Context, hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(context context.Context, script string) *redis.StringCmd
}

var luascript = redis.NewScript(`
redis.replicate_commands()
local rate_limit_key = KEYS[1]
local maxlimit = ARGV[1]
local incby = ARGV[2]
local exp = ARGV[3]
local act = redis.call("INCRBY", rate_limit_key, incby)
redis.call("EXPIRE", rate_limit_key, tonumber(exp))
if act > tonumber(maxlimit) then
  local final = redis.call("DECRBY", rate_limit_key, incby)
  redis.call("EXPIRE", rate_limit_key, tonumber(exp))
  return {final, 0}
end
return {act, 1}`)

// Limit instructions.
type Limit struct {
	// InFlight is the max number of InFlight before rate
	// limit hits and the Limiter return not Allowed in the Result.
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

// getTimeoutSecond returns the timeout as floored seconds, minimum 1.
func (l *Limit) getTimeoutSecond() int {
	v := int(l.Timeout.Seconds())
	if v < 1 {
		return 1
	}
	return v
}

// Limiter controls how frequently events are allowed to happen.
type Limiter struct {
	rdb          rediser
	errFlushChan chan flushTask
	wg           *sync.WaitGroup
	wgin         *sync.WaitGroup
	flusher      bool
	enabled      bool
}

// NewLimiter returns a new Limiter.
func NewLimiter(rdb rediser, enableflusher bool) *Limiter {
	l := &Limiter{
		enabled:      true,
		rdb:          rdb,
		errFlushChan: make(chan flushTask, flushBufferLength),
		wg:           &sync.WaitGroup{},
		wgin:         &sync.WaitGroup{},
		flusher:      enableflusher,
	}
	if isNil(rdb) {
		l.enabled = false
	}
	if l.flusherEnabled() {
		l.wg.Add(1)
		go l.bgflusher()
	}
	return l
}

// Close the Limiter gracefully.
func (l *Limiter) Close() {
	if l.flusherEnabled() {
		// Wait for all writes
		l.wgin.Wait()
		for {
			if len(l.errFlushChan) == 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		close(l.errFlushChan)
		l.wg.Wait()
	}
}

// flusher runs as a routine to flush keys struck by redis communication
// issues, to avoid incorrect limits caused by redis outtage.
func (l *Limiter) bgflusher() {
	for {
		ftask, ok := <-l.errFlushChan
		if !ok {
			break
		}
		l.runTask(&ftask)
	}
	l.wg.Done()
}

// runTask is a recursive function for exp backoff.
func (l *Limiter) runTask(ftask *flushTask) {
	if ftask.expired() {
		return
	}
	ftask.blockWait()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	err := l.delete(ctx, ftask.Key)
	cancel()
	if err != nil {
		l.runTask(ftask)
	}
}

// Inc is shorthand for IncN(ctx, key, limit, 1).
func (l *Limiter) Inc(ctx context.Context, key string, limit *Limit) (*Result, error) {
	return l.IncN(ctx, key, limit, 1)
}

// IncDynamic is shorthand for IncNDynamic(ctx, key, limit, 1, maxlimit).
func (l *Limiter) IncDynamic(ctx context.Context, key string, limit *Limit, maxlimit int64) (*Result, error) {
	return l.increaseByN(ctx, key, limit, 1, maxlimit)
}

// addTaskToQueue adds a FlushTask to the flushbuffer.
func (l *Limiter) addTaskToQueue(f flushTask) {
	if len(l.errFlushChan) >= flushBufferLength-flushBufferLengthTimingMargin {
		return
	}
	l.errFlushChan <- f
}

// addKeyToFlushQueue adds a key to the backlog flushQueue if it's not full.
func (l *Limiter) addKeyToFlushQueue(key string) {
	f := flushTask{
		Key: key,
		try: 0,
	}
	l.addTaskToQueue(f)
	l.wgin.Done()
}

// flusherEnabled returns if flusher is enabled.
func (l *Limiter) flusherEnabled() bool {
	return l.flusher
}

// IncNDynamic reports whether n events may happen at time now with dynamic limit.
func (l *Limiter) IncNDynamic(ctx context.Context, key string, limit *Limit, n int, maxlimit int64) (*Result, error) {
	return l.increaseByN(ctx, key, limit, n, maxlimit)
}

// IncN reports whether n events may happen at time now.
func (l *Limiter) IncN(ctx context.Context, key string, limit *Limit, n int) (*Result, error) {
	return l.increaseByN(ctx, key, limit, n, limit.InFlight)
}

// increaseByN executes the answer for whether n events may happen at time now.
func (l *Limiter) increaseByN(ctx context.Context, key string, limit *Limit, n int, maxlimit int64) (*Result, error) {
	nkey := redisPrefix + key
	res := &Result{
		Limit:     limit,
		Allowed:   true,
		Remaining: maxlimit,
		key:       key,
		n:         0,
	}

	if !l.enabled {
		return res, nil
	}

	// Execute using one rdb-server roundtrip.
	values := []interface{}{maxlimit, n, limit.getTimeoutSecond()}
	v, err := luascript.Run(ctx, l.rdb, []string{nkey}, values...).Result()
	if err != nil {
		// We cannot trust failed keys, flush it
		if l.flusherEnabled() {
			l.wgin.Add(1)
			go l.addKeyToFlushQueue(key)
		}
		return res, err
	}
	values = v.([]interface{})

	raw := values[0].(int64)
	success := values[1].(int64)

	cur := maxZero(raw)

	res.Allowed = success > 0
	res.Remaining = maxZero(maxlimit - cur)
	if res.Allowed {
		res.n = n
	}

	// Let Flusher repair any keys with negative count (incorrect state)
	if res.Allowed && raw < 1 && l.flusherEnabled() {
		res.n = 0
		l.wgin.Add(1)
		go l.addKeyToFlushQueue(key)
	}

	return res, nil
}

func (l *Limiter) delete(ctx context.Context, key string) error {
	pipe := l.rdb.TxPipeline()
	pipe.Del(ctx, redisPrefix+key)
	_, err := pipe.Exec(ctx)
	return err
}

// Decr decreases inflight value, closing the in-flight.
func (l *Limiter) Decr(ctx context.Context, r *Result) error {
	if r.n < 1 {
		return nil
	}

	if !l.enabled {
		return nil
	}

	pipe := l.rdb.TxPipeline()

	// DECRBY + EX
	decr := pipe.DecrBy(ctx, redisPrefix+r.key, int64(r.n))
	pipe.Expire(ctx, redisPrefix+r.key, r.Limit.Timeout)

	_, err := pipe.Exec(ctx)

	// Let Flusher repair any keys with negative count (incorrect state) or on failures
	if (decr.Val() < 1 || err != nil) && l.flusherEnabled() {
		l.wgin.Add(1)
		go l.addKeyToFlushQueue(r.key)
		return err
	}
	return err
}

// FlushTask is instruction to flush an key due to failures.
type flushTask struct {
	Key string
	try int
}

// expired explains if the task has exceeded it's retries.
func (f *flushTask) expired() bool {
	return f.try >= flushRetries
}

// blockWait adds blocking timeout for retry logic to slow down the thread,
// and avoid bursts to a broken redis.
func (f *flushTask) blockWait() {
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

	// Internal counter for landing
	n int
}

// maxZero returns the larger of x or 0.
func maxZero(x int64) int64 {
	if x < 0 {
		return 0
	}
	return x
}

func isNil(i interface{}) bool {
	if i == nil {
		return true
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}

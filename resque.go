// Package resque is a pure-Go (CGO=0) reimplementation of the queue and job
// model of the Ruby [Resque] background-job library, backed by Redis through
// the [github.com/redis/go-redis/v9] client.
//
// It writes and reads exactly the keys and JSON payloads that a real Resque
// (and a Ruby MRI worker) uses, so a job enqueued here can be reserved by a
// Ruby Resque worker and vice-versa:
//
//   - a job is the JSON object {"class":"<Name>","args":[...]} RPUSH'd to the
//     list resque:queue:<name>, and the queue name is SADD'd to resque:queues;
//   - a worker registers under resque:workers, records the job it is running
//     under resque:worker:<id>, tracks resque:stat:processed / resque:stat:failed,
//     and writes the Resque failure hash to the resque:failed list.
//
// The actual body of a job — Resque's Job#perform — is Ruby, so it is an
// injected seam ([WithPerform]); likewise the class→queue mapping that Ruby
// derives from a job class ([WithQueueResolver]) and the wall clock
// ([WithClock]). Everything else — the wire format, key layout and worker
// bookkeeping — is deterministic Go with no goroutines and no sleeps.
//
// It is the Resque backend for [go-embedded-ruby], a sibling of
// [go-ruby-redis] and [go-ruby-set], but is a standalone, reusable module.
//
// [Resque]: https://github.com/resque/resque
// [go-embedded-ruby]: https://github.com/go-embedded-ruby/ruby
// [go-ruby-redis]: https://github.com/go-ruby-redis/redis
// [go-ruby-set]: https://github.com/go-ruby-set/set
package resque

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// defaultNamespace is the Redis::Namespace prefix Resque uses by default.
const defaultNamespace = "resque"

var (
	// ErrNoQueueResolver is returned by [Resque.Enqueue] / [Resque.Dequeue]
	// when no [WithQueueResolver] seam has been configured, mirroring the fact
	// that Ruby derives the queue from the job class.
	ErrNoQueueResolver = errors.New("resque: no queue resolver configured")
	// ErrNoPerform is returned by [Job.Perform] when no [WithPerform] seam has
	// been configured (the job body is Ruby).
	ErrNoPerform = errors.New("resque: no perform seam configured")
)

// PerformFunc is the injectable seam for a job body (Resque's Job#perform).
// It receives the decoded class name and arguments and returns the error the
// Ruby body raised, if any.
type PerformFunc func(class string, args []any) error

// QueueResolver maps a job class to its queue, mirroring the Ruby idiom where a
// job class responds to `queue`. It is consulted by [Resque.Enqueue] and
// [Resque.Dequeue].
type QueueResolver func(class string) (string, error)

// Clock returns the current time; it is injectable so worker and failure
// bookkeeping is deterministic in tests.
type Clock func() time.Time

// Resque is a handle onto a Redis-backed Resque instance. It is safe to share a
// single value; it holds no mutable state of its own.
type Resque struct {
	rdb       *redis.Client
	ctx       context.Context
	namespace string
	now       Clock
	perform   PerformFunc
	resolver  QueueResolver
}

// Option configures a [Resque] in [New].
type Option func(*Resque)

// WithNamespace overrides the Redis key prefix (default "resque").
func WithNamespace(ns string) Option { return func(r *Resque) { r.namespace = ns } }

// WithContext sets the context passed to every Redis command.
func WithContext(ctx context.Context) Option { return func(r *Resque) { r.ctx = ctx } }

// WithClock injects the wall clock used for worker and failure timestamps.
func WithClock(c Clock) Option { return func(r *Resque) { r.now = c } }

// WithPerform injects the job-body seam (Resque's Job#perform).
func WithPerform(f PerformFunc) Option { return func(r *Resque) { r.perform = f } }

// WithQueueResolver injects the class→queue mapping used by [Resque.Enqueue].
func WithQueueResolver(q QueueResolver) Option { return func(r *Resque) { r.resolver = q } }

// New wraps a go-redis client in a [Resque] handle. The client is owned by the
// caller (including Close).
func New(rdb *redis.Client, opts ...Option) *Resque {
	r := &Resque{
		rdb:       rdb,
		ctx:       context.Background(),
		namespace: defaultNamespace,
		now:       time.Now,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// key builds a namespaced Redis key, e.g. key("queue:jobs") -> "resque:queue:jobs".
func (r *Resque) key(suffix string) string { return r.namespace + ":" + suffix }

// resolve maps a class to its queue via the configured resolver.
func (r *Resque) resolve(class string) (string, error) {
	if r.resolver == nil {
		return "", ErrNoQueueResolver
	}
	return r.resolver(class)
}

// encode serialises v the way Ruby's Resque.encode (MultiJson) does: compact
// JSON with no HTML escaping and no trailing newline.
func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// payload is the on-wire job object: {"class":...,"args":[...]}.
type payload struct {
	Class string `json:"class"`
	Args  []any  `json:"args"`
}

// encodePayload produces the exact bytes Resque RPUSHes for a job.
func encodePayload(class string, args []any) ([]byte, error) {
	if args == nil {
		args = []any{}
	}
	return encode(payload{Class: class, Args: args})
}

// Enqueue pushes a job onto the queue derived from class via the configured
// [WithQueueResolver], mirroring Resque.enqueue(klass, *args).
func (r *Resque) Enqueue(class string, args ...any) error {
	queue, err := r.resolve(class)
	if err != nil {
		return err
	}
	return r.EnqueueTo(queue, class, args...)
}

// EnqueueTo pushes a job onto the named queue, mirroring
// Resque.enqueue_to(queue, klass, *args). It SADDs the queue to resque:queues
// and RPUSHes the {"class":...,"args":[...]} payload to resque:queue:<queue>.
func (r *Resque) EnqueueTo(queue, class string, args ...any) error {
	data, err := encodePayload(class, args)
	if err != nil {
		return err
	}
	if err := r.rdb.SAdd(r.ctx, r.key("queues"), queue).Err(); err != nil {
		return err
	}
	return r.rdb.RPush(r.ctx, r.key("queue:"+queue), data).Err()
}

// Dequeue removes matching jobs from the queue derived from class, mirroring
// Resque.dequeue(klass, *args). It returns the number of jobs removed.
func (r *Resque) Dequeue(class string, args ...any) (int64, error) {
	queue, err := r.resolve(class)
	if err != nil {
		return 0, err
	}
	return r.DestroyFrom(queue, class, args...)
}

// DestroyFrom removes matching jobs from the named queue, mirroring
// Resque::Job.destroy(queue, klass, *args). With no args every job of the class
// is removed; with args only jobs whose payload equals the encoded class+args
// are removed. It returns the number of jobs removed.
func (r *Resque) DestroyFrom(queue, class string, args ...any) (int64, error) {
	qkey := r.key("queue:" + queue)
	if len(args) == 0 {
		vals, err := r.rdb.LRange(r.ctx, qkey, 0, -1).Result()
		if err != nil {
			return 0, err
		}
		var destroyed int64
		for _, v := range vals {
			j, err := decode([]byte(v))
			if err != nil {
				return destroyed, err
			}
			if j.Class == class {
				n, err := r.rdb.LRem(r.ctx, qkey, 0, v).Result()
				if err != nil {
					return destroyed, err
				}
				destroyed += n
			}
		}
		return destroyed, nil
	}
	data, err := encodePayload(class, args)
	if err != nil {
		return 0, err
	}
	return r.rdb.LRem(r.ctx, qkey, 0, data).Result()
}

// Size returns the number of jobs on a queue (Resque.size).
func (r *Resque) Size(queue string) (int64, error) {
	return r.rdb.LLen(r.ctx, r.key("queue:"+queue)).Result()
}

// Peek returns up to count jobs starting at index start without removing them
// (Resque.peek).
func (r *Resque) Peek(queue string, start, count int64) ([]*Job, error) {
	stop := start + count - 1
	vals, err := r.rdb.LRange(r.ctx, r.key("queue:"+queue), start, stop).Result()
	if err != nil {
		return nil, err
	}
	jobs := make([]*Job, 0, len(vals))
	for _, v := range vals {
		j, err := decode([]byte(v))
		if err != nil {
			return nil, err
		}
		j.Queue = queue
		j.r = r
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// Pop removes and returns the next job on a queue, or nil if it is empty
// (Resque.pop).
func (r *Resque) Pop(queue string) (*Job, error) { return r.popDecode(queue) }

// popDecode LPOPs one entry off a queue and decodes it. A nil job with a nil
// error means the queue was empty.
func (r *Resque) popDecode(queue string) (*Job, error) {
	val, err := r.rdb.LPop(r.ctx, r.key("queue:"+queue)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	j, err := decode([]byte(val))
	if err != nil {
		return nil, err
	}
	j.Queue = queue
	j.r = r
	return j, nil
}

// Queues returns the registered queue names, sorted for determinism
// (Resque.queues).
func (r *Resque) Queues() ([]string, error) {
	vals, err := r.rdb.SMembers(r.ctx, r.key("queues")).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(vals)
	return vals, nil
}

// Workers returns the registered worker ids, sorted for determinism
// (Resque.workers).
func (r *Resque) Workers() ([]string, error) {
	vals, err := r.rdb.SMembers(r.ctx, r.key("workers")).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(vals)
	return vals, nil
}

// Stat returns a Resque counter (resque:stat:<name>), 0 when unset.
func (r *Resque) Stat(name string) (int64, error) {
	v, err := r.rdb.Get(r.ctx, r.key("stat:"+name)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Processed returns the number of jobs processed (Resque.info[:processed]).
func (r *Resque) Processed() (int64, error) { return r.Stat("processed") }

// FailedStat returns the failed-jobs counter (Resque.info[:failed]).
func (r *Resque) FailedStat() (int64, error) { return r.Stat("failed") }

// FailedCount returns the length of the resque:failed list
// (Resque::Failure.count).
func (r *Resque) FailedCount() (int64, error) {
	return r.rdb.LLen(r.ctx, r.key("failed")).Result()
}

// incrStat increments a Resque counter by one (Resque::Stat.incr).
func (r *Resque) incrStat(name string) error {
	return r.rdb.IncrBy(r.ctx, r.key("stat:"+name), 1).Err()
}

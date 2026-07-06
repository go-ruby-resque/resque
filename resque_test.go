package resque

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// TestEnqueueToPayloadExact asserts the bytes RPUSH'd are byte-compatible with
// what a real Ruby Resque writes: {"class":"<Name>","args":[...]} with no HTML
// escaping and no trailing newline, and that the queue is registered.
func TestEnqueueToPayloadExact(t *testing.T) {
	r, rdb := newTest(t)
	if err := r.EnqueueTo("email", "SendEmail", 42, "a<b>&c", []any{1, 2}); err != nil {
		t.Fatalf("EnqueueTo: %v", err)
	}

	got, err := rdb.LIndex(context.Background(), "resque:queue:email", 0).Result()
	if err != nil {
		t.Fatalf("LIndex: %v", err)
	}
	want := `{"class":"SendEmail","args":[42,"a<b>&c",[1,2]]}`
	mustEqual(t, "payload", got, want)

	members, err := rdb.SMembers(context.Background(), "resque:queues").Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 1 || members[0] != "email" {
		t.Fatalf("resque:queues = %v, want [email]", members)
	}
}

// TestEnqueueNoArgsPayload asserts an argument-less job encodes "args":[].
func TestEnqueueNoArgsPayload(t *testing.T) {
	r, rdb := newTest(t)
	if err := r.EnqueueTo("jobs", "Reindex"); err != nil {
		t.Fatalf("EnqueueTo: %v", err)
	}
	got, _ := rdb.LIndex(context.Background(), "resque:queue:jobs", 0).Result()
	mustEqual(t, "payload", got, `{"class":"Reindex","args":[]}`)
}

func TestEnqueueViaResolver(t *testing.T) {
	seen := ""
	r, rdb := newTest(t, WithQueueResolver(func(class string) (string, error) {
		seen = class
		return "critical", nil
	}))
	if err := r.Enqueue("Backup", 1); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	mustEqual(t, "resolver class", seen, "Backup")
	n, _ := rdb.LLen(context.Background(), "resque:queue:critical").Result()
	mustEqual(t, "critical size", n, int64(1))
}

func TestEnqueueNoResolver(t *testing.T) {
	r, _ := newTest(t)
	if err := r.Enqueue("X"); !errors.Is(err, ErrNoQueueResolver) {
		t.Fatalf("Enqueue: got %v, want ErrNoQueueResolver", err)
	}
}

func TestEnqueueResolverError(t *testing.T) {
	r, _ := newTest(t, WithQueueResolver(func(string) (string, error) { return "", errBoom }))
	if err := r.Enqueue("X"); !errors.Is(err, errBoom) {
		t.Fatalf("Enqueue: got %v, want boom", err)
	}
}

func TestEnqueueEncodeError(t *testing.T) {
	r, _ := newTest(t)
	// A channel cannot be JSON-encoded.
	if err := r.EnqueueTo("q", "C", make(chan int)); err == nil {
		t.Fatal("EnqueueTo: want encode error")
	}
}

func TestEnqueueRedisErrors(t *testing.T) {
	// SADD fails.
	r, rdb := newTest(t)
	failCmd(rdb, "sadd")
	wantBoom(t, "EnqueueTo sadd", r.EnqueueTo("q", "C"))

	// SADD ok, RPUSH fails.
	r2, rdb2 := newTest(t)
	failCmd(rdb2, "rpush")
	wantBoom(t, "EnqueueTo rpush", r2.EnqueueTo("q", "C"))
}

func TestSizePeekPop(t *testing.T) {
	r, _ := newTest(t)
	for _, n := range []int{1, 2, 3} {
		if err := r.EnqueueTo("q", "C", n); err != nil {
			t.Fatalf("EnqueueTo: %v", err)
		}
	}
	size, _ := r.Size("q")
	mustEqual(t, "size", size, int64(3))

	jobs, err := r.Peek("q", 0, 2)
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	mustEqual(t, "peek len", len(jobs), 2)
	mustEqual(t, "peek[0] class", jobs[0].Class, "C")
	mustEqual(t, "peek[0] queue", jobs[0].Queue, "q")

	job, err := r.Pop("q")
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	mustEqual(t, "pop class", job.Class, "C")
	// Args decode as json.Number.
	if got := job.Args[0]; got.(interface{ String() string }).String() != "1" {
		t.Fatalf("pop arg = %v, want 1", got)
	}
	size, _ = r.Size("q")
	mustEqual(t, "size after pop", size, int64(2))
}

func TestPopEmpty(t *testing.T) {
	r, _ := newTest(t)
	job, err := r.Pop("empty")
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if job != nil {
		t.Fatalf("Pop empty: got %v, want nil", job)
	}
}

func TestPopRedisError(t *testing.T) {
	r, rdb := newTest(t)
	failCmd(rdb, "lpop")
	if _, err := r.Pop("q"); !errors.Is(err, errBoom) {
		t.Fatalf("Pop: got %v, want boom", err)
	}
}

func TestPopDecodeError(t *testing.T) {
	r, rdb := newTest(t)
	rdb.RPush(context.Background(), "resque:queue:q", "not-json")
	if _, err := r.Pop("q"); err == nil {
		t.Fatal("Pop: want decode error")
	}
}

func TestPeekRedisError(t *testing.T) {
	r, rdb := newTest(t)
	failCmd(rdb, "lrange")
	if _, err := r.Peek("q", 0, 1); !errors.Is(err, errBoom) {
		t.Fatalf("Peek: got %v, want boom", err)
	}
}

func TestPeekDecodeError(t *testing.T) {
	r, rdb := newTest(t)
	rdb.RPush(context.Background(), "resque:queue:q", "not-json")
	if _, err := r.Peek("q", 0, 1); err == nil {
		t.Fatal("Peek: want decode error")
	}
}

func TestQueuesSorted(t *testing.T) {
	r, _ := newTest(t)
	for _, q := range []string{"z", "a", "m"} {
		r.EnqueueTo(q, "C")
	}
	qs, err := r.Queues()
	if err != nil {
		t.Fatalf("Queues: %v", err)
	}
	if !reflect.DeepEqual(qs, []string{"a", "m", "z"}) {
		t.Fatalf("Queues = %v, want sorted", qs)
	}
}

func TestQueuesRedisError(t *testing.T) {
	r, rdb := newTest(t)
	failCmd(rdb, "smembers")
	if _, err := r.Queues(); !errors.Is(err, errBoom) {
		t.Fatalf("Queues: got %v, want boom", err)
	}
}

func TestWorkersSortedAndError(t *testing.T) {
	r, rdb := newTest(t)
	rdb.SAdd(context.Background(), "resque:workers", "b:2:q", "a:1:q")
	ws, err := r.Workers()
	if err != nil {
		t.Fatalf("Workers: %v", err)
	}
	if !reflect.DeepEqual(ws, []string{"a:1:q", "b:2:q"}) {
		t.Fatalf("Workers = %v, want sorted", ws)
	}
	failCmd(rdb, "smembers")
	if _, err := r.Workers(); !errors.Is(err, errBoom) {
		t.Fatalf("Workers err: got %v, want boom", err)
	}
}

func TestDequeueByArgs(t *testing.T) {
	r, _ := newTest(t, WithQueueResolver(func(string) (string, error) { return "q", nil }))
	r.EnqueueTo("q", "C", 1)
	r.EnqueueTo("q", "C", 2)
	r.EnqueueTo("q", "C", 1)

	n, err := r.Dequeue("C", 1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	mustEqual(t, "removed", n, int64(2))
	size, _ := r.Size("q")
	mustEqual(t, "remaining", size, int64(1))
}

func TestDestroyByClass(t *testing.T) {
	r, _ := newTest(t)
	r.EnqueueTo("q", "A", 1)
	r.EnqueueTo("q", "B", 2)
	r.EnqueueTo("q", "A", 3)

	n, err := r.DestroyFrom("q", "A")
	if err != nil {
		t.Fatalf("DestroyFrom: %v", err)
	}
	mustEqual(t, "removed", n, int64(2))
	size, _ := r.Size("q")
	mustEqual(t, "remaining", size, int64(1))
}

func TestDequeueNoResolver(t *testing.T) {
	r, _ := newTest(t)
	if _, err := r.Dequeue("C"); !errors.Is(err, ErrNoQueueResolver) {
		t.Fatalf("Dequeue: got %v, want ErrNoQueueResolver", err)
	}
}

func TestDestroyErrors(t *testing.T) {
	// LRANGE fails (class path).
	r, rdb := newTest(t)
	failCmd(rdb, "lrange")
	if _, err := r.DestroyFrom("q", "A"); !errors.Is(err, errBoom) {
		t.Fatalf("DestroyFrom lrange: got %v, want boom", err)
	}

	// Decode error (class path): a bad entry in the list.
	r2, rdb2 := newTest(t)
	rdb2.RPush(context.Background(), "resque:queue:q", "not-json")
	if _, err := r2.DestroyFrom("q", "A"); err == nil {
		t.Fatal("DestroyFrom decode: want error")
	}

	// LREM fails (class path): matching entry present, lrem injected to fail.
	r3, rdb3 := newTest(t)
	r3.EnqueueTo("q", "A", 1)
	failCmd(rdb3, "lrem")
	if _, err := r3.DestroyFrom("q", "A"); !errors.Is(err, errBoom) {
		t.Fatalf("DestroyFrom lrem: got %v, want boom", err)
	}

	// Encode error (args path): channel arg.
	r4, _ := newTest(t)
	if _, err := r4.DestroyFrom("q", "A", make(chan int)); err == nil {
		t.Fatal("DestroyFrom encode: want error")
	}

	// LREM fails (args path).
	r5, rdb5 := newTest(t)
	failCmd(rdb5, "lrem")
	if _, err := r5.DestroyFrom("q", "A", 1); !errors.Is(err, errBoom) {
		t.Fatalf("DestroyFrom args lrem: got %v, want boom", err)
	}
}

func TestStats(t *testing.T) {
	r, rdb := newTest(t)

	// Unset -> 0.
	p, err := r.Processed()
	if err != nil {
		t.Fatalf("Processed: %v", err)
	}
	mustEqual(t, "processed unset", p, int64(0))

	rdb.Set(context.Background(), "resque:stat:processed", "7", 0)
	p, _ = r.Processed()
	mustEqual(t, "processed", p, int64(7))

	f, _ := r.FailedStat()
	mustEqual(t, "failed unset", f, int64(0))

	// Corrupt value -> parse error.
	rdb.Set(context.Background(), "resque:stat:failed", "notint", 0)
	if _, err := r.FailedStat(); err == nil {
		t.Fatal("FailedStat corrupt: want error")
	}

	// Redis error.
	failCmd(rdb, "get")
	if _, err := r.Stat("processed"); !errors.Is(err, errBoom) {
		t.Fatalf("Stat get: got %v, want boom", err)
	}
}

func TestFailedCount(t *testing.T) {
	r, rdb := newTest(t)
	rdb.RPush(context.Background(), "resque:failed", "{}", "{}")
	n, err := r.FailedCount()
	if err != nil {
		t.Fatalf("FailedCount: %v", err)
	}
	mustEqual(t, "failed count", n, int64(2))
}

func TestOptionsAndNamespace(t *testing.T) {
	ctx := context.Background()
	r, rdb := newTest(t,
		WithNamespace("myapp"),
		WithContext(ctx),
		WithPerform(func(string, []any) error { return nil }),
	)
	mustEqual(t, "namespace key", r.key("queue:x"), "myapp:queue:x")
	if err := r.EnqueueTo("x", "C"); err != nil {
		t.Fatalf("EnqueueTo: %v", err)
	}
	n, _ := rdb.LLen(ctx, "myapp:queue:x").Result()
	mustEqual(t, "namespaced size", n, int64(1))
}

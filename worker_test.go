package resque

import (
	"context"
	"errors"
	"testing"
)

func newWorker(r *Resque) *Worker {
	return r.NewWorker(WorkerConfig{Hostname: "box", PID: 42, Queues: []string{"jobs"}})
}

func TestWorkerID(t *testing.T) {
	r, _ := newTest(t)
	w := r.NewWorker(WorkerConfig{Hostname: "host", PID: 7, Queues: []string{"a", "b"}})
	mustEqual(t, "id", w.ID(), "host:7:a,b")
}

func TestRegisterAndUnregister(t *testing.T) {
	ctx := context.Background()
	r, rdb := newTest(t)
	w := newWorker(r)

	if err := w.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	isMember, _ := rdb.SIsMember(ctx, "resque:workers", "box:42:jobs").Result()
	if !isMember {
		t.Fatal("Register: worker not in resque:workers")
	}
	started, _ := rdb.Get(ctx, "resque:worker:box:42:jobs:started").Result()
	mustEqual(t, "started", started, "2026-07-06 12:00:00 +0000")

	// Seed per-worker bookkeeping so Unregister clears it.
	rdb.Set(ctx, "resque:worker:box:42:jobs", "{}", 0)
	rdb.Set(ctx, "resque:stat:processed:box:42:jobs", "3", 0)
	rdb.Set(ctx, "resque:stat:failed:box:42:jobs", "1", 0)

	if err := w.Unregister(); err != nil {
		t.Fatalf("Unregister: %v", err)
	}
	isMember, _ = rdb.SIsMember(ctx, "resque:workers", "box:42:jobs").Result()
	if isMember {
		t.Fatal("Unregister: worker still in resque:workers")
	}
	for _, k := range []string{
		"resque:worker:box:42:jobs",
		"resque:worker:box:42:jobs:started",
		"resque:stat:processed:box:42:jobs",
		"resque:stat:failed:box:42:jobs",
	} {
		if n, _ := rdb.Exists(ctx, k).Result(); n != 0 {
			t.Fatalf("Unregister: key %s survived", k)
		}
	}
}

func TestRegisterErrors(t *testing.T) {
	r, rdb := newTest(t)
	failCmd(rdb, "sadd")
	wantBoom(t, "Register sadd", newWorker(r).Register())

	r2, rdb2 := newTest(t)
	failCmd(rdb2, "set")
	wantBoom(t, "Register set", newWorker(r2).Register())
}

func TestUnregisterErrors(t *testing.T) {
	r, rdb := newTest(t)
	failCmd(rdb, "srem")
	wantBoom(t, "Unregister srem", newWorker(r).Unregister())

	r2, rdb2 := newTest(t)
	failCmd(rdb2, "del")
	wantBoom(t, "Unregister del", newWorker(r2).Unregister())
}

// TestWorkingOnPayloadExact asserts the resque:worker:<id> record matches the
// Resque format byte-for-byte.
func TestWorkingOnPayloadExact(t *testing.T) {
	ctx := context.Background()
	r, rdb := newTest(t)
	r.EnqueueTo("jobs", "Job", 1)
	job, _ := r.Pop("jobs")
	w := newWorker(r)

	if err := w.WorkingOn(job); err != nil {
		t.Fatalf("WorkingOn: %v", err)
	}
	got, _ := rdb.Get(ctx, "resque:worker:box:42:jobs").Result()
	want := `{"queue":"jobs","run_at":"2026-07-06T12:00:00Z","payload":{"class":"Job","args":[1]}}`
	mustEqual(t, "working_on payload", got, want)
}

func TestWorkingOnErrors(t *testing.T) {
	// Encode error: invalid raw payload.
	r, _ := newTest(t)
	bad := &Job{Queue: "q", Class: "C", raw: []byte("not-json"), r: r}
	if err := newWorker(r).WorkingOn(bad); err == nil {
		t.Fatal("WorkingOn encode: want error")
	}

	// Redis SET error.
	r2, rdb2 := newTest(t)
	r2.EnqueueTo("jobs", "C")
	job, _ := r2.Pop("jobs")
	failCmd(rdb2, "set")
	wantBoom(t, "WorkingOn set", newWorker(r2).WorkingOn(job))
}

func TestDoneWorking(t *testing.T) {
	ctx := context.Background()
	r, rdb := newTest(t)
	w := newWorker(r)
	rdb.Set(ctx, "resque:worker:box:42:jobs", "{}", 0)

	if err := w.DoneWorking(); err != nil {
		t.Fatalf("DoneWorking: %v", err)
	}
	p, _ := r.Processed()
	mustEqual(t, "processed", p, int64(1))
	perWorker, _ := r.Stat("processed:box:42:jobs")
	mustEqual(t, "processed per-worker", perWorker, int64(1))
	if n, _ := rdb.Exists(ctx, "resque:worker:box:42:jobs").Result(); n != 0 {
		t.Fatal("DoneWorking: busy marker survived")
	}
}

func TestDoneWorkingErrors(t *testing.T) {
	// First INCRBY fails.
	r, rdb := newTest(t)
	failNth(rdb, "incrby", 1)
	wantBoom(t, "DoneWorking incr1", newWorker(r).DoneWorking())

	// Second INCRBY fails.
	r2, rdb2 := newTest(t)
	failNth(rdb2, "incrby", 2)
	wantBoom(t, "DoneWorking incr2", newWorker(r2).DoneWorking())

	// DEL fails.
	r3, rdb3 := newTest(t)
	failCmd(rdb3, "del")
	wantBoom(t, "DoneWorking del", newWorker(r3).DoneWorking())
}

func TestReportFailedErrors(t *testing.T) {
	// First INCRBY (stat:failed) fails.
	r, rdb := newTest(t, WithPerform(func(string, []any) error { return errBoom }))
	r.EnqueueTo("jobs", "C")
	job, _ := r.Pop("jobs")
	failNth(rdb, "incrby", 1)
	if err := newWorker(r).process(job); !errors.Is(err, errBoom) {
		t.Fatalf("process reportFailed incr1: got %v, want boom", err)
	}

	// Second INCRBY (stat:failed:<id>) fails (tail return of reportFailed).
	r2, rdb2 := newTest(t, WithPerform(func(string, []any) error { return errBoom }))
	r2.EnqueueTo("jobs", "C")
	job2, _ := r2.Pop("jobs")
	failNth(rdb2, "incrby", 2)
	if err := newWorker(r2).process(job2); !errors.Is(err, errBoom) {
		t.Fatalf("process reportFailed incr2: got %v, want boom", err)
	}
}

func TestProcessSuccess(t *testing.T) {
	ran := false
	r, _ := newTest(t, WithPerform(func(string, []any) error { ran = true; return nil }))
	r.EnqueueTo("jobs", "C")
	job, _ := r.Pop("jobs")
	if err := newWorker(r).process(job); err != nil {
		t.Fatalf("process: %v", err)
	}
	if !ran {
		t.Fatal("perform seam not invoked")
	}
}

func TestProcessFailureRecords(t *testing.T) {
	r, _ := newTest(t, WithPerform(func(string, []any) error { return errBoom }))
	r.EnqueueTo("jobs", "C", 1)
	job, _ := r.Pop("jobs")
	w := newWorker(r)
	if err := w.process(job); err != nil {
		t.Fatalf("process: %v", err)
	}
	f, _ := r.FailedStat()
	mustEqual(t, "failed stat", f, int64(1))
	fc, _ := r.FailedCount()
	mustEqual(t, "failed list", fc, int64(1))
}

func TestProcessFailAndFailError(t *testing.T) {
	// process where Job.Fail's RPUSH fails (covers process's tail return).
	r, rdb := newTest(t, WithPerform(func(string, []any) error { return errBoom }))
	r.EnqueueTo("jobs", "C")
	job, _ := r.Pop("jobs")
	failCmd(rdb, "rpush")
	if err := newWorker(r).process(job); !errors.Is(err, errBoom) {
		t.Fatalf("process fail-rpush: got %v, want boom", err)
	}
}

func TestWorkOne(t *testing.T) {
	r, _ := newTest(t, WithPerform(func(string, []any) error { return nil }))
	r.EnqueueTo("jobs", "C")
	ok, err := newWorker(r).WorkOne()
	if err != nil {
		t.Fatalf("WorkOne: %v", err)
	}
	if !ok {
		t.Fatal("WorkOne: got false, want true")
	}

	// Empty queues -> false, nil.
	ok, err = newWorker(r).WorkOne()
	if err != nil {
		t.Fatalf("WorkOne empty: %v", err)
	}
	if ok {
		t.Fatal("WorkOne empty: got true, want false")
	}
}

func TestWorkOneErrors(t *testing.T) {
	// Reserve error.
	r, rdb := newTest(t)
	failCmd(rdb, "lpop")
	if _, err := newWorker(r).WorkOne(); !errors.Is(err, errBoom) {
		t.Fatalf("WorkOne reserve: got %v, want boom", err)
	}

	// WorkingOn (SET) error.
	r2, rdb2 := newTest(t, WithPerform(func(string, []any) error { return nil }))
	r2.EnqueueTo("jobs", "C")
	failCmd(rdb2, "set")
	if _, err := newWorker(r2).WorkOne(); !errors.Is(err, errBoom) {
		t.Fatalf("WorkOne workingon: got %v, want boom", err)
	}

	// process error (perform ok, but DoneWorking is fine; force fail via
	// reportFailed path is separate — here force process via Fail rpush).
	r3, rdb3 := newTest(t, WithPerform(func(string, []any) error { return errBoom }))
	r3.EnqueueTo("jobs", "C")
	failCmd(rdb3, "rpush")
	if _, err := newWorker(r3).WorkOne(); !errors.Is(err, errBoom) {
		t.Fatalf("WorkOne process: got %v, want boom", err)
	}

	// DoneWorking error (perform succeeds, DEL... use incrby fail).
	r4, rdb4 := newTest(t, WithPerform(func(string, []any) error { return nil }))
	r4.EnqueueTo("jobs", "C")
	failCmd(rdb4, "incrby")
	if _, err := newWorker(r4).WorkOne(); !errors.Is(err, errBoom) {
		t.Fatalf("WorkOne doneworking: got %v, want boom", err)
	}
}

func TestWorkDrainsQueue(t *testing.T) {
	ctx := context.Background()
	performed := 0
	r, rdb := newTest(t, WithPerform(func(string, []any) error { performed++; return nil }))
	r.EnqueueTo("jobs", "C", 1)
	r.EnqueueTo("jobs", "C", 2)

	n, err := newWorker(r).Work()
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	mustEqual(t, "processed count", n, 2)
	mustEqual(t, "performed", performed, 2)

	p, _ := r.Processed()
	mustEqual(t, "processed stat", p, int64(2))
	// Worker unregistered at the end.
	if m, _ := rdb.SIsMember(ctx, "resque:workers", "box:42:jobs").Result(); m {
		t.Fatal("Work: worker still registered")
	}
}

func TestWorkCountsFailuresAsProcessed(t *testing.T) {
	r, _ := newTest(t, WithPerform(func(string, []any) error { return errBoom }))
	r.EnqueueTo("jobs", "C")

	n, err := newWorker(r).Work()
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	mustEqual(t, "processed count", n, 1)
	f, _ := r.FailedStat()
	mustEqual(t, "failed stat", f, int64(1))
	p, _ := r.Processed()
	mustEqual(t, "processed stat", p, int64(1))
}

func TestWorkErrors(t *testing.T) {
	// Register error.
	r, rdb := newTest(t)
	failCmd(rdb, "sadd")
	if _, err := newWorker(r).Work(); !errors.Is(err, errBoom) {
		t.Fatalf("Work register: got %v, want boom", err)
	}

	// WorkOne error mid-loop (register ok, reserve fails).
	r2, rdb2 := newTest(t)
	r2.EnqueueTo("jobs", "C")
	failCmd(rdb2, "lpop")
	if _, err := newWorker(r2).Work(); !errors.Is(err, errBoom) {
		t.Fatalf("Work workone: got %v, want boom", err)
	}

	// Unregister error (empty queue -> loop breaks -> unregister srem fails).
	r3, rdb3 := newTest(t)
	failCmd(rdb3, "srem")
	if _, err := newWorker(r3).Work(); !errors.Is(err, errBoom) {
		t.Fatalf("Work unregister: got %v, want boom", err)
	}
}

package resque

import (
	"context"
	"errors"
	"testing"
)

func TestReserveMultiQueue(t *testing.T) {
	r, _ := newTest(t)
	// low is empty; high has a job. Reserve should skip low and take from high.
	r.EnqueueTo("high", "Urgent", 1)

	job, err := r.Reserve("low", "high")
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if job == nil {
		t.Fatal("Reserve: got nil, want job")
	}
	mustEqual(t, "class", job.Class, "Urgent")
	mustEqual(t, "queue", job.Queue, "high")

	// Both empty now.
	job, err = r.Reserve("low", "high")
	if err != nil {
		t.Fatalf("Reserve empty: %v", err)
	}
	if job != nil {
		t.Fatalf("Reserve empty: got %v, want nil", job)
	}
}

func TestReserveError(t *testing.T) {
	r, rdb := newTest(t)
	failCmd(rdb, "lpop")
	if _, err := r.Reserve("q"); !errors.Is(err, errBoom) {
		t.Fatalf("Reserve: got %v, want boom", err)
	}
}

func TestDecodeArgsNull(t *testing.T) {
	// A payload with "args":null must decode to a non-nil empty slice.
	j, err := decode([]byte(`{"class":"C","args":null}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if j.Args == nil || len(j.Args) != 0 {
		t.Fatalf("args = %v, want []", j.Args)
	}
}

func TestPerformSeam(t *testing.T) {
	// No seam configured.
	r, _ := newTest(t)
	r.EnqueueTo("q", "C")
	job, _ := r.Pop("q")
	if err := job.Perform(); !errors.Is(err, ErrNoPerform) {
		t.Fatalf("Perform no seam: got %v, want ErrNoPerform", err)
	}

	// Seam that succeeds and receives class + args.
	var gotClass string
	var gotArgs []any
	r2, _ := newTest(t, WithPerform(func(class string, args []any) error {
		gotClass, gotArgs = class, args
		return nil
	}))
	r2.EnqueueTo("q", "Hello", 1)
	job2, _ := r2.Pop("q")
	if err := job2.Perform(); err != nil {
		t.Fatalf("Perform: %v", err)
	}
	mustEqual(t, "class", gotClass, "Hello")
	mustEqual(t, "args len", len(gotArgs), 1)

	// Seam that raises.
	r3, _ := newTest(t, WithPerform(func(string, []any) error { return errBoom }))
	r3.EnqueueTo("q", "C")
	job3, _ := r3.Pop("q")
	if err := job3.Perform(); !errors.Is(err, errBoom) {
		t.Fatalf("Perform raise: got %v, want boom", err)
	}
}

func TestJobErrorError(t *testing.T) {
	mustEqual(t, "message", (&JobError{Message: "bad input"}).Error(), "bad input")
	mustEqual(t, "class fallback", (&JobError{Exception: "ArgumentError"}).Error(), "ArgumentError")
}

// TestFailPayloadExact asserts the failure record matches the Resque failure
// hash format byte-for-byte, with a JobError supplying the Ruby exception.
func TestFailPayloadExact(t *testing.T) {
	r, rdb := newTest(t)
	r.EnqueueTo("mail", "SendMail", 5)
	job, _ := r.Pop("mail")
	w := r.NewWorker(WorkerConfig{Hostname: "box", PID: 99, Queues: []string{"mail"}})

	cause := &JobError{
		Exception: "ArgumentError",
		Message:   "wrong number of arguments",
		Backtrace: []string{"app.rb:1:in `run'", "app.rb:9:in `main'"},
	}
	if err := job.Fail(cause, w); err != nil {
		t.Fatalf("Fail: %v", err)
	}

	got, _ := rdb.LIndex(context.Background(), "resque:failed", 0).Result()
	want := `{"failed_at":"2026/07/06 12:00:00 UTC","payload":{"class":"SendMail","args":[5]},"exception":"ArgumentError","error":"wrong number of arguments","backtrace":["app.rb:1:in ` + "`run'" + `","app.rb:9:in ` + "`main'" + `"],"worker":"box:99:mail","queue":"mail"}`
	mustEqual(t, "failure payload", got, want)
}

// TestFailPlainError covers the non-JobError path: default exception class and
// an empty backtrace serialised as [].
func TestFailPlainError(t *testing.T) {
	r, rdb := newTest(t)
	r.EnqueueTo("q", "C")
	job, _ := r.Pop("q")
	if err := job.Fail(errors.New("kaboom"), nil); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	got, _ := rdb.LIndex(context.Background(), "resque:failed", 0).Result()
	want := `{"failed_at":"2026/07/06 12:00:00 UTC","payload":{"class":"C","args":[]},"exception":"RuntimeError","error":"kaboom","backtrace":[],"worker":"","queue":"q"}`
	mustEqual(t, "failure payload", got, want)
}

// TestFailJobErrorDefaults covers a JobError with an empty Exception (defaults
// to RuntimeError) and a nil Backtrace (serialised as []).
func TestFailJobErrorDefaults(t *testing.T) {
	r, rdb := newTest(t)
	r.EnqueueTo("q", "C")
	job, _ := r.Pop("q")
	if err := job.Fail(&JobError{Message: "oops"}, nil); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	got, _ := rdb.LIndex(context.Background(), "resque:failed", 0).Result()
	want := `{"failed_at":"2026/07/06 12:00:00 UTC","payload":{"class":"C","args":[]},"exception":"RuntimeError","error":"oops","backtrace":[],"worker":"","queue":"q"}`
	mustEqual(t, "failure payload", got, want)
}

func TestFailEncodeError(t *testing.T) {
	// A job whose raw payload is invalid JSON makes the failure record fail to
	// encode (the payload is embedded verbatim as json.RawMessage).
	r, _ := newTest(t)
	job := &Job{Queue: "q", Class: "C", raw: []byte("not-json"), r: r}
	if err := job.Fail(errors.New("x"), nil); err == nil {
		t.Fatal("Fail: want encode error")
	}
}

func TestFailRedisError(t *testing.T) {
	r, rdb := newTest(t)
	r.EnqueueTo("q", "C")
	job, _ := r.Pop("q")
	failCmd(rdb, "rpush")
	if err := job.Fail(errors.New("x"), nil); !errors.Is(err, errBoom) {
		t.Fatalf("Fail: got %v, want boom", err)
	}
}

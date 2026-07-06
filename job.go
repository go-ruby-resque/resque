package resque

import (
	"bytes"
	"encoding/json"
	"errors"
)

// defaultException is the exception class recorded for a failure whose cause is
// not a [*JobError] (a plain Go error carries no Ruby class name). Ruby raises
// RuntimeError for a bare `raise "msg"`, so that is the faithful default.
const defaultException = "RuntimeError"

// failedAtLayout matches Ruby's Time.now.utc.strftime("%Y/%m/%d %H:%M:%S %Z")
// used by Resque::Failure, e.g. "2026/07/06 12:00:00 UTC".
const failedAtLayout = "2006/01/02 15:04:05 MST"

// Job is a reserved unit of work: the decoded {"class":...,"args":[...]}
// payload plus the queue it came from.
type Job struct {
	// Queue is the queue the job was taken from.
	Queue string
	// Class is the Ruby job class name.
	Class string
	// Args are the decoded arguments (JSON numbers stay as json.Number).
	Args []any

	raw []byte  // the exact payload bytes, reused verbatim downstream
	r   *Resque // owning handle (perform seam, clock, redis)
}

// JobError describes a Ruby exception raised by a job body. A [PerformFunc] may
// return one so that [Job.Fail] records the true Ruby exception class, message
// and backtrace; a plain error is recorded with the [defaultException] class.
type JobError struct {
	// Exception is the Ruby exception class name (e.g. "ArgumentError").
	Exception string
	// Message is the exception message (exception.to_s).
	Message string
	// Backtrace is the Ruby backtrace, innermost frame first.
	Backtrace []string
}

// Error implements error.
func (e *JobError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return e.Exception
}

// decode parses a Resque job payload, preserving the exact bytes for later
// reuse (working-on / failure records embed the payload verbatim).
func decode(data []byte) (*Job, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var p payload
	if err := dec.Decode(&p); err != nil {
		return nil, err
	}
	if p.Args == nil {
		p.Args = []any{}
	}
	return &Job{
		Class: p.Class,
		Args:  p.Args,
		raw:   append([]byte(nil), data...),
	}, nil
}

// Reserve LPOPs the next job from the first non-empty queue, decodes it, and
// returns it ready to perform. A nil job with a nil error means every queue was
// empty (Resque::Job.reserve over a worker's queue list).
func (r *Resque) Reserve(queues ...string) (*Job, error) {
	for _, q := range queues {
		j, err := r.popDecode(q)
		if err != nil {
			return nil, err
		}
		if j != nil {
			return j, nil
		}
	}
	return nil, nil
}

// Perform runs the job body via the configured [WithPerform] seam. It returns
// [ErrNoPerform] if no seam is set, otherwise the error the body raised.
func (j *Job) Perform() error {
	if j.r == nil || j.r.perform == nil {
		return ErrNoPerform
	}
	return j.r.perform(j.Class, j.Args)
}

// failure is the Resque::Failure::Redis record; field order is the exact
// insertion order Ruby serialises.
type failure struct {
	FailedAt  string          `json:"failed_at"`
	Payload   json.RawMessage `json:"payload"`
	Exception string          `json:"exception"`
	Error     string          `json:"error"`
	Backtrace []string        `json:"backtrace"`
	Worker    string          `json:"worker"`
	Queue     string          `json:"queue"`
}

// failInfo extracts the exception class, message and backtrace from a cause.
func failInfo(err error) (exception, message string, backtrace []string) {
	var je *JobError
	if errors.As(err, &je) {
		exception = je.Exception
		if exception == "" {
			exception = defaultException
		}
		backtrace = je.Backtrace
		if backtrace == nil {
			backtrace = []string{}
		}
		return exception, je.Message, backtrace
	}
	return defaultException, err.Error(), []string{}
}

// Fail records a job failure in the resque:failed list using the Resque failure
// hash format. The worker may be nil (its id is recorded as the empty string).
func (j *Job) Fail(cause error, w *Worker) error {
	exception, message, backtrace := failInfo(cause)
	workerID := ""
	if w != nil {
		workerID = w.ID()
	}
	rec := failure{
		FailedAt:  j.r.now().UTC().Format(failedAtLayout),
		Payload:   json.RawMessage(j.raw),
		Exception: exception,
		Error:     message,
		Backtrace: backtrace,
		Worker:    workerID,
		Queue:     j.Queue,
	}
	data, err := encode(rec)
	if err != nil {
		return err
	}
	return j.r.rdb.RPush(j.r.ctx, j.r.key("failed"), data).Err()
}

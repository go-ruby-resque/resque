package resque

import (
	"encoding/json"
	"strconv"
	"strings"
)

// Ruby time layouts used by the worker registry.
const (
	// startedLayout matches Ruby's Time.now.to_s, e.g. "2026-07-06 12:00:00 +0000".
	startedLayout = "2006-01-02 15:04:05 -0700"
	// runAtLayout matches Ruby's Time.now.utc.iso8601, e.g. "2026-07-06T12:00:00Z".
	runAtLayout = "2006-01-02T15:04:05Z07:00"
)

// Worker models a Resque worker: it reserves jobs from an ordered list of
// queues, performs them and keeps the Resque registry and stats up to date.
// It runs synchronously — no goroutines, no sleeps — so its behaviour is fully
// deterministic.
type Worker struct {
	r        *Resque
	Hostname string
	PID      int
	Queues   []string
}

// WorkerConfig identifies a worker. Hostname and PID are seams (Ruby reads them
// from the process) so worker ids are deterministic in tests.
type WorkerConfig struct {
	Hostname string
	PID      int
	Queues   []string
}

// NewWorker builds a worker bound to this handle.
func (r *Resque) NewWorker(cfg WorkerConfig) *Worker {
	return &Worker{
		r:        r,
		Hostname: cfg.Hostname,
		PID:      cfg.PID,
		Queues:   cfg.Queues,
	}
}

// ID returns the Resque worker id "<hostname>:<pid>:<queues>".
func (w *Worker) ID() string {
	return w.Hostname + ":" + strconv.Itoa(w.PID) + ":" + strings.Join(w.Queues, ",")
}

// Register adds the worker to resque:workers and records its start time
// (Resque::Worker#register_worker + #started!).
func (w *Worker) Register() error {
	if err := w.r.rdb.SAdd(w.r.ctx, w.r.key("workers"), w.ID()).Err(); err != nil {
		return err
	}
	started := w.r.now().Format(startedLayout)
	return w.r.rdb.Set(w.r.ctx, w.r.key("worker:"+w.ID()+":started"), started, 0).Err()
}

// Unregister removes the worker and its per-worker bookkeeping
// (Resque::Worker#unregister_worker).
func (w *Worker) Unregister() error {
	id := w.ID()
	if err := w.r.rdb.SRem(w.r.ctx, w.r.key("workers"), id).Err(); err != nil {
		return err
	}
	return w.r.rdb.Del(w.r.ctx,
		w.r.key("worker:"+id),
		w.r.key("worker:"+id+":started"),
		w.r.key("stat:processed:"+id),
		w.r.key("stat:failed:"+id),
	).Err()
}

// workingOn is the resque:worker:<id> record while a job runs.
type workingOn struct {
	Queue   string          `json:"queue"`
	RunAt   string          `json:"run_at"`
	Payload json.RawMessage `json:"payload"`
}

// WorkingOn marks the worker as busy on job (Resque::Worker#working_on).
func (w *Worker) WorkingOn(j *Job) error {
	rec := workingOn{
		Queue:   j.Queue,
		RunAt:   w.r.now().UTC().Format(runAtLayout),
		Payload: json.RawMessage(j.raw),
	}
	data, err := encode(rec)
	if err != nil {
		return err
	}
	return w.r.rdb.Set(w.r.ctx, w.r.key("worker:"+w.ID()), data, 0).Err()
}

// DoneWorking clears the busy marker and bumps processed counters
// (Resque::Worker#done_working + #processed!).
func (w *Worker) DoneWorking() error {
	id := w.ID()
	if err := w.r.incrStat("processed"); err != nil {
		return err
	}
	if err := w.r.incrStat("processed:" + id); err != nil {
		return err
	}
	return w.r.rdb.Del(w.r.ctx, w.r.key("worker:"+id)).Err()
}

// reportFailed bumps failed counters (Resque::Worker#report_failed).
func (w *Worker) reportFailed() error {
	if err := w.r.incrStat("failed"); err != nil {
		return err
	}
	return w.r.incrStat("failed:" + w.ID())
}

// process runs one reserved job: perform it, and on error record the failure
// (Resque::Worker#perform). A perform error is bookkeeping, not a worker error,
// so the loop keeps going; a Redis error while recording is returned.
func (w *Worker) process(j *Job) error {
	perr := j.Perform()
	if perr == nil {
		return nil
	}
	if err := w.reportFailed(); err != nil {
		return err
	}
	return j.Fail(perr, w)
}

// WorkOne reserves and processes a single job. It reports whether a job was
// found; a false with a nil error means every queue was empty.
func (w *Worker) WorkOne() (bool, error) {
	j, err := w.r.Reserve(w.Queues...)
	if err != nil {
		return false, err
	}
	if j == nil {
		return false, nil
	}
	if err := w.WorkingOn(j); err != nil {
		return false, err
	}
	if err := w.process(j); err != nil {
		return false, err
	}
	if err := w.DoneWorking(); err != nil {
		return false, err
	}
	return true, nil
}

// Work registers the worker, drains its queues one job at a time until they are
// empty (Resque's interval-zero work loop), then unregisters. It returns the
// number of jobs processed. Failed jobs still count as processed, matching
// Resque, where done_working runs in an ensure block.
func (w *Worker) Work() (int, error) {
	if err := w.Register(); err != nil {
		return 0, err
	}
	n := 0
	for {
		ok, err := w.WorkOne()
		if err != nil {
			return n, err
		}
		if !ok {
			break
		}
		n++
	}
	if err := w.Unregister(); err != nil {
		return n, err
	}
	return n, nil
}

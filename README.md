<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-resque/brand/main/social/go-ruby-resque-resque.png" alt="go-ruby-resque/resque" width="720"></p>

# resque — go-ruby-resque

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-resque.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo) reimplementation of the queue and job model of the Ruby
[`resque`](https://github.com/resque/resque) background-job library**, backed by
Redis through the [`go-redis`](https://github.com/redis/go-redis) client. It
writes and reads the **exact keys and JSON payloads** that a real Resque — and a
Ruby MRI worker — uses, so a job enqueued from Go can be reserved by a Ruby
Resque worker and vice-versa.

It is the Resque backend for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby), but is a
**standalone, reusable** module — a sibling of
[go-ruby-redis](https://github.com/go-ruby-redis/redis) and
[go-ruby-set](https://github.com/go-ruby-set/set).

> **What it is — and isn't.** The wire format, key layout and worker bookkeeping
> are fully deterministic and live here as pure Go. The two things that are
> genuinely Ruby — the **body of a job** (`Job#perform`) and the **class→queue**
> mapping a job class derives — are **injected seams**, mirroring the go-ruby-\*
> pattern. The host (a Ruby VM, or your Go code) owns those; everything else is
> byte-for-byte Resque.

## Byte-compatible with Resque

A job is the JSON object Resque `RPUSH`es, with no HTML escaping and no trailing
newline:

```json
{"class":"SendEmail","args":[42,"a<b>&c",[1,2]]}
```

| Concern            | Redis key                          | Value |
| ------------------ | ---------------------------------- | ----- |
| queue registry     | `resque:queues` (set)              | queue names |
| a queue            | `resque:queue:<name>` (list)       | job payloads |
| failures           | `resque:failed` (list)             | Resque failure hash |
| worker registry    | `resque:workers` (set)             | worker ids `<host>:<pid>:<queues>` |
| a busy worker      | `resque:worker:<id>`               | `{"queue":…,"run_at":…,"payload":…}` |
| worker start time  | `resque:worker:<id>:started`       | `Time#to_s` |
| counters           | `resque:stat:processed` / `:failed`| integers |

The failure record uses the `Resque::Failure` field order verbatim:

```json
{"failed_at":"2026/07/06 12:00:00 UTC","payload":{"class":"C","args":[]},
 "exception":"RuntimeError","error":"kaboom","backtrace":[],"worker":"","queue":"q"}
```

## Features

- **Enqueue / dequeue** — `Enqueue(class, args…)` (queue resolved from the class
  via the injected resolver), `EnqueueTo(queue, class, args…)`, `Dequeue` and
  `DestroyFrom` (remove by class, or by exact class+args payload).
- **Queue introspection** — `Size`, `Peek(queue, start, count)`, `Pop`,
  `Queues`, `Workers`.
- **Jobs** — `Reserve(queues…)` (LPOP the first non-empty queue + decode),
  `Job.Perform()` via the injectable `PerformFunc` seam, and `Job.Fail(err,
  worker)` which writes the Resque failure hash. A `*JobError` carries the true
  Ruby exception class, message and backtrace; a plain error is recorded as
  `RuntimeError`.
- **Worker** — a synchronous, goroutine-free `Worker`: `Register` /
  `Unregister`, `WorkingOn`, `DoneWorking`, `WorkOne`, and a `Work` loop that
  drains its queues one job at a time (interval-zero) and keeps every Resque
  counter and registry key faithful. Failed jobs still count as processed, just
  as in Resque's `ensure`-block `done_working`.
- **Deterministic** — injectable clock (`WithClock`) and worker identity; no
  sleeps and no goroutines, so behaviour is fully reproducible.
- **CGO-free** and validated on all six supported 64-bit targets (amd64, arm64,
  riscv64, loong64, ppc64le, s390x) across Linux, macOS and Windows.

## Usage

```go
package main

import (
	"fmt"

	"github.com/go-ruby-resque/resque"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	r := resque.New(rdb,
		// The class→queue mapping is Ruby's job — inject it.
		resque.WithQueueResolver(func(class string) (string, error) {
			return "default", nil
		}),
		// The job body is Ruby — inject it.
		resque.WithPerform(func(class string, args []any) error {
			fmt.Printf("running %s%v\n", class, args)
			return nil
		}),
	)

	// Enqueue a job, byte-compatible with a Ruby Resque producer.
	_ = r.Enqueue("SendEmail", 42, "hello")

	// A worker drains the queue.
	w := r.NewWorker(resque.WorkerConfig{
		Hostname: "host", PID: 1234, Queues: []string{"default"},
	})
	processed, _ := w.Work()
	fmt.Println("processed:", processed)
}
```

## Tests & coverage

The tests run against an **in-process [miniredis](https://github.com/alicebob/miniredis)**
started per test (torn down via `t.Cleanup`), so no external Redis server is
needed and nothing leaks between tests. miniredis is a **test-only** dependency:
the runtime import graph pulls in the redis client only.

```sh
go test -race ./...            # race-clean
# 100% line coverage, enforced in CI:
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # total: 100.0%
```

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-ruby-resque/resque authors.

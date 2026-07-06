package resque

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// errBoom is the injected Redis failure used to exercise error branches.
var errBoom = errors.New("boom")

// fixedClock is a deterministic clock: 2026-07-06 12:00:00 UTC.
func fixedClock() time.Time {
	return time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
}

// newTest starts an in-process miniredis, wires a go-redis client to it, and
// returns a Resque handle plus the raw client. Everything is torn down via
// t.Cleanup so no server, connection or goroutine leaks between tests.
func newTest(t *testing.T, opts ...Option) (*Resque, *redis.Client) {
	t.Helper()
	srv, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(srv.Close)

	rdb := redis.NewClient(&redis.Options{Addr: srv.Addr()})
	t.Cleanup(func() {
		if err := rdb.Close(); err != nil {
			t.Errorf("client close: %v", err)
		}
	})

	defOpts := []Option{WithClock(fixedClock)}
	r := New(rdb, append(defOpts, opts...)...)
	return r, rdb
}

// failHook injects errBoom into commands selected by match.
type failHook struct{ match func(redis.Cmder) bool }

func (h failHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h failHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h failHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.match(cmd) {
			cmd.SetErr(errBoom)
			return errBoom
		}
		return next(ctx, cmd)
	}
}

// failAt returns a matcher that fails the nth (1-based) invocation of the named
// command; nth == 0 fails every invocation.
func failAt(name string, nth int) func(redis.Cmder) bool {
	count := 0
	return func(c redis.Cmder) bool {
		if strings.EqualFold(c.Name(), name) {
			count++
			return nth == 0 || count == nth
		}
		return false
	}
}

// failCmd installs a hook failing every invocation of the named command.
func failCmd(rdb *redis.Client, name string) { rdb.AddHook(failHook{failAt(name, 0)}) }

// failNth installs a hook failing the nth invocation of the named command.
func failNth(rdb *redis.Client, name string, nth int) { rdb.AddHook(failHook{failAt(name, nth)}) }

// mustEqual fails the test unless got == want.
func mustEqual[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// wantBoom asserts err is the injected failure.
func wantBoom(t *testing.T, where string, err error) {
	t.Helper()
	if !errors.Is(err, errBoom) {
		t.Fatalf("%s: got err %v, want boom", where, err)
	}
}

var _ = context.Background

package ratelimiter

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time          { return f.now }
func (f *fakeClock) Advance(d time.Duration) { f.now = f.now.Add(d) }

func TestTokenBucket(t *testing.T) {
	t.Run("allows request when tokens are available", func(t *testing.T) {
		tb := NewTokenBucket(5.0, 2.0)
		if !tb.Allow("key-1") {
			t.Fail()
		}
	})

	t.Run("denies request when bucket is empty", func(t *testing.T) {
		tb := NewTokenBucket(1.0, 0.0)
		if !tb.Allow("key-2") {
			t.Fail()
		}

		if tb.Allow("key-2") {
			t.Fail()
		}
	})

	t.Run("refills tokens over time", func(t *testing.T) {
		clock := &fakeClock{now: time.Now()}
		tb := newTokenBucketWithClock(2.0, 1.0, clock)

		if !tb.Allow("key-3") {
			t.Fail()
		}

		if !tb.Allow("key-3") {
			t.Fail()
		}

		if tb.Allow("key-3") {
			t.Fail()
		}

		clock.Advance(2 * time.Second)

		if !tb.Allow("key-3") {
			t.Fail()
		}
	})

	t.Run("does not exceed capacity on refill", func(t *testing.T) {
		clock := &fakeClock{now: time.Now()}
		tb := newTokenBucketWithClock(3.0, 1.0, clock)

		if !tb.Allow("key-4") {
			t.Fail()
		}

		if !tb.Allow("key-4") {
			t.Fail()
		}

		if !tb.Allow("key-4") {
			t.Fail()
		}

		if tb.Allow("key-4") {
			t.Fail()
		}

		clock.Advance(5 * time.Second)

		if !tb.Allow("key-4") {
			t.Fatal("expected allow after refill")
		}

		if tb.buckets["key-4"].tokens > 3.0 {
			t.Fail()
		}
	})

	t.Run("handles multiple keys independently", func(t *testing.T) {
		tb := NewTokenBucket(2.0, 1.0)

		if !tb.Allow("key-5") {
			t.Fail()
		}

		if !tb.Allow("key-6") {
			t.Fail()
		}

		if !tb.Allow("key-5") {
			t.Fail()
		}

		if !tb.Allow("key-6") {
			t.Fail()
		}

		if tb.Allow("key-5") {
			t.Fail()
		}

		if tb.Allow("key-6") {
			t.Fail()
		}
	})

	t.Run("handles concurrent access", func(t *testing.T) {
		clock := &fakeClock{now: time.Unix(0, 0)}
		tb := newTokenBucketWithClock(10.0, 0.0, clock) // rate 0 so no refills

		var allowed atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					if tb.Allow("key-7") {
						allowed.Add(1)
					}
				}
			}()
		}
		wg.Wait()

		// 50 requests, capacity 10, no refill → exactly 10 should succeed.
		if got := allowed.Load(); got != 10 {
			t.Fatalf("expected 10 allowed, got %d", got)
		}
	})
}

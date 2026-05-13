package ratelimiter

import (
	"testing"
	"time"
)

type fakeTicker struct {
	ch chan time.Time
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time)}
}

func (ft *fakeTicker) C() <-chan time.Time { return ft.ch }
func (ft *fakeTicker) Stop()               {}
func (ft *fakeTicker) Tick(d time.Duration) {
	ft.ch <- time.Now().Add(d)
}

func TestLeakyBucket(t *testing.T) {
	t.Run("accept request to queue if space available", func(t *testing.T) {
		lb := NewLeakyBucket[string](5, 2)

		if !lb.Push("key-1", "req-1") {
			t.Fail()
		}
	})

	t.Run("decline request to queue if queue is full", func(t *testing.T) {
		lb := NewLeakyBucket[string](2, 2)

		if !lb.Push("key-1", "req-1") {
			t.Fail()
		}

		if !lb.Push("key-1", "req-2") {
			t.Fail()
		}

		if lb.Push("key-1", "req-3") {
			t.Fail()
		}
	})

	t.Run("request in queue should decrease as per the rate", func(t *testing.T) {
		ticker := newFakeTicker()
		lb := newLeakyBucketWithClock[string](2, 2, func() Ticker {
			return ticker
		})

		if !lb.Push("key-1", "req-1") {
			t.Fail()
		}

		if !lb.Push("key-1", "req-2") {
			t.Fail()
		}

		qu, exist := lb.queues["key-1"]
		if !exist {
			t.Fail()
		}

		if len(qu) != 2 {
			t.Fail()
		}

		// Tick in a goroutine: the unbuffered channel means Tick blocks until
		// consume reads it, then consume blocks on lb.out <- item until we drain.
		go ticker.Tick(1 * time.Second)

		for i := int64(0); i < lb.rate; i++ {
			select {
			case <-lb.out:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for item to be processed")
			}
		}

		if len(qu) != 0 {
			t.Errorf("expected empty queue, got %d items", len(qu))
		}
	})

}

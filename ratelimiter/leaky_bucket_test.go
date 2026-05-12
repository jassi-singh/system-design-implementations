package ratelimiter

import (
	"testing"
	"time"
)

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
		ticker := &fakeTicker{ticker : time.NewTicker(time.Second)}
		lb := newLeakyBucketWithClock[string](2, 2, ticker)

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

		ticker.Advance(1 * time.Second)

		qu, _ = lb.queues["key-1"]

		if len(qu) != 0 {
			t.Fail()
		}

	})

}

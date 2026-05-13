package ratelimiter

import (
	"sync"
	"time"
)

type TokenBucket struct {
	capacity float64
	rate     float64
	buckets  map[string]*Bucket
	mu       sync.Mutex
	clock    Clock
}

type Bucket struct {
	tokens     float64
	lastRefill time.Time
}

func NewTokenBucket(capacity, rate float64) *TokenBucket {
	return newTokenBucketWithClock(capacity, rate, realClock{})
}

func newTokenBucketWithClock(capacity, rate float64, clock Clock) *TokenBucket {
	return &TokenBucket{
		capacity: capacity,
		rate:     rate,
		buckets:  make(map[string]*Bucket),
		clock:    clock,
	}
}

func (tb *TokenBucket) Allow(key string) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := tb.clock.Now()

	bucket, exist := tb.buckets[key]
	if !exist {
		bucket = &Bucket{
			tokens:     tb.capacity,
			lastRefill: now,
		}
		tb.buckets[key] = bucket
	}

	elapsedSeconds := now.Sub(bucket.lastRefill).Seconds()
	bucket.tokens += elapsedSeconds * tb.rate
	if bucket.tokens > tb.capacity {
		bucket.tokens = tb.capacity
	}
	bucket.lastRefill = now

	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens -= 1

	return true
}

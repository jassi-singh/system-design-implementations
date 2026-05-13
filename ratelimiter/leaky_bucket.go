package ratelimiter

import (
	"sync"
	"time"
)

/*
Leaky Bucket Algo
- check if queue is full
- add to que if not full
- reject if full
- at a fixed rate queue is processed
*/

type LeakyBucket[T any] struct {
	size   int64
	rate   int64
	queues map[string]chan T
	mu     sync.Mutex
	out    chan T

	newTicker func() Ticker
}

func NewLeakyBucket[T any](size, rate int64) *LeakyBucket[T] {
	return newLeakyBucketWithClock[T](size, rate, func() Ticker {
		return &realTicker{ticker: time.NewTicker(time.Second)}
	})
}

func newLeakyBucketWithClock[T any](size, rate int64, newTicker func() Ticker) *LeakyBucket[T] {
	return &LeakyBucket[T]{
		size:      size,
		rate:      rate,
		queues:    make(map[string]chan T),
		mu:        sync.Mutex{},
		out:       make(chan T),
		newTicker: newTicker,
	}
}

func (lb *LeakyBucket[T]) Push(key string, item T) bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	qu, exist := lb.queues[key]
	if !exist {
		qu = make(chan T, lb.size)
		lb.queues[key] = qu

		go lb.consume(qu)
	}

	qLen := len(qu)

	if qLen == int(lb.size) {
		return false
	}

	qu <- item

	return true
}

func (lb *LeakyBucket[T]) consume(qu chan T) {
	ticker := lb.newTicker()
	defer ticker.Stop()

	for {
		<-ticker.C()
	drain:
		for i := int64(0); i < lb.rate; i++ {
			select {
			case item := <-qu:
				lb.out <- item
			default:
				break drain
			}
		}
	}
}

package consistenthashing

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

// TestLoadDistribution measures how evenly keys spread across servers and shows
// how virtual nodes tighten the distribution. The headline metric is the
// coefficient of variation (stddev / mean) — the relative spread of load. Lower
// is more balanced, and it should shrink roughly as 1/sqrt(vnodes).
//
// Run `go test -run TestLoadDistribution -v` to see the table.
func TestLoadDistribution(t *testing.T) {
	const (
		servers = 10
		keys    = 100_000
	)

	vnodeCounts := []int{1, 10, 50, 100, 250, 500}

	t.Logf("%d keys across %d servers", keys, servers)
	t.Logf("%-8s %-12s %-12s %-8s %-8s", "vnodes", "mean", "stddev", "cv", "max/min")
	t.Logf("%s", strings.Repeat("-", 50))

	cv := make(map[int]float64, len(vnodeCounts))
	for _, vnodes := range vnodeCounts {
		counts := distribute(t, servers, vnodes, keys)
		mean, stddev, spread := stats(counts)
		cv[vnodes] = stddev / mean
		t.Logf("%-8d %-12.1f %-12.1f %-8.4f %-8.2f", vnodes, mean, stddev, cv[vnodes], spread)
	}

	// With a healthy number of virtual nodes, load is well balanced.
	if got := cv[250]; got > 0.2 {
		t.Errorf("coefficient of variation at 250 vnodes = %.4f, want <= 0.2", got)
	}
	// Virtual nodes are the whole point: many vnodes must beat a single one.
	if cv[500] >= cv[1] {
		t.Errorf("500 vnodes (cv=%.4f) not more balanced than 1 vnode (cv=%.4f)", cv[500], cv[1])
	}
}

// distribute builds a ring with the given number of servers and virtual nodes,
// routes `keys` distinct keys through it, and returns the per-server key counts.
func distribute(t *testing.T, servers, vnodes, keys int) []int {
	t.Helper()

	ch := New(vnodes)
	index := make(map[string]int, servers)
	for i := range servers {
		name := fmt.Sprintf("server-%d", i)
		ch.AddServer(name)
		index[name] = i
	}

	counts := make([]int, servers)
	for i := range keys {
		server, err := ch.GetKey(fmt.Sprintf("key-%d", i))
		if err != nil {
			t.Fatalf("GetKey: unexpected error: %v", err)
		}
		counts[index[server]]++
	}
	return counts
}

// stats returns the mean, population standard deviation, and max/min ratio of a
// set of per-server load counts.
func stats(counts []int) (mean, stddev, spread float64) {
	n := float64(len(counts))

	sum, lo, hi := 0, math.MaxInt, 0
	for _, c := range counts {
		sum += c
		lo = min(lo, c)
		hi = max(hi, c)
	}
	mean = float64(sum) / n

	var sumSq float64
	for _, c := range counts {
		d := float64(c) - mean
		sumSq += d * d
	}
	stddev = math.Sqrt(sumSq / n)

	if lo > 0 {
		spread = float64(hi) / float64(lo)
	}
	return mean, stddev, spread
}

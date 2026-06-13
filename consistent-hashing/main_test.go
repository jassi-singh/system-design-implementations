package consistenthashing

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

func TestGetKey(t *testing.T) {
	t.Run("empty ring returns error", func(t *testing.T) {
		ch := New(3)
		if _, err := ch.GetKey("anykey"); err == nil {
			t.Error("want error, got nil")
		}
	})

	t.Run("single server catches all keys", func(t *testing.T) {
		ch := New(3)
		ch.AddServer("serverA")

		for _, key := range []string{"key1", "key2", "hello", "world", "test123"} {
			got, err := ch.GetKey(key)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", key, err)
			}
			if got != "serverA" {
				t.Errorf("GetKey(%q) = %q, want serverA", key, got)
			}
		}
	})

	t.Run("same key always returns the same server", func(t *testing.T) {
		ch := New(3)
		ch.AddServer("serverA")
		ch.AddServer("serverB")

		key := "consistent-key"
		first, err := ch.GetKey(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i := 0; i < 10; i++ {
			got, err := ch.GetKey(key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != first {
				t.Errorf("iteration %d: GetKey(%q) = %q, want %q", i, key, got, first)
			}
		}
	})

	t.Run("routing matches ring logic with 1 virtual node", func(t *testing.T) {
		ch := New(1)
		servers := []string{"serverA", "serverB", "serverC"}
		nodes := map[uint64]string{}
		for _, s := range servers {
			ch.AddServer(s)
			nodes[Hash(s+"_0")] = s
		}

		for _, key := range []string{"key1", "key2", "key3", "user1", "user2", "data", "hello", "world"} {
			want := referenceRoute(key, nodes)
			got, err := ch.GetKey(key)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", key, err)
			}
			if got != want {
				t.Errorf("GetKey(%q) = %q, want %q", key, got, want)
			}
		}
	})

	t.Run("routing matches ring logic with multiple virtual nodes", func(t *testing.T) {
		const vnodes = 5
		ch := New(vnodes)
		servers := []string{"serverA", "serverB", "serverC"}
		nodes := map[uint64]string{}
		for _, s := range servers {
			ch.AddServer(s)
			for i := 0; i < vnodes; i++ {
				nodes[Hash(fmt.Sprintf("%s_%d", s, i))] = s
			}
		}

		for _, key := range []string{"key1", "key2", "key3", "user1", "user2", "data", "hello", "world"} {
			want := referenceRoute(key, nodes)
			got, err := ch.GetKey(key)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", key, err)
			}
			if got != want {
				t.Errorf("GetKey(%q) = %q, want %q", key, got, want)
			}
		}
	})
}

func TestAddServer(t *testing.T) {
	t.Run("first node range starts at zero", func(t *testing.T) {
		ch := New(1)
		ranges := ch.AddServer("serverA")

		if len(ranges) != 1 {
			t.Fatalf("got %d ranges, want 1", len(ranges))
		}
		if ranges[0].Start != 0 {
			t.Errorf("range Start = %d, want 0", ranges[0].Start)
		}
		if want := Hash("serverA_0"); ranges[0].End != want {
			t.Errorf("range End = %d, want %d", ranges[0].End, want)
		}
	})

	t.Run("subsequent node range starts at previous node", func(t *testing.T) {
		ch := New(1)
		ch.AddServer("serverA")

		posA := Hash("serverA_0")
		posB := Hash("serverB_0")

		ranges := ch.AddServer("serverB")
		if len(ranges) != 1 {
			t.Fatalf("got %d ranges, want 1", len(ranges))
		}

		// Both code paths in getAffectedRange yield posA as Start:
		//   posA < posB: Floor(posB) = posA          (floor path)
		//   posA > posB: Floor(posB) not found → Right() = posA  (wrap-around path)
		if ranges[0].Start != posA {
			t.Errorf("range Start = %d, want posA=%d", ranges[0].Start, posA)
		}
		if ranges[0].End != posB {
			t.Errorf("range End = %d, want posB=%d", ranges[0].End, posB)
		}
	})

	t.Run("returns one range per virtual node", func(t *testing.T) {
		ch := New(3)
		ranges := ch.AddServer("serverA")
		if len(ranges) != 3 {
			t.Errorf("got %d ranges, want 3", len(ranges))
		}
	})

	t.Run("adding same server twice does not change routing", func(t *testing.T) {
		ch := New(3)
		ch.AddServer("serverA")
		ch.AddServer("serverB")

		keys := []string{"key1", "key2", "key3", "user1"}
		before := make(map[string]string)
		for _, k := range keys {
			s, err := ch.GetKey(k)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", k, err)
			}
			before[k] = s
		}

		ch.AddServer("serverA")

		for _, k := range keys {
			got, err := ch.GetKey(k)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", k, err)
			}
			if got != before[k] {
				t.Errorf("after duplicate add: GetKey(%q) = %q, want %q", k, got, before[k])
			}
		}
	})

	t.Run("only keys in affected range are rerouted", func(t *testing.T) {
		ch := New(1)
		ch.AddServer("serverA")

		keys := make([]string, 30)
		for i := range keys {
			keys[i] = fmt.Sprintf("test-key-%d", i)
		}

		before := make(map[string]string)
		for _, key := range keys {
			s, err := ch.GetKey(key)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", key, err)
			}
			before[key] = s
		}

		ranges := ch.AddServer("serverB")
		if len(ranges) != 1 {
			t.Fatalf("got %d ranges, want 1", len(ranges))
		}
		affected := ranges[0]

		for _, key := range keys {
			keyHash := Hash(key)
			got, err := ch.GetKey(key)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", key, err)
			}

			if ringRangeContains(affected, keyHash) {
				if got != "serverB" {
					t.Errorf("key %q (hash=%d) in affected range: got %q, want serverB", key, keyHash, got)
				}
			} else {
				if got != before[key] {
					t.Errorf("key %q (hash=%d) outside affected range: got %q, want %q", key, keyHash, got, before[key])
				}
			}
		}
	})

	t.Run("concurrent access is safe", func(t *testing.T) {
		ch := New(10)
		ch.AddServer("serverA")
		ch.AddServer("serverB")

		var wg sync.WaitGroup
		for i := range 1000 {
			wg.Add(2)
			go func(i int) {
				defer wg.Done()
				ch.GetKey(fmt.Sprintf("key-%d", i))
			}(i)
			go func(i int) {
				defer wg.Done()
				if i%2 == 0 {
					ch.AddServer(fmt.Sprintf("server-%d", i))
				} else {
					ch.RemoveServer(fmt.Sprintf("server-%d", i))
				}
			}(i)
		}
		wg.Wait()
	})
}

func TestRemoveServer(t *testing.T) {
	t.Run("removing server reroutes its keys to remaining server", func(t *testing.T) {
		ch := New(3)
		ch.AddServer("serverA")
		ch.AddServer("serverB")
		ch.RemoveServer("serverB")

		for _, key := range []string{"key1", "key2", "key3", "user1", "user2", "data", "hello", "world"} {
			got, err := ch.GetKey(key)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", key, err)
			}
			if got != "serverA" {
				t.Errorf("GetKey(%q) = %q, want serverA", key, got)
			}
		}
	})

	t.Run("removing all servers returns error on lookup", func(t *testing.T) {
		ch := New(1)
		ch.AddServer("serverA")
		ch.RemoveServer("serverA")

		if _, err := ch.GetKey("anykey"); err == nil {
			t.Error("want error after removing all servers, got nil")
		}
	})

	t.Run("removing nonexistent server does not change routing", func(t *testing.T) {
		ch := New(3)
		ch.AddServer("serverA")

		keys := []string{"key1", "key2", "key3"}
		before := make(map[string]string)
		for _, k := range keys {
			s, err := ch.GetKey(k)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", k, err)
			}
			before[k] = s
		}

		ch.RemoveServer("serverB") // never added

		for _, k := range keys {
			got, err := ch.GetKey(k)
			if err != nil {
				t.Fatalf("GetKey(%q): unexpected error: %v", k, err)
			}
			if got != before[k] {
				t.Errorf("after removing nonexistent server: GetKey(%q) = %q, want %q", k, got, before[k])
			}
		}
	})
}

// referenceRoute returns the expected server for a key by simulating ring routing.
func referenceRoute(key string, nodes map[uint64]string) string {
	keyHash := Hash(key)

	positions := make([]uint64, 0, len(nodes))
	for pos := range nodes {
		positions = append(positions, pos)
	}
	sort.Slice(positions, func(i, j int) bool { return positions[i] < positions[j] })

	for _, pos := range positions {
		if keyHash <= pos {
			return nodes[pos]
		}
	}
	return nodes[positions[0]] // wrap around to smallest node
}

// ringRangeContains reports whether hash falls in (Start, End] or, for wrap-around
// ranges where Start >= End, in (Start, maxUint64] ∪ [0, End].
func ringRangeContains(r Range, hash uint64) bool {
	if r.Start < r.End {
		return hash > r.Start && hash <= r.End
	}
	return hash > r.Start || hash <= r.End
}

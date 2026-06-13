package consistenthashing

import (
	"fmt"
	"testing"
)

// BenchmarkGetKey measures lookup cost as the ring grows. Each server adds 100
// virtual nodes, so the tree holds servers*100 entries. Lookups are a single
// red-black tree Ceiling, so ns/op should grow only logarithmically with the
// ring size — roughly flat across these sizes.
func BenchmarkGetKey(b *testing.B) {
	for _, servers := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("servers=%d", servers), func(b *testing.B) {
			ch := New(100)
			for i := range servers {
				ch.AddServer(fmt.Sprintf("server-%d", i))
			}

			// Power-of-two count so we can index with a cheap mask.
			const nKeys = 1024
			keys := make([]string, nKeys)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%d", i)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ch.GetKey(keys[i&(nKeys-1)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAddServer measures the cost of joining a server, which inserts one
// node per virtual node into the ring. Cost scales with the virtual-node count.
func BenchmarkAddServer(b *testing.B) {
	for _, vnodes := range []int{1, 100, 500} {
		b.Run(fmt.Sprintf("vnodes=%d", vnodes), func(b *testing.B) {
			ch := New(vnodes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ch.AddServer(fmt.Sprintf("server-%d", i))
			}
		})
	}
}

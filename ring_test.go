package main

import (
	"fmt"
	"testing"
)

func TestReplicasAreDistinct(t *testing.T) {
	r := NewRing(50)
	for _, n := range []string{"a", "b", "c", "d"} {
		r.AddNode(n)
	}
	rep := r.Replicas("some-key", 3)
	if len(rep) != 3 {
		t.Fatalf("got %d replicas, want 3", len(rep))
	}
	seen := map[string]bool{}
	for _, n := range rep {
		if seen[n] {
			t.Fatalf("duplicate replica %s", n)
		}
		seen[n] = true
	}
}

func TestDistributionIsRoughlyEven(t *testing.T) {
	r := NewRing(200)
	nodes := []string{"a", "b", "c", "d", "e"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	counts := map[string]int{}
	total := 10000
	for i := 0; i < total; i++ {
		counts[r.PrimaryNode(fmt.Sprintf("key-%d", i))]++
	}

	expected := total / len(nodes)
	for _, n := range nodes {
		if counts[n] < expected*6/10 || counts[n] > expected*14/10 {
			t.Fatalf("node %s got %d keys, expected ~%d", n, counts[n], expected)
		}
	}
}

func TestMinimalRemappingOnRemove(t *testing.T) {
	r := NewRing(200)
	nodes := []string{"a", "b", "c", "d", "e"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	total := 10000
	before := make([]string, total)
	for i := 0; i < total; i++ {
		before[i] = r.PrimaryNode(fmt.Sprintf("key-%d", i))
	}

	r.RemoveNode("c")

	moved := 0
	for i := 0; i < total; i++ {
		if r.PrimaryNode(fmt.Sprintf("key-%d", i)) != before[i] {
			moved++
		}
	}

	// Only keys that lived on "c" (~1/5) should move — the point of consistent hashing.
	if frac := float64(moved) / float64(total); frac > 0.30 {
		t.Fatalf("remapped %.1f%% of keys, want < 30%%", frac*100)
	}
}

package main

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// ConsistentHashRing maps keys to nodes so that adding or removing a node only
// remaps a small fraction of keys (roughly 1/N), which is what makes it useful
// for partitioning data across a distributed cluster. Each physical node is
// placed at many positions on the ring (virtual nodes) for even distribution.
type ConsistentHashRing struct {
	virtualNodes int
	positions    []uint32          // sorted ring positions
	owner        map[uint32]string // position -> node id
	nodes        map[string]bool
}

func NewRing(virtualNodes int) *ConsistentHashRing {
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
		owner:        map[uint32]string{},
		nodes:        map[string]bool{},
	}
}

func (r *ConsistentHashRing) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (r *ConsistentHashRing) AddNode(node string) {
	if r.nodes[node] {
		return
	}
	r.nodes[node] = true
	for i := 0; i < r.virtualNodes; i++ {
		p := r.hash(node + "#" + strconv.Itoa(i))
		r.positions = append(r.positions, p)
		r.owner[p] = node
	}
	sort.Slice(r.positions, func(i, j int) bool { return r.positions[i] < r.positions[j] })
}

func (r *ConsistentHashRing) RemoveNode(node string) {
	if !r.nodes[node] {
		return
	}
	delete(r.nodes, node)
	kept := r.positions[:0:0]
	for _, p := range r.positions {
		if r.owner[p] == node {
			delete(r.owner, p)
		} else {
			kept = append(kept, p)
		}
	}
	r.positions = kept
}

// Replicas returns up to n distinct nodes responsible for key, walking the ring
// clockwise from the key's position.
func (r *ConsistentHashRing) Replicas(key string, n int) []string {
	if len(r.positions) == 0 {
		return nil
	}
	h := r.hash(key)
	start := sort.Search(len(r.positions), func(i int) bool { return r.positions[i] >= h })
	if start == len(r.positions) {
		start = 0
	}

	out := make([]string, 0, n)
	seen := map[string]bool{}
	for i := 0; i < len(r.positions) && len(out) < n; i++ {
		node := r.owner[r.positions[(start+i)%len(r.positions)]]
		if !seen[node] {
			seen[node] = true
			out = append(out, node)
		}
	}
	return out
}

func (r *ConsistentHashRing) PrimaryNode(key string) string {
	if rep := r.Replicas(key, 1); len(rep) > 0 {
		return rep[0]
	}
	return ""
}

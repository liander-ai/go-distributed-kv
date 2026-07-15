package main

import (
	"errors"
	"sync"
)

var (
	ErrQuorum   = errors.New("quorum not reached")
	ErrNotFound = errors.New("key not found")
)

type node struct {
	mu   sync.RWMutex
	data map[string]string
	up   bool
}

// Cluster is an in-process, replicated key-value store. Keys are partitioned
// across nodes with a consistent hash ring, each key is stored on `replication`
// nodes, and reads/writes require a majority (quorum) of those replicas.
type Cluster struct {
	ring        *ConsistentHashRing
	nodes       map[string]*node
	replication int
}

func NewCluster(nodeIDs []string, replication, virtualNodes int) *Cluster {
	c := &Cluster{
		ring:        NewRing(virtualNodes),
		nodes:       map[string]*node{},
		replication: replication,
	}
	for _, id := range nodeIDs {
		c.nodes[id] = &node{data: map[string]string{}, up: true}
		c.ring.AddNode(id)
	}
	return c
}

func majority(replication int) int { return replication/2 + 1 }

// Put writes key=val to the key's replica nodes. It succeeds only if a majority
// of replicas are up and acknowledge the write.
func (c *Cluster) Put(key, val string) error {
	acks := 0
	for _, id := range c.ring.Replicas(key, c.replication) {
		n := c.nodes[id]
		n.mu.Lock()
		if n.up {
			n.data[key] = val
			acks++
		}
		n.mu.Unlock()
	}
	if acks < majority(c.replication) {
		return ErrQuorum
	}
	return nil
}

// Get reads key from the replica nodes and returns the value agreed by a
// majority of the responding replicas.
func (c *Cluster) Get(key string) (string, error) {
	counts := map[string]int{}
	responses := 0
	for _, id := range c.ring.Replicas(key, c.replication) {
		n := c.nodes[id]
		n.mu.RLock()
		if n.up {
			responses++
			if v, ok := n.data[key]; ok {
				counts[v]++
			}
		}
		n.mu.RUnlock()
	}
	if responses < majority(c.replication) {
		return "", ErrQuorum
	}

	best, bestCount := "", 0
	for v, cnt := range counts {
		if cnt > bestCount {
			best, bestCount = v, cnt
		}
	}
	if bestCount == 0 {
		return "", ErrNotFound
	}
	return best, nil
}

// Kill marks a node down, simulating a failure.
func (c *Cluster) Kill(id string) {
	if n, ok := c.nodes[id]; ok {
		n.mu.Lock()
		n.up = false
		n.mu.Unlock()
	}
}

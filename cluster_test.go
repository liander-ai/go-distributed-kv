package main

import "testing"

func TestPutGet(t *testing.T) {
	c := NewCluster([]string{"n1", "n2", "n3", "n4", "n5"}, 3, 50)
	if err := c.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if got != "v" {
		t.Fatalf("got %q, want v", got)
	}
}

func TestSurvivesOneReplicaFailure(t *testing.T) {
	c := NewCluster([]string{"n1", "n2", "n3", "n4", "n5"}, 3, 50)
	c.Put("k", "v")

	rep := c.ring.Replicas("k", 3)
	c.Kill(rep[0]) // one of the three replicas goes down

	got, err := c.Get("k")
	if err != nil {
		t.Fatalf("read failed after one failure: %v", err)
	}
	if got != "v" {
		t.Fatalf("got %q, want v", got)
	}
}

func TestQuorumLostWithTwoFailures(t *testing.T) {
	c := NewCluster([]string{"n1", "n2", "n3", "n4", "n5"}, 3, 50)
	c.Put("k", "v")

	rep := c.ring.Replicas("k", 3)
	c.Kill(rep[0])
	c.Kill(rep[1]) // only one replica left -> no majority

	if _, err := c.Get("k"); err != ErrQuorum {
		t.Fatalf("expected ErrQuorum, got %v", err)
	}
}

func TestGetMissingKey(t *testing.T) {
	c := NewCluster([]string{"n1", "n2", "n3"}, 3, 50)
	if _, err := c.Get("nope"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// A small in-process distributed key-value store: consistent-hash partitioning,
// replication, and quorum reads/writes that tolerate a node failure.
package main

import "fmt"

func main() {
	cluster := NewCluster([]string{"n1", "n2", "n3", "n4", "n5"}, 3, 50)

	cluster.Put("user:42", "alice")
	cluster.Put("order:7", "shipped")

	v, _ := cluster.Get("user:42")
	fmt.Println("user:42 =", v)

	replicas := cluster.ring.Replicas("user:42", 3)
	fmt.Println("replicas of user:42:", replicas)

	// Kill one replica; the value survives via quorum on the remaining two.
	cluster.Kill(replicas[0])
	v, err := cluster.Get("user:42")
	fmt.Printf("killed %s -> user:42 = %q (err=%v)\n", replicas[0], v, err)
}

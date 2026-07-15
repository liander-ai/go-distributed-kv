# Go Distributed KV

An in-process, replicated key-value store in **Go** that demonstrates the core ideas behind distributed data stores: **consistent-hash partitioning**, **replication**, and **quorum** reads/writes that tolerate node failures.

## How it works

- **Consistent hashing** (`ring.go`) — each node is placed at many positions on a hash ring (virtual nodes), so keys are spread evenly and removing a node only remaps ~`1/N` of keys instead of reshuffling everything.
- **Replication** (`cluster.go`) — every key is stored on the next `R` distinct nodes clockwise on the ring.
- **Quorum** — a write succeeds only if a majority of replicas ack, and a read returns the value agreed by a majority. So with `R = 3`, the store keeps serving through **one** node failure and correctly reports quorum loss at **two**.

## Example

```
user:42 = alice
replicas of user:42: [n2 n5 n4]
killed n2 -> user:42 = "alice" (err=<nil>)     # survived a replica failure
```

## Run

```bash
go run .
```

## Tests

```bash
go test ./... -race
```

Tests cover: replicas are distinct, keys distribute roughly evenly, removing a node remaps only a small fraction of keys, and reads survive one failure but correctly return `ErrQuorum` at two.

## Files

```
ring.go       consistent hash ring (virtual nodes, clockwise replica walk)
cluster.go    replicated store with quorum Put/Get + node failure
main.go       demo
*_test.go      test suites
```

## Stack

- **Go** standard library only (`hash/crc32`, `sort`, `sync`)

## License

MIT

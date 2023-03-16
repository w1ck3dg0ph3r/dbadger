# DBadger

[![Go Reference](https://pkg.go.dev/badge/github.com/w1ck3dg0ph3r/dbadger.svg)](https://pkg.go.dev/github.com/w1ck3dg0ph3r/dbadger)
[![Go Report Card](https://goreportcard.com/badge/github.com/w1ck3dg0ph3r/dbadger)](https://goreportcard.com/report/github.com/w1ck3dg0ph3r/dbadger)
[![ci-dbadger](https://github.com/w1ck3dg0ph3r/dbadger/actions/workflows/ci-dbadger.yml/badge.svg)](https://github.com/w1ck3dg0ph3r/dbadger/actions/workflows/ci-dbadger.yml)

<img src=".README/dbadger.png" alt="Logo" width="167">

DBadger is a distributed embeddable key-value database based on [BadgerDB](https://github.com/dgraph-io/badger) persistent storage and [Raft](https://github.com/hashicorp/raft) consensus algorithm.

**DBadger:**
- uses BadgerDB as both data store and log store and simple fsync-ed file for stable store.
- uses GRPC for inter-node communication and redirects requests to the leader node when neccessary.
- reuses single port for both Raft and GRPC by multiplexing TCP streams.
- supports communication over TLS between nodes.




## Rationale

The idea was to make an easier to embed KV store to replace etcd in simple use cases.



## Usage

```go
config := dbadger.DefaultConfig("./datapath", "127.0.0.1:7001").WithBootstrap(true)
db, err := dbadger.Start(config)
if err != nil {
  log.Fatal(err)
}

// Run your application
db.Set(context.TODO(), []byte("key"), []byte("value"))
db.Get(context.TODO(), []byte("key"), dbadger.Eventual)

if err := db.Close(); err != nil {
  log.Fatal(err)
}
```



## CLI Example

**cmd/example-cli** is a simple terminal UI node example, showing key-value pairs, cluster nodes, stats and logs for the node. See `go run ./cmd/example-cli -h` or `cmd/example-cli/main.go` for available flags.

### Launching cluster

```bash
# Bootstrap first node
go run ./cmd/example-cli --db .data/db1 --bind 127.0.0.1:7001 --bootstrap

# Join follower nodes
go run ./cmd/example-cli --db .data/db2 --bind 127.0.0.1:7002 --join 127.0.0.1:7001

go run ./cmd/example-cli --db .data/db3 --bind 127.0.0.1:7003 --join 127.0.0.1:7001
```

### Recovering cluster

To recover cluster from a single node, use:

```bash
go run ./cmd/example-cli --db .data/db1 --bind 127.0.0.1:7001 --recover
```

After the recovery the recovered node will be a new leader, and you will be able to join new clean nodes to this cluster.

### TLS

```bash
# Bootstrap first node
go run ./cmd/example-cli --db .data/db1 --ca .tls/ca.crt --cert .tls/server1.lan.crt --key .tls/server1.lan.pem --bind 127.0.0.1:7001 --bootstrap

# Join follower nodes
go run ./cmd/example-cli --db .data/db2 --ca .tls/ca.crt --cert .tls/server2.lan.crt --key .tls/server2.lan.pem --bind 127.0.0.1:7002 --join 127.0.0.1:7001
go run ./cmd/example-cli --db .data/db3 --ca .tls/ca.crt --cert .tls/server3.lan.crt --key .tls/server3.lan.pem --bind 127.0.0.1:7003 --join 127.0.0.1:7001
```



## Operations

### Get

`func (db *DB) Get(ctx context.Context, key []byte, readPreference ReadPreference) ([]byte, error)`

Get returns value corresponding to the given key.

```go
val, err := db.Get(context.TODO(), key, dbadger.LeaderPreference)
```

### GetMany

`func (db *DB) GetMany(ctx context.Context, keys [][]byte, readPreference ReadPreference) (values [][]byte, _ error)`

GetMany returns values corresponding to the given keys.

```go
keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
values, err := db.GetMany(context.TODO(), keys, dbadger.LeaderPreference)
```

### GetPrefix

`func (db *DB) GetPrefix(ctx context.Context, prefix []byte, readPreference ReadPreference) (keys, values [][]byte, _ error)`

GetPrefix returns values for the keys with the given prefix.

```go
values, err := db.GetPrefix(context.TODO(), []byte("prefix_"), dbadger.LeaderPreference)
```

### GetRange

`func (db *DB) GetRange(ctx context.Context, min, max []byte, count uint64, readPreference ReadPreference) (keys, values [][]byte, _ error)`

GetRange returns maximum of count values for the keys in range [min, max].
Both min and max can be nil.

```go
keys, values, err := db.GetRange(context.TODO(), []byte("key1"), nil, 10)
```

### Set

`func (db *DB) Set(ctx context.Context, key, value []byte) error`

Set adds a key-value pair.

```go
err := db.Set(context.TODO(), []byte("key"), []byte("value"))
```

### SetMany

`func (db *DB) SetMany(ctx context.Context, keys, values [][]byte) error`

SetMany adds multiple key-value pairs.

### Delete

`func (db *DB) Delete(ctx context.Context, key []byte) error`

Delete removes a key.

### DeleteMany

`func (db *DB) DeleteMany(ctx context.Context, keys [][]byte) error`

DeleteMany removes multiple keys.

### DeletePrefix

`func (db *DB) DeletePrefix(ctx context.Context, prefix []byte) error`

DeletePrefix removes keys with the given prefix.

### DeleteRange

`func (db *DB) DeleteRange(ctx context.Context, min, max []byte) (keys [][]byte, _ error)`

DeleteRange removes keys in range [min, max] and returns keys that has been removed.

### DeleteAll

`func (db *DB) DeleteAll(ctx context.Context) error`

DeleteAll removes all keys.

### Snapshot

`func (db *DB) Snapshot() (id string, err error)`

Snapshot forces DB to take a snapshot, e.g. for backup purposes.
Returns snapshot id and error, if any.

### Restore

`func (db *DB) Restore(id string) error`

Restore forces DB to consume a snapshot, such as if restoring from a backup.
This should not be used in normal operation, only for disaster recovery onto a new cluster.



## Contributing

Any contributions are welcome in the form of issues and pull requests in this repository.
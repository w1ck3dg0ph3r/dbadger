// Package dbadger provies a distributed embeddable key-value database based on [BadgerDB] persistent storage and [Raft] consensus algorithm.
//
//   - Uses BadgerDB as both data store and log store and simple fsync-ed file for stable store.
//   - Uses GRPC for inter-node communication and redirects requests to the leader node when necessary.
//   - Listens on a single port for both Raft and GRPC traffic by multiplexing TCP streams.
//
// The main type in dbadger is [DB] that must be started with [Start].
//
// # Usage
//
//	config := dbadger.DefaultConfig("./datapath", "127.0.0.1:7001").WithBootstrap(true)
//	db, err := dbadger.Start(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Run your application
//	db.Set(context.TODO(), []byte("key"), []byte("value"))
//	db.Get(context.TODO(), []byte("key"), dbadger.Eventual)
//
//	if err := db.Close(); err != nil {
//		log.Fatal(err)
//	}
//
// See the README.md, examples and tests for information.
//
// [BadgerDB]: https://pkg.go.dev/github.com/dgraph-io/badger/v4
// [Raft]: https://pkg.go.dev/github.com/hashicorp/raft
package dbadger

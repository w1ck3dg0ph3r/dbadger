package dbadger

import (
	"context"
	"fmt"

	bc "github.com/w1ck3dg0ph3r/dbadger/internal/bytesconv"
	"github.com/w1ck3dg0ph3r/dbadger/internal/rpc"
)

// ReadPreference determines how DB routes read requests to the nodes of the cluster.
type ReadPreference = rpc.ReadPreference

const (
	// LeaderPreference tells DB to read data from the current cluster leader.
	// This ensures that no stale data is returned from the read operation.
	LeaderPreference = rpc.ReadPreference_LEADER

	// LocalPreference tells DB to read data from the current node.
	// This is faster than routing request to the leader, but may return stale data.
	LocalPreference = rpc.ReadPreference_LOCAL
)

// Get returns value corresponding to the given key.
func (db *DB) Get(ctx context.Context, key []byte, readPreference ReadPreference) ([]byte, error) {
	res, err := db.service.Get(ctx, &rpc.CommandGet{Key: key, ReadPreference: readPreference})
	if err != nil {
		return nil, mapError(err)
	}
	return res.Value, nil
}

// GetString returns value corresponding to the given key.
func (db *DB) GetString(ctx context.Context, key string, readPreference ReadPreference) (string, error) {
	res, err := db.Get(ctx, bc.ToBytes(key), readPreference)
	if err != nil {
		return "", err
	}
	return bc.ToString(res), nil
}

// GetMany returns values corresponding to the given keys.
func (db *DB) GetMany(ctx context.Context, keys [][]byte, readPreference ReadPreference) (values [][]byte, _ error) {
	res, err := db.service.GetMany(ctx, &rpc.CommandGetMany{Keys: keys, ReadPreference: readPreference})
	if err != nil {
		return nil, mapError(err)
	}
	return res.Values, nil
}

// GetManyString returns values corresponding to the given keys.
func (db *DB) GetManyString(ctx context.Context, keys []string, readPreference ReadPreference) (values []string, _ error) {
	res, err := db.GetMany(ctx, bc.ToBytesSlice(keys), readPreference)
	return bc.ToStringSlice(res), err
}

// GetPrefix returns values for the keys with the given prefix.
func (db *DB) GetPrefix(ctx context.Context, prefix []byte, readPreference ReadPreference) (keys, values [][]byte, _ error) {
	res, err := db.service.GetPrefix(ctx, &rpc.CommandGetPrefix{Prefix: prefix, ReadPreference: readPreference})
	if err != nil {
		return nil, nil, mapError(err)
	}
	return res.Keys, res.Values, nil
}

// GetPrefixString returns values for the keys with the given prefix.
func (db *DB) GetPrefixString(ctx context.Context, prefix string, readPreference ReadPreference) (keys, values []string, _ error) {
	bkeys, bvalues, err := db.GetPrefix(ctx, bc.ToBytes(prefix), readPreference)
	return bc.ToStringSlice(bkeys), bc.ToStringSlice(bvalues), err
}

// GetRange returns maximum of count values for the keys in range [min, max].
//
// Both min and max can be nil.
func (db *DB) GetRange(ctx context.Context, min, max []byte, count uint64, readPreference ReadPreference) (keys, values [][]byte, _ error) {
	res, err := db.service.GetRange(ctx, &rpc.CommandGetRange{Min: min, Max: max, Count: count, ReadPreference: readPreference})
	if err != nil {
		return nil, nil, mapError(err)
	}
	return res.Keys, res.Values, nil
}

// GetRangeString returns maximum of count values for the keys in range [min, max].
//
// Both min and max can be nil.
func (db *DB) GetRangeString(ctx context.Context, min, max string, count uint64, readPreference ReadPreference) (keys, values []string, _ error) {
	bkeys, bvalues, err := db.GetRange(ctx, bc.ToBytes(min), bc.ToBytes(max), count, readPreference)
	return bc.ToStringSlice(bkeys), bc.ToStringSlice(bvalues), err
}

// Set adds a key-value pair.
func (db *DB) Set(ctx context.Context, key, value []byte) error {
	_, err := db.service.Set(ctx, &rpc.CommandSet{Key: key, Value: value})
	return mapError(err)
}

// SetString adds a key-value pair.
func (db *DB) SetString(ctx context.Context, key, value string) error {
	return db.Set(ctx, bc.ToBytes(key), bc.ToBytes(value))
}

// SetMany adds multiple key-value pairs.
func (db *DB) SetMany(ctx context.Context, keys, values [][]byte) error {
	_, err := db.service.SetMany(ctx, &rpc.CommandSetMany{Keys: keys, Values: values})
	return mapError(err)
}

// SetManyString adds multiple key-value pairs.
func (db *DB) SetManyString(ctx context.Context, keys, values []string) error {
	return db.SetMany(ctx, bc.ToBytesSlice(keys), bc.ToBytesSlice(values))
}

// Delete removes a key.
func (db *DB) Delete(ctx context.Context, key []byte) error {
	_, err := db.service.Delete(ctx, &rpc.CommandDelete{Key: key})
	return mapError(err)
}

// DeleteString removes a key.
func (db *DB) DeleteString(ctx context.Context, key string) error {
	return db.Delete(ctx, bc.ToBytes(key))
}

// DeleteMany removes multiple keys.
func (db *DB) DeleteMany(ctx context.Context, keys [][]byte) error {
	_, err := db.service.DeleteMany(ctx, &rpc.CommandDeleteMany{Keys: keys})
	return mapError(err)
}

// DeleteManyString removes multiple keys.
func (db *DB) DeleteManyString(ctx context.Context, keys []string) error {
	return db.DeleteMany(ctx, bc.ToBytesSlice(keys))
}

// DeletePrefix removes keys with the given prefix.
func (db *DB) DeletePrefix(ctx context.Context, prefix []byte) error {
	_, err := db.service.DeletePrefix(ctx, &rpc.CommandDeletePrefix{Prefix: prefix})
	return mapError(err)
}

// DeletePrefixString removes keys with the given prefix.
func (db *DB) DeletePrefixString(ctx context.Context, prefix string) error {
	return db.DeletePrefix(ctx, bc.ToBytes(prefix))
}

// DeleteRange removes keys in range [min, max] and returns keys that has been removed.
func (db *DB) DeleteRange(ctx context.Context, min, max []byte) (keys [][]byte, _ error) {
	res, err := db.service.DeleteRange(ctx, &rpc.CommandDeleteRange{Min: min, Max: max})
	if err != nil {
		return nil, mapError(err)
	}
	return res.Keys, nil
}

// DeleteRangeString removes keys in range [min, max] and returns keys that has been removed.
func (db *DB) DeleteRangeString(ctx context.Context, min, max string) (keys []string, _ error) {
	bkeys, err := db.DeleteRange(ctx, bc.ToBytes(min), bc.ToBytes(max))
	return bc.ToStringSlice(bkeys), err
}

// DeleteAll removes all keys.
func (db *DB) DeleteAll(ctx context.Context) error {
	_, err := db.service.DeleteAll(ctx, &rpc.CommandDeleteAll{})
	return mapError(err)
}

// Snapshot forces DB to take a snapshot, e.g. for backup purposes.
// Returns snapshot id and error, if any.
func (db *DB) Snapshot() (id string, err error) {
	_ = db.raftNode.Barrier(0).Error()
	db.raftNode.Snapshot()
	sf := db.raftNode.Snapshot()
	if err := sf.Error(); err != nil {
		return "", fmt.Errorf("can't take snapshot: %w", err)
	}
	meta, rc, err := sf.Open()
	if err != nil {
		return "", fmt.Errorf("can't open snapshot: %w", err)
	}
	_ = rc.Close()
	return meta.ID, nil
}

// Restore forces DB to consume a snapshot, such as if restoring from a backup.
// This should not be used in normal operation, only for disaster recovery onto a new cluster.
func (db *DB) Restore(id string) error {
	meta, rc, err := db.snapshotStore.Open(id)
	if err != nil {
		return fmt.Errorf("can't open snapshot: %w", err)
	}
	return db.raftNode.Restore(meta, rc, 0)
}

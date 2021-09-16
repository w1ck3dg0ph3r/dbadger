package stores

import (
	"bytes"
	"io"
	"runtime"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	"github.com/w1ck3dg0ph3r/dbadger/internal/rpc"
)

type DataStore struct {
	badgerStore
}

func DefaultDataStoreConfig(path string) *Config {
	return &Config{
		Path:     path,
		InMemory: false,

		GCEnabled:      true,
		GCInterval:     5 * time.Minute,
		GCDiscardRatio: 0.5,
	}
}

func NewDataStore(config *Config) (*DataStore, error) {
	var err error
	fsm := &DataStore{}
	fsm.badgerStore, err = newBadgerStore(config)
	if err != nil {
		return nil, err
	}
	return fsm, nil
}

func (s *DataStore) Apply(log *raft.Log) any {
	if log.Type != raft.LogCommand {
		return nil
	}

	cmd := &rpc.Command{}
	if err := proto.Unmarshal(log.Data, cmd); err != nil {
		panic(err)
	}

	switch cmd := cmd.Command.(type) {

	case *rpc.Command_Set:
		if err := s.Set(cmd.Set.Key, cmd.Set.Value); err != nil {
			return panicOn(err)
		}
		return &rpc.ResultSet{}

	case *rpc.Command_SetMany:
		if err := s.SetMany(cmd.SetMany.Keys, cmd.SetMany.Values); err != nil {
			return panicOn(err)
		}
		return &rpc.ResultSetMany{}

	case *rpc.Command_Delete:
		if err := s.Delete(cmd.Delete.Key); err != nil {
			return panicOn(err)
		}
		return &rpc.ResultDelete{}

	case *rpc.Command_DeleteMany:
		if err := s.DeleteMany(cmd.DeleteMany.Keys); err != nil {
			return panicOn(err)
		}
		return &rpc.ResultDeleteMany{}

	case *rpc.Command_DeletePrefix:
		if err := s.DeletePrefix(cmd.DeletePrefix.Prefix); err != nil {
			return panicOn(err)
		}
		return &rpc.ResultDeletePrefix{}

	case *rpc.Command_DeleteRange:
		keys, err := s.DeleteRange(cmd.DeleteRange.Min, cmd.DeleteRange.Max)
		if err != nil {
			return panicOn(err)
		}
		return &rpc.ResultDeleteRange{Keys: keys}

	case *rpc.Command_DeleteAll:
		if err := s.DeleteAll(); err != nil {
			return panicOn(err)
		}
		return &rpc.ResultDeleteAll{}
	}

	return nil
}

func (s *DataStore) Get(key []byte) ([]byte, error) {
	var res []byte
	err := s.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(key)
		if err != nil {
			return err
		}
		res, err = it.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *DataStore) GetMany(keys [][]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	values := make([][]byte, len(keys))
	err := s.db.View(func(txn *badger.Txn) error {
		for i, key := range keys {
			item, err := txn.Get(key)
			if err != nil {
				continue
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			values[i] = value
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

func (s *DataStore) GetPrefix(prefix []byte) (keys, values [][]byte, err error) {
	const prefetchSize = 100
	err = s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         prefix,
			PrefetchValues: true,
			PrefetchSize:   prefetchSize,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keys = append(keys, item.Key())
			values = append(values, value)
		}
		return nil
	})
	return keys, values, err
}

func (s *DataStore) GetRange(min, max []byte, count int) (keys, values [][]byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		// Iterate over common prefix of keys min and max
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         commonPrefix(min, max),
			PrefetchValues: true,
			PrefetchSize:   count,
		})
		defer it.Close()

		remaining := count
		it.Seek(min)
		for remaining > 0 {
			if !it.Valid() {
				return nil
			}

			item := it.Item()

			// Stop if we past max key
			maxCmp := -1
			if len(max) > 0 {
				maxCmp = bytes.Compare(item.Key(), max)
			}
			if maxCmp > 0 {
				break
			}

			// Append key and value to the result
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keys = append(keys, item.KeyCopy(nil))
			values = append(values, value)

			// Stop if we have reached max key
			if maxCmp == 0 {
				break
			}

			remaining--
			if remaining > 0 {
				it.Next()
			}
		}
		return nil
	})
	return keys, values, err
}

func (s *DataStore) Set(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *DataStore) SetMany(keys, values [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != len(values) {
		panic("keys/values size mismatch")
	}
	txn := s.db.NewTransaction(true)
	for i := range keys {
		err := txn.Set(keys[i], values[i])
		// Commit and start new transaction if the transaction is too big
		if err == badger.ErrTxnTooBig {
			if err = txn.Commit(); err != nil {
				return err
			}
			txn = s.db.NewTransaction(true)
			err = txn.Set(keys[i], values[i])
		}
		if err != nil {
			txn.Discard()
			return err
		}
	}
	return txn.Commit()
}

func (s *DataStore) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *DataStore) DeleteMany(keys [][]byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *DataStore) DeletePrefix(prefix []byte) error {
	return s.db.DropPrefix(prefix)
}

func (s *DataStore) DeleteRange(min, max []byte) (keys [][]byte, err error) {
	err = s.db.Update(func(txn *badger.Txn) error {
		// Iterate over common prefix of keys min and max
		it := txn.NewIterator(badger.IteratorOptions{
			Prefix:         commonPrefix(min, max),
			PrefetchValues: false,
		})

		// Collect keys to delete from range [min...max]
		for it.Seek(min); it.Valid(); it.Next() {
			item := it.Item()

			// Stop if we past max key
			maxCmp := -1
			if len(max) > 0 {
				maxCmp = bytes.Compare(item.Key(), max)
			}
			if maxCmp > 0 {
				break
			}

			keys = append(keys, item.KeyCopy(nil))

			// Stop if we reached max key
			if maxCmp == 0 {
				break
			}
		}
		it.Close()

		// Delete keys
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	return keys, err
}

func (s *DataStore) DeleteAll() error {
	return s.db.DropAll()
}

//nolint:ireturn,nolintlint // Required by raft.FSM interface
func (s *DataStore) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{db: s.db}, nil
}

func (s *DataStore) Restore(r io.ReadCloser) error {
	defer r.Close()
	if err := s.db.DropAll(); err != nil {
		return err
	}
	if err := s.db.Load(r, runtime.NumCPU()); err != nil {
		return err
	}
	return nil
}

func commonPrefix(a, b []byte) []byte {
	end := 0
	minlen := len(a)
	if len(b) < len(a) {
		minlen = len(b)
	}
	for i := 0; i < minlen; i++ {
		if a[i] == b[i] {
			end++
		}
	}
	if end == 0 {
		return nil
	}
	return a[0:end]
}

type snapshot struct {
	db *badger.DB
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	_, err := s.db.Backup(sink, 0)
	return err
}

func (s *snapshot) Release() {
	s.db = nil
}

func panicOn(err error) error {
	switch err {
	case badger.ErrInvalidRequest, badger.ErrInvalidKey, badger.ErrEmptyKey:
		return err
	default:
		panic(err)
	}
}

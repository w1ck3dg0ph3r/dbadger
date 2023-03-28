package stores

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

type LogStore struct {
	badgerStore
}

func DefaultLogStoreConfig(path string, inmem bool) *Config {
	defaults := badger.DefaultOptions(path)
	return &Config{
		Path:     path,
		InMemory: inmem,

		SyncWrites:              defaults.SyncWrites,
		MaxLevels:               defaults.MaxLevels,
		LevelSizeMultiplier:     defaults.LevelSizeMultiplier,
		BaseTableSize:           defaults.BaseTableSize,
		BaseLevelSize:           defaults.BaseLevelSize,
		ValueLogFileSize:        defaults.ValueLogFileSize,
		ValueLogMaxEntries:      defaults.ValueLogMaxEntries,
		NumMemtables:            defaults.NumMemtables,
		MemTableSize:            defaults.MemTableSize,
		BlockSize:               defaults.BlockSize,
		BlockCacheSize:          defaults.BlockCacheSize,
		NumLevelZeroTables:      defaults.NumLevelZeroTables,
		NumLevelZeroTablesStall: defaults.NumLevelZeroTablesStall,
		NumCompactors:           defaults.NumCompactors,
		CompactL0OnClose:        defaults.CompactL0OnClose,
		Compression:             true,

		GCEnabled: false,

		Logger: nil,
	}
}

func NewLogStore(config *Config) (*LogStore, error) {
	var err error
	s := &LogStore{}
	s.badgerStore, err = newBadgerStore(config)
	if err != nil {
		return nil, err
	}
	return s, nil
}

const logIndexLength = 8

// FirstIndex returns the first index written. 0 for no entries.
func (s *LogStore) FirstIndex() (uint64, error) {
	var index uint64
	if err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		it.Rewind()
		if !it.Valid() {
			index = 0
			return nil
		}
		key := it.Item().Key()
		index = keyToIndex(key)
		return nil
	}); err != nil {
		return 0, err
	}
	return index, nil
}

// LastIndex returns the last index written. 0 for no entries.
func (s *LogStore) LastIndex() (uint64, error) {
	var index uint64
	if err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Reverse: true, PrefetchValues: false})
		defer it.Close()
		it.Rewind()
		if !it.Valid() {
			index = 0
			return nil
		}
		key := it.Item().Key()
		index = keyToIndex(key)
		return nil
	}); err != nil {
		return 0, err
	}
	return index, nil
}

// GetLog gets a log entry at a given index.
func (s *LogStore) GetLog(index uint64, log *raft.Log) error {
	key := indexToKey(index)
	if err := s.db.View(func(txn *badger.Txn) error {
		it, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return raft.ErrLogNotFound
			}
			return err
		}
		if err := it.Value(func(val []byte) error {
			return decodeLogEntry(val, log)
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// StoreLog stores a log entry.
func (s *LogStore) StoreLog(log *raft.Log) error {
	key := indexToKey(log.Index)
	if err := s.db.Update(func(txn *badger.Txn) error {
		val, err := encodeLogEntry(log)
		if err != nil {
			return err
		}
		if err := txn.Set(key, val); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// StoreLogs stores multiple log entries.
func (s *LogStore) StoreLogs(logs []*raft.Log) error {
	txn := s.db.NewTransaction(true)
	for _, log := range logs {
		key := indexToKey(log.Index)
		val, err := encodeLogEntry(log)
		if err != nil {
			return err
		}
		err = txn.Set(key, val)
		// Commit and start new transaction if the transaction is too big
		if err == badger.ErrTxnTooBig {
			if err = txn.Commit(); err != nil {
				return err
			}
			txn = s.db.NewTransaction(true)
			err = txn.Set(key, val)
		}
		if err != nil {
			txn.Discard()
			return err
		}
	}
	return txn.Commit()
}

// DeleteRange deletes a range of log entries. The range is inclusive.
func (s *LogStore) DeleteRange(min, max uint64) error {
	if err := s.db.Update(func(txn *badger.Txn) error {
		for index := min; index <= max; index++ {
			key := indexToKey(index)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// DeleteAll deletes all log entries.
func (s *LogStore) DeleteAll() error {
	return s.db.DropAll()
}

func indexToKey(index uint64) []byte {
	res := make([]byte, logIndexLength)
	binary.BigEndian.PutUint64(res, index)
	return res
}

func keyToIndex(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

func encodeLogEntry(val *raft.Log) ([]byte, error) {
	var err error
	w := &bytes.Buffer{}

	write := func(what any) {
		if err != nil {
			return
		}
		switch what := what.(type) {
		case []byte:
			size := uint64(len(what))
			err = binary.Write(w, binary.LittleEndian, size)
			if err != nil {
				return
			}
			_, err = w.Write(what)
		case time.Time:
			timestamp := what.UnixNano()
			err = binary.Write(w, binary.LittleEndian, timestamp)
		default:
			err = binary.Write(w, binary.LittleEndian, what)
		}
	}

	write(val.Index)
	write(val.Term)
	write(val.Type)
	write(val.Data)
	write(val.Extensions)
	write(val.AppendedAt)

	if err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func decodeLogEntry(val []byte, dest *raft.Log) error {
	var err error
	r := bytes.NewReader(val)

	read := func(dest any) {
		if err != nil {
			return
		}
		switch dest := dest.(type) {
		case *[]byte:
			var size uint64
			err = binary.Read(r, binary.LittleEndian, &size)
			if err != nil {
				return
			}
			*dest = make([]byte, size)
			_, err = io.ReadFull(r, *dest)
		case *time.Time:
			var timestamp int64
			err = binary.Read(r, binary.LittleEndian, &timestamp)
			if err != nil {
				return
			}
			*dest = time.Unix(0, timestamp).UTC()
			if dest.UnixNano() == (time.Time{}).UnixNano() {
				*dest = time.Time{}
			}
		default:
			err = binary.Read(r, binary.LittleEndian, dest)
		}
	}

	read(&dest.Index)
	read(&dest.Term)
	read(&dest.Type)
	read(&dest.Data)
	read(&dest.Extensions)
	read(&dest.AppendedAt)

	return err
}

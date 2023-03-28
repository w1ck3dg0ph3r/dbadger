package stores

import (
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

type Config struct {
	Path     string
	InMemory bool

	SyncWrites bool

	MaxLevels           int
	LevelSizeMultiplier int
	BaseTableSize       int64
	BaseLevelSize       int64
	ValueLogFileSize    int64
	ValueLogMaxEntries  uint32

	NumMemtables int
	MemTableSize int64

	BlockSize      int
	BlockCacheSize int64

	NumLevelZeroTables      int
	NumLevelZeroTablesStall int
	NumCompactors           int
	CompactL0OnClose        bool

	Compression bool

	GCEnabled      bool
	GCInterval     time.Duration
	GCDiscardRatio float64

	Logger Logger
}

type Logger interface {
	Errorf(string, ...any)
	Warningf(string, ...any)
	Infof(string, ...any)
	Debugf(string, ...any)
}

type badgerStore struct {
	config *Config

	db *badger.DB

	shutdownCh chan struct{}
}

func newBadgerStore(config *Config) (badgerStore, error) {
	var err error
	s := badgerStore{
		config:     config,
		shutdownCh: make(chan struct{}),
	}
	if config.InMemory {
		config.Path = ""
	}
	s.db, err = badger.Open(badgerOptions(config))
	if err != nil {
		return s, err
	}
	if !config.InMemory && config.GCEnabled {
		go s.runGarbageCollection()
	}
	return s, nil
}

func (s *badgerStore) Close() error {
	close(s.shutdownCh)
	return s.db.Close()
}

func (s *badgerStore) runGarbageCollection() {
	ticker := time.NewTicker(s.config.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := s.db.RunValueLogGC(s.config.GCDiscardRatio)
			if err != nil && err != badger.ErrRejected {
				if s.config.Logger != nil {
					s.config.Logger.Debugf("garbage collection error: %s", err)
				}
			}
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *badgerStore) Stats() map[string]string {
	lsm, vlog := s.db.Size()
	return map[string]string{
		"size_lsm":  strconv.FormatInt(lsm, 10),
		"size_vlog": strconv.FormatInt(vlog, 10),
	}
}

func badgerOptions(c *Config) badger.Options {
	o := badger.DefaultOptions(c.Path)
	o.InMemory = c.InMemory
	o.SyncWrites = c.SyncWrites
	o.MaxLevels = c.MaxLevels
	o.LevelSizeMultiplier = c.LevelSizeMultiplier
	o.BaseTableSize = c.BaseTableSize
	o.BaseLevelSize = c.BaseLevelSize
	o.ValueLogFileSize = c.ValueLogFileSize
	o.ValueLogMaxEntries = c.ValueLogMaxEntries
	o.NumMemtables = c.NumMemtables
	o.MemTableSize = c.MemTableSize
	o.BlockSize = c.BlockSize
	o.BlockCacheSize = c.BlockCacheSize
	o.NumLevelZeroTables = c.NumLevelZeroTables
	o.NumLevelZeroTablesStall = c.NumLevelZeroTablesStall
	o.NumCompactors = c.NumCompactors
	o.CompactL0OnClose = c.CompactL0OnClose
	if c.Compression {
		o.Compression = options.Snappy
	} else {
		o.Compression = options.None
	}
	o.Logger = c.Logger
	return o
}

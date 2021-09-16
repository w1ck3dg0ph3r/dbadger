package stores

import (
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Config struct {
	Path     string
	InMemory bool

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

func (c *Config) WithPath(path string) *Config {
	c.Path = path
	return c
}

func (c *Config) WithInMemory(inMemory bool) *Config {
	c.InMemory = inMemory
	return c
}

func (c *Config) WithGC(interval time.Duration, discardRatio float64) *Config {
	c.GCEnabled = true
	c.GCInterval = interval
	c.GCDiscardRatio = discardRatio
	return c
}

func (c *Config) WithLogger(logger Logger) *Config {
	c.Logger = logger
	return c
}

type badgerStore struct {
	config *Config

	db *badger.DB
}

func newBadgerStore(config *Config) (badgerStore, error) {
	var err error
	s := badgerStore{
		config: config,
	}
	path := config.Path
	if config.InMemory {
		path = ""
	}
	s.db, err = badger.Open(badger.DefaultOptions(path).WithLogger(config.Logger).WithInMemory(config.InMemory))
	if err != nil {
		return s, err
	}
	if !config.InMemory && config.GCEnabled {
		go s.runGarbageCollection()
	}
	return s, nil
}

func (s *badgerStore) Close() error {
	return s.db.Close()
}

func (s *badgerStore) runGarbageCollection() {
	ticker := time.NewTicker(s.config.GCInterval)
	defer ticker.Stop()
	for range ticker.C {
		err := s.db.RunValueLogGC(s.config.GCDiscardRatio)
		if err != nil && err != badger.ErrRejected {
			if s.config.Logger != nil {
				s.config.Logger.Debugf("garbage collection error: %s", err)
			}
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

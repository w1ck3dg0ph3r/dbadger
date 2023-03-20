package stores

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	bc "github.com/w1ck3dg0ph3r/dbadger/internal/bytesconv"
)

const (
	stableStoreFileName = "config.json"
)

// StableStore is used to provide stable storage of key configurations to raft consensus algorithm.
type StableStore struct {
	config *StableStoreConfig

	mut      sync.RWMutex
	values   map[string]string
	filename string
	file     *os.File
}

type StableStoreConfig struct {
	Path     string // Config directory path.
	Sync     bool   // Fsync after each write.
	InMemory bool
}

func NewStableStore(config *StableStoreConfig) (*StableStore, error) {
	s := &StableStore{
		config:   config,
		values:   map[string]string{},
		filename: filepath.Join(config.Path, stableStoreFileName),
	}
	if err := s.testWrite(); err != nil {
		return nil, err
	}
	if err := s.loadConfig(); err != nil {
		return nil, err
	}
	return s, nil
}

func DefaultStableStoreConfig(path string) *StableStoreConfig {
	return &StableStoreConfig{
		Path:     path,
		InMemory: false,
		Sync:     true,
	}
}

func (c *StableStoreConfig) WithPath(path string) *StableStoreConfig {
	c.Path = path
	return c
}

func (c *StableStoreConfig) WithSync(sync bool) *StableStoreConfig {
	c.Sync = sync
	return c
}

func (c *StableStoreConfig) WithInMemory(inMemory bool) *StableStoreConfig {
	c.InMemory = inMemory
	return c
}

func (s *StableStore) Set(key []byte, val []byte) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.values[bc.ToString(key)] = bc.ToString(val)
	if err := s.saveConfig(); err != nil {
		return err
	}
	return nil
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (s *StableStore) Get(key []byte) ([]byte, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if val, ok := s.values[bc.ToString(key)]; ok {
		return bc.ToBytes(val), nil
	}
	return nil, nil
}

func (s *StableStore) SetUint64(key []byte, val uint64) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	s.values[bc.ToString(key)] = strconv.FormatUint(val, 10)
	if err := s.saveConfig(); err != nil {
		return err
	}
	return nil
}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (s *StableStore) GetUint64(key []byte) (uint64, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if val, ok := s.values[bc.ToString(key)]; ok {
		val, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return 0, err
		}
		return val, nil
	}
	return 0, nil
}

func (s *StableStore) Close() {
	if s.file != nil {
		_ = s.file.Close()
	}
}

func (s *StableStore) PrintConfig(out io.Writer) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	for k, v := range s.values {
		fmt.Fprintf(out, "\"%s\" = \"%s\"\n", k, v)
	}
}

func (s *StableStore) testWrite() error {
	if s.config.InMemory {
		return nil
	}

	fn := filepath.Join(s.config.Path, stableStoreFileName+".test")
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Remove(fn); err != nil {
		return err
	}
	return nil
}

func (s *StableStore) loadConfig() error {
	if s.config.InMemory {
		return nil
	}

	buf, err := os.ReadFile(s.filename)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(buf) == 0 {
		return nil
	}
	if err := json.Unmarshal(buf, &s.values); err != nil {
		return err
	}
	return nil
}

func (s *StableStore) saveConfig() error {
	if s.config.InMemory {
		return nil
	}

	var err error

	buf, err := json.Marshal(s.values)
	if err != nil {
		return err
	}

	if s.file == nil {
		s.file, err = os.Create(s.filename)
		if err != nil {
			return err
		}
	}

	err = s.file.Truncate(0)
	if err != nil {
		return err
	}
	_, err = s.file.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = s.file.Write(buf)
	if err != nil {
		return err
	}

	if s.config.Sync {
		err = s.file.Sync()
		if err != nil {
			return err
		}
	}

	return nil
}

package stores

import (
	"testing"

	"github.com/stretchr/testify/assert"

	bc "github.com/w1ck3dg0ph3r/dbadger/internal/bytesconv"
)

func TestConfigStore_Get(t *testing.T) {
	t.Parallel()

	s, err := NewStableStore(DefaultStableStoreConfig(t.TempDir()))
	assert.NoError(t, err)

	k, err := s.Get(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Nil(t, k)

	_ = s.Set(bc.ToBytes("key1"), bc.ToBytes("value1"))
	k, err = s.Get(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(k))
}

func TestConfigStore_GetInMemory(t *testing.T) {
	t.Parallel()

	s, err := NewStableStore(DefaultStableStoreConfig(t.TempDir()).WithInMemory(true))
	assert.NoError(t, err)

	k, err := s.Get(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Nil(t, k)

	_ = s.Set(bc.ToBytes("key1"), bc.ToBytes("value1"))
	k, err = s.Get(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(k))
}

func TestConfigStore_GetUint64(t *testing.T) {
	t.Parallel()

	s, err := NewStableStore(DefaultStableStoreConfig(t.TempDir()))
	assert.NoError(t, err)

	k, err := s.GetUint64(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Zero(t, k)

	_ = s.SetUint64(bc.ToBytes("key1"), 42)
	k, err = s.GetUint64(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), k)
}

func TestConfigStore_GetUint64InMemory(t *testing.T) {
	t.Parallel()

	s, err := NewStableStore(DefaultStableStoreConfig(t.TempDir()).WithInMemory(true))
	assert.NoError(t, err)

	k, err := s.GetUint64(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Zero(t, k)

	_ = s.SetUint64(bc.ToBytes("key1"), 42)
	k, err = s.GetUint64(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), k)
}

func TestConfigStore_LoadConfig(t *testing.T) {
	t.Parallel()

	path := t.TempDir()

	s, _ := NewStableStore(DefaultStableStoreConfig(path))
	_ = s.Set(bc.ToBytes("key1"), bc.ToBytes("value1"))
	_ = s.SetUint64(bc.ToBytes("key2"), 42)
	s.Close()

	s, _ = NewStableStore(DefaultStableStoreConfig(path))
	sk, err := s.Get(bc.ToBytes("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(sk))
	ik, err := s.GetUint64(bc.ToBytes("key2"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), ik)
}

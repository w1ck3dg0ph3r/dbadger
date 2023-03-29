package stores

import (
	"math/rand"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
)

func TestLogStore_FirstIndex(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		indexes []uint64
		last    uint64
	}{
		{"empty", nil, 0},
		{"one index", []uint64{1}, 1},
		{"many indexes", []uint64{1, 2, 3, 4, 5}, 1},
		{"many non consecutive", []uint64{4, 5, 3, 6, 7}, 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ls := createLogStore(t)
			defer ls.Close()
			for _, i := range tc.indexes {
				_ = ls.StoreLog(&raft.Log{Index: i})
			}
			last, err := ls.FirstIndex()
			assert.NoError(t, err)
			assert.Equal(t, tc.last, last)
		})
	}
}

func TestLogStore_LastIndex(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		indexes []uint64
		last    uint64
	}{
		{"empty", nil, 0},
		{"one index", []uint64{1}, 1},
		{"many indexes", []uint64{1, 2, 3, 4, 5}, 5},
		{"many non consecutive", []uint64{4, 5, 2, 3, 1}, 5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ls := createLogStore(t)
			defer ls.Close()
			for _, i := range tc.indexes {
				_ = ls.StoreLog(&raft.Log{Index: i})
			}
			last, err := ls.LastIndex()
			assert.NoError(t, err)
			assert.Equal(t, tc.last, last)
		})
	}
}

func TestLogStore_SetGetLog(t *testing.T) {
	t.Parallel()

	t.Run("non existing", func(t *testing.T) {
		ls := createLogStore(t)
		defer ls.Close()
		var l raft.Log
		err := ls.GetLog(1, &l)
		assert.ErrorIs(t, err, raft.ErrLogNotFound)
	})

	t.Run("existing", func(t *testing.T) {
		ls := createLogStore(t)
		defer ls.Close()
		l := &raft.Log{
			Index:      uint64(rand.Int()),
			Term:       uint64(rand.Int()),
			Type:       raft.LogConfiguration,
			Data:       []byte("testdata"),
			Extensions: []byte("extensions"),
			AppendedAt: time.Now().UTC(),
		}

		err := ls.StoreLog(l)
		assert.NoError(t, err)

		r := &raft.Log{}
		err = ls.GetLog(l.Index, r)
		assert.NoError(t, err)
		assertEqualLog(t, l, r)
	})

	t.Run("zero time", func(t *testing.T) {
		ls := createLogStore(t)
		defer ls.Close()
		l := &raft.Log{
			AppendedAt: time.Time{},
		}

		err := ls.StoreLog(l)
		assert.NoError(t, err)

		r := &raft.Log{}
		err = ls.GetLog(l.Index, r)
		assert.NoError(t, err)
		assert.True(t, r.AppendedAt.IsZero())
	})
}

func TestLogStore_StoreLogs(t *testing.T) {
	t.Parallel()

	ls := createLogStore(t)
	defer ls.Close()
	var logs []*raft.Log
	dataSize := ls.db.MaxBatchSize() / 100
	for i := 0; i < 100; i++ {
		logs = append(logs, &raft.Log{
			Index:      uint64(i + 1),
			Term:       uint64(rand.Int()),
			Type:       raft.LogCommand,
			Data:       make([]byte, dataSize),
			Extensions: []byte("extensions"),
			AppendedAt: time.Now().UTC(),
		})
	}
	err := ls.StoreLogs(logs)
	assert.NoError(t, err)
	for i := range logs {
		l := &raft.Log{}
		err = ls.GetLog(uint64(i+1), l)
		assert.NoError(t, err)
		assert.Equal(t, logs[i], l)
	}
}

func TestLogStore_DeleteRange(t *testing.T) {
	t.Parallel()

	ls := createLogStore(t)
	defer ls.Close()

	var logs []*raft.Log
	for i := 1; i <= 10; i++ {
		logs = append(logs, &raft.Log{
			Index: uint64(i),
		})
	}
	_ = ls.StoreLogs(logs)

	err := ls.DeleteRange(3, 7)
	assert.NoError(t, err)
	for i := 1; i <= 10; i++ {
		l := &raft.Log{}
		err = ls.GetLog(uint64(i), l)
		if i < 3 || i > 7 {
			assert.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, raft.ErrLogNotFound)
		}
	}
}

func createLogStore(t *testing.T) *LogStore {
	logStore, err := NewLogStore(DefaultLogStoreConfig("", true))
	assert.NoError(t, err)
	return logStore
}

func assertEqualLog(t *testing.T, e, a *raft.Log) {
	assert.Equal(t, e.Index, a.Index)
	assert.Equal(t, e.Term, a.Term)
	assert.Equal(t, e.Type, a.Type)
	assert.Equal(t, e.Data, a.Data)
	assert.Equal(t, e.Extensions, a.Extensions)
	assert.Equal(t, e.AppendedAt, a.AppendedAt)
}

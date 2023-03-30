package test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/w1ck3dg0ph3r/dbadger"
)

func TestOperations(t *testing.T) {
	ctx := context.Background()
	const nodeCount = 5
	cluster := createCluster(t, nodeCount, Variant{InMemory: true})
	randomNode := func() *dbadger.DB {
		return cluster[rand.Intn(nodeCount)]
	}

	assertContent := func(t testing.TB, keys []string, values []string) {
		k, v, err := cluster[0].GetPrefixString(ctx, "", dbadger.LeaderPreference)
		assert.NoError(t, err)
		assert.Equal(t, keys, k, "keys differ")
		assert.Equal(t, values, v, "values differ")
	}

	t.Run("Set", func(t *testing.T) {
		assert.NoError(t, randomNode().SetString(ctx, "a", "1"))
		assertContent(t, []string{"a"}, []string{"1"})
	})
	t.Run("SetMany", func(t *testing.T) {
		assert.NoError(t, randomNode().SetManyString(ctx, []string{"b", "c"}, []string{"2", "3"}))
		assertContent(t, []string{"a", "b", "c"}, []string{"1", "2", "3"})
	})
	t.Run("Get", func(t *testing.T) {
		v, err := randomNode().GetString(ctx, "a", dbadger.LeaderPreference)
		assert.NoError(t, err)
		assert.Equal(t, "1", v)
	})
	t.Run("GetMany", func(t *testing.T) {
		v, err := randomNode().GetManyString(ctx, []string{"a", "b", "c"}, dbadger.LeaderPreference)
		assert.NoError(t, err)
		assert.Equal(t, []string{"1", "2", "3"}, v)
	})
	t.Run("GetPrefix", func(t *testing.T) {
		k, v, err := randomNode().GetPrefixString(ctx, "b", dbadger.LeaderPreference)
		assert.NoError(t, err)
		assert.Equal(t, []string{"b"}, k)
		assert.Equal(t, []string{"2"}, v)
		t.Run("empty prefix", func(t *testing.T) {
			k, v, err := randomNode().GetPrefixString(ctx, "", dbadger.LeaderPreference)
			assert.NoError(t, err)
			assert.Equal(t, []string{"a", "b", "c"}, k)
			assert.Equal(t, []string{"1", "2", "3"}, v)
		})
	})
	t.Run("GetRange", func(t *testing.T) {
		k, v, err := randomNode().GetRangeString(ctx, "a", "b", 2, dbadger.LeaderPreference)
		assert.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, k)
		assert.Equal(t, []string{"1", "2"}, v)
	})
	restoreDeleted := func() {
		_ = cluster[0].SetManyString(ctx, []string{"a", "b", "c"}, []string{"1", "2", "3"})
	}
	t.Run("Delete", func(t *testing.T) {
		t.Cleanup(restoreDeleted)
		assert.NoError(t, randomNode().DeleteString(ctx, "b"))
		v, err := randomNode().GetString(ctx, "b", dbadger.LeaderPreference)
		assert.ErrorIs(t, err, dbadger.ErrNotFound)
		assert.Equal(t, "", v)
		assertContent(t, []string{"a", "c"}, []string{"1", "3"})
	})
	t.Run("DeleteMany", func(t *testing.T) {
		t.Cleanup(restoreDeleted)
		assert.NoError(t, randomNode().DeleteManyString(ctx, []string{"a", "c"}))
		assertContent(t, []string{"b"}, []string{"2"})
	})
	t.Run("DeletePrefix", func(t *testing.T) {
		t.Cleanup(restoreDeleted)
		assert.NoError(t, randomNode().DeletePrefixString(ctx, "a"))
		assertContent(t, []string{"b", "c"}, []string{"2", "3"})
		t.Run("empty prefix", func(t *testing.T) {
			t.Cleanup(restoreDeleted)
			assert.NoError(t, randomNode().DeletePrefixString(ctx, ""))
			assertContent(t, []string{}, []string{})
		})
	})
	t.Run("DeleteRange", func(t *testing.T) {
		t.Cleanup(restoreDeleted)
		k, err := randomNode().DeleteRangeString(ctx, "a", "b")
		assert.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, k)
		assertContent(t, []string{"c"}, []string{"3"})
	})
	t.Run("DeleteAll", func(t *testing.T) {
		t.Cleanup(restoreDeleted)
		assert.NoError(t, randomNode().DeleteAll(ctx))
		assertContent(t, []string{}, []string{})
	})

	stopCluster(t, cluster)
}

package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/w1ck3dg0ph3r/dbadger"
)

func TestCluster_HappyTimes(t *testing.T) {
	runVariants(t, func(t *testing.T, variant Variant) {
		cluster := createCluster(t, 3, variant)

		const goroutines = 4
		const count = 25

		keys, values := generateKeyValues(goroutines * count)

		wg := sync.WaitGroup{}
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			g := g
			go func() {
				defer wg.Done()
				for i := g * count; i < g*count+count; i++ {
					err := cluster[i%len(cluster)].Set(context.Background(), keys[i], values[i])
					assert.NoError(t, err)
				}
			}()
		}
		wg.Wait()

		wg = sync.WaitGroup{}
		wg.Add(goroutines)
		for g := 0; g < goroutines; g++ {
			g := g
			go func() {
				defer wg.Done()
				for i := g * count; i < g*count+count; i++ {
					var value []byte
					err := retry([]error{dbadger.ErrUnavailable, dbadger.ErrNotFound}, 3*time.Second, func() error {
						var err error
						value, err = cluster[i%len(cluster)].Get(context.Background(), keys[i], dbadger.LocalPreference)
						return err
					})
					assert.NoError(t, err)
					assert.Equal(t, values[i], value, "values do not match")
				}
			}()
		}
		wg.Wait()

		stopCluster(t, cluster)
	})
}

func TestCluster_ReplicationHappens(t *testing.T) {
	runVariants(t, func(t *testing.T, variant Variant) {
		cluster := createCluster(t, 3, variant)

		leader := cluster[0]
		follower := cluster[2]

		t.Run("with leader read preference", func(t *testing.T) {
			key := []byte("strong_key")
			value := []byte("strong_value")

			err := leader.Set(context.Background(), key, value)
			assert.NoError(t, err)

			var res []byte
			err = retry([]error{dbadger.ErrUnavailable, dbadger.ErrNotFound}, 3*time.Second, func() error {
				var err error
				res, err = follower.Get(context.Background(), key, dbadger.LeaderPreference)
				return err
			})
			assert.NoError(t, err)
			assert.Equal(t, value, res)
		})

		t.Run("with self read preference", func(t *testing.T) {
			key := []byte("eventual_key")
			value := []byte("eventual_value")

			err := leader.Set(context.Background(), key, value)
			assert.NoError(t, err)

			var res []byte
			assert.NoError(t, retry([]error{dbadger.ErrNotFound}, 3*time.Second, func() error {
				var err error
				res, err = follower.Get(context.Background(), key, dbadger.LocalPreference)
				return err
			}))
			assert.Equal(t, value, res)
		})

		stopCluster(t, cluster)
	})
}

func TestCluster_NewLeaderIsElected(t *testing.T) {
	runVariants(t, func(t *testing.T, variant Variant) {
		cluster := createCluster(t, 3, variant)
		prevLeader := getClusterLeader(t, cluster)
		prevLeaderAddr := cluster[prevLeader].Addr()
		cluster = removeNode(t, cluster, prevLeader)
		newLeader := getClusterLeader(t, cluster)
		newLeaderAddr := cluster[newLeader].Addr()
		assert.NotEqual(t, prevLeaderAddr, newLeaderAddr)
		stopCluster(t, cluster)
	})
}

func TestCluster_MajorityLost(t *testing.T) {
	runVariants(t, func(t *testing.T, variant Variant) {
		cluster := createCluster(t, 3, variant)
		cluster = removeNode(t, cluster, getClusterLeader(t, cluster))
		cluster = removeNode(t, cluster, getClusterLeader(t, cluster))

		err := retry([]error{dbadger.ErrUnavailable}, 15*time.Second, func() error {
			_, err := cluster[0].GetString(context.Background(), "key", dbadger.LeaderPreference)
			return err
		})
		assert.ErrorIs(t, err, dbadger.ErrNoLeader)

		stopCluster(t, cluster)
	})
}

func TestCluster_BackupRestore(t *testing.T) {
	t.Skip("Need to find a reproducible way to test restoring of a snapshot")
	runVariants(t, func(t *testing.T, variant Variant) {
		cluster := createCluster(t, 3, variant)
		keys, values := generateKeyValues(100)

		// Write data
		assert.NoError(t, cluster[0].SetMany(context.Background(), keys, values))

		// Wait untill replication is done
		assert.NoError(t, retry([]error{dbadger.ErrNotFound}, 3*time.Second, func() error {
			_, err := cluster[2].Get(context.Background(), keys[len(keys)-1], dbadger.LocalPreference)
			return err
		}))

		// Snapshot leader node
		snapshotId, err := cluster[0].Snapshot()
		assert.NoError(t, err)

		// Delete all keys
		err = retry([]error{dbadger.ErrUnavailable, dbadger.ErrConflict}, 3*time.Second, func() error {
			return cluster[0].DeleteAll(context.Background())
		})
		assert.NoError(t, err)

		// Restore snapshot
		assert.NoError(t, cluster[0].Restore(snapshotId))

		retry([]error{dbadger.ErrUnavailable, dbadger.ErrNotFound}, 3*time.Second, func() error {
			_, err := cluster[0].Get(context.Background(), keys[len(keys)-1], dbadger.LeaderPreference)
			return err
		})

		// Check data
		res, err := cluster[0].GetMany(context.Background(), keys, dbadger.LeaderPreference)
		assert.NoError(t, err)
		assert.Equal(t, values, res)

		stopCluster(t, cluster)
	})
}

func BenchmarkWrite(b *testing.B) {
	cluster := createCluster(b, 3, Variant{InMemory: true})
	server := cluster[0]
	keys, values := generateKeyValues(b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := server.Set(context.Background(), keys[n], values[n])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	stopCluster(b, cluster)
}

func BenchmarkRead(b *testing.B) {
	cluster := createCluster(b, 3, Variant{InMemory: true})
	server := cluster[0]
	keys, values := generateKeyValues(b.N)

	for n := 0; n < b.N; n++ {
		err := server.Set(context.Background(), keys[n], values[n])
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		val, err := server.Get(context.Background(), keys[n], dbadger.LocalPreference)
		if err != nil {
			b.Fatal(err)
		}
		_ = val
	}
	b.StopTimer()

	stopCluster(b, cluster)
}

func generateKeyValues(count int) ([][]byte, [][]byte) {
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := 0; i < count; i++ {
		keys[i] = []byte(fmt.Sprintf("key%010d", i))
		values[i] = []byte(fmt.Sprintf("value%010d", i))
	}
	return keys, values
}

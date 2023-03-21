package stores

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	bc "github.com/w1ck3dg0ph3r/dbadger/internal/bytesconv"
	"github.com/w1ck3dg0ph3r/dbadger/internal/rpc"
)

func TestDataStore_SetGet(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		key   []byte
		value []byte
		set   bool  // Set this kev=value using Apply
		get   bool  // Get this key and expect result to be value
		panic bool  // Should Apply panic
		err   error // What error should Get return
	}{
		{"normal value", bc.ToBytes("my_key"), bc.ToBytes("my_value"), true, true, false, nil},
		{"empty key", nil, bc.ToBytes("value"), true, false, false, badger.ErrEmptyKey},
		{"empty value", bc.ToBytes("key"), nil, true, true, false, nil},
		{"missing key", bc.ToBytes("missing_key"), nil, false, true, false, badger.ErrKeyNotFound},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ds := createDataStore(t)
			defer ds.Close()
			if tc.set {
				if tc.panic {
					assert.Panics(t, func() {
						ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_Set{Set: &rpc.CommandSet{
							Key:   tc.key,
							Value: tc.value,
						}}}))
					})
					return
				}
				assert.NotPanics(t, func() {
					ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_Set{Set: &rpc.CommandSet{
						Key:   tc.key,
						Value: tc.value,
					}}}))
				})
			}
			if tc.get {
				res, err := ds.Get(tc.key)
				if tc.err != nil {
					assert.ErrorIs(t, err, tc.err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tc.value, res)
				}
			}
		})
	}
}

func TestDataStore_SetGetMany(t *testing.T) {
	t.Parallel()

	keys := [][]byte{
		bc.ToBytes("akey1"),
		bc.ToBytes("bkey2"),
		bc.ToBytes("ckey3"),
		nil,
	}
	values := [][]byte{
		bc.ToBytes("test_value_1"),
		bc.ToBytes("test_value_2"),
		bc.ToBytes("test_value_3"),
		nil,
	}

	cases := []struct {
		name   string
		keys   [][]byte
		values [][]byte
		set    bool
		get    bool
		panic  bool
		err    error
	}{
		{"normal values", keys[:3], values[:3], true, true, false, nil},
		{"no pairs", nil, nil, true, true, false, nil},
		{"empty key", keys[3:], values[3:], true, true, false, nil},
		{"missing keys", keys[:1], values[3:], false, true, false, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := createDataStore(t)
			defer ds.Close()
			if tc.set {
				if tc.panic {
					assert.Panics(t, func() {
						ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_SetMany{SetMany: &rpc.CommandSetMany{
							Keys:   tc.keys,
							Values: tc.values,
						}}}))
					})
					return
				}
				assert.NotPanics(t, func() {
					ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_SetMany{SetMany: &rpc.CommandSetMany{
						Keys:   tc.keys,
						Values: tc.values,
					}}}))
				})
			}
			if tc.get {
				res, err := ds.GetMany(tc.keys)
				if tc.err != nil {
					assert.ErrorIs(t, err, tc.err)
				} else {
					assert.NoError(t, err)
					assert.Equal(t, tc.values, res)
				}
			}
		})
	}
}

func TestDataStore_SetManyOverBatchLimit(t *testing.T) {
	t.Parallel()

	ds := createDataStore(t)
	defer ds.Close()
	maxSize := ds.db.MaxBatchSize()
	count := 10
	valueSize := maxSize/int64(count) + 1

	var keys [][]byte
	var values [][]byte
	for i := 0; i < count; i++ {
		keys = append(keys, bc.ToBytes(fmt.Sprintf("key%02d", i+1)))
		value := make([]byte, valueSize)
		value[0] = byte(i + 1)
		values = append(values, value)
	}

	err := ds.SetMany(keys, values)
	assert.NoError(t, err)

	for i := range keys {
		val, err := ds.Get(keys[i])
		assert.NoError(t, err)
		assert.Equal(t, byte(i+1), val[0])
	}
}

func TestDataStore_GetPrefix(t *testing.T) {
	t.Parallel()

	ds := createDataStore(t)
	defer ds.Close()

	keys := [][]byte{
		bc.ToBytes("prefix1_key1"),
		bc.ToBytes("prefix1_key2"),
		bc.ToBytes("prefix2_key1"),
		bc.ToBytes("prefix2_key2"),
		bc.ToBytes("prefix2_key3"),
		bc.ToBytes("prefix3_key1"),
		bc.ToBytes("prefix3_key2"),
		bc.ToBytes("prefix3_key3"),
		bc.ToBytes("prefix3_key4"),
	}

	values := [][]byte{
		bc.ToBytes("value1"),
		bc.ToBytes("value2"),
		bc.ToBytes("value3"),
		bc.ToBytes("value4"),
		bc.ToBytes("value5"),
		bc.ToBytes("value6"),
		bc.ToBytes("value7"),
		bc.ToBytes("value8"),
		bc.ToBytes("value9"),
	}

	cases := []struct {
		name   string
		prefix string
		keys   [][]byte
		values [][]byte
	}{
		{"common prefix", "pref", keys[:], values[:]},
		{"prefix 1", "prefix1_", keys[:2], values[:2]},
		{"prefix 2", "prefix2_", keys[2:5], values[2:5]},
		{"prefix 3", "prefix3_", keys[5:], values[5:]},
	}

	err := ds.SetMany(keys, values)
	assert.NoError(t, err)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keys, values, err := ds.GetPrefix(bc.ToBytes(tc.prefix))
			assert.NoError(t, err)
			assert.Equal(t, tc.keys, keys)
			assert.Equal(t, tc.values, values)
		})
	}
}

func TestDataStore_GetRange(t *testing.T) {
	t.Parallel()

	ds := createDataStore(t)
	defer ds.Close()

	var keys [][]byte
	var values [][]byte
	for i := 0; i < 20; i++ {
		keys = append(keys, bc.ToBytes(fmt.Sprintf("key%02d", i+1)))
		values = append(values, bc.ToBytes(fmt.Sprintf("value%02d", i+1)))
	}

	cases := []struct {
		name   string
		min    string
		max    string
		count  int
		keys   [][]byte
		values [][]byte
	}{
		{"all", "", "", 20, keys[:], values[:]},
		{"none count", "key01", "key20", 0, nil, nil},
		{"none min", "key21", "", 20, nil, nil},
		{"none max", "", "key00", 20, nil, nil},
		{"count", "", "", 7, keys[:7], values[:7]},
		{"min", "key05", "", 20, keys[4:], values[4:]},
		{"min and count", "key05", "", 5, keys[4:9], values[4:9]},
		{"max", "", "key10", 20, keys[:10], values[:10]},
		{"max and count", "", "key10", 5, keys[:5], values[:5]},
		{"min and max", "key13", "key17", 20, keys[12:17], values[12:17]},
		{"min max and count", "key01", "key10", 5, keys[0:5], values[0:5]},
	}

	err := ds.SetMany(keys, values)
	assert.NoError(t, err)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var min, max []byte
			if tc.min != "" {
				min = bc.ToBytes(tc.min)
			}
			if tc.max != "" {
				max = bc.ToBytes(tc.max)
			}
			keys, values, err := ds.GetRange(min, max, tc.count)
			assert.NoError(t, err)
			assert.Equal(t, tc.keys, keys)
			assert.Equal(t, tc.values, values)
		})
	}
}

func TestDataStore_Delete(t *testing.T) {
	t.Parallel()

	keys := [][]byte{
		bc.ToBytes("key1"),
		bc.ToBytes("key2"),
		bc.ToBytes("key3"),
		bc.ToBytes("key4"),
		bc.ToBytes("key5"),
	}

	cases := []struct {
		name   string
		delete []byte
		result [][]byte
		panics bool
	}{
		{"existing key 1", bc.ToBytes("key1"), keys[1:], false},
		{"existing key 2", bc.ToBytes("key3"), sliceBytes(keys, 0, 2, 3, 5), false},
		{"existing key 3", bc.ToBytes("key5"), keys[:4], false},
		{"missing key", bc.ToBytes("key0"), keys[:], false},
		{"empty key", nil, keys[:], false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := createDataStore(t)
			defer ds.Close()
			_ = ds.SetMany(keys, keys)
			if tc.panics {
				assert.Panics(t, func() {
					ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_Delete{Delete: &rpc.CommandDelete{
						Key: tc.delete,
					}}}))
				})
				return
			}
			assert.NotPanics(t, func() {
				ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_Delete{Delete: &rpc.CommandDelete{
					Key: tc.delete,
				}}}))
			})
			keys, _, _ := ds.GetRange(nil, nil, 100)
			assert.Equal(t, tc.result, keys)
		})
	}
}

func TestDataStore_DeleteMany(t *testing.T) {
	t.Parallel()

	keys := [][]byte{
		bc.ToBytes("key1"),
		bc.ToBytes("key2"),
		bc.ToBytes("key3"),
		bc.ToBytes("key4"),
		bc.ToBytes("key5"),
	}

	cases := []struct {
		name   string
		delete [][]byte
		result [][]byte
		panics bool
	}{
		{"existing key", [][]byte{bc.ToBytes("key1")}, keys[1:], false},
		{"existing keys", [][]byte{bc.ToBytes("key2"), bc.ToBytes("key3"), bc.ToBytes("key4")}, sliceBytes(keys, 0, 1, 4, 5), false},
		{"duplicate keys", [][]byte{bc.ToBytes("key3"), bc.ToBytes("key3")}, sliceBytes(keys, 0, 2, 3, 5), false},
		{"missing key", [][]byte{bc.ToBytes("key0")}, keys[:], false},
		{"empty keys", nil, keys[:], false},
		{"empty key", [][]byte{nil}, keys[:], false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := createDataStore(t)
			defer ds.Close()
			_ = ds.SetMany(keys, keys)
			if tc.panics {
				assert.Panics(t, func() {
					ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_DeleteMany{DeleteMany: &rpc.CommandDeleteMany{
						Keys: tc.delete,
					}}}))
				})
				return
			}
			assert.NotPanics(t, func() {
				ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_DeleteMany{DeleteMany: &rpc.CommandDeleteMany{
					Keys: tc.delete,
				}}}))
			})
			keys, _, _ := ds.GetRange(nil, nil, 100)
			assert.Equal(t, tc.result, keys)
		})
	}
}

func TestDataStore_DeletePrefix(t *testing.T) {
	t.Parallel()

	keys := [][]byte{
		bc.ToBytes("prefix1_key1"),
		bc.ToBytes("prefix1_key2"),
		bc.ToBytes("prefix2_key3"),
		bc.ToBytes("prefix3_key4"),
		bc.ToBytes("prefix3_key5"),
	}

	cases := []struct {
		name   string
		prefix []byte
		result [][]byte
		panics bool
	}{
		{"existing prefix 1", bc.ToBytes("prefix1"), keys[2:], false},
		{"existing prefix 2", bc.ToBytes("prefix2"), sliceBytes(keys, 0, 2, 3, 5), false},
		{"existing prefix 3", bc.ToBytes("prefix3"), keys[:3], false},
		{"common prefix", bc.ToBytes("prefix"), nil, false},
		{"missing prefix", bc.ToBytes("prefix4"), keys[:], false},
		{"empty prefix deletes all", nil, nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := createDataStore(t)
			defer ds.Close()
			_ = ds.SetMany(keys, keys)
			if tc.panics {
				assert.Panics(t, func() {
					ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_DeletePrefix{DeletePrefix: &rpc.CommandDeletePrefix{
						Prefix: tc.prefix,
					}}}))
				})
				return
			}
			assert.NotPanics(t, func() {
				ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_DeletePrefix{DeletePrefix: &rpc.CommandDeletePrefix{
					Prefix: tc.prefix,
				}}}))
			})
			keys, _, _ := ds.GetRange(nil, nil, 100)
			assert.Equal(t, tc.result, keys)
		})
	}
}

func TestDataStore_DeleteRange(t *testing.T) {
	t.Parallel()

	var keys [][]byte
	var values [][]byte
	for i := 0; i < 20; i++ {
		keys = append(keys, bc.ToBytes(fmt.Sprintf("key%02d", i+1)))
		values = append(values, bc.ToBytes(fmt.Sprintf("value%02d", i+1)))
	}

	cases := []struct {
		name    string
		min     string
		max     string
		deleted [][]byte
		result  [][]byte
		panics  bool
	}{
		{"all", "", "", keys[:], nil, false},
		{"none min", "key21", "", nil, keys[:], false},
		{"none max", "", "key00", nil, keys[:], false},
		{"min", "key05", "", keys[4:], keys[:4], false},
		{"max", "", "key10", keys[:10], keys[10:], false},
		{"min and max", "key13", "key17", keys[12:17], sliceBytes(keys, 0, 12, 17, 20), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := createDataStore(t)
			defer ds.Close()
			_ = ds.SetMany(keys, values)
			var deleted *rpc.ResultDeleteRange
			if tc.panics {
				assert.Panics(t, func() {
					ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_DeleteRange{DeleteRange: &rpc.CommandDeleteRange{
						Min: bc.ToBytes(tc.min),
						Max: bc.ToBytes(tc.max),
					}}}))
				})
				return
			}
			assert.NotPanics(t, func() {
				res := ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_DeleteRange{DeleteRange: &rpc.CommandDeleteRange{
					Min: bc.ToBytes(tc.min),
					Max: bc.ToBytes(tc.max),
				}}}))
				deleted = res.(*rpc.ResultDeleteRange)
			})
			assert.Equal(t, tc.deleted, deleted.Keys)
			keys, _, _ := ds.GetRange(nil, nil, 100)
			assert.Equal(t, tc.result, keys)
		})
	}
}

func TestDataStore_DeleteAll(t *testing.T) {
	t.Parallel()

	ds := createDataStore(t)
	defer ds.Close()

	keys := [][]byte{
		bc.ToBytes("key1"),
		bc.ToBytes("key2"),
		bc.ToBytes("key3"),
		bc.ToBytes("key4"),
		bc.ToBytes("key5"),
	}

	_ = ds.SetMany(keys, keys)

	assert.NotPanics(t, func() {
		ds.Apply(logCommand(&rpc.Command{Command: &rpc.Command_DeleteAll{DeleteAll: &rpc.CommandDeleteAll{}}}))
	})
	res, _, _ := ds.GetRange(nil, nil, 100)
	assert.Empty(t, res)
}

func TestDataStore_Snapshots(t *testing.T) {
	t.Parallel()

	ds := createDataStore(t)
	defer ds.Close()

	var keys [][]byte
	var values [][]byte
	for i := 0; i < 20; i++ {
		keys = append(keys, bc.ToBytes(fmt.Sprintf("key%02d", i+1)))
		values = append(values, bc.ToBytes(fmt.Sprintf("value%02d", i+1)))
	}
	_ = ds.SetMany(keys, values)

	// Create snapshot
	snap, err := ds.Snapshot()
	assert.NoError(t, err)
	assert.NotNil(t, snap)

	// Persist snapshot
	var sink snapshotSink
	err = snap.Persist(&sink)
	assert.NoError(t, err)
	snap.Release()

	// Restore snapshot
	ds = createDataStore(t)
	err = ds.Restore(sink.Reader())
	assert.NoError(t, err)

	resKeys, resValues, err := ds.GetRange(nil, nil, 100)
	assert.NoError(t, err)
	assert.Equal(t, keys, resKeys)
	assert.Equal(t, values, resValues)
}

func logCommand(cmd *rpc.Command) *raft.Log {
	b, _ := proto.Marshal(cmd)
	return &raft.Log{
		Type: raft.LogCommand,
		Data: b,
	}
}

func createDataStore(t *testing.T) *DataStore {
	fsm, err := NewDataStore(DefaultDataStoreConfig("").WithInMemory(true))
	assert.NoError(t, err)
	return fsm
}

func sliceBytes(s [][]byte, idxs ...int) [][]byte {
	var res [][]byte
	for i := 0; i < len(idxs); i += 2 {
		res = append(res, s[idxs[i]:idxs[i+1]]...)
	}
	return res
}

type snapshotSink struct {
	buf *bytes.Buffer
}

func (s *snapshotSink) Write(b []byte) (n int, err error) {
	if s.buf == nil {
		s.buf = &bytes.Buffer{}
	}
	return s.buf.Write(b)
}

func (s *snapshotSink) Close() error {
	return nil
}

func (s *snapshotSink) ID() string {
	return ""
}

func (s *snapshotSink) Cancel() error {
	return nil
}

func (s *snapshotSink) Reader() *snapshotReadCloser {
	return &snapshotReadCloser{r: bytes.NewReader(s.buf.Bytes())}
}

type snapshotReadCloser struct {
	r *bytes.Reader
}

func (s *snapshotReadCloser) Read(b []byte) (n int, err error) {
	return s.r.Read(b)
}

func (s *snapshotReadCloser) Close() error {
	return nil
}

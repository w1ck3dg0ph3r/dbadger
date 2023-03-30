package dbadger

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/w1ck3dg0ph3r/dbadger/internal/stores"
)

// Config is DBadger node config.
//
// See [DefaultConfig] for defaults.
type Config struct {
	// Path is the path of the directory where data will be stored in. If directory does not exists it will be created.
	Path string

	// Bind is a network address for the node to listen on (for example, "127.0.0.1:7001", "[::1]:7001", ":7001").
	Bind Address

	// TLS configures certificates and keys for the node.
	//
	// Zero value means TLS is disabled.
	TLS TLSConfig

	// InMemory makes node run in in-memory mode, which means everything is stored in memory and no files are created.
	//
	// All data will be lost when node is stopped or crashed. Used primarily for testing purposes.
	InMemory bool

	//////////////////////////////////////////////////////////////
	// Startup mode
	//////////////////////////////////////////////////////////////

	// Bootstrap a new cluster from this node.
	//
	// A cluster should only be bootstrapped once from a single primary node.
	// After that other nodes can join the cluster.
	Bootstrap bool

	// Recover cluster from a loss of quorum (e.g. when multiple nodes die at the same time)
	// by forcing a new cluster configuration. This works by reading all the current state
	// for this node, creating a snapshot with the new configuration, and then truncating the log.
	//
	// Typically to bring the cluster back up you should choose a node to become a new leader,
	// recover that node and then join new clean-sate nodes as usual.
	Recover bool

	// Join an existing cluster via given node.
	//
	// It is safe to join a cluster via any node. If the target node is not a leader of the cluster
	// it will forward the request to the cluster's leader.
	Join Address

	// DisableLeaveOnStop makes the node Stop without leaving the cluster.
	//
	// Setting this to true is useful for simulation of node failure during testing.
	DisableLeaveOnStop bool

	//////////////////////////////////////////////////////////////
	// Consensus options
	//////////////////////////////////////////////////////////////

	// HeartbeatTimeout specifies the time in follower state without contact from a leader before
	// Raft attempts an election.
	HeartbeatTimeout time.Duration

	// ElectionTimeout specifies the time in candidate state without contact from a leader before
	// Raft attempts an election.
	ElectionTimeout time.Duration

	// LeaderLeaseTimeout is used to control how long the lease lasts for being the leader without
	// being able to contact a quorum of nodes. If we reach this interval without contact,
	// the node will step down as leader.
	LeaderLeaseTimeout time.Duration

	// CommitTimeout specifies the time without an Apply operation before the leader sends an
	// AppendEntry RPC to followers, to ensure a timely commit of log entries.
	CommitTimeout time.Duration

	// SnapshotInterval controls how often Raft checks if it should perform a snapshot.
	//
	// Raft randomly staggers between this value and 2x this value to avoid the entire cluster
	// from performing a snapshot at once.
	SnapshotInterval time.Duration

	// SnapshotThreshold controls how many outstanding logs there must be before Raft performs a snapshot.
	//
	// This is to prevent excessive snapshotting by replaying a small set of logs instead.
	SnapshotThreshold uint64

	// SnapshotRetention controls how many snapshots are retained. Must be at least 1.
	SnapshotRetention int

	// TrailingLogs controls how many logs Raft leaves after a snapshot.
	//
	// This is used so that a follower can quickly replay logs instead of being forced
	// to receive an entire snapshot.
	TrailingLogs uint64

	//////////////////////////////////////////////////////////////
	// Store options
	//////////////////////////////////////////////////////////////

	// LogStoreConfig configures replicated log storage.
	LogStoreConfig *StoreConfig

	// DataStoreConfig configures data storage.
	DataStoreConfig *StoreConfig

	// Logger configures which logger DB uses.
	//
	// Leaving this nil will disable logging.
	Logger Logger
}

// DefaultConfig return default [Config].
func DefaultConfig(path string, bind Address) *Config {
	return &Config{
		Path: path,
		Bind: bind,

		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		SnapshotInterval:   120 * time.Second,
		SnapshotThreshold:  8192,
		SnapshotRetention:  1,
		TrailingLogs:       10240,

		LogStoreConfig:  DefaultLogStoreConfig(),
		DataStoreConfig: DefaultDataStoreConfig(),
	}
}

// DefaultConfigInMemory returns default [Config] with InMemory mode turned on.
func DefaultConfigInMemory(bind Address) *Config {
	return DefaultConfig("", bind).WithInMemory(true)
}

// WithInMemory returns [Config] with InMemory set to the given value.
func (c *Config) WithInMemory(inmem bool) *Config {
	c.InMemory = inmem
	return c
}

// WithTLS returns [Config] with TLS configuration set to the given values.
func (c *Config) WithTLS(ca, cert, key []byte) *Config {
	c.TLS = TLSConfig{
		CA:   ca,
		Cert: cert,
		Key:  key,
	}
	return c
}

// WithTLSFiles returns [Config] with TLS configuration set to the given values.
func (c *Config) WithTLSFiles(caFile, certFile, keyFile string) *Config {
	c.TLS = TLSConfig{
		CAFile:   caFile,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
	return c
}

// WithBootstrap returns [Config] with Bootstrap set to the given value.
func (c *Config) WithBootstrap(bootstrap bool) *Config {
	c.Bootstrap = bootstrap
	return c
}

// WithRecover returns [Config] with Recover set to the given value.
func (c *Config) WithRecover(recover bool) *Config {
	c.Recover = recover
	return c
}

// WithJoin returns [Config] with Join set to the given value.
func (c *Config) WithJoin(join Address) *Config {
	c.Join = join
	return c
}

// WithHeartbeatTimeout returns [Config] with HeartbeatTimeout set to the given value.
func (c *Config) WithHeartbeatTimeout(v time.Duration) *Config {
	c.HeartbeatTimeout = v
	return c
}

// WithElectionTimeout returns [Config] with ElectionTimeout set to the given value.
func (c *Config) WithElectionTimeout(v time.Duration) *Config {
	c.ElectionTimeout = v
	return c
}

// WithLeaderLeaseTimeout returns [Config] with LeaderLeaseTimeout set to the given value.
func (c *Config) WithLeaderLeaseTimeout(v time.Duration) *Config {
	c.LeaderLeaseTimeout = v
	return c
}

// WithCommitTimeout returns [Config] with CommitTimeout set to the given value.
func (c *Config) WithCommitTimeout(v time.Duration) *Config {
	c.CommitTimeout = v
	return c
}

// WithSnapshotInterval returns [Config] with SnapshotInterval set to the given value.
func (c *Config) WithSnapshotInterval(v time.Duration) *Config {
	c.SnapshotInterval = v
	return c
}

// WithSnapshotThreshold returns [Config] with SnapshotThreshold set to the given value.
func (c *Config) WithSnapshotThreshold(v uint64) *Config {
	c.SnapshotThreshold = v
	return c
}

// WithSnapshotRetention returns [Config] with SnapshotRetention set to the given value.
func (c *Config) WithSnapshotRetention(v int) *Config {
	c.SnapshotRetention = v
	return c
}

// WithTrailingLogs returns [Config] with TrailingLogs set to the given value.
func (c *Config) WithTrailingLogs(v uint64) *Config {
	c.TrailingLogs = v
	return c
}

// WithLogStoreConfig returns [Config] with LogStoreConfig set to the given value.
func (c *Config) WithLogStoreConfig(v *StoreConfig) *Config {
	c.LogStoreConfig = v
	return c
}

// WithDataStoreConfig returns [Config] with DataStoreConfig set to the given value.
func (c *Config) WithDataStoreConfig(v *StoreConfig) *Config {
	c.DataStoreConfig = v
	return c
}

// WithLogger returns [Config] with Logger set to the given value.
func (c *Config) WithLogger(logger Logger) *Config {
	c.Logger = logger
	return c
}

func (c *Config) validate() error { //nolint:cyclop
	var err error
	if c.Path == "" && !c.InMemory {
		return fmt.Errorf("either Path or InMemory must be set")
	}
	if c.Bind == "" {
		return fmt.Errorf("empty Bind address")
	}
	_, err = net.ResolveTCPAddr("tcp", string(c.Bind))
	if err != nil {
		return fmt.Errorf("invalid Bind address: %w", err)
	}
	if c.Bootstrap && c.Recover {
		return fmt.Errorf("can not use Bootstrap and Recover together")
	}
	if c.Bootstrap && c.Join != "" {
		return fmt.Errorf("can not use Bootstrap and Join together")
	}
	if c.Recover && c.Join != "" {
		return fmt.Errorf("can not use Recover and Join together")
	}
	if c.Join != "" {
		_, err = net.ResolveTCPAddr("tcp", string(c.Join))
		if err != nil {
			return fmt.Errorf("invalid Join address: %w", err)
		}
	}

	if c.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("HeartbeatTimeout is too low")
	}
	if c.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("ElectionTimeout is too low")
	}
	if c.CommitTimeout < time.Millisecond {
		return fmt.Errorf("CommitTimeout is too low")
	}
	if c.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("SnapshotInterval is too low")
	}
	if c.SnapshotRetention < 1 {
		return fmt.Errorf("SnapshotRetention must be at least 1")
	}
	if c.LeaderLeaseTimeout < 5*time.Millisecond {
		return fmt.Errorf("LeaderLeaseTimeout is too low")
	}
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		return fmt.Errorf("LeaderLeaseTimeout (%s) cannot be larger than HeartbeatTimeout (%s)", c.LeaderLeaseTimeout, c.HeartbeatTimeout)
	}
	if c.ElectionTimeout < c.HeartbeatTimeout {
		return fmt.Errorf("ElectionTimeout (%s) must be equal or greater than HeartbeatTimeout (%s)", c.ElectionTimeout, c.HeartbeatTimeout)
	}

	return nil
}

// TLSConfig is a TLS configuration.
//
// PEM encoded options are prioritized over file paths. You can mix
// both file and PEM options in the same config.
type TLSConfig struct {
	CA   []byte // PEM encoded TLS certificate authority.
	Cert []byte // PEM encoded TLS certificate.
	Key  []byte // PEM encoded TLS private key.

	CAFile   string // TLS certificate authority file path.
	CertFile string // TLS certificate file path.
	KeyFile  string // TLS private key file path.
}

func (c *TLSConfig) parse() (ca *x509.CertPool, cert *tls.Certificate, err error) {
	if c.CA == nil && c.CAFile != "" {
		c.CA, err = os.ReadFile(c.CAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read ca file: %w", err)
		}
	}

	if c.Cert == nil && c.CertFile != "" {
		c.Cert, err = os.ReadFile(c.CertFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read cert file: %w", err)
		}
	}

	if c.Key == nil && c.KeyFile != "" {
		c.Key, err = os.ReadFile(c.KeyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read key file: %w", err)
		}
	}

	if len(c.CA) == 0 || len(c.Cert) == 0 || len(c.Key) == 0 {
		return nil, nil, nil
	}

	ca = x509.NewCertPool()
	caPEM := c.CA
	for len(caPEM) > 0 {
		var block *pem.Block
		block, caPEM = pem.Decode(caPEM)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, fmt.Errorf("parse ca: %w", err)
		}
		ca.AddCert(cert)
	}

	tlscert, err := tls.X509KeyPair(c.Cert, c.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("parse cert/key: %w", err)
	}
	cert = &tlscert

	return ca, cert, nil
}

// StoreConfig is a persistent storage configuration.
type StoreConfig struct {
	// SyncWrites controls whether Badger would call an additional msync after writes to flush mmap
	// buffer over to disk to survive hard reboots. Most users should not need to do this.
	SyncWrites bool

	// MaxLevels is the maximum number of levels of compaction allowed in the LSM.
	MaxLevels int
	// LevelSizeMultiplier sets the ratio between the maximum sizes of contiguous levels in the LSM.
	// Once a level grows to be larger than this ratio, the compaction process will be triggered.
	LevelSizeMultiplier int
	// BaseTableSize sets the maximum size in bytes for LSM table or file in the base level.
	BaseTableSize int64
	// BaseLevelSize sets the maximum size target for the base level.
	BaseLevelSize int64
	// ValueLogFileSize sets the maximum size of a single value log file.
	ValueLogFileSize int64
	// ValueLogMaxEntries sets the maximum number of entries a value log file can hold approximately.
	//
	// The actual size limit of a value log file is the
	// minimum of ValueLogFileSize and ValueLogMaxEntries.
	ValueLogMaxEntries uint32

	// NumMemtables sets the maximum number of tables to keep in memory before stalling.
	NumMemtables int
	// MemTableSize sets the maximum size in bytes for memtable table.
	MemTableSize int64

	// BlockSize sets the size of any block in SSTable. SSTable is divided into multiple blocks internally.
	BlockSize int
	// BlockCacheSize specifies how much data cache should be held in memory. A small size
	// of cache means lower memory consumption and lookups/iterations would take
	// longer. It is recommended to use a cache if you're using compression or encryption.
	// If compression and encryption both are disabled, adding a cache will lead to
	// unnecessary overhead which will affect the read performance. Setting size to
	// zero disables the cache altogether.
	BlockCacheSize int64

	// NumLevelZeroTables sets the maximum number of Level 0 tables before compaction starts.
	NumLevelZeroTables int
	// NumLevelZeroTablesStall sets the number of Level 0 tables that once reached causes the DB to
	// stall until compaction succeeds.
	NumLevelZeroTablesStall int
	// NumCompactors sets the number of compaction workers to run concurrently.  Setting this to
	// zero stops compactions, which could eventually cause writes to block forever.
	NumCompactors int
	// CompactL0OnClose sets whether Level 0 should be compacted before closing the DB. This ensures
	// that both reads and writes are efficient when the DB is opened later.
	CompactL0OnClose bool

	// Compression enables snappy compression for every block.
	Compression bool

	// GCEnabled enables value log garbage collection.
	GCEnabled bool
	// GCInterval sets value log garbage collection interval.
	GCInterval time.Duration
	// GCDiscardRatio sets threshold for rewriting files during garbage collection.
	//
	// During value log garbage collection Badger will rewrite files when it can discard
	// at least discardRatio space of that file.
	GCDiscardRatio float64
}

// DefaultLogStoreConfig returns default replicated log storage config.
func DefaultLogStoreConfig() *StoreConfig {
	return &StoreConfig{
		SyncWrites:              false,
		MaxLevels:               7,
		LevelSizeMultiplier:     10,
		BaseTableSize:           2 << 20,
		BaseLevelSize:           10 << 20,
		ValueLogFileSize:        1<<30 - 1,
		ValueLogMaxEntries:      1e6,
		NumMemtables:            5,
		MemTableSize:            64 << 20,
		BlockSize:               4 << 10,
		BlockCacheSize:          256 << 20,
		NumLevelZeroTables:      5,
		NumLevelZeroTablesStall: 15,
		NumCompactors:           4,
		CompactL0OnClose:        false,
		Compression:             false,
		GCEnabled:               false,
		GCInterval:              0,
		GCDiscardRatio:          0.0,
	}
}

// DefaultDataStoreConfig returns default data storage config.
func DefaultDataStoreConfig() *StoreConfig {
	return &StoreConfig{
		SyncWrites:              false,
		MaxLevels:               7,
		LevelSizeMultiplier:     10,
		BaseTableSize:           2 << 20,
		BaseLevelSize:           10 << 20,
		ValueLogFileSize:        1<<30 - 1,
		ValueLogMaxEntries:      1e6,
		NumMemtables:            5,
		MemTableSize:            64 << 20,
		BlockSize:               4 << 10,
		BlockCacheSize:          256 << 20,
		NumLevelZeroTables:      5,
		NumLevelZeroTablesStall: 15,
		NumCompactors:           4,
		CompactL0OnClose:        false,
		Compression:             true,
		GCEnabled:               true,
		GCInterval:              5 * time.Minute,
		GCDiscardRatio:          0.5,
	}
}

// WithSyncWrites returns [StorageConfig] with SyncWrites set to the given value.
func (c *StoreConfig) WithSyncWrites(v bool) *StoreConfig {
	c.SyncWrites = v
	return c
}

// WithMaxLevels returns [StorageConfig] with MaxLevels set to the given value.
func (c *StoreConfig) WithMaxLevels(v int) *StoreConfig {
	c.MaxLevels = v
	return c
}

// WithLevelSizeMultiplier returns [StorageConfig] with LevelSizeMultiplier set to the given value.
func (c *StoreConfig) WithLevelSizeMultiplier(v int) *StoreConfig {
	c.LevelSizeMultiplier = v
	return c
}

// WithBaseTableSize returns [StorageConfig] with BaseTableSize set to the given value.
func (c *StoreConfig) WithBaseTableSize(v int64) *StoreConfig {
	c.BaseTableSize = v
	return c
}

// WithBaseLevelSize returns [StorageConfig] with BaseLevelSize set to the given value.
func (c *StoreConfig) WithBaseLevelSize(v int64) *StoreConfig {
	c.BaseLevelSize = v
	return c
}

// WithValueLogFileSize returns [StorageConfig] with ValueLogFileSize set to the given value.
func (c *StoreConfig) WithValueLogFileSize(v int64) *StoreConfig {
	c.ValueLogFileSize = v
	return c
}

// WithValueLogMaxEntries returns [StorageConfig] with ValueLogMaxEntries set to the given value.
func (c *StoreConfig) WithValueLogMaxEntries(v uint32) *StoreConfig {
	c.ValueLogMaxEntries = v
	return c
}

// WithNumMemtables returns [StorageConfig] with NumMemtables set to the given value.
func (c *StoreConfig) WithNumMemtables(v int) *StoreConfig {
	c.NumMemtables = v
	return c
}

// WithMemTableSize returns [StorageConfig] with MemTableSize set to the given value.
func (c *StoreConfig) WithMemTableSize(v int64) *StoreConfig {
	c.MemTableSize = v
	return c
}

// WithBlockSize returns [StorageConfig] with BlockSize set to the given value.
func (c *StoreConfig) WithBlockSize(v int) *StoreConfig {
	c.BlockSize = v
	return c
}

// WithBlockCacheSize returns [StorageConfig] with BlockCacheSize set to the given value.
func (c *StoreConfig) WithBlockCacheSize(v int64) *StoreConfig {
	c.BlockCacheSize = v
	return c
}

// WithNumLevelZeroTables returns [StorageConfig] with NumLevelZeroTables set to the given value.
func (c *StoreConfig) WithNumLevelZeroTables(v int) *StoreConfig {
	c.NumLevelZeroTables = v
	return c
}

// WithNumLevelZeroTablesStall returns [StorageConfig] with NumLevelZeroTablesStall set to the given value.
func (c *StoreConfig) WithNumLevelZeroTablesStall(v int) *StoreConfig {
	c.NumLevelZeroTablesStall = v
	return c
}

// WithNumCompactors returns [StorageConfig] with NumCompactors set to the given value.
func (c *StoreConfig) WithNumCompactors(v int) *StoreConfig {
	c.NumCompactors = v
	return c
}

// WithCompactL0OnClose returns [StorageConfig] with CompactL0OnClose set to the given value.
func (c *StoreConfig) WithCompactL0OnClose(v bool) *StoreConfig {
	c.CompactL0OnClose = v
	return c
}

// WithCompression returns [StorageConfig] with Compression set to the given value.
func (c *StoreConfig) WithCompression(v bool) *StoreConfig {
	c.Compression = v
	return c
}

// WithGC returns [StorageConfig] with value log garbage collection enabled.
func (c *StoreConfig) WithGC(interval time.Duration, discardRatio float64) *StoreConfig {
	c.GCEnabled = true
	c.GCInterval = interval
	c.GCDiscardRatio = discardRatio
	return c
}

func (c *StoreConfig) storeConfig(path string, inmem bool, logger badger.Logger) *stores.Config {
	return &stores.Config{
		Path:                    path,
		InMemory:                inmem,
		SyncWrites:              c.SyncWrites,
		MaxLevels:               c.MaxLevels,
		LevelSizeMultiplier:     c.LevelSizeMultiplier,
		BaseTableSize:           c.BaseTableSize,
		BaseLevelSize:           c.BaseLevelSize,
		ValueLogFileSize:        c.ValueLogFileSize,
		ValueLogMaxEntries:      c.ValueLogMaxEntries,
		NumMemtables:            c.NumMemtables,
		MemTableSize:            c.MemTableSize,
		BlockSize:               c.BlockSize,
		BlockCacheSize:          c.BlockCacheSize,
		NumLevelZeroTables:      c.NumLevelZeroTables,
		NumLevelZeroTablesStall: c.NumLevelZeroTablesStall,
		NumCompactors:           c.NumCompactors,
		CompactL0OnClose:        c.CompactL0OnClose,
		Compression:             c.Compression,
		GCEnabled:               c.GCEnabled,
		GCInterval:              c.GCInterval,
		GCDiscardRatio:          c.GCDiscardRatio,
		Logger:                  logger,
	}
}

package dbadger

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"

	"github.com/w1ck3dg0ph3r/dbadger/internal/mux"
	"github.com/w1ck3dg0ph3r/dbadger/internal/rpc"
	"github.com/w1ck3dg0ph3r/dbadger/internal/stores"
)

// DB is a DBadger node.
type DB struct {
	config *Config
	log    Logger

	muxer *mux.Mux

	service  *service
	executor *executor

	ca      *x509.CertPool
	cert    *tls.Certificate
	tlsconf *tls.Config

	data          *stores.DataStore
	logStore      *stores.LogStore
	stableStore   *stores.StableStore
	snapshotStore raft.SnapshotStore

	raftNode      *raft.Raft
	raftConfig    *raft.Config
	raftTransport raft.Transport
	raftLeader    raft.ServerAddress
	observations  chan raft.Observation

	grpcListener net.Listener
	grpcServer   *grpc.Server

	// Use [DB.grpcLeader] and [DB.grpcLeaderConn] to access leader connection and client
	// in a thread-safe manner.
	_grpcLeaderMut  sync.Mutex
	_grpcLeaderConn *grpc.ClientConn
	_grpcLeader     rpc.ServiceClient

	wg         sync.WaitGroup
	shutdownCh chan struct{}
	running    bool
}

// Address is a network address of a DB node.
type Address string

// Start creates and starts DB.
//
// You must always call [DB.Stop] before terminating.
func Start(cfg *Config) (*DB, error) {
	var err error

	if err := cfg.validate(); err != nil {
		panic(err.Error())
	}

	db := &DB{
		config:     cfg,
		shutdownCh: make(chan struct{}),
		running:    true,
	}
	db.service = &service{DB: db}
	db.executor = &executor{DB: db}

	db.log = nullLogger{}
	if db.config.Logger != nil {
		db.log = db.config.Logger
	}

	db.ca, db.cert, err = db.config.TLS.parse()
	if err != nil {
		return nil, err
	}

	if err = db.createStores(cfg.Path); err != nil {
		_ = db.Stop()
		return nil, err
	}

	if err = db.createMultiplexer(); err != nil {
		_ = db.Stop()
		return nil, err
	}

	if err = db.startRaft(cfg.Bootstrap, cfg.Recover, cfg.Join); err != nil {
		_ = db.Stop()
		return nil, err
	}

	if err = db.startGrpc(); err != nil {
		_ = db.Stop()
		return nil, err
	}

	db.startMultiplexer()

	db.wg.Add(1)
	go db.run()

	return db, nil
}

// Addr returns DB address.
func (db *DB) Addr() Address {
	return Address(db.muxer.Addr().String())
}

// IsLeader returns whether node is a cluster leader.
func (db *DB) IsLeader() bool {
	return db.raftNode != nil && db.raftNode.State() == raft.Leader
}

// IsReady returns whether node is ready to process commands.
func (db *DB) IsReady() bool {
	if db.IsLeader() {
		return true
	}
	if db.raftNode.State() == raft.Follower {
		leaderConn := db.grpcLeaderConn()
		ready := leaderConn != nil && leaderConn.GetState() == connectivity.Ready
		return ready
	}
	return false
}

// Leader returns cluster leader's address.
func (db *DB) Leader() Address {
	return Address(db.raftNode.Leader())
}

// Nodes returns cluster nodes.
func (db *DB) Nodes() []Address {
	cf := db.raftNode.GetConfiguration()
	if cf.Error() != nil {
		return nil
	}
	nodes := make([]Address, 0, len(cf.Configuration().Servers))
	for _, s := range cf.Configuration().Servers {
		nodes = append(nodes, Address(s.Address))
	}
	return nodes
}

// Stats returns internal stats for debugging purposes.
func (db *DB) Stats() map[string]string {
	stats := make(map[string]string, 32)

	if db.raftNode != nil {
		raftStats := db.raftNode.Stats()
		for k, v := range raftStats {
			if strings.HasPrefix(k, "latest_configuration") {
				continue
			}
			if strings.HasPrefix(k, "protocol_version") {
				continue
			}
			stats["raft_"+k] = v
		}
	}

	if db.data != nil {
		dataStats := db.data.Stats()
		for k, v := range dataStats {
			stats["data_"+k] = v
		}
	}

	if db.log != nil {
		logStats := db.logStore.Stats()
		for k, v := range logStats {
			stats["log_"+k] = v
		}
	}

	return stats
}

// Stop stops DB.
func (db *DB) Stop() error {
	if !db.running {
		return nil
	}
	close(db.shutdownCh)

	if db.raftNode != nil {
		db.log.Infof("leaving cluster...")
		_, _ = db.service.RemovePeer(context.Background(), &rpc.CommandRemovePeer{Addr: string(db.config.Bind)})
	}

	if db.grpcServer != nil {
		db.log.Infof("closing grpc...")
		db.grpcServer.GracefulStop()
	}

	if db.raftNode != nil {
		db.log.Infof("closing raft...")
		if err := db.raftNode.Shutdown().Error(); err != nil {
			db.log.Errorf("close raft: %s", err)
		}
	}

	if leaderConn := db.grpcLeaderConn(); leaderConn != nil {
		db.log.Infof("closing grpc leader connection...")
		if err := leaderConn.Close(); err != nil {
			db.log.Errorf("close grpc leader connection: %s", err)
		}
	}

	if db.muxer != nil {
		db.log.Infof("closing multiplexer...")
		if err := db.muxer.Close(); err != nil {
			db.log.Errorf("close multiplexer: %s", err)
		}
	}

	if db.data != nil {
		db.log.Infof("closing fsm...")
		if err := db.data.Close(); err != nil {
			db.log.Errorf("close fsm: %s", err)
		}
	}

	if db.logStore != nil {
		db.log.Infof("closing log store...")
		if err := db.logStore.Close(); err != nil {
			db.log.Errorf("close log store: %s", err)
		}
	}

	db.log.Infof("waiting...")
	db.wg.Wait()

	db.log.Infof("done")

	db.running = false

	return nil
}

func (db *DB) createStores(basePath string) error {
	var err error

	db.log.Infof("opening fsm...")
	db.data, err = stores.NewDataStore(
		stores.DefaultDataStoreConfig(filepath.Join(basePath, "data")).
			WithInMemory(db.config.InMemory).
			WithLogger(newBadgerLogAdapter("fsm: ", db.log.Debugf)))
	if err != nil {
		return err
	}

	db.log.Infof("opening log store...")
	db.logStore, err = stores.NewLogStore(stores.DefaultLogStoreConfig(filepath.Join(basePath, "logs")).
		WithInMemory(db.config.InMemory).
		WithLogger(newBadgerLogAdapter("log store: ", db.log.Debugf)))
	if err != nil {
		return err
	}

	db.stableStore, err = stores.NewStableStore(stores.DefaultStableStoreConfig(basePath).WithInMemory(db.config.InMemory))
	if err != nil {
		return err
	}

	if db.config.InMemory {
		db.snapshotStore, err = raft.NewInmemSnapshotStore(), nil
	} else {
		db.snapshotStore, err = raft.NewFileSnapshotStoreWithLogger(
			basePath,
			db.config.SnapshotRetention,
			newRaftLogAdapter("snapshot store: ", db.log.Debugf),
		)
	}
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) createMultiplexer() error {
	var err error

	if db.tlsEnabled() {
		db.muxer, err = mux.ListenTLS("tcp", string(db.config.Bind), db.tlsConfig())
	} else {
		db.muxer, err = mux.Listen("tcp", string(db.config.Bind))
	}
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) startMultiplexer() {
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		if err := db.muxer.Serve(); err != nil {
			db.log.Errorf("multiplexer: %s", err)
		}
	}()
}

const (
	raftStream = 1
	grpcStream = 2
)

func (db *DB) startRaft(bootstrap bool, recover bool, join Address) error {
	var err error

	db.raftConfig = raft.DefaultConfig()
	db.raftConfig.LocalID = raft.ServerID(db.Addr())
	db.raftConfig.Logger = newRaftLogAdapter("raft: ", db.log.Debugf)
	db.raftConfig.HeartbeatTimeout = db.config.HeartbeatTimeout
	db.raftConfig.ElectionTimeout = db.config.ElectionTimeout
	db.raftConfig.LeaderLeaseTimeout = db.config.LeaderLeaseTimeout
	db.raftConfig.CommitTimeout = db.config.CommitTimeout
	db.raftConfig.SnapshotInterval = db.config.SnapshotInterval
	db.raftConfig.SnapshotThreshold = db.config.SnapshotThreshold
	db.raftConfig.TrailingLogs = db.config.TrailingLogs
	db.raftConfig.NoSnapshotRestoreOnStart = true

	if db.tlsEnabled() {
		db.raftTransport = mux.NewRaftTransportTLS(db.muxer, raftStream, 3, 1*time.Second, db.tlsConfig(), db.raftConfig.Logger)
	} else {
		db.raftTransport = mux.NewRaftTransport(db.muxer, raftStream, 3, 1*time.Second, db.raftConfig.Logger)
	}

	if recover {
		if err = db.recoverCluster(); err != nil {
			return err
		}
	}

	db.raftNode, err = raft.NewRaft(db.raftConfig, db.data, db.logStore, db.stableStore, db.snapshotStore, db.raftTransport)
	if err != nil {
		return err
	}

	db.observations = make(chan raft.Observation, 16)
	db.raftNode.RegisterObserver(raft.NewObserver(db.observations, true, nil))

	if bootstrap {
		if err = db.bootstrapCluster(); err != nil {
			return err
		}
	}

	if join != "" {
		if err = db.joinCluster(join); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) bootstrapCluster() error {
	db.log.Infof("bootstrapping cluster...")

	addr := db.Addr()
	nodes := raft.Configuration{Servers: []raft.Server{
		{Suffrage: raft.Voter, ID: raft.ServerID(addr), Address: raft.ServerAddress(addr)},
	}}

	if err := db.raftNode.BootstrapCluster(nodes).Error(); err != nil {
		return err
	}

	return nil
}

func (db *DB) recoverCluster() error {
	db.log.Infof("recovering cluster...")

	addr := db.Addr()
	nodes := raft.Configuration{Servers: []raft.Server{
		{ID: raft.ServerID(addr), Address: raft.ServerAddress(addr)},
	}}

	if err := raft.RecoverCluster(db.raftConfig, db.data, db.logStore, db.stableStore, db.snapshotStore, db.raftTransport, nodes); err != nil {
		return err
	}

	return nil
}

func (db *DB) joinCluster(leaderAddr Address) error {
	db.log.Infof("joining cluster...")

	if err := db.connectToLeader(leaderAddr); err != nil {
		return fmt.Errorf("can't connect to leader: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := db.grpcLeader().AddPeer(ctx, &rpc.CommandAddPeer{Addr: string(db.Addr())})
	if err != nil {
		return fmt.Errorf("can't join leader: %w", err)
	}

	return nil
}

func (db *DB) connectToLeader(addr Address) error {
	var err error

	db._grpcLeaderMut.Lock()
	defer db._grpcLeaderMut.Unlock()

	if db._grpcLeaderConn != nil {
		if err := db._grpcLeaderConn.Close(); err != nil {
			db.log.Warningf("can't close previous leader connection: %s", err)
		}
	}

	if db.tlsEnabled() {
		db._grpcLeaderConn, err = mux.NewGRPCConnTLS(
			string(addr),
			grpcStream,
			db.tlsConfig(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	} else {
		db._grpcLeaderConn, err = mux.NewGRPCConn(
			string(addr),
			grpcStream,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
	if err != nil {
		return err
	}
	db._grpcLeader = rpc.NewServiceClient(db._grpcLeaderConn)

	return nil
}

func (db *DB) grpcLeader() rpc.ServiceClient { //nolint:ireturn
	db._grpcLeaderMut.Lock()
	defer db._grpcLeaderMut.Unlock()
	return db._grpcLeader
}

func (db *DB) grpcLeaderConn() *grpc.ClientConn {
	db._grpcLeaderMut.Lock()
	defer db._grpcLeaderMut.Unlock()
	return db._grpcLeaderConn
}

//nolint:gochecknoglobals // Required in case multiple instances of DB are created since grpclog.Logger is not mutex protected
var grpcLoggerOnce sync.Once

func (db *DB) startGrpc() error { //nolint:unparam
	grpcLoggerOnce.Do(func() {
		grpclog.SetLoggerV2(newGRPCLogAdapter("grpc: ", db.log.Debugf))
	})

	db.grpcServer = grpc.NewServer()
	rpc.RegisterServiceServer(db.grpcServer, db.service)
	db.grpcListener = db.muxer.Listen(grpcStream)

	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		if err := db.grpcServer.Serve(db.grpcListener); err != nil {
			db.log.Errorf("grpc: %s", err)
		}
	}()

	return nil
}

func (db *DB) run() {
	running := true
	for running {
		select {
		case o := <-db.observations:
			db.observe(&o)
		case <-db.shutdownCh:
			running = false
		}
	}
	db.wg.Done()
}

func (db *DB) observe(o *raft.Observation) {
	switch data := o.Data.(type) {
	case raft.RaftState:
		db.log.Infof("state changed: %s", strings.ToLower(data.String()))
	case raft.PeerObservation:
		if data.Removed {
			db.log.Infof("peer removed: %s", data.Peer.ID)
		} else {
			db.log.Infof("peer added: %s", data.Peer.ID)
		}
		db.logConfiguration(db.log.Infof)
	case raft.LeaderObservation:
		db.raftLeader = data.LeaderAddr
		if data.LeaderAddr != "" {
			db.log.Infof("leader changed: %s", data.LeaderAddr)
			if Address(data.LeaderAddr) != db.Addr() {
				if err := db.connectToLeader(Address(data.LeaderAddr)); err != nil {
					db.log.Errorf("can't connect to new leader: %s", err)
				}
			}
		} else {
			db.log.Infof("leader lost")
		}
		// New config is not available straight after leader change
		time.AfterFunc(100*time.Millisecond, func() {
			db.logConfiguration(db.log.Infof)
		})
	}
}

func (db *DB) logConfiguration(log logFunc) {
	cf := db.raftNode.GetConfiguration()
	if err := cf.Error(); err != nil {
		db.log.Errorf("can't get configuration: %s", err)
		return
	}
	c := cf.Configuration()
	if len(c.Servers) == 0 {
		db.log.Errorf("server list empty")
		return
	}
	leader := db.raftNode.Leader()
	servers := make([]string, 0, len(c.Servers))
	for i := range c.Servers {
		var s string
		if c.Servers[i].Address == leader {
			s = "{" + string(c.Servers[i].Address) + "}"
		} else {
			s = string(c.Servers[i].Address)
		}
		servers = append(servers, s)
	}
	log("current configuration: [%s]", strings.Join(servers, ", "))
}

func (db *DB) tlsEnabled() bool {
	return db.ca != nil && db.cert != nil
}

func (db *DB) tlsConfig() *tls.Config {
	host, _, _ := net.SplitHostPort(string(db.config.Bind))
	if db.tlsconf == nil {
		db.tlsconf = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			ServerName:   host,
			RootCAs:      db.ca,
			ClientCAs:    db.ca,
			Certificates: []tls.Certificate{*db.cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}
	}
	return db.tlsconf
}

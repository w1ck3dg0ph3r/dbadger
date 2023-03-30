package test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/w1ck3dg0ph3r/dbadger"
)

type Variant struct {
	Name     string
	InMemory bool
	TLS      bool
}

var variants = []Variant{
	{Name: "inmemory", InMemory: true, TLS: false},
	{Name: "inmemory_tls", InMemory: true, TLS: true},
	{Name: "ondisk", InMemory: false, TLS: false},
}

func runVariants(t *testing.T, f func(t *testing.T, variant Variant)) {
	for _, variant := range variants {
		t.Run(variant.Name, func(t *testing.T) {
			f(t, variant)
		})
	}
}

func createServer(t testing.TB, bind dbadger.Address, bootstrap bool, join dbadger.Address, cert *TestCert, variant Variant) *dbadger.DB {
	var cfg *dbadger.Config
	if variant.InMemory {
		cfg = dbadger.DefaultConfigInMemory(bind)
	} else {
		cfg = dbadger.DefaultConfig(t.TempDir(), bind)
	}
	if variant.TLS && cert != nil {
		cfg = cfg.WithTLS(cert.CA, cert.Cert, cert.Key)
	}
	cfg = cfg.WithBootstrap(bootstrap).WithJoin(join).WithRecover(false)
	cfg.DisableLeaveOnStop = true
	// cfg.WithLogger(testLogger.WithField("server", bind))

	dbCh := make(chan *dbadger.DB)
	go func() {
		db, err := dbadger.Start(cfg)
		for err != nil {
			t.Log(err)
			time.Sleep(300 * time.Millisecond)
			db, err = dbadger.Start(cfg)
		}
		dbCh <- db
	}()

	select {
	case db := <-dbCh:
		return db
	case <-time.NewTimer(15 * time.Second).C:
		t.Fatal("createServer timed out")
		return nil
	}
}

func createCluster(t testing.TB, servers int, variant Variant) []*dbadger.DB {
	var serverAddrs []string
	for i := 1; i <= servers; i++ {
		serverAddrs = append(serverAddrs, fmt.Sprintf("127.0.0.1:%d", 7420+i))
	}

	var certs []TestCert
	if variant.TLS {
		certs = GenerateTestCertificates(serverAddrs)
	}

	var cluster []*dbadger.DB
	for i := 0; i < servers; i++ {
		bind := dbadger.Address(serverAddrs[i])
		bootstrap := i == 0
		join := dbadger.Address("")
		if i > 0 {
			join = cluster[0].Addr()
		}
		var serverCert *TestCert
		if variant.TLS {
			serverCert = &certs[i]
		}
		server := createServer(t, bind, bootstrap, join, serverCert, variant)
		time.Sleep(300 * time.Millisecond)
		for !server.IsReady() {
			time.Sleep(300 * time.Millisecond)
		}
		cluster = append(cluster, server)
	}

	assert.True(t, cluster[0].IsLeader())
	for i := range serverAddrs {
		assertClusterNodes(t, cluster[i], serverAddrs)
	}

	return cluster
}

func removeNode(t testing.TB, cluster []*dbadger.DB, node int) []*dbadger.DB {
	assert.NoError(t, cluster[node].Stop())
	res := make([]*dbadger.DB, 0, len(cluster)-1)
	res = append(res, cluster[:node]...)
	res = append(res, cluster[node+1:]...)
	return res
}

func getClusterLeader(t testing.TB, cluster []*dbadger.DB) int {
	const timeout = 15 * time.Second
	leaderCh := make(chan int)
	go func() {
		for {
			leaderAddr := cluster[0].Leader()
			for leaderAddr == "" {
				time.Sleep(500 * time.Millisecond)
				leaderAddr = cluster[0].Leader()
			}
			for i := range cluster {
				if cluster[i].Addr() == leaderAddr {
					leaderCh <- i
					return
				}
			}
		}
	}()
	select {
	case <-time.After(timeout):
		t.Fatal("get leader timed out")
	case leader := <-leaderCh:
		return leader
	}
	return 0
}

func assertClusterNodes(t testing.TB, server *dbadger.DB, expected []string) {
	nodes := server.Nodes()
	for _, addr := range expected {
		present := false
		for _, nodeAddr := range nodes {
			if addr == string(nodeAddr) {
				present = true
				break
			}
		}
		if !present {
			t.Errorf(`node "%s" is not present in cluster`, addr)
		}
	}
}

func stopCluster(t testing.TB, cluster []*dbadger.DB) {
	for _, server := range cluster {
		err := server.Stop()
		assert.NoError(t, err)
	}
}

type dummyerr struct{}

func (dummyerr) Error() string { return "" }

func retry(targets []error, timout time.Duration, f func() error) error {
	start := time.Now()
	backoff := 300 * time.Millisecond
	err := f()
	for err != nil {
		if err == nil || time.Since(start) > timout {
			return err
		}
		targetError := false
		for _, target := range targets {
			if errors.Is(err, target) {
				targetError = true
				break
			}
		}
		if !targetError {
			return err
		}
		time.Sleep(backoff)
		backoff += 300 * time.Millisecond
		err = f()
	}
	return err
}

var testLogger = newLogger()

func newLogger() *logrus.Logger {
	l := logrus.New()
	l.Formatter = &logrus.TextFormatter{ForceColors: true}
	l.Level = logrus.InfoLevel
	return l
}

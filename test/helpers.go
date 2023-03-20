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
	// cfg.WithLogger(testLogger.WithField("server", bind))

	dbCh := make(chan *dbadger.DB)
	go func() {
		db, err := dbadger.Start(cfg)
		for err != nil {
			fmt.Println(err)
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
	var serverNames []string
	for i := 1; i <= servers; i++ {
		serverNames = append(serverNames, fmt.Sprintf("127.0.0.1:%d", 7420+i))
	}

	var certs []TestCert
	if variant.TLS {
		certs = GenerateTestCertificates(serverNames)
	}

	var cluster []*dbadger.DB
	for i := 0; i < servers; i++ {
		bind := dbadger.Address(serverNames[i])
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
		for !server.IsReady() {
			time.Sleep(100 * time.Millisecond)
		}
		cluster = append(cluster, server)
	}
	return cluster
}

func stopCluster(t testing.TB, cluster []*dbadger.DB) {
	for _, server := range cluster {
		err := server.Stop()
		assert.NoError(t, err)
	}
}

type dummyerr struct{}

func (dummyerr) Error() string { return "" }

func retry(maxAttempts int, timeout time.Duration, f func() error) error {
	var err error = dummyerr{}
	attempts := 0
	backoff := 100 * time.Millisecond
	start := time.Now()
	for err != nil {
		err = f()
		if err == nil {
			return nil
		}
		if !errors.Is(err, dbadger.ErrUnavailable) &&
			!errors.Is(err, dbadger.ErrNoLeader) &&
			!errors.Is(err, dbadger.ErrNotFound) {
			return err
		}
		attempts++
		if attempts >= maxAttempts {
			return err
		}
		if time.Since(start) > timeout {
			return err
		}
		time.Sleep(backoff)
		backoff *= 2
	}
	return nil
}

var testLogger = newLogger()

func newLogger() *logrus.Logger {
	l := logrus.New()
	l.Formatter = &logrus.TextFormatter{ForceColors: true}
	l.Level = logrus.InfoLevel
	return l
}

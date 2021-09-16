package test

import (
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/w1ck3dg0ph3r/dbadger"
)

type TestingT interface {
	Fatal(...any)
	Errorf(string, ...any)
}

func createServer(t TestingT, bind dbadger.Address, bootstrap bool, join dbadger.Address) *dbadger.DB {
	cfg := &dbadger.Config{
		Bind:      bind,
		InMemory:  true,
		Bootstrap: bootstrap,
		Join:      join,
		// Logger:    testLogger.WithField("server", bind),
	}

	dbCh := make(chan struct{})
	var db *dbadger.DB
	var err error
	go func() {
		db, err = dbadger.Start(cfg)
		for err != nil {
			time.Sleep(300 * time.Millisecond)
			db, err = dbadger.Start(cfg)
		}
		dbCh <- struct{}{}
	}()

	select {
	case <-dbCh:
		return db
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal(err)
		return nil
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

func createCluster(t TestingT, servers int) []*dbadger.DB {
	var cluster []*dbadger.DB
	for i := 1; i <= servers; i++ {
		bind := dbadger.Address(fmt.Sprintf("127.0.0.1:%d", 7420+i))
		bootstrap := i == 1
		join := dbadger.Address("")
		if i > 1 {
			join = cluster[0].Addr()
		}
		server := createServer(t, bind, bootstrap, join)
		for !server.IsReady() {
			time.Sleep(100 * time.Millisecond)
		}
		cluster = append(cluster, server)
	}
	return cluster
}

func stopCluster(t TestingT, cluster []*dbadger.DB) {
	for _, server := range cluster {
		err := server.Stop()
		assert.NoError(t, err)
	}
}

var testLogger = newLogger()

func newLogger() *logrus.Logger {
	l := logrus.New()
	l.Formatter = &logrus.TextFormatter{ForceColors: true}
	l.Level = logrus.InfoLevel
	return l
}

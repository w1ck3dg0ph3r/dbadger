package dbadger

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
)

// Config is DBadger node config.
type Config struct {
	// The path of the directory where data will be stored in.
	// If directory does not exists it will be created.
	Path string

	// Network address for the node to listen on (for example, "127.0.0.1:7001", "[::1]:7001", ":7001").
	Bind Address

	// TLS configuration.
	TLS TLSConfig

	// Bootstrap a new cluster from this node.
	//
	// A cluster should only be bootstrapped once from a single primary node.
	// After that other nodes can join the cluster.
	Bootstrap bool

	// Recover cluster from a loss of quorum (e.g. when multiple nodes die at the same time)
	// by forcing a new cluster configuration. This works by reading all the current state
	// for this node, creating a snapshot with the new configuration, and then truncating the log.
	//
	// Typicaly to bring the cluster back up you should choose a node to become a new leader, recover that node
	// and then join new clean-sate nodes as usual.
	Recover bool

	// Join an existing cluster via given node.
	//
	// It is safe to join a cluster via any node. If the target node is not a leader of the cluster
	// it will forward the request to the cluster's leader.
	Join Address

	// Run in InMemory mode, which means everything is stored in memory and no files are created.
	//
	// All data will be lost when node is stopped or crashed. Used primarily for testing purposes.
	InMemory bool

	// Logger configures wich logger DB uses.
	//
	// Leaving this nil will disable logging.
	Logger Logger
}

// DefaultConfig return default [Config].
func DefaultConfig(path string, bind Address) *Config {
	return &Config{
		Path: path,
		Bind: bind,
	}
}

// DefaultConfigInMemory returns default [Config] with InMemory mode turned on.
func DefaultConfigInMemory(bind Address) *Config {
	return &Config{
		Bind:     bind,
		InMemory: true,
	}
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

// WithLogger returns [Config] with Logger set to the given value.
func (c *Config) WithLogger(logger Logger) *Config {
	c.Logger = logger
	return c
}

func (c *Config) validate() error {
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

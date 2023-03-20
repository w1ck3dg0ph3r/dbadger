package dbadger

import (
	"fmt"
	"net"
)

// Config is DBadger node config.
type Config struct {
	Path string // Data directory path

	Bind Address // Bind address

	TLS TLSConfig // TLS configuration

	Bootstrap bool    // Bootstrap new node
	Recover   bool    // Recover node
	Join      Address // Join cluster

	InMemory bool

	Logger Logger
}

type TLSConfig struct {
	CA   []byte // PEM encoded TLS certificate authority
	Cert []byte // PEM encoded TLS certificate
	Key  []byte // PEM encoded TLS private key

	CAFile   string // TLS certificate authority file path
	CertFile string // TLS certificate file path
	KeyFile  string // TLS private key file path
}

func DefaultConfig(path string, bind Address) *Config {
	return &Config{
		Path: path,
		Bind: bind,
	}
}

func DefaultConfigInMemory(bind Address) *Config {
	return &Config{
		Bind:     bind,
		InMemory: true,
	}
}

func (c *Config) WithInMemory(inmem bool) *Config {
	c.InMemory = inmem
	return c
}

func (c *Config) WithTLS(ca, cert, key []byte) *Config {
	c.TLS = TLSConfig{
		CA:   ca,
		Cert: cert,
		Key:  key,
	}
	return c
}

func (c *Config) WithTLSFiles(caFile, certFile, keyFile string) *Config {
	c.TLS = TLSConfig{
		CAFile:   caFile,
		CertFile: certFile,
		KeyFile:  keyFile,
	}
	return c
}

func (c *Config) WithLogger(logger Logger) *Config {
	c.Logger = logger
	return c
}

func (c *Config) WithBootstrap(bootstrap bool) *Config {
	c.Bootstrap = bootstrap
	return c
}

func (c *Config) WithRecover(recover bool) *Config {
	c.Recover = recover
	return c
}

func (c *Config) WithJoin(join Address) *Config {
	c.Join = join
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

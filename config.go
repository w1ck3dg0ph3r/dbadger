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
	Path string // Data directory path

	Bind Address // Bind address

	TLS TLSConfig // TLS configuration

	Bootstrap bool    // Bootstrap new node
	Recover   bool    // Recover node
	Join      Address // Join cluster

	InMemory bool

	Logger Logger
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

// TLSConfig is a TLS configuration.
//
// PEM encoded options are prioritized over file paths. You can mix
// both file and PEM options in the same config.
type TLSConfig struct {
	CA   []byte // PEM encoded TLS certificate authority
	Cert []byte // PEM encoded TLS certificate
	Key  []byte // PEM encoded TLS private key

	CAFile   string // TLS certificate authority file path
	CertFile string // TLS certificate file path
	KeyFile  string // TLS private key file path
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

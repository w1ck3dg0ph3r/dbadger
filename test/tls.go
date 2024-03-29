package test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func GenerateTestTLSConfigs(serverNames []string) []*tls.Config {
	const bitLength = 2048
	const expiration = 365 * 24 * time.Hour

	// Generate CA key
	capk, err := rsa.GenerateKey(rand.Reader, bitLength)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	// Create CA certificate
	cacerttpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "ca.test",
		},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(expiration),
	}
	cacertBytes, err := x509.CreateCertificate(rand.Reader, cacerttpl, cacerttpl, &capk.PublicKey, capk)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	cacert, err := x509.ParseCertificate(cacertBytes)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	ca := x509.NewCertPool()
	ca.AddCert(cacert)

	// Create server certificates
	var tlsConfigs []*tls.Config
	for i, serverName := range serverNames {
		pk, _ := rsa.GenerateKey(rand.Reader, bitLength)
		pkpem := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(pk),
		})
		certtpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i)),
			Subject: pkix.Name{
				CommonName: serverName,
			},
			DNSNames:    []string{serverName},
			IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
			KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			NotAfter:    time.Now().Add(expiration),
		}
		certBytes, err := x509.CreateCertificate(rand.Reader, certtpl, cacert, &pk.PublicKey, capk)
		if err != nil {
			fmt.Println(err)
		}
		certpem := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certBytes,
		})
		cert, err := tls.X509KeyPair(certpem, pkpem)
		if err != nil {
			fmt.Println(err)
			return nil
		}

		tlsConfigs = append(tlsConfigs, &tls.Config{
			ServerName:         serverName,
			RootCAs:            ca,
			ClientCAs:          ca,
			Certificates:       []tls.Certificate{cert},
			ClientAuth:         tls.RequireAndVerifyClientCert,
			InsecureSkipVerify: true,
		})
	}

	return tlsConfigs
}

type TestCert struct {
	CA   []byte
	Cert []byte
	Key  []byte
}

func GenerateTestCertificates(serverNames []string) (certs []TestCert) {
	const bitLength = 2048
	const expiration = 365 * 24 * time.Hour

	// Generate CA key
	capk, _ := rsa.GenerateKey(rand.Reader, bitLength)

	// Create CA certificate
	cacerttpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "ca.test",
		},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(expiration),
	}
	cacert, err := x509.CreateCertificate(rand.Reader, cacerttpl, cacerttpl, &capk.PublicKey, capk)
	if err != nil {
		fmt.Println(err)
		return
	}
	cacertpem := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cacert,
	})

	parent, err := x509.ParseCertificate(cacert)
	if err != nil {
		fmt.Println(err)
	}
	root := x509.NewCertPool()
	root.AddCert(parent)

	for i, serverName := range serverNames {
		pk, _ := rsa.GenerateKey(rand.Reader, bitLength)
		pkpem := pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(pk),
		})

		certtpl := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i)),
			Subject: pkix.Name{
				CommonName: serverName,
			},
			DNSNames:    []string{serverName},
			KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageDataEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			NotAfter:    time.Now().Add(expiration),
		}

		host, _, _ := net.SplitHostPort(serverName)
		serverIP := net.ParseIP(host)
		if serverIP != nil {
			certtpl.IPAddresses = []net.IP{serverIP}
		}

		cert, err := x509.CreateCertificate(rand.Reader, certtpl, parent, &pk.PublicKey, capk)
		if err != nil {
			fmt.Println(err)
			return
		}
		certpem := pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert,
		})
		certs = append(certs, TestCert{
			CA:   cacertpem,
			Cert: certpem,
			Key:  pkpem,
		})

		child, err := x509.ParseCertificate(cert)
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = child.Verify(x509.VerifyOptions{
			Roots: root,
		})
		if err != nil {
			fmt.Println(err)
		}
	}
	return
}

func GenerateTestCertificateFiles(destDir string, serverNames []string) {
	certs := GenerateTestCertificates(serverNames)
	if len(certs) != len(serverNames) {
		return
	}
	_ = os.MkdirAll(destDir, fs.ModeDir|fs.ModePerm)
	_ = os.WriteFile(filepath.Join(destDir, "ca.crt"), certs[0].CA, 0o600)
	for i := range certs {
		_ = os.WriteFile(filepath.Join(destDir, fmt.Sprintf("node%d.crt", i+1)), certs[i].Cert, 0o600)
		_ = os.WriteFile(filepath.Join(destDir, fmt.Sprintf("node%d.pem", i+1)), certs[i].Key, 0o600)
	}
}

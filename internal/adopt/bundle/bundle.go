/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package bundle recovers a cluster's secrets bundle from a live control-plane config and proves
// it matches every node (docs/scope.md §3.5, §7.1 stage 1). Errors name nodes and fields only,
// never secret values.
package bundle

import (
	"crypto"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
)

var (
	// ErrNotControlPlane means the config cannot carry the full bundle: worker configs hold no CA keys.
	ErrNotControlPlane = errors.New("secrets can only be recovered from a control-plane config")
	// ErrIncomplete means the control-plane config lacks parts of the bundle.
	ErrIncomplete = errors.New("control-plane config does not contain a complete secrets bundle")
	// ErrAESCBC means the cluster still uses an AESCBC encryption secret. Talos 1.14's generator
	// renders it as a secretbox provider, so regenerating config would break decryption of
	// existing Kubernetes Secrets (machinery config/generate/kubernetes.go:65-77).
	ErrAESCBC = errors.New("cluster uses an AESCBC encryption secret, which config generation cannot reproduce")
)

// Recover rebuilds the secrets bundle from a live control-plane config.
//
// What a bundle cannot represent (acceptedCAs from an unfinished rotation, extra etcd encryption
// keys) stays in the live config and surfaces as patches during patch derivation.
func Recover(cfg config.Provider, contract *config.VersionContract) (*secrets.Bundle, error) {
	if cfg.Machine() == nil || cfg.Machine().Type() != machine.TypeControlPlane {
		return nil, ErrNotControlPlane
	}

	b, err := secrets.NewBundleFromConfig(secrets.NewClock(), cfg)
	if err != nil {
		return nil, fmt.Errorf("recover bundle: %w", err)
	}

	if b.Secrets.AESCBCEncryptionSecret != "" {
		return nil, ErrAESCBC
	}

	if missing := missingParts(b); len(missing) > 0 {
		return nil, fmt.Errorf("%w: missing %s", ErrIncomplete, strings.Join(missing, ", "))
	}

	if err := b.Validate(contract); err != nil {
		return nil, fmt.Errorf("recovered bundle is invalid: %w", err)
	}

	return b, nil
}

// authority is one of the certificate authorities a bundle holds.
type authority struct {
	// name is how status.certificates lists the authority, field how a mismatch names it.
	name, field string
	// pem returns its certificate and its key as the bundle has them: nothing of what it lacks.
	pem func(*secrets.Certs) (crt, key []byte)
}

// authorities are the authorities of a bundle, in the order a bundle that lacks some names them.
// Everything that goes through a bundle's authorities goes through this list.
var authorities = []authority{
	{"os", fieldOSCA, func(c *secrets.Certs) ([]byte, []byte) {
		if c.OS == nil {
			return nil, nil
		}

		return c.OS.Crt, c.OS.Key
	}},
	{"etcd", fieldEtcdCA, func(c *secrets.Certs) ([]byte, []byte) {
		if c.Etcd == nil {
			return nil, nil
		}

		return c.Etcd.Crt, c.Etcd.Key
	}},
	{"kubernetes", fieldK8sCA, func(c *secrets.Certs) ([]byte, []byte) {
		if c.K8s == nil {
			return nil, nil
		}

		return c.K8s.Crt, c.K8s.Key
	}},
	{"kubernetes-aggregator", fieldAggregatorCA, func(c *secrets.Certs) ([]byte, []byte) {
		if c.K8sAggregator == nil {
			return nil, nil
		}

		return c.K8sAggregator.Crt, c.K8sAggregator.Key
	}},
}

// AuthorityCertificates returns the certificate of each certificate authority of the bundle, by
// the name status.certificates lists it under: os, kubernetes, kubernetes-aggregator and etcd. One
// the bundle does not have, or that is not a certificate, is left out. Only the certificates are
// read, never their keys.
func AuthorityCertificates(b *secrets.Bundle) map[string]*x509.Certificate {
	out := map[string]*x509.Certificate{}
	if b == nil || b.Certs == nil {
		return out
	}

	for _, ca := range authorities {
		crt, _ := ca.pem(b.Certs)

		if cert, err := parseCertificate(crt); err == nil {
			out[ca.name] = cert
		}
	}

	return out
}

func missingParts(b *secrets.Bundle) []string {
	var missing []string

	check := func(name string, present bool) {
		if !present {
			missing = append(missing, name)
		}
	}

	certs := b.Certs

	for _, ca := range authorities {
		crt, key := ca.pem(certs)
		check(ca.field+" key", len(crt) > 0 && len(key) > 0)
	}

	check("service account key", certs.K8sServiceAccount != nil && len(certs.K8sServiceAccount.Key) > 0)
	check("cluster ID", b.Cluster.ID != "")
	check("cluster secret", b.Cluster.Secret != "")
	check("trustd token", b.TrustdInfo.Token != "")
	check("bootstrap token", b.Secrets.BootstrapToken != "" && b.Secrets.BootstrapToken != ".")
	check("secretbox encryption secret", b.Secrets.SecretboxEncryptionSecret != "")

	return missing
}

// Node is one node's live config, named for error messages.
type Node struct {
	Name   string
	Config config.Provider
}

// MismatchError lists every node field that differs from the bundle.
type MismatchError struct {
	Mismatches []string
}

func (e *MismatchError) Error() string {
	return "secrets bundle does not match the live cluster: " + strings.Join(e.Mismatches, "; ")
}

// Verify checks that every node's config carries the bundle's secrets: all of them on control
// planes, and the CA certificates and tokens on workers. Certificates and keys are compared by
// public key, so re-encoded PEM still matches.
func Verify(b *secrets.Bundle, nodes []Node) error {
	want, err := fingerprints(b)
	if err != nil {
		return fmt.Errorf("fingerprint bundle: %w", err)
	}

	var mismatches []string

	for _, n := range nodes {
		got, err := nodeFingerprints(n.Config)
		if err != nil {
			mismatches = append(mismatches, fmt.Sprintf("%s: %v", n.Name, err))

			continue
		}

		for _, field := range compareFields(n.Config.Machine().Type()) {
			if field.worker && got.isWorker {
				if !slices.Contains(got.k8sAccepted, want.values[field.name]) {
					mismatches = append(mismatches, fmt.Sprintf("%s: %s", n.Name, field.name))
				}

				continue
			}

			if got.values[field.name] != want.values[field.name] {
				mismatches = append(mismatches, fmt.Sprintf("%s: %s", n.Name, field.name))
			}
		}
	}

	if len(mismatches) > 0 {
		return &MismatchError{Mismatches: mismatches}
	}

	return nil
}

// Field names used in fingerprints and mismatch messages.
const (
	fieldOSCA           = "Talos API CA"
	fieldEtcdCA         = "etcd CA"
	fieldK8sCA          = "Kubernetes CA"
	fieldAggregatorCA   = "aggregator CA"
	fieldServiceAccount = "service account key"
	fieldClusterID      = "cluster ID"
	fieldClusterSecret  = "cluster secret"
	fieldTrustdToken    = "trustd token"
	fieldBootstrapToken = "bootstrap token"
	fieldSecretbox      = "secretbox encryption secret"
)

type compareField struct {
	name string
	// worker: on workers the value is checked against the Kubernetes accepted CAs instead.
	worker bool
}

// Fields every node carries, and the ones only control planes carry.
var (
	commonFields = []compareField{
		{name: fieldOSCA}, {name: fieldClusterID}, {name: fieldClusterSecret},
		{name: fieldTrustdToken}, {name: fieldBootstrapToken}, {name: fieldK8sCA, worker: true},
	}
	controlPlaneFields = []compareField{
		{name: fieldEtcdCA}, {name: fieldAggregatorCA}, {name: fieldServiceAccount}, {name: fieldSecretbox},
	}
)

func compareFields(t machine.Type) []compareField {
	if t != machine.TypeControlPlane {
		return commonFields
	}

	return slices.Concat(commonFields, controlPlaneFields)
}

type fingerprintSet struct {
	values      map[string]string
	k8sAccepted []string
	isWorker    bool
}

func fingerprints(b *secrets.Bundle) (fingerprintSet, error) {
	set := fingerprintSet{values: map[string]string{
		fieldClusterID:      hash(b.Cluster.ID),
		fieldClusterSecret:  hash(b.Cluster.Secret),
		fieldTrustdToken:    hash(b.TrustdInfo.Token),
		fieldBootstrapToken: hash(b.Secrets.BootstrapToken),
		fieldSecretbox:      hash(b.Secrets.SecretboxEncryptionSecret),
	}}

	for _, ca := range authorities {
		crt, _ := ca.pem(b.Certs)

		fp, err := certFingerprint(crt)
		if err != nil {
			return set, fmt.Errorf("%s: %w", ca.field, err)
		}

		set.values[ca.field] = fp
	}

	fp, err := keyFingerprint(b.Certs.K8sServiceAccount.Key)
	if err != nil {
		return set, fmt.Errorf("%s: %w", fieldServiceAccount, err)
	}

	set.values[fieldServiceAccount] = fp

	return set, nil
}

func nodeFingerprints(cfg config.Provider) (fingerprintSet, error) {
	set := fingerprintSet{values: map[string]string{}, isWorker: cfg.Machine().Type() != machine.TypeControlPlane}

	if ca := cfg.Machine().Security().IssuingCA(); ca != nil {
		if fp, err := certFingerprint(ca.Crt); err == nil {
			set.values[fieldOSCA] = fp
		}
	}

	set.values[fieldTrustdToken] = hash(cfg.Machine().Security().Token())
	set.values[fieldBootstrapToken] = hash(cfg.Cluster().Token().ID() + "." + cfg.Cluster().Token().Secret())

	if identity := cfg.DiscoveryIdentityConfig(); identity != nil {
		set.values[fieldClusterID] = hash(identity.ClusterID())
		set.values[fieldClusterSecret] = hash(identity.ClusterSecret())
	}

	if ca := cfg.K8sAPIServerCAConfig(); ca != nil {
		if issuing := ca.IssuingCA(); issuing != nil {
			if fp, err := certFingerprint(issuing.Crt); err == nil {
				set.values[fieldK8sCA] = fp
			}
		}

		for _, accepted := range ca.AcceptedCAs() {
			if fp, err := certFingerprint(accepted.Crt); err == nil {
				set.k8sAccepted = append(set.k8sAccepted, fp)
			}
		}
	}

	if set.isWorker {
		return set, nil
	}

	// Control planes carry every secret; reuse machinery's own extraction for the rest.
	nb, err := secrets.NewBundleFromConfig(secrets.NewClock(), cfg)
	if err != nil {
		return set, errors.New("secrets not extractable")
	}

	if nb.Certs.Etcd != nil {
		set.values[fieldEtcdCA], _ = certFingerprint(nb.Certs.Etcd.Crt) //nolint:errcheck // empty means mismatch
	}

	if nb.Certs.K8sAggregator != nil {
		set.values[fieldAggregatorCA], _ = certFingerprint(nb.Certs.K8sAggregator.Crt) //nolint:errcheck // empty means mismatch
	}

	if nb.Certs.K8sServiceAccount != nil {
		set.values[fieldServiceAccount], _ = keyFingerprint(nb.Certs.K8sServiceAccount.Key) //nolint:errcheck // empty means mismatch
	}

	set.values[fieldSecretbox] = hash(nb.Secrets.SecretboxEncryptionSecret)

	return set, nil
}

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))

	return hex.EncodeToString(sum[:])
}

// parseCertificate parses a PEM certificate. Its errors say what the input is not, and never
// quote it.
func parseCertificate(pemBytes []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("not PEM")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.New("not a certificate")
	}

	return cert, nil
}

// certFingerprint is the SHA-256 of a PEM certificate's public key.
func certFingerprint(pemBytes []byte) (string, error) {
	cert, err := parseCertificate(pemBytes)
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)

	return hex.EncodeToString(sum[:]), nil
}

// keyFingerprint is the SHA-256 of a PEM private key's public key.
func keyFingerprint(pemBytes []byte) (string, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return "", errors.New("not PEM")
	}

	var (
		key any
		err error
	)

	switch block.Type {
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	default:
		key, err = x509.ParsePKCS8PrivateKey(block.Bytes)
	}

	if err != nil {
		return "", errors.New("not a private key")
	}

	signer, ok := key.(crypto.Signer)
	if !ok {
		return "", errors.New("unsupported key type")
	}

	der, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return "", errors.New("unsupported public key")
	}

	sum := sha256.Sum256(der)

	return hex.EncodeToString(sum[:]), nil
}

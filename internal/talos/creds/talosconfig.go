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

// Package creds loads and validates Talos API client credentials.
// Credential material is only ever held in memory and never appears in errors or logs.
package creds

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/siderolabs/talos/pkg/machinery/role"
	corev1 "k8s.io/api/core/v1"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

// DefaultTalosconfigKey is the Secret key read when a credentials reference names no key.
const DefaultTalosconfigKey = talosv1alpha1.DefaultTalosconfigKey

// Error is a credentials failure carrying the condition reason to report.
// Messages never include certificate or key material.
type Error struct {
	Reason  string
	Message string
}

func (e *Error) Error() string { return e.Message }

func newError(reason, format string, args ...any) *Error {
	return &Error{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// Reason returns the condition reason for err, or ParseError for errors not produced by this package.
func Reason(err error) string {
	var credsErr *Error
	if errors.As(err, &credsErr) {
		return credsErr.Reason
	}

	return talosv1alpha1.ReasonParseError
}

// Endpoint is a Talos API endpoint. Endpoints are always IP addresses, never DNS names:
// cluster DNS may point at the very node being upgraded (docs/scope.md §4.2).
type Endpoint struct {
	Addr netip.Addr
	// Port is 0 when the default apid port applies.
	Port uint16
}

// String renders the endpoint in the form the Talos client accepts.
func (e Endpoint) String() string {
	if e.Port == 0 {
		return e.Addr.String()
	}

	return netip.AddrPortFrom(e.Addr, e.Port).String()
}

// ParseEndpoint parses "ip", "ip:port" or "[ipv6]:port" and rejects hostnames.
func ParseEndpoint(s string) (Endpoint, error) {
	if addr, err := netip.ParseAddr(s); err == nil {
		return Endpoint{Addr: addr}, nil
	}

	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return Endpoint{}, fmt.Errorf("endpoint %q is not an IP address", s)
	}

	addr, err := netip.ParseAddr(host)
	if err != nil {
		return Endpoint{}, fmt.Errorf("endpoint %q is not an IP address", s)
	}

	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil || port == 0 {
		return Endpoint{}, fmt.Errorf("endpoint %q has an invalid port", s)
	}

	return Endpoint{Addr: addr, Port: uint16(port)}, nil
}

// Credentials is a validated Talos API client identity.
type Credentials struct {
	// Context is the talosconfig context, ready for client.WithConfigContext.
	Context *clientconfig.Context
	// Endpoints are node IPs: from the talosconfig, or supplied for Talos-issued credentials.
	Endpoints []Endpoint
	// Roles carried in the client certificate's Organization field.
	Roles role.Set
	// NotAfter is the client certificate's expiry.
	NotAfter time.Time
}

// LoadFromSecret reads and validates a talosconfig stored in a Secret, whose endpoints must be
// node IPs. An empty key means DefaultTalosconfigKey.
func LoadFromSecret(secret *corev1.Secret, key string, now time.Time) (*Credentials, error) {
	if key == "" {
		key = DefaultTalosconfigKey
	}

	data, err := secretData(secret, key)
	if err != nil {
		return nil, err
	}

	return Load(data, string(secret.Data["context"]), now)
}

// LoadIssuedFromSecret reads a talosconfig Talos issued for a talos.dev ServiceAccount, from the
// Secret Talos created for it (key talosv1alpha1.TalosServiceAccountKey). Its endpoint is the
// in-cluster Service name, which the operator never dials (docs/scope.md §4.2): the caller
// supplies node IPs instead.
func LoadIssuedFromSecret(secret *corev1.Secret, endpoints []Endpoint, now time.Time) (*Credentials, error) {
	data, err := secretData(secret, talosv1alpha1.TalosServiceAccountKey)
	if err != nil {
		return nil, err
	}

	c, _, err := loadIdentity(data, "", now)
	if err != nil {
		return nil, err
	}

	c.Endpoints = append([]Endpoint(nil), endpoints...)

	return c, nil
}

func secretData(secret *corev1.Secret, key string) ([]byte, error) {
	data, ok := secret.Data[key]
	if !ok || len(data) == 0 {
		return nil, newError(talosv1alpha1.ReasonKeyMissing, "secret %s/%s has no %q key", secret.Namespace, secret.Name, key)
	}

	return data, nil
}

// Load parses a talosconfig and validates the selected context, including that its endpoints
// are IPs. An empty contextName selects the talosconfig's current context.
func Load(data []byte, contextName string, now time.Time) (*Credentials, error) {
	c, contextName, err := loadIdentity(data, contextName, now)
	if err != nil {
		return nil, err
	}

	if len(c.Context.Endpoints) == 0 {
		return nil, newError(talosv1alpha1.ReasonParseError, "talosconfig context %q has no endpoints", contextName)
	}

	for _, raw := range c.Context.Endpoints {
		ep, err := ParseEndpoint(raw)
		if err != nil {
			return nil, newError(talosv1alpha1.ReasonParseError, "talosconfig context %q: %v", contextName, err)
		}

		c.Endpoints = append(c.Endpoints, ep)
	}

	return c, nil
}

// loadIdentity validates the selected context's client certificate and returns the context's
// name. The operator requires os:admin because the machine config resource is sensitive
// (docs/scope.md §7.1).
func loadIdentity(data []byte, contextName string, now time.Time) (*Credentials, string, error) {
	cfg, err := clientconfig.FromBytes(data)
	if err != nil {
		// The parser error can quote input, so it is not wrapped.
		return nil, "", newError(talosv1alpha1.ReasonParseError, "talosconfig is not valid YAML")
	}

	if contextName == "" {
		contextName = cfg.Context
	}

	ctx, ok := cfg.Contexts[contextName]
	if !ok || ctx == nil {
		return nil, "", newError(talosv1alpha1.ReasonParseError, "talosconfig has no context %q", contextName)
	}

	if ctx.Auth.Basic != nil || ctx.Auth.SideroV1 != nil {
		return nil, "", newError(talosv1alpha1.ReasonParseError,
			"talosconfig context %q uses an unsupported auth method; a client certificate is required", contextName)
	}

	cert, err := clientCertificate(ctx)
	if err != nil {
		return nil, "", newError(talosv1alpha1.ReasonParseError, "talosconfig context %q: %v", contextName, err)
	}

	if !now.Before(cert.NotAfter) {
		return nil, "", newError(talosv1alpha1.ReasonExpired,
			"talosconfig context %q client certificate expired at %s", contextName, cert.NotAfter.UTC().Format(time.RFC3339))
	}

	// Roles unknown to this machinery version are kept (forward compatibility); only admin matters.
	roles, _ := role.Parse(cert.Subject.Organization)

	if !roles.IncludesAny(role.MakeSet(role.Admin)) {
		return nil, "", newError(talosv1alpha1.ReasonInsufficientRole,
			"talosconfig context %q has roles %v; %s is required", contextName, roles.Strings(), role.Admin)
	}

	return &Credentials{
		Context:  ctx,
		Roles:    roles,
		NotAfter: cert.NotAfter,
	}, contextName, nil
}

// clientCertificate decodes the context's client certificate without exposing its bytes in errors.
func clientCertificate(ctx *clientconfig.Context) (*x509.Certificate, error) {
	if ctx.CA == "" || ctx.Crt == "" || ctx.Key == "" {
		return nil, errors.New("ca, crt and key are all required")
	}

	crtPEM, err := base64.StdEncoding.DecodeString(ctx.Crt)
	if err != nil {
		return nil, errors.New("crt is not valid base64")
	}

	block, _ := pem.Decode(crtPEM)
	if block == nil || !strings.HasSuffix(block.Type, "CERTIFICATE") {
		return nil, errors.New("crt is not a PEM certificate")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errors.New("crt is not a valid certificate")
	}

	return cert, nil
}

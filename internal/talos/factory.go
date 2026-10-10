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

package talos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/client"
	"google.golang.org/grpc"

	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

// DefaultCallTimeout bounds every Talos API call whose context has no deadline (docs/scope.md §8.2).
const DefaultCallTimeout = 30 * time.Second

// Target identifies one cluster's Talos API: who to authenticate as and which endpoints to use.
type Target struct {
	// Key identifies the cluster, e.g. the TalosCluster UID. Clients are cached per key.
	Key string
	// Credentials authenticate the client.
	Credentials *creds.Credentials
	// Endpoints are already selected and ordered (see SelectEndpoints).
	Endpoints []creds.Endpoint
}

// idleClientTTL is how long a cached client outlives its last use. It is far longer than any
// reconcile holds on to one, so a client is never closed under a caller.
const idleClientTTL = 10 * time.Minute

// Factory builds and caches Talos clients, one per cluster, access level, identity and endpoint
// list. Controllers ask for the same cluster concurrently, and during a rollout with different
// endpoints from one moment to the next (the node being changed is never an endpoint), so a
// client is not closed when another is asked for: it is closed once it has not been used for
// idleClientTTL, which is also how the client of a rotated certificate goes away.
type Factory struct {
	// CallTimeout overrides DefaultCallTimeout when non-zero.
	CallTimeout time.Duration

	mu      sync.Mutex
	clients map[clientKey]*cachedClient
	// now is the clock; tests replace it.
	now func() time.Time
}

// clientKey identifies one cached connection.
type clientKey struct {
	// key is the cluster, access what the connection may do.
	key    string
	access access
	// fingerprint covers the identity and the endpoints.
	fingerprint string
}

type cachedClient struct {
	client   *client.Client
	lastUsed time.Time
}

// NewFactory returns an empty Factory.
func NewFactory() *Factory {
	return &Factory{clients: map[clientKey]*cachedClient{}, now: time.Now}
}

// Reader returns a read-only client for the target.
func (f *Factory) Reader(ctx context.Context, t Target) (Reader, error) {
	c, err := f.client(ctx, t, accessReadOnly)
	if err != nil {
		return nil, err
	}

	return &machineryReader{client: c, timeout: f.callTimeout()}, nil
}

// ConfigWriter returns a client that can also apply machine config. Callers hand it out only
// while spec.management.config is Manage (docs/scope.md §8.1, invariant 6). Its connection is
// separate from the Reader's and allows nothing else that mutates a node.
func (f *Factory) ConfigWriter(ctx context.Context, t Target) (ConfigWriter, error) {
	c, err := f.client(ctx, t, accessConfig)
	if err != nil {
		return nil, err
	}

	return &machineryWriter{machineryReader{client: c, timeout: f.callTimeout()}}, nil
}

// Upgrader returns a client that can also upgrade and reboot nodes. Callers hand it out only
// while spec.management.upgrades is Enabled (docs/scope.md §8.1, invariant 6). Its connection is
// separate from the others and cannot apply config.
func (f *Factory) Upgrader(ctx context.Context, t Target) (Upgrader, error) {
	c, err := f.client(ctx, t, accessUpgrade)
	if err != nil {
		return nil, err
	}

	return &machineryUpgrader{machineryReader{client: c, timeout: f.callTimeout()}}, nil
}

func (f *Factory) callTimeout() time.Duration {
	if f.CallTimeout != 0 {
		return f.CallTimeout
	}

	return DefaultCallTimeout
}

// Invalidate closes and forgets every cached client of the cluster key.
func (f *Factory) Invalidate(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for k, cached := range f.clients {
		if k.key == key {
			_ = cached.client.Close() //nolint:errcheck // best effort on a client being discarded
			delete(f.clients, k)
		}
	}
}

// Close closes all cached clients.
func (f *Factory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	for key, cached := range f.clients {
		_ = cached.client.Close() //nolint:errcheck // best effort on shutdown
		delete(f.clients, key)
	}
}

func (f *Factory) client(ctx context.Context, t Target, a access) (*client.Client, error) {
	if t.Key == "" {
		return nil, errors.New("target key is required")
	}

	if t.Credentials == nil || t.Credentials.Context == nil {
		return nil, errors.New("target credentials are required")
	}

	if len(t.Endpoints) == 0 {
		return nil, ErrNoEndpoints
	}

	key := clientKey{key: t.Key, access: a, fingerprint: targetFingerprint(t)}

	f.mu.Lock()
	defer f.mu.Unlock()

	now := f.now()

	for k, cached := range f.clients {
		if k != key && now.Sub(cached.lastUsed) >= idleClientTTL {
			_ = cached.client.Close() //nolint:errcheck // unused: superseded credentials or endpoints
			delete(f.clients, k)
		}
	}

	if cached, ok := f.clients[key]; ok {
		cached.lastUsed = now

		return cached.client, nil
	}

	endpoints := make([]string, 0, len(t.Endpoints))
	for _, ep := range t.Endpoints {
		endpoints = append(endpoints, ep.String())
	}

	// Copy the context so the client never shares mutable state with the caller.
	configContext := *t.Credentials.Context
	configContext.Endpoints = endpoints
	configContext.Nodes = nil

	c, err := client.New(ctx,
		client.WithConfigContext(&configContext),
		client.WithEndpoints(endpoints...),
		client.WithGRPCDialOptions(
			grpc.WithChainUnaryInterceptor(a.unaryInterceptor()),
			grpc.WithChainStreamInterceptor(a.streamInterceptor()),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create Talos client: %w", err)
	}

	f.clients[key] = &cachedClient{client: c, lastUsed: now}

	return c, nil
}

// targetFingerprint changes whenever the identity or endpoints of a target change.
func targetFingerprint(t Target) string {
	h := sha256.New()

	writeField := func(s string) {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}

	writeField(t.Credentials.Context.CA)
	writeField(t.Credentials.Context.Crt)
	writeField(t.Credentials.Context.Key)

	parts := make([]string, 0, len(t.Endpoints))
	for _, ep := range t.Endpoints {
		parts = append(parts, ep.String())
	}

	writeField(strings.Join(parts, ","))

	return hex.EncodeToString(h.Sum(nil))
}

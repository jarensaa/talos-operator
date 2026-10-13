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

// Package target resolves a TalosCluster into the clients that reach it: Talos clients built from
// its credentials (issued by Talos, or a supplied Secret), and a Kubernetes client for the
// managed cluster. The read-only client is there for the asking. Each client that can change a
// node is handed out only while spec.management enables what it does, so that nothing which only
// observes can come by one (docs/scope.md §8.1, invariant 6).
//
// The operator is self-hosted first, so the managed cluster's Kubernetes API is the manager's
// own; a management-cluster mode would swap that client (docs/scope.md §4.2).
package target

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

// SecretRefIndex indexes TalosClusters by the names of the Secrets they use: the credentials
// (supplied, or issued by Talos) and the secrets bundle.
const SecretRefIndex = "spec.secretRefs"

// ReaderFactory builds read-only Talos clients; *talos.Factory implements it.
type ReaderFactory interface {
	Reader(ctx context.Context, t talos.Target) (talos.Reader, error)
}

// ClientFactory also builds the clients that can change a node, one for each kind of connection
// (applying config, upgrading, removing, upkeep, and the maintenance connection to a machine
// without a config); *talos.Factory implements it.
type ClientFactory interface {
	ReaderFactory
	ConfigWriter(ctx context.Context, t talos.Target) (talos.ConfigWriter, error)
	Upgrader(ctx context.Context, t talos.Target) (talos.Upgrader, error)
	Remover(ctx context.Context, t talos.Target) (talos.Remover, error)
	Keeper(ctx context.Context, t talos.Target) (talos.Keeper, error)
	Maintenance(ctx context.Context, addr netip.Addr) (talos.Maintenance, error)
}

// Target is everything needed to observe one cluster.
type Target struct {
	// Talos reads the cluster's Talos API.
	Talos talos.Reader
	// Kube reads the managed cluster's Kubernetes API.
	Kube client.Client
	// Credentials are the validated credentials in use (roles, expiry).
	Credentials *creds.Credentials
	// Endpoints are the Talos API endpoints in use, in preference order.
	Endpoints []creds.Endpoint
}

// Resolver turns TalosClusters into Targets.
type Resolver struct {
	// Client reads Secrets in the TalosCluster's namespace; it is also the managed cluster's
	// Kubernetes client in self-hosted mode.
	Client client.Client
	// Factory builds Talos clients. It caches them per cluster, access, credentials and endpoints:
	// a rotated Secret, or another choice of endpoints, takes effect on the next resolve with a
	// client of its own, and the one it supersedes is closed once nobody has asked for it for a
	// while (talos.Factory).
	Factory ClientFactory
	// Now is the clock used to check credential expiry; nil means time.Now.
	Now func() time.Time
}

// Resolve loads tc's credentials and returns its Target. Credential problems are returned as
// *creds.Error so the caller can report the reason in the CredentialsValid condition.
func (r *Resolver) Resolve(ctx context.Context, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions) (*Target, error) {
	return connect(ctx, r, tc, opts, nil, func(ctx context.Context, t talos.Target) (*Target, error) {
		reader, err := r.Factory.Reader(ctx, t)
		if err != nil {
			return nil, err
		}

		return &Target{Talos: reader, Kube: r.Client, Credentials: t.Credentials, Endpoints: t.Endpoints}, nil
	})
}

// ErrKubernetesAPI marks a failure of the Kubernetes API the operator runs against, as opposed to
// anything about the Talos cluster or its credentials. Callers retry instead of reporting the
// cluster as unreachable.
var ErrKubernetesAPI = errors.New("reading the Kubernetes API")

// Why a client that can change a node is refused: spec.management does not enable what it does.
var (
	// ErrNotManaged is returned for a write client while config is not Manage.
	ErrNotManaged = errors.New("a config writer is only available while spec.management.config is Manage")
	// ErrUpgradesDisabled is returned for an upgrade client while upgrades are not enabled.
	ErrUpgradesDisabled = errors.New("an upgrader is only available while spec.management.upgrades is Enabled")
	// ErrMembershipDisabled is returned for the clients that add and remove machines while
	// membership is not enabled.
	ErrMembershipDisabled = errors.New("machines are only added and removed while spec.management.membership is Enabled")
	// ErrMaintenanceDisabled is returned for the client that keeps etcd while maintenance is not enabled.
	ErrMaintenanceDisabled = errors.New("etcd is only defragmented while spec.management.maintenance is Enabled")
)

// refusedUnless is the gate of a client that can change a node: nil while spec.management
// enables what the client does, and the reason it is refused otherwise.
func refusedUnless(enabled bool, refusal error) error {
	if enabled {
		return nil
	}

	return refusal
}

// connect is what every Resolve method does. A client that is refused is refused before anything
// is read or built. Otherwise tc's credentials are loaded, its endpoints selected, and the client
// built for them, under the cluster's UID as the key its connections are cached by.
func connect[T any](ctx context.Context, r *Resolver, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions, refused error,
	build func(context.Context, talos.Target) (T, error),
) (T, error) {
	var none T

	if refused != nil {
		return none, refused
	}

	c, endpoints, err := r.identity(ctx, tc, opts)
	if err != nil {
		return none, err
	}

	return build(ctx, talos.Target{Key: string(tc.UID), Credentials: c, Endpoints: endpoints})
}

// ResolveWriter returns a client that can apply machine config to tc's nodes. It refuses unless
// spec.management.config is Manage, so nothing that only observes can obtain one
// (docs/scope.md §8.1, invariant 6).
func (r *Resolver) ResolveWriter(ctx context.Context, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions,
) (talos.ConfigWriter, error) {
	return connect(ctx, r, tc, opts, refusedUnless(tc.Spec.Management.ManagesConfig(), ErrNotManaged), r.Factory.ConfigWriter)
}

// ResolveUpgrader returns a client that can upgrade and reboot tc's nodes. It refuses unless
// spec.management.upgrades is Enabled and config is Manage, so nothing else can obtain one
// (docs/scope.md §8.1, invariant 6).
func (r *Resolver) ResolveUpgrader(ctx context.Context, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions,
) (talos.Upgrader, error) {
	return connect(ctx, r, tc, opts, refusedUnless(tc.Spec.Management.MayUpgrade(), ErrUpgradesDisabled), r.Factory.Upgrader)
}

// ResolveRemover returns a client that can take a machine out of tc. It refuses unless
// spec.management.membership is Enabled and config is Manage, so nothing else can obtain one
// (docs/scope.md §8.1, invariant 6).
func (r *Resolver) ResolveRemover(ctx context.Context, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions,
) (talos.Remover, error) {
	return connect(ctx, r, tc, opts, refusedUnless(tc.Spec.Management.MayChangeMembership(), ErrMembershipDisabled), r.Factory.Remover)
}

// ResolveMaintenance returns the connection to a machine that waits in maintenance mode at addr,
// to be added to tc. It refuses unless spec.management.membership is Enabled and config is
// Manage. The connection carries none of the cluster's credentials; the caller closes it.
func (r *Resolver) ResolveMaintenance(ctx context.Context, tc *talosv1alpha1.TalosCluster, addr netip.Addr,
) (talos.Maintenance, error) {
	if err := refusedUnless(tc.Spec.Management.MayChangeMembership(), ErrMembershipDisabled); err != nil {
		return nil, err
	}

	return r.Factory.Maintenance(ctx, addr)
}

// ResolveKeeper returns a client that can defragment tc's etcd. It refuses unless
// spec.management.maintenance is Enabled and config is Manage, so nothing else can obtain one
// (docs/scope.md §8.1, invariant 6).
func (r *Resolver) ResolveKeeper(ctx context.Context, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions,
) (talos.Keeper, error) {
	return connect(ctx, r, tc, opts, refusedUnless(tc.Spec.Management.MayMaintain(), ErrMaintenanceDisabled), r.Factory.Keeper)
}

// identity loads tc's credentials and selects its endpoints.
func (r *Resolver) identity(ctx context.Context, tc *talosv1alpha1.TalosCluster, opts talos.SelectOptions,
) (*creds.Credentials, []creds.Endpoint, error) {
	ref := tc.CredentialsSecret()

	secret := &corev1.Secret{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: ref.Name}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, nil, fmt.Errorf("%w: read credentials secret: %w", ErrKubernetesAPI, err)
		}

		if tc.UsesTalosServiceAccount() {
			return nil, nil, &creds.Error{
				Reason:  talosv1alpha1.ReasonAwaitingCredentials,
				Message: fmt.Sprintf("Talos has not issued credentials for ServiceAccount %s/%s yet", tc.Namespace, tc.TalosServiceAccountName()),
			}
		}

		return nil, nil, &creds.Error{
			Reason:  talosv1alpha1.ReasonSecretNotFound,
			Message: fmt.Sprintf("credentials secret %s/%s not found", tc.Namespace, ref.Name),
		}
	}

	now := time.Now
	if r.Now != nil {
		now = r.Now
	}

	var (
		c   *creds.Credentials
		err error
	)

	if tc.UsesTalosServiceAccount() {
		nodes, nodesErr := ControlPlaneEndpoints(ctx, r.Client)
		if nodesErr != nil {
			return nil, nil, nodesErr
		}

		c, err = creds.LoadIssuedFromSecret(secret, nodes, now())
	} else {
		c, err = creds.LoadFromSecret(secret, ref.Key, now())
	}

	if err != nil {
		return nil, nil, err
	}

	endpoints, err := talos.SelectEndpoints(c.Endpoints, opts)
	if err != nil {
		return nil, nil, err
	}

	return c, endpoints, nil
}

// controlPlaneLabel marks control-plane Nodes; Talos sets it on every control plane.
const controlPlaneLabel = "node-role.kubernetes.io/control-plane"

// ControlPlaneEndpoints returns the InternalIP of every control-plane Node, sorted. Talos-issued
// talosconfigs name the in-cluster Service talos.default, which the operator never dials: an
// upgrade must reach a chosen node through a different, healthy one (docs/scope.md §4.2).
func ControlPlaneEndpoints(ctx context.Context, c client.Reader) ([]creds.Endpoint, error) {
	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes, client.HasLabels{controlPlaneLabel}); err != nil {
		return nil, fmt.Errorf("%w: list control-plane nodes: %w", ErrKubernetesAPI, err)
	}

	var endpoints []creds.Endpoint

	for _, n := range nodes.Items {
		for _, a := range n.Status.Addresses {
			if a.Type != corev1.NodeInternalIP {
				continue
			}

			if addr, err := netip.ParseAddr(a.Address); err == nil {
				endpoints = append(endpoints, creds.Endpoint{Addr: addr})

				break
			}
		}
	}

	if len(endpoints) == 0 {
		return nil, errors.New("no control-plane Node reports an InternalIP to reach the Talos API through")
	}

	slices.SortFunc(endpoints, func(a, b creds.Endpoint) int { return a.Addr.Compare(b.Addr) })

	return endpoints, nil
}

// IndexSecretRefs is the field indexer for SecretRefIndex.
func IndexSecretRefs(obj client.Object) []string {
	tc, ok := obj.(*talosv1alpha1.TalosCluster)
	if !ok {
		return nil
	}

	names := []string{tc.SecretsReference().Name}
	if n := tc.CredentialsSecret().Name; n != names[0] {
		names = append(names, n)
	}

	return names
}

// ClustersForSecret maps a Secret to the TalosClusters referencing it, so rotated credentials
// (Talos re-issues them every few hours) or an annotated secrets bundle trigger a reconcile. It
// requires the SecretRefIndex field index.
func ClustersForSecret(c client.Reader) func(context.Context, client.Object) []reconcile.Request {
	return func(ctx context.Context, obj client.Object) []reconcile.Request {
		var list talosv1alpha1.TalosClusterList
		if err := c.List(ctx, &list,
			client.InNamespace(obj.GetNamespace()),
			client.MatchingFields{SecretRefIndex: obj.GetName()},
		); err != nil {
			// The clusters using this Secret are not reconciled for the change; they pick it up on
			// their next periodic pass.
			logf.FromContext(ctx).Error(err, "watch mapping failed", "lookup", "clusters using the Secret",
				"namespace", obj.GetNamespace(), "name", obj.GetName())

			return nil
		}

		reqs := make([]reconcile.Request, 0, len(list.Items))
		for _, tc := range list.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&tc)})
		}

		return reqs
	}
}

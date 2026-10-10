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

package target_test

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/role"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
	"github.com/jarensaa/talos-operator/internal/target"
)

const ns = "talos-operator"

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	if err := talosv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	return s
}

// cluster returns a TalosCluster whose credentials come from the named talosconfig Secret.
func cluster(name, secret string) *talosv1alpha1.TalosCluster {
	return &talosv1alpha1.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(name + "-uid")},
		Spec: talosv1alpha1.TalosClusterSpec{Credentials: talosv1alpha1.Credentials{
			Source:    talosv1alpha1.CredentialsSourceSecret,
			SecretRef: &talosv1alpha1.SecretKeyReference{Name: secret},
		}},
	}
}

// issuedCluster returns the TalosCluster "happy", whose credentials Talos issues (the default).
func issuedCluster() *talosv1alpha1.TalosCluster {
	return &talosv1alpha1.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "happy", Namespace: ns, UID: types.UID("happy-uid")},
	}
}

// node returns a Kubernetes Node with an InternalIP, optionally labelled as a control plane.
func node(name, ip string, controlPlane bool) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{}},
		Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{
			{Type: corev1.NodeHostName, Address: name},
			{Type: corev1.NodeInternalIP, Address: ip},
		}},
	}

	if controlPlane {
		n.Labels["node-role.kubernetes.io/control-plane"] = ""
	}

	return n
}

func secret(name string, data []byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{creds.DefaultTalosconfigKey: data},
	}
}

func resolver(t *testing.T, objs ...client.Object) (*target.Resolver, *fake.Factory) {
	t.Helper()

	c := fakeclient.NewClientBuilder().
		WithScheme(scheme(t)).
		WithObjects(objs...).
		WithIndex(&talosv1alpha1.TalosCluster{}, target.SecretRefIndex, target.IndexSecretRefs).
		Build()

	f := &fake.Factory{Scenario: fake.NewScenario()}

	return &target.Resolver{Client: c, Factory: f}, f
}

func TestResolveBuildsTargetFromSecret(t *testing.T) {
	tc := cluster("happy", "talosconfig")
	r, f := resolver(t, tc, secret("talosconfig", talostest.AdminTalosconfig(t, "10.0.0.11", "10.0.0.12")))

	got, err := r.Resolve(context.Background(), tc, talos.SelectOptions{Exclude: netip.MustParseAddr("10.0.0.11")})
	if err != nil {
		t.Fatal(err)
	}

	if got.Talos == nil || got.Kube == nil {
		t.Fatal("target must carry both clients")
	}

	targets := f.Targets()
	if len(targets) != 1 || targets[0].Key != "happy-uid" {
		t.Fatalf("factory targets: %+v", targets)
	}

	// The excluded node is never used as an endpoint.
	if len(targets[0].Endpoints) != 1 || targets[0].Endpoints[0].Addr != netip.MustParseAddr("10.0.0.12") {
		t.Fatalf("endpoints: %+v", targets[0].Endpoints)
	}
}

func TestResolveReportsCredentialReasons(t *testing.T) {
	tests := []struct {
		name   string
		secret *corev1.Secret
		want   string
	}{
		{"missing secret", nil, talosv1alpha1.ReasonSecretNotFound},
		{"missing key", &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "talosconfig", Namespace: ns}}, talosv1alpha1.ReasonKeyMissing},
		{"reader role", secret("talosconfig", talostest.Talosconfig(t, role.MakeSet(role.Reader), time.Hour, "10.5.0.2")),
			talosv1alpha1.ReasonInsufficientRole},
		{"expired", secret("talosconfig", talostest.Talosconfig(t, role.MakeSet(role.Admin), time.Nanosecond, "10.5.0.2")),
			talosv1alpha1.ReasonExpired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := cluster("c", "talosconfig")
			objs := []client.Object{tc}

			if tt.secret != nil {
				objs = append(objs, tt.secret)
			}

			r, _ := resolver(t, objs...)
			r.Now = func() time.Time { return time.Now().Add(time.Minute) }

			_, err := r.Resolve(context.Background(), tc, talos.SelectOptions{})
			if got := creds.Reason(err); got != tt.want {
				t.Fatalf("reason: got %s, want %s (err: %v)", got, tt.want, err)
			}
		})
	}
}

func TestSecretChangesEnqueueReferencingClusters(t *testing.T) {
	a := cluster("a", "shared")
	b := cluster("b", "shared")
	other := cluster("other", "different")
	r, _ := resolver(t, a, b, other)

	reqs := target.ClustersForSecret(r.Client)(context.Background(), secret("shared", nil))
	if len(reqs) != 2 {
		t.Fatalf("expected both clusters referencing the secret, got %+v", reqs)
	}

	for _, req := range reqs {
		if req.Name == "other" {
			t.Fatal("a cluster referencing another secret must not be enqueued")
		}
	}
}

func TestClustersForSecretIncludesTheSecretsBundle(t *testing.T) {
	explicit := cluster("explicit", "talosconfig")
	explicit.Spec.SecretsRef = &talosv1alpha1.SecretKeyReference{Name: "bundle"}

	r, _ := resolver(t, explicit, cluster("defaulted", "talosconfig"))
	enqueue := target.ClustersForSecret(r.Client)

	for secretName, want := range map[string]string{"bundle": "explicit", "defaulted-talos-secrets": "defaulted"} {
		reqs := enqueue(context.Background(), secret(secretName, nil))
		if len(reqs) != 1 || reqs[0].Name != want {
			t.Errorf("secret %s: expected cluster %s, got %+v", secretName, want, reqs)
		}
	}
}

func TestResolveTalosIssuedCredentialsDialsControlPlaneIPs(t *testing.T) {
	tc := issuedCluster()
	issued := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: tc.TalosServiceAccountName(), Namespace: ns},
		Data:       map[string][]byte{talosv1alpha1.TalosServiceAccountKey: talostest.IssuedTalosconfig(t)},
	}

	r, f := resolver(t, tc, issued,
		node("cp-2", "10.0.0.12", true), node("cp-1", "10.0.0.11", true), node("worker", "10.0.0.20", false))

	got, err := r.Resolve(context.Background(), tc, talos.SelectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	want := []netip.Addr{netip.MustParseAddr("10.0.0.11"), netip.MustParseAddr("10.0.0.12")}

	eps := f.Targets()[0].Endpoints

	dialled := make([]netip.Addr, 0, len(eps))
	for _, ep := range eps {
		dialled = append(dialled, ep.Addr)
	}

	if !slices.Equal(dialled, want) {
		t.Fatalf("endpoints: got %v, want control-plane IPs %v (never talos.default or workers)", dialled, want)
	}

	if !got.Credentials.Roles.Includes(role.Admin) {
		t.Fatalf("roles: %v", got.Credentials.Roles.Strings())
	}
}

func TestResolveWaitsForTalosToIssueCredentials(t *testing.T) {
	tc := issuedCluster()
	r, _ := resolver(t, tc, node("cp-1", "10.0.0.11", true))

	_, err := r.Resolve(context.Background(), tc, talos.SelectOptions{})
	if got := creds.Reason(err); got != talosv1alpha1.ReasonAwaitingCredentials {
		t.Fatalf("reason: got %s (%v)", got, err)
	}
}

func TestResolveNeedsAControlPlaneNode(t *testing.T) {
	tc := issuedCluster()
	issued := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: tc.TalosServiceAccountName(), Namespace: ns},
		Data:       map[string][]byte{talosv1alpha1.TalosServiceAccountKey: talostest.IssuedTalosconfig(t)},
	}

	r, _ := resolver(t, tc, issued, node("worker", "10.0.0.20", false))

	_, err := r.Resolve(context.Background(), tc, talos.SelectOptions{})
	if err == nil || !strings.Contains(err.Error(), "no control-plane Node") {
		t.Fatalf("expected a missing control plane to be reported, got %v", err)
	}
}

func TestIssuedCredentialsSecretEnqueuesItsCluster(t *testing.T) {
	tc := issuedCluster()
	r, _ := resolver(t, tc)

	reqs := target.ClustersForSecret(r.Client)(context.Background(), secret(tc.TalosServiceAccountName(), nil))
	if len(reqs) != 1 || reqs[0].Name != "happy" {
		t.Fatalf("a re-issued certificate must re-reconcile its cluster, got %+v", reqs)
	}
}

func TestResolveWriterNeedsConfigManage(t *testing.T) {
	tc := cluster("happy", "talosconfig")
	r, f := resolver(t, tc, secret("talosconfig", talostest.AdminTalosconfig(t, "10.0.0.11", "10.0.0.12")))
	outside := talos.SelectOptions{Exclude: netip.MustParseAddr("10.0.0.11")}

	// Observing never yields a client that can write: not by default, not when asked nicely.
	for _, mode := range []talosv1alpha1.ConfigManagement{"", talosv1alpha1.ConfigManagementObserve} {
		tc.Spec.Management.Config = mode

		if _, err := r.ResolveWriter(context.Background(), tc, outside); !errors.Is(err, target.ErrNotManaged) {
			t.Fatalf("config %q: expected ErrNotManaged, got %v", mode, err)
		}
	}

	if len(f.WriterTargets()) != 0 {
		t.Fatal("no write client may even be built while observing")
	}

	tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage

	w, err := r.ResolveWriter(context.Background(), tc, outside)
	if err != nil || w == nil {
		t.Fatalf("expected a writer once config is Manage: %v", err)
	}

	// It reaches the node through the same endpoints as the reads: never the node being changed.
	targets := f.WriterTargets()
	if len(targets) != 1 || len(targets[0].Endpoints) != 1 || targets[0].Endpoints[0].Addr != netip.MustParseAddr("10.0.0.12") {
		t.Fatalf("writer targets: %+v", targets)
	}

	if len(f.Targets()) != 0 {
		t.Fatal("a writer is its own client, not the reader's")
	}
}

// A failing Kubernetes API says nothing about the Talos cluster or its credentials. Callers must
// be able to tell, or they report a healthy cluster as unreachable.
func TestResolveMarksKubernetesAPIFailures(t *testing.T) {
	unavailable := apierrors.NewServiceUnavailable("etcd is compacting")

	for name, funcs := range map[string]interceptor.Funcs{
		"reading the credentials Secret": {
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return unavailable
			},
		},
		"listing control-plane Nodes": {
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return unavailable
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			tc := issuedCluster()
			c := fakeclient.NewClientBuilder().WithScheme(scheme(t)).
				WithObjects(tc, secret(tc.CredentialsSecret().Name, talostest.IssuedTalosconfig(t))).
				WithInterceptorFuncs(funcs).Build()
			r := &target.Resolver{Client: c, Factory: &fake.Factory{Scenario: fake.NewScenario()}}

			_, err := r.Resolve(context.Background(), tc, talos.SelectOptions{})
			if !errors.Is(err, target.ErrKubernetesAPI) || !strings.Contains(err.Error(), "etcd is compacting") {
				t.Fatalf("expected a Kubernetes API failure that says what failed, got %v", err)
			}

			var credsErr *creds.Error
			if errors.As(err, &credsErr) {
				t.Fatalf("a Kubernetes API failure is not a credentials problem: %v", err)
			}
		})
	}

	// A Secret that is simply not there is a fact about the credentials, not a failure.
	tc := cluster("happy", "talosconfig")
	r, _ := resolver(t, tc)

	if _, err := r.Resolve(context.Background(), tc, talos.SelectOptions{}); errors.Is(err, target.ErrKubernetesAPI) {
		t.Fatalf("a missing Secret must stay a credentials problem: %v", err)
	}
}

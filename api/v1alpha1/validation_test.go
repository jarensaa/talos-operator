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

package v1alpha1_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

const namespace = "default"

// k8sClient talks to an envtest API server with the generated CRDs installed, so CEL rules,
// enums, patterns and defaults are exercised exactly as a real cluster would apply them.
var k8sClient client.Client

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		// Run via `make test`, which provisions the envtest binaries.
		os.Exit(0)
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		panic(err)
	}

	scheme := runtime.NewScheme()
	if err := talosv1alpha1.AddToScheme(scheme); err != nil {
		panic(err)
	}

	if k8sClient, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		panic(err)
	}

	if err := installAdmissionPolicies(); err != nil {
		panic(err)
	}

	code := m.Run()

	if err := env.Stop(); err != nil {
		panic(err)
	}

	os.Exit(code)
}

func minimalCluster(name string) *talosv1alpha1.TalosCluster {
	return &talosv1alpha1.TalosCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
	}
}

func validMachine(name string) *talosv1alpha1.TalosMachine {
	return &talosv1alpha1.TalosMachine{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: talosv1alpha1.TalosMachineSpec{
			PoolRef:  corev1.LocalObjectReference{Name: "control-plane"},
			Address:  "10.0.0.11",
			Identity: talosv1alpha1.MachineIdentity{SystemUUID: "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"},
		},
	}
}

func validPool(name string) *talosv1alpha1.TalosMachinePool {
	return &talosv1alpha1.TalosMachinePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: talosv1alpha1.TalosMachinePoolSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "cluster"},
			Role:       talosv1alpha1.MachineRoleControlPlane,
		},
	}
}

func requireRejected(t *testing.T, err error, wantMessage string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected rejection containing %q, got success", wantMessage)
	}

	if !strings.Contains(err.Error(), wantMessage) {
		t.Fatalf("expected rejection containing %q, got: %v", wantMessage, err)
	}
}

func TestMinimalObserveClusterIsDefaulted(t *testing.T) {
	ctx := context.Background()
	tc := minimalCluster("minimal")

	if err := k8sClient.Create(ctx, tc); err != nil {
		t.Fatalf("create minimal TalosCluster: %v", err)
	}

	got := &talosv1alpha1.TalosCluster{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), got); err != nil {
		t.Fatal(err)
	}

	want := talosv1alpha1.Management{
		Config:     talosv1alpha1.ConfigManagementObserve,
		Upgrades:   talosv1alpha1.CapabilityDisabled,
		Membership: talosv1alpha1.CapabilityDisabled,
	}
	if got.Spec.Management != want {
		t.Fatalf("management defaults: got %+v, want %+v", got.Spec.Management, want)
	}
}

func TestClusterNameImmutableOnceSet(t *testing.T) {
	ctx := context.Background()
	tc := minimalCluster("immutable-name")

	if err := k8sClient.Create(ctx, tc); err != nil {
		t.Fatal(err)
	}

	// Setting it for the first time (copying from the adoption proposal) is allowed.
	tc.Spec.ClusterName = "my-cluster"
	if err := k8sClient.Update(ctx, tc); err != nil {
		t.Fatalf("first set of clusterName: %v", err)
	}

	tc.Spec.ClusterName = "renamed"
	requireRejected(t, k8sClient.Update(ctx, tc), "clusterName is immutable once set")
}

func TestInvalidManagementModeRejected(t *testing.T) {
	tc := minimalCluster("bad-mode")
	tc.Spec.Management.Config = "Takeover"

	requireRejected(t, k8sClient.Create(context.Background(), tc), "spec.management.config")
}

func TestRolloutDefaultsAndTimings(t *testing.T) {
	ctx := context.Background()
	minutes := func(m float64) *metav1.Duration {
		return &metav1.Duration{Duration: time.Duration(m * float64(time.Minute))}
	}

	defaulted := minimalCluster("rollout-defaults")
	if err := k8sClient.Create(ctx, defaulted); err != nil {
		t.Fatal(err)
	}

	if got := defaulted.Spec.Rollout.DriftPolicy; got != talosv1alpha1.DriftPolicyReport {
		t.Fatalf("drift must be reported, not undone, unless asked for: got %q", got)
	}

	accepted := []talosv1alpha1.Rollout{
		{Soak: minutes(0)},
		{Soak: minutes(3)},
		{TryTimeout: minutes(4)},
		{TryTimeout: minutes(30), Soak: minutes(20), StepTimeout: minutes(21)},
		{DriftPolicy: talosv1alpha1.DriftPolicyEnforce},
	}

	for i, ro := range accepted {
		tc := minimalCluster(fmt.Sprintf("rollout-ok-%d", i))
		tc.Spec.Rollout = ro

		if err := k8sClient.Create(ctx, tc); err != nil {
			t.Errorf("%+v must be accepted: %v", ro, err)
		}
	}

	const tooShort = "tryTimeout must exceed soak by at least 2m"

	rejected := []struct {
		rollout talosv1alpha1.Rollout
		message string
	}{
		// A try that ends before it could be verified, soaked and confirmed only ever rolls back.
		{talosv1alpha1.Rollout{TryTimeout: minutes(3)}, tooShort},
		{talosv1alpha1.Rollout{Soak: minutes(4)}, tooShort},
		{talosv1alpha1.Rollout{TryTimeout: minutes(10), Soak: minutes(9)}, tooShort},
		{talosv1alpha1.Rollout{TryTimeout: minutes(30), Soak: minutes(10)}, "stepTimeout must exceed soak"},
		{talosv1alpha1.Rollout{Soak: minutes(-1)}, "soak must not be negative"},
		{talosv1alpha1.Rollout{DriftPolicy: "Ignore"}, "spec.rollout.driftPolicy"},
	}

	for i, tt := range rejected {
		tc := minimalCluster(fmt.Sprintf("rollout-bad-%d", i))
		tc.Spec.Rollout = tt.rollout

		requireRejected(t, k8sClient.Create(ctx, tc), tt.message)
	}
}

func TestConfigSourceRequiresExactlyOneSource(t *testing.T) {
	tc := minimalCluster("bad-config")
	tc.Spec.Config = []talosv1alpha1.ConfigSource{{
		Inline:    "apiVersion: v1alpha1\nkind: KubeProxyConfig\nenabled: false\n",
		ConfigRef: &talosv1alpha1.ConfigReference{Name: "shared"},
	}}

	requireRejected(t, k8sClient.Create(context.Background(), tc), "exactly one of inline, configRef or secretKeyRef")

	pool := validPool("empty-source")
	pool.Spec.Config = []talosv1alpha1.ConfigSource{{}}
	requireRejected(t, k8sClient.Create(context.Background(), pool), "exactly one of inline, configRef or secretKeyRef")
}

func TestCredentialsDefaultToTalosIssued(t *testing.T) {
	tc := minimalCluster("default-credentials")
	if err := k8sClient.Create(context.Background(), tc); err != nil {
		t.Fatal(err)
	}

	if tc.Spec.Credentials.Source != talosv1alpha1.CredentialsSourceTalosServiceAccount || tc.Spec.Credentials.SecretRef != nil {
		t.Fatalf("credentials: %+v", tc.Spec.Credentials)
	}
}

func TestCredentialsSecretRefMatchesSource(t *testing.T) {
	missing := minimalCluster("secret-without-ref")
	missing.Spec.Credentials = talosv1alpha1.Credentials{Source: talosv1alpha1.CredentialsSourceSecret}
	requireRejected(t, k8sClient.Create(context.Background(), missing), "secretRef is required for source Secret")

	stray := minimalCluster("ref-without-secret")
	stray.Spec.Credentials = talosv1alpha1.Credentials{SecretRef: &talosv1alpha1.SecretKeyReference{Name: "talosconfig"}}
	requireRejected(t, k8sClient.Create(context.Background(), stray), "secretRef is required for source Secret")

	ok := minimalCluster("secret-credentials")
	ok.Spec.Credentials = talosv1alpha1.Credentials{
		Source:    talosv1alpha1.CredentialsSourceSecret,
		SecretRef: &talosv1alpha1.SecretKeyReference{Name: "talosconfig"},
	}

	if err := k8sClient.Create(context.Background(), ok); err != nil {
		t.Fatal(err)
	}
}

func TestSecretConfigRequiresKey(t *testing.T) {
	tm := validMachine("secret-config-key")
	tm.Spec.Config = []talosv1alpha1.ConfigSource{{SecretKeyRef: &talosv1alpha1.SecretKeyReference{Name: "documents"}}}

	requireRejected(t, k8sClient.Create(context.Background(), tm), "secretKeyRef.key is required")
}

func TestPoolRoleAndClusterImmutable(t *testing.T) {
	ctx := context.Background()
	pool := validPool("role-immutable")

	if err := k8sClient.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}

	pool.Spec.Role = talosv1alpha1.MachineRoleWorker
	requireRejected(t, k8sClient.Update(ctx, pool), "role is immutable")

	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), pool); err != nil {
		t.Fatal(err)
	}

	pool.Spec.ClusterRef.Name = "other"
	requireRejected(t, k8sClient.Update(ctx, pool), "clusterRef is immutable")
}

func TestMachineNeedsAPoolAndMayMove(t *testing.T) {
	ctx := context.Background()

	orphan := validMachine("no-pool")
	orphan.Spec.PoolRef.Name = ""
	requireRejected(t, k8sClient.Create(ctx, orphan), "poolRef.name is required")

	tm := validMachine("moves")
	if err := k8sClient.Create(ctx, tm); err != nil {
		t.Fatal(err)
	}

	tm.Spec.PoolRef.Name = "other-control-plane"
	if err := k8sClient.Update(ctx, tm); err != nil {
		t.Fatalf("a machine may change pools: %v", err)
	}
}

func TestMachineConfigNeedsDocuments(t *testing.T) {
	mc := &talosv1alpha1.TalosMachineConfig{ObjectMeta: metav1.ObjectMeta{Name: "empty", Namespace: namespace}}
	requireRejected(t, k8sClient.Create(context.Background(), mc), "spec.documents")
}

func TestMachineAddressMustBeIP(t *testing.T) {
	tm := validMachine("hostname-address")
	tm.Spec.Address = "cluster.example.com"

	requireRejected(t, k8sClient.Create(context.Background(), tm), "address must be an IP address")
}

func TestMachineDesiredStateDefaultsToPresent(t *testing.T) {
	ctx := context.Background()
	tm := validMachine("default-present")

	if err := k8sClient.Create(ctx, tm); err != nil {
		t.Fatal(err)
	}

	got := &talosv1alpha1.TalosMachine{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), got); err != nil {
		t.Fatal(err)
	}

	if got.Spec.DesiredState != talosv1alpha1.DesiredStatePresent {
		t.Fatalf("desiredState default: got %q, want Present", got.Spec.DesiredState)
	}
}

// Messages of the admission policies in config/admission/policies.yaml.
const (
	gateMessage     = "may become Manage only once status shows Adopted=True"
	deletionMessage = "cannot be deleted"
)

// installAdmissionPolicies creates the shipped ValidatingAdmissionPolicies and waits until the
// API server enforces them; it picks up new policies asynchronously.
func installAdmissionPolicies() error {
	ctx := context.Background()

	data, err := os.ReadFile(filepath.Join("..", "..", "config", "admission", "policies.yaml"))
	if err != nil {
		return err
	}

	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)

	for {
		var raw map[string]any
		if err := decoder.Decode(&raw); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		if len(raw) == 0 {
			continue
		}

		if err := k8sClient.Create(ctx, &unstructured.Unstructured{Object: raw}); err != nil {
			return err
		}
	}

	return waitForEnforcement(ctx)
}

func waitForEnforcement(ctx context.Context) error {
	deadline := time.Now().Add(30 * time.Second)

	for gate, guard := false, false; !gate || !guard; time.Sleep(200 * time.Millisecond) {
		if time.Now().After(deadline) {
			return errors.New("admission policies were not enforced within 30s")
		}

		if !gate {
			probe := minimalCluster("policy-probe")
			probe.Spec.Management.Config = talosv1alpha1.ConfigManagementManage

			err := k8sClient.Create(ctx, probe)
			if err == nil {
				_ = k8sClient.Delete(ctx, probe) //nolint:errcheck // retried until the policy is active
			}

			gate = err != nil && strings.Contains(err.Error(), gateMessage)
		}

		if !guard {
			probe := validMachine("policy-probe")
			if err := k8sClient.Create(ctx, probe); err != nil {
				return err
			}

			err := k8sClient.Delete(ctx, probe)
			if guard = err != nil && strings.Contains(err.Error(), deletionMessage); guard {
				if err := orphanAndDelete(ctx, probe); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func orphanAndDelete(ctx context.Context, tm *talosv1alpha1.TalosMachine) error {
	tm.Annotations = map[string]string{talosv1alpha1.AnnotationOrphan: "true"}
	if err := k8sClient.Update(ctx, tm); err != nil {
		return err
	}

	return k8sClient.Delete(ctx, tm)
}

// setAdopted writes the Adopted condition through the status subresource, as the operator does.
func setAdopted(t *testing.T, tc *talosv1alpha1.TalosCluster, status metav1.ConditionStatus) {
	t.Helper()

	meta.SetStatusCondition(&tc.Status.Conditions, metav1.Condition{
		Type: talosv1alpha1.ConditionAdopted, Status: status, Reason: talosv1alpha1.ReasonSucceeded, Message: "test",
	})

	if err := k8sClient.Status().Update(context.Background(), tc); err != nil {
		t.Fatal(err)
	}
}

func TestManagementGateRefusesManageUntilAdopted(t *testing.T) {
	ctx := context.Background()

	born := minimalCluster("born-managed")
	born.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
	requireRejected(t, k8sClient.Create(ctx, born), gateMessage)

	tc := minimalCluster("gated")
	if err := k8sClient.Create(ctx, tc); err != nil {
		t.Fatal(err)
	}

	tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
	requireRejected(t, k8sClient.Update(ctx, tc), gateMessage)

	// A spec update cannot smuggle in its own status: it is read from the stored object.
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tc), tc); err != nil {
		t.Fatal(err)
	}

	setAdopted(t, tc, metav1.ConditionTrue)

	tc.Spec.Management.Config = talosv1alpha1.ConfigManagementManage
	if err := k8sClient.Update(ctx, tc); err != nil {
		t.Fatalf("Manage after Adopted=True: %v", err)
	}

	// Once managed, a later invalidated proof does not lock the spec.
	setAdopted(t, tc, metav1.ConditionFalse)

	tc.Spec.Management.Upgrades = talosv1alpha1.CapabilityEnabled
	if err := k8sClient.Update(ctx, tc); err != nil {
		t.Fatalf("spec update while managed: %v", err)
	}
}

func TestCapabilitiesNeedManagedConfig(t *testing.T) {
	for _, set := range []func(*talosv1alpha1.Management){
		func(m *talosv1alpha1.Management) { m.Upgrades = talosv1alpha1.CapabilityEnabled },
		func(m *talosv1alpha1.Management) { m.Membership = talosv1alpha1.CapabilityEnabled },
	} {
		tc := minimalCluster("capability")
		set(&tc.Spec.Management)
		requireRejected(t, k8sClient.Create(context.Background(), tc), "can be Enabled only with config: Manage")
	}
}

func TestPresentMachinesCannotBeDeleted(t *testing.T) {
	ctx := context.Background()

	present := validMachine("present")
	if err := k8sClient.Create(ctx, present); err != nil {
		t.Fatal(err)
	}

	requireRejected(t, k8sClient.Delete(ctx, present), deletionMessage)

	if err := orphanAndDelete(ctx, present); err != nil {
		t.Fatalf("an orphaned machine can be deleted: %v", err)
	}

	removed := validMachine("removed")
	removed.Spec.DesiredState = talosv1alpha1.DesiredStateRemoved
	if err := k8sClient.Create(ctx, removed); err != nil {
		t.Fatal(err)
	}

	if err := k8sClient.Delete(ctx, removed); err != nil {
		t.Fatalf("a Removed machine can be deleted: %v", err)
	}
}

// The samples are documentation; they must stay valid against the CRDs and admission policies.
func TestSamplesAreValid(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "config", "samples", "talos_*.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no samples found: %v", err)
	}

	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}

		var raw map[string]any
		if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096).Decode(&raw); err != nil {
			t.Fatalf("%s: %v", p, err)
		}

		obj := &unstructured.Unstructured{Object: raw}
		obj.SetNamespace(namespace)
		obj.SetName("sample-" + obj.GetName())

		if err := k8sClient.Create(context.Background(), obj); err != nil {
			t.Errorf("%s: %v", filepath.Base(p), err)
		}
	}
}

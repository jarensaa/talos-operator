//go:build e2e

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

// Package e2e runs against the disposable QEMU Talos cluster from `make e2e-cluster-up`
// (docs/dev/e2e.md). Run with `make test-e2e`.
package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/controller"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/target"
)

// testNamespace holds every object the e2e suite creates; it is deleted afterwards.
const testNamespace = "talos-operator-e2e"

var (
	k8sClient       client.Client
	talosconfigPath string
	stopManager     context.CancelFunc
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "talos-operator e2e suite")
}

var _ = BeforeSuite(func(ctx context.Context) {
	talosconfigPath = os.Getenv("E2E_TALOSCONFIG")
	kubeconfigPath := os.Getenv("E2E_KUBECONFIG")

	if talosconfigPath == "" || kubeconfigPath == "" {
		Skip("E2E_TALOSCONFIG/E2E_KUBECONFIG not set; run `make e2e-cluster-up` and use `make test-e2e`")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	Expect(err).NotTo(HaveOccurred())

	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(talosv1alpha1.AddToScheme(scheme)).To(Succeed())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())

	By("installing the CRDs into the e2e cluster")
	_, err = envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
		Paths:              []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfPathMissing: true,
	})
	Expect(err).NotTo(HaveOccurred())

	By("installing the admission policies, so specs run under production's rules")
	installAdmissionPolicies(ctx)

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}
	if err := k8sClient.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		Expect(err).NotTo(HaveOccurred())
	}

	By("starting the operator's controllers in-process, watching the e2e namespace")
	startManager(cfg, scheme)
}, NodeTimeout(5*time.Minute))

// startManager runs the controllers as main.go wires them, against the e2e cluster. Running
// in-process keeps the suite independent of image builds; the operator still talks to the
// real Talos API and Kubernetes API.
func startManager(cfg *rest.Config, scheme *runtime.Scheme) {
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Cache:                  cache.Options{DefaultNamespaces: map[string]cache.Config{testNamespace: {}}},
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	Expect(err).NotTo(HaveOccurred())

	factory := talos.NewFactory()

	resolver := &target.Resolver{Client: mgr.GetClient(), Factory: factory}

	Expect((&controller.TalosClusterReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Resolver:        resolver,
		Recorder:        mgr.GetEventRecorder("talos-operator-e2e"),
		APIReader:       mgr.GetAPIReader(),
		ObserveInterval: 10 * time.Second,
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&controller.TalosMachineReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Resolver:        resolver,
		ObserveInterval: 10 * time.Second,
	}).SetupWithManager(mgr)).To(Succeed())

	pools := &controller.TalosMachinePoolReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	Expect(pools.SetupWithManager(mgr)).To(Succeed())

	configs := &controller.TalosMachineConfigReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	Expect(configs.SetupWithManager(mgr)).To(Succeed())

	var mgrCtx context.Context
	mgrCtx, stopManager = context.WithCancel(context.Background())

	go func() {
		defer GinkgoRecover()
		defer factory.Close()

		Expect(mgr.Start(mgrCtx)).To(Succeed())
	}()
}

var _ = AfterSuite(func(ctx context.Context) {
	if k8sClient == nil {
		return
	}

	if stopManager != nil {
		stopManager()
	}

	By("deleting the e2e namespace")
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, ns))).To(Succeed())
}, NodeTimeout(2*time.Minute))

// createCredentialsSecret stores the e2e cluster's admin talosconfig as the operator expects it.
func createCredentialsSecret(ctx context.Context, name string) *corev1.Secret {
	data, err := os.ReadFile(talosconfigPath)
	Expect(err).NotTo(HaveOccurred())

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Data:       map[string][]byte{"talosconfig": data},
	}
	Expect(k8sClient.Create(ctx, secret)).To(Succeed())

	DeferCleanup(func(ctx context.Context) {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed())
	})

	return secret
}

// installAdmissionPolicies creates or updates the shipped ValidatingAdmissionPolicies
// (config/admission/policies.yaml), which the chart installs in production.
func installAdmissionPolicies(ctx context.Context) {
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "admission", "policies.yaml"))
	Expect(err).NotTo(HaveOccurred())

	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)

	for {
		var raw map[string]any
		if err := decoder.Decode(&raw); errors.Is(err, io.EOF) {
			return
		} else {
			Expect(err).NotTo(HaveOccurred())
		}

		if len(raw) == 0 {
			continue
		}

		obj := &unstructured.Unstructured{Object: raw}

		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(obj.GroupVersionKind())

		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), existing); apierrors.IsNotFound(err) {
			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			continue
		} else {
			Expect(err).NotTo(HaveOccurred())
		}

		obj.SetResourceVersion(existing.GetResourceVersion())
		Expect(k8sClient.Update(ctx, obj)).To(Succeed())
	}
}

// deleteMachine releases a TalosMachine the way a user stops managing one: the deletion guard
// refuses to delete a Present machine unless it is annotated as orphaned.
func deleteMachine(ctx context.Context, tm *talosv1alpha1.TalosMachine) {
	GinkgoHelper()

	current := &talosv1alpha1.TalosMachine{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tm), current); apierrors.IsNotFound(err) {
		return
	} else {
		Expect(err).NotTo(HaveOccurred())
	}

	// A patch of the one annotation, not an update of the object as it was read: the machine's
	// controller writes its status at any moment, and an update would lose to it.
	released := current.DeepCopy()
	if released.Annotations == nil {
		released.Annotations = map[string]string{}
	}

	released.Annotations[talosv1alpha1.AnnotationOrphan] = "true"
	Expect(client.IgnoreNotFound(k8sClient.Patch(ctx, released, client.MergeFrom(current)))).To(Succeed())
	Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, released))).To(Succeed())
}

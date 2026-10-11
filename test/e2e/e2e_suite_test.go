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

// Package e2e runs against the disposable QEMU Talos cluster from `make e2e-cluster-up`, with the
// operator deployed into it by `make e2e-deploy` (docs/dev/e2e.md). Run with `make test-e2e`.
package e2e

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
)

const (
	// testNamespace is where the chart is installed and where every object of the suite lives.
	// The harness allows Talos API access for exactly this namespace.
	testNamespace = "talos-operator-e2e"
	// operatorName is the chart's release, and with it the name of its Deployment.
	operatorName = "talos-operator"
	// leaderLease is the Lease the replicas elect their leader with (cmd/main.go).
	leaderLease = "talos-operator.jarensaa.github.io"
)

var (
	k8sClient       client.Client
	restConfig      *rest.Config
	talosconfigPath string
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)

	// Ginkgo ends a suite after an hour unless told otherwise. This one upgrades Talos and then
	// Kubernetes on every node, and takes longer than that on the larger clusters. `go test
	// -timeout` (Makefile) is the bound that counts.
	suite, reporter := GinkgoConfiguration()
	suite.Timeout = 150 * time.Minute

	RunSpecs(t, "talos-operator e2e suite", suite, reporter)
}

// The suite starts nothing itself: the operator under test is the one the chart installed, with
// its CRDs, admission policies and permissions. That is what a user runs.
var _ = BeforeSuite(func(ctx context.Context) {
	talosconfigPath = os.Getenv("E2E_TALOSCONFIG")
	kubeconfigPath := os.Getenv("E2E_KUBECONFIG")

	if talosconfigPath == "" || kubeconfigPath == "" {
		Skip("E2E_TALOSCONFIG/E2E_KUBECONFIG not set; run `make e2e-cluster-up e2e-deploy` and use `make test-e2e`")
	}

	var err error

	restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	Expect(err).NotTo(HaveOccurred())

	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(talosv1alpha1.AddToScheme(scheme)).To(Succeed())

	k8sClient, err = client.New(restConfig, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())

	By("waiting for the deployed operator: two replicas, on different nodes, ready")
	Eventually(func(g Gomega) {
		deployment := &appsv1.Deployment{}
		g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: operatorName}, deployment)).
			To(Succeed(), "the operator is not deployed; run `make e2e-deploy`")
		g.Expect(deployment.Spec.Replicas).To(HaveValue(BeNumerically(">=", 2)))
		g.Expect(deployment.Status.ReadyReplicas).To(Equal(*deployment.Spec.Replicas))

		nodes := map[string]bool{}
		for _, pod := range operatorPods(ctx, g) {
			nodes[pod.Spec.NodeName] = true
		}

		g.Expect(nodes).To(HaveLen(int(*deployment.Spec.Replicas)), "replicas must not share a node")
	}).WithContext(ctx).WithTimeout(3 * time.Minute).Should(Succeed())
}, NodeTimeout(5*time.Minute))

// operatorPods lists the operator's pods.
func operatorPods(ctx context.Context, g Gomega) []corev1.Pod {
	var pods corev1.PodList
	g.Expect(k8sClient.List(ctx, &pods, client.InNamespace(testNamespace),
		client.MatchingLabels{"app.kubernetes.io/name": operatorName})).To(Succeed())

	return pods.Items
}

// operatorLeader returns the pod that holds the leader Lease, or nil if none of the pods does.
func operatorLeader(ctx context.Context, g Gomega) *corev1.Pod {
	lease := &coordinationv1.Lease{}
	g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: leaderLease}, lease)).To(Succeed())
	g.Expect(lease.Spec.HolderIdentity).NotTo(BeNil(), "nobody leads")

	// The identity is the pod's name and a random suffix.
	holder, _, _ := strings.Cut(*lease.Spec.HolderIdentity, "_")

	for _, pod := range operatorPods(ctx, g) {
		if pod.Name == holder {
			return &pod
		}
	}

	return nil
}

// metricsReader is the identity the suite reads the operator's metrics with. The endpoint is
// served as the chart configures it: over HTTPS, to callers the Kubernetes API authenticates and
// authorizes for /metrics.
const metricsReader = "e2e-metrics-reader"

var metricsAccess sync.Once

func metricsToken(ctx context.Context) string {
	GinkgoHelper()

	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: metricsReader, Namespace: testNamespace}}

	metricsAccess.Do(func() {
		role := &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: testNamespace + "-" + metricsReader},
			Rules:      []rbacv1.PolicyRule{{NonResourceURLs: []string{"/metrics"}, Verbs: []string{"get"}}},
		}
		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: role.Name},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.ServiceAccountKind, Name: sa.Name, Namespace: sa.Namespace}},
		}

		for _, obj := range []client.Object{sa.DeepCopy(), role, binding} {
			if err := k8sClient.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}
		}
	})

	request := &authenticationv1.TokenRequest{}
	Expect(k8sClient.SubResource("token").Create(ctx, sa, request)).To(Succeed())

	return request.Status.Token
}

// operatorCounter sums a counter over every replica, read from each pod's own metrics endpoint.
// A replica counts from its start, so this is what the replicas running now have counted.
//
// It waits for every replica to be ready and reads again when a read fails. The scenarios take
// nodes away and restart the API server; a pod can be Running, on a node that just came back,
// before its endpoint answers, and a read that fails then says nothing about the counter.
func operatorCounter(ctx context.Context, name string) float64 {
	GinkgoHelper()

	var total float64

	Eventually(func(g Gomega) {
		sum := 0.0

		for _, families := range operatorMetrics(ctx, g) {
			if family := families[name]; family != nil {
				for _, m := range family.GetMetric() {
					sum += m.GetCounter().GetValue()
				}
			}
		}

		total = sum
	}).WithContext(ctx).WithTimeout(5 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

	return total
}

// operatorGauge reads a gauge from every replica: the value of the one series that has the
// labels given, in pairs, or nothing for a replica that exports no such series. Every replica
// exports the state of the resources, the leader or not, and an alert reads whichever is scraped.
func operatorGauge(ctx context.Context, g Gomega, name string, pairs ...string) []float64 {
	GinkgoHelper()

	var out []float64

	for _, families := range operatorMetrics(ctx, g) {
		family := families[name]
		if family == nil {
			continue
		}

		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range m.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}

			matches := true
			for i := 0; i+1 < len(pairs); i += 2 {
				matches = matches && labels[pairs[i]] == pairs[i+1]
			}

			if matches {
				out = append(out, m.GetGauge().GetValue())
			}
		}
	}

	return out
}

// operatorMetrics is what every replica exports, by pod. It expects every replica to be ready
// and every read to succeed, and is called until they are and do.
func operatorMetrics(ctx context.Context, g Gomega) map[string]map[string]*dto.MetricFamily {
	GinkgoHelper()

	token := metricsToken(ctx)

	deployment := &appsv1.Deployment{}
	g.Expect(k8sClient.Get(ctx, client.ObjectKey{Namespace: testNamespace, Name: operatorName}, deployment)).To(Succeed())
	g.Expect(deployment.Status.ReadyReplicas).To(Equal(*deployment.Spec.Replicas), "every replica must be ready to be asked")

	out := map[string]map[string]*dto.MetricFamily{}

	for _, pod := range operatorPods(ctx, g) {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
			continue
		}

		families, err := scrapePod(ctx, &pod, token)
		g.Expect(err).NotTo(HaveOccurred(), "metrics of %s", pod.Name)

		out[pod.Name] = families
	}

	return out
}

// scrapePod reads one pod's metrics endpoint, through a port-forward to the pod.
func scrapePod(ctx context.Context, pod *corev1.Pod, token string) (map[string]*dto.MetricFamily, error) {
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}

	transport, upgrader, err := spdy.RoundTripperFor(restConfig)
	if err != nil {
		return nil, err
	}

	url := clientset.CoreV1().RESTClient().Post().Resource("pods").Namespace(pod.Namespace).Name(pod.Name).
		SubResource("portforward").URL()
	stop, ready := make(chan struct{}), make(chan struct{})

	defer close(stop)

	forwarder, err := portforward.New(spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, url),
		[]string{"0:8443"}, stop, ready, io.Discard, io.Discard)
	if err != nil {
		return nil, err
	}

	failed := make(chan error, 1)
	go func() { failed <- forwarder.ForwardPorts() }()

	select {
	case <-ready:
	case err := <-failed:
		return nil, fmt.Errorf("port-forward: %w", err)
	case <-time.After(30 * time.Second):
		return nil, errors.New("port-forward did not become ready")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	ports, err := forwarder.GetPorts()
	if err != nil {
		return nil, err
	}

	return scrape(ctx, fmt.Sprintf("https://127.0.0.1:%d/metrics", ports[0].Local), token)
}

// scrape reads a metrics endpoint as a reader the Kubernetes API authorizes.
func scrape(ctx context.Context, url, token string) (map[string]*dto.MetricFamily, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	// The endpoint's certificate is the pod's own self-signed one; the forwarded port is the pod.
	scraper := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // see above

	resp, err := scraper.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close() //nolint:errcheck // a read-only response body

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("the metrics endpoint must serve an authorized reader, and answered %s", resp.Status)
	}

	parser := expfmt.NewTextParser(model.UTF8Validation)

	return parser.TextToMetricFamilies(resp.Body)
}

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

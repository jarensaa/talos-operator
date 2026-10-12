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
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"go.yaml.in/yaml/v4"
	"k8s.io/client-go/rest"

	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

const (
	// kubeFrom is the Kubernetes version these tests upgrade from.
	kubeFrom        = "1.36.2"
	kubeTo          = "1.37.1"
	apiControlPlane = "cp-1"
	apiWorker1      = "w-1"
	garbage         = "garbage"
)

// generated is a machine config as Talos generates it for a contract: Kubernetes components in
// documents of their own from 1.14 on, in the v1alpha1 document before.
func generated(t *testing.T, contract *config.VersionContract, role machine.Type) []byte {
	t.Helper()

	in := talostest.Input(t)
	in.Contract, in.Role, in.KubernetesVersion = contract, role, kubeFrom

	return talostest.Config(t, in)
}

var contracts = map[string]*config.VersionContract{"1.14": config.TalosVersion1_14, "1.13": config.TalosVersion1_13}

func TestKubernetesImages(t *testing.T) {
	for name, contract := range contracts {
		t.Run(name, func(t *testing.T) {
			got, err := KubernetesImages(generated(t, contract, machine.TypeControlPlane))
			if err != nil {
				t.Fatal(err)
			}

			want := map[string]string{
				ComponentAPIServer:         "registry.k8s.io/kube-apiserver:v1.36.2",
				ComponentControllerManager: "registry.k8s.io/kube-controller-manager:v1.36.2",
				ComponentScheduler:         "registry.k8s.io/kube-scheduler:v1.36.2",
				ComponentProxy:             "registry.k8s.io/kube-proxy:v1.36.2",
				ComponentKubelet:           "ghcr.io/siderolabs/kubelet:v1.36.2",
			}
			if len(got) != len(want) {
				t.Fatalf("got %v", got)
			}

			for component, image := range want {
				if got[component] != image {
					t.Errorf("%s: got %q, want %q", component, got[component], image)
				}
			}

			// A worker runs a kubelet and nothing else of Kubernetes.
			worker, err := KubernetesImages(generated(t, contract, machine.TypeWorker))
			if err != nil {
				t.Fatal(err)
			}

			if len(worker) != 1 || worker[ComponentKubelet] != want[ComponentKubelet] {
				t.Fatalf("worker: got %v", worker)
			}
		})
	}

	if _, err := KubernetesImages([]byte("not a machine config")); err == nil {
		t.Fatal("a config that does not parse has no images")
	}
}

// The patch changes one image and nothing else, in whichever form the config keeps it.
func TestWithKubernetesImage(t *testing.T) {
	for name, contract := range contracts {
		t.Run(name, func(t *testing.T) {
			raw := generated(t, contract, machine.TypeControlPlane)

			before, err := KubernetesImages(raw)
			if err != nil {
				t.Fatal(err)
			}

			for _, component := range KubernetesComponents {
				image := strings.Replace(before[component], "v1.36.2", "v1.37.1", 1)

				patched, err := WithKubernetesImage(raw, component, image)
				if err != nil {
					t.Fatalf("%s: %v", component, err)
				}

				after, err := KubernetesImages(patched)
				if err != nil {
					t.Fatalf("%s: %v", component, err)
				}

				for _, other := range KubernetesComponents {
					want := before[other]
					if other == component {
						want = image
					}

					if after[other] != want {
						t.Errorf("patching %s: %s is %q, want %q", component, other, after[other], want)
					}
				}

				// Nothing else moved: putting the old image back gives the config it was.
				restored, err := WithKubernetesImage(patched, component, before[component])
				if err != nil {
					t.Fatal(err)
				}

				again, err := WithKubernetesImage(raw, component, before[component])
				if err != nil {
					t.Fatal(err)
				}

				if string(restored) != string(again) {
					t.Errorf("patching %s and back changed more than the image", component)
				}

				// And the same patch twice is the same config: a step that is repeated sends what
				// the first attempt sent.
				twice, err := WithKubernetesImage(patched, component, image)
				if err != nil {
					t.Fatal(err)
				}

				if string(twice) != string(patched) {
					t.Errorf("patching %s twice is not the same as once", component)
				}
			}
		})
	}

	raw := generated(t, config.TalosVersion1_14, machine.TypeWorker)
	if _, err := WithKubernetesImage(raw, "etcd", "registry.k8s.io/etcd:v3.6.0"); err == nil {
		t.Fatal("only the components of a Kubernetes version are patched")
	}

	// An error never quotes the config: it holds the cluster's keys.
	if _, err := WithKubernetesImage([]byte("version: v1alpha1\nmachine:\n  token: s3cr3t\n  type: [\n"), ComponentKubelet, "x"); err == nil ||
		strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("got %v", err)
	}
}

func TestKubernetesUpgradeProblem(t *testing.T) {
	current := map[string]string{apiControlPlane: "v1.14.2", apiWorker1: "v1.14.1"}

	tests := []struct {
		name     string
		from, to string
		talos    map[string]string
		why      string
	}{
		{"the next minor", kubeFrom, kubeTo, current, ""},
		{"a patch release", "1.37.0", kubeTo, current, ""},
		{"with a v", "v1.36.2", "v1.37.1", current, ""},
		{"no nodes read", kubeFrom, kubeTo, nil, ""},
		{"a minor skipped", "1.35.4", kubeTo, current, "one at a time"},
		{"a downgrade", kubeTo, kubeFrom, current, "never backwards"},
		{"a version nobody knows a way to", kubeTo, "1.99.0", current, "no upgrade"},
		// The message names the node and says which side has to move.
		{"a Talos too old for it", kubeFrom, kubeTo, map[string]string{apiControlPlane: "v1.14.2", apiWorker1: "v1.13.4"}, "w-1 (v1.13.4)"},
		{"a Talos too old for it, what to do", kubeFrom, kubeTo, map[string]string{apiWorker1: "v1.13.4"}, "upgrade Talos first"},
		{garbage, kubeFrom, "latest", current, "no upgrade path"},
		{"a Talos version that is none", kubeFrom, kubeTo, map[string]string{apiWorker1: "dev"}, "unparsable Talos version"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := KubernetesUpgradeProblem(tt.from, tt.to, tt.talos)
			if (tt.why == "") != (got == "") || !strings.Contains(got, tt.why) {
				t.Fatalf("got %q, want it to mention %q", got, tt.why)
			}
		})
	}
}

// kubernetesAPI is a Kubernetes API that serves lists of the given resources and knows no others.
// It records what it was asked for.
type kubernetesAPI struct {
	mu     sync.Mutex
	asked  []string
	counts map[string]int
	status int
}

func (k *kubernetesAPI) serve(t *testing.T) *rest.Config {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k.mu.Lock()
		k.asked = append(k.asked, r.URL.Path)
		count, known := k.counts[r.URL.Path]
		k.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")

		var answer string

		switch {
		case k.status != 0:
			w.WriteHeader(k.status)
			answer = `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`
		case !known:
			w.WriteHeader(http.StatusNotFound)
			answer = `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`
		default:
			items := strings.TrimSuffix(strings.Repeat(`{"metadata":{"name":"x"}},`, count), ",")
			answer = `{"kind":"List","apiVersion":"v1","items":[` + items + `]}`
		}

		_, _ = w.Write([]byte(answer)) //nolint:errcheck // a test server
	}))
	t.Cleanup(srv.Close)

	return &rest.Config{Host: srv.URL}
}

// removedAPIPaths are the lists the checks ask the Kubernetes API for, over every hop between the
// Kubernetes versions this operator's Talos range runs. The chart grants exactly these
// (charts/talos-operator/files/upgrade-check-rules.yaml); TestChartGrantsTheUpgradeChecks holds
// the two together.
func removedAPIPaths(t *testing.T) []string {
	t.Helper()

	api := &kubernetesAPI{}
	cfg := api.serve(t)

	for minor := 31; minor <= 40; minor++ {
		from, to := "1."+strconv.Itoa(minor)+".0", "1."+strconv.Itoa(minor+1)+".0"
		if KubernetesUpgradeProblem(from, to, nil) != "" {
			continue
		}

		if _, err := RemovedAPIObjects(context.Background(), cfg, from, to); err != nil {
			t.Fatalf("%s to %s: %v", from, to, err)
		}
	}

	slices.Sort(api.asked)

	return slices.Compact(api.asked)
}

func TestRemovedAPIObjects(t *testing.T) {
	const workloads = "/apis/scheduling.k8s.io/v1alpha2/workloads"

	api := &kubernetesAPI{counts: map[string]int{workloads: 2, "/apis/scheduling.k8s.io/v1alpha2/podgroups": 0}}
	cfg := api.serve(t)
	ctx := context.Background()

	found, err := RemovedAPIObjects(ctx, cfg, kubeFrom, kubeTo)
	if err != nil {
		t.Fatal(err)
	}

	// An API with objects stands in the way. One that is served without any, or not at all, does not.
	if len(found) != 1 || !strings.Contains(found[0], "2 object(s) of workloads.v1alpha2.scheduling.k8s.io") {
		t.Fatalf("got %q", found)
	}

	if !slices.Contains(api.asked, workloads) {
		t.Fatalf("asked for %v", api.asked)
	}

	// A patch release removes nothing.
	api.asked = nil

	if found, err := RemovedAPIObjects(ctx, cfg, "1.37.0", kubeTo); err != nil || len(found) != 0 || len(api.asked) != 0 {
		t.Fatalf("got %q, %v, asked %v", found, err, api.asked)
	}

	// What may not be read is not fine.
	api.status = http.StatusForbidden

	if _, err := RemovedAPIObjects(ctx, cfg, kubeFrom, kubeTo); err == nil {
		t.Fatal("a list that is refused must fail the check")
	}

	if _, err := RemovedAPIObjects(ctx, cfg, kubeFrom, "latest"); err == nil {
		t.Fatal("a version that is none must fail the check")
	}

	if len(removedAPIPaths(t)) == 0 {
		t.Fatal("the hops this operator supports remove no API at all, which the list of them contradicts")
	}
}

func staticPod(t *testing.T, api *testAPI, id string, command ...string) {
	t.Helper()

	args := make([]any, 0, len(command))
	for _, c := range command {
		args = append(args, c)
	}

	pod := k8s.NewStaticPod(k8s.NamespaceName, id)
	pod.TypedSpec().Pod = map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"spec": map[string]any{"containers": []any{map[string]any{"name": id, "command": args}}},
	}
	create(t, api, pod)
}

// What the checks read from a node: how its static pods and its kubelet are started.
func TestKubernetesUpgradeChecksReadTheNodes(t *testing.T) {
	api, c := serve(t)
	r := reader(c)
	ctx := context.Background()
	nodes := ClusterInfo{ControlPlanes: []netip.Addr{apiNode}}

	staticPod(t, api, k8s.APIServerID, "/usr/local/bin/kube-apiserver", "--feature-gates=AnyVolumeDataSource=true", "--v=2")
	staticPod(t, api, k8s.ControllerManagerID, "/usr/local/bin/kube-controller-manager", "--v=2")

	kubelet := k8s.NewKubeletSpec(k8s.NamespaceName, k8s.KubeletID)
	kubelet.TypedSpec().Image = "ghcr.io/siderolabs/kubelet:v1.36.2"
	kubelet.TypedSpec().Args = []string{"--container-hints=/etc/hints.json", "--v=2"}
	create(t, api, kubelet)

	found, err := r.KubernetesUpgradeChecks(ctx, kubeFrom, kubeTo, nodes)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"kube-apiserver on " + apiNode.String() + " sets the feature gate AnyVolumeDataSource, which the new version removed",
		"kubelet on " + apiNode.String() + " is started with --container-hints, which the new version removed",
	}
	if !slices.Equal(found, want) {
		t.Fatalf("got %q\nwant %q", found, want)
	}

	// Every read was a COSI read, addressed to the node that was asked about.
	for _, addressed := range api.addressedTo(cosiGetMethod) {
		if addressed != apiNode.String() {
			t.Fatalf("a read went to %s", addressed)
		}
	}

	// The same flags are fine where nothing removes them.
	if found, err := r.KubernetesUpgradeChecks(ctx, "1.35.4", kubeFrom, nodes); err != nil || len(found) != 0 {
		t.Fatalf("got %q, %v", found, err)
	}

	if found, err := r.KubernetesUpgradeChecks(ctx, kubeFrom, kubeTo, ClusterInfo{}); err != nil || len(found) != 0 {
		t.Fatalf("no nodes, nothing found: got %q, %v", found, err)
	}

	if _, err := r.KubernetesUpgradeChecks(ctx, kubeFrom, "latest", nodes); err == nil {
		t.Fatal("a version that is none must fail the check")
	}
}

// The image of a static pod goes where the kubelet runs pods from, the kubelet's own where Talos
// runs its services from.
func TestUpgraderPullsKubernetesImagesToWhereTheyRun(t *testing.T) {
	api, c := serve(t, dial(accessUpgrade)...)
	api.pull = []*machineapi.ImageServicePullResponse{pulling(), pulled("image")}

	u := &machineryUpgrader{forfeiter{*reader(c)}}
	ctx := context.Background()

	// Everything Talos runs from a node's config. kube-proxy is not: see below.
	runs := []string{ComponentAPIServer, ComponentControllerManager, ComponentScheduler, ComponentKubelet}

	for _, component := range runs {
		if err := u.PullKubernetesImage(ctx, apiNode, component, "registry.example.com/"+component+":v1.37.1"); err != nil {
			t.Fatalf("%s: %v", component, err)
		}
	}

	if len(api.pulls) != len(runs) {
		t.Fatalf("pulls: %+v", api.pulls)
	}

	for i, component := range runs {
		got := api.pulls[i]
		driver, namespace := common.ContainerDriver_CRI, common.ContainerdNamespace_NS_CRI

		if component == ComponentKubelet {
			driver, namespace = common.ContainerDriver_CONTAINERD, common.ContainerdNamespace_NS_SYSTEM
		}

		if got.GetImageRef() != "registry.example.com/"+component+":v1.37.1" || got.GetContainerd().GetDriver() != driver ||
			got.GetContainerd().GetNamespace() != namespace {
			t.Errorf("%s: %+v", component, got)
		}
	}

	err := u.PullKubernetesImage(ctx, apiNode, ComponentProxy, "registry.k8s.io/kube-proxy:v1.37.1")
	if err == nil || len(api.pulls) != len(runs) {
		t.Fatal("what Talos does not run from a node's config is not pulled")
	}
}

// The checks may only ask what the operator is allowed to ask: the chart grants a list of exactly
// the APIs go-kubernetes names as removed, over every hop this operator supports. A newer
// go-kubernetes that knows of another removed API fails here until the chart grants it too.
func TestChartGrantsTheUpgradeChecks(t *testing.T) {
	raw, err := os.ReadFile("../../charts/talos-operator/files/upgrade-check-rules.yaml")
	if err != nil {
		t.Fatal(err)
	}

	var granted struct {
		Rules []struct {
			APIGroups []string `yaml:"apiGroups"`
			Resources []string `yaml:"resources"`
			Verbs     []string `yaml:"verbs"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(raw, &granted); err != nil {
		t.Fatal(err)
	}

	var may []string

	for _, rule := range granted.Rules {
		if !slices.Equal(rule.Verbs, []string{"list"}) {
			t.Fatalf("the checks only list: %+v", rule)
		}

		for _, group := range rule.APIGroups {
			for _, resource := range rule.Resources {
				may = append(may, group+"/"+resource)
			}
		}
	}

	paths := removedAPIPaths(t)
	asks := make([]string, 0, len(paths))

	for _, path := range paths {
		// /apis/<group>/<version>/<resource>
		parts := strings.Split(strings.TrimPrefix(path, "/apis/"), "/")
		if len(parts) != 3 {
			t.Fatalf("unexpected path %q", path)
		}

		asks = append(asks, parts[0]+"/"+parts[2])
	}

	slices.Sort(may)
	slices.Sort(asks)

	if !slices.Equal(may, slices.Compact(asks)) {
		t.Fatalf("the chart grants lists of %v, the checks ask for %v", may, asks)
	}
}

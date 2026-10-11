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

package render_test

import (
	"net/netip"
	"slices"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/render"
)

const (
	argBind    = "bind-address"
	argMetrics = "listen-metrics-urls"
)

// The metrics of etcd, the controller manager and the scheduler are served where a Prometheus
// reaches them, in the form of config the contract has: a 1.14 config keeps the components in
// documents of their own and may not also name them in the v1alpha1 config, an older one has
// nothing else.
func TestControlPlaneMetricsAreSwitchedOnInTheFormTheContractHas(t *testing.T) {
	for _, tt := range []struct {
		name     string
		contract *config.VersionContract
		node     string
		etcd     string
	}{
		{"documents", config.TalosVersion1_14, "10.0.0.11", "http://10.0.0.11:2381"},
		{"v1alpha1", config.TalosVersion1_13, "10.0.0.11", "http://10.0.0.11:2381"},
		{"an IPv6 node", config.TalosVersion1_14, "fd00::11", "http://[fd00::11]:2381"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := input(t)
			in.Contract = tt.contract
			in.MetricsAddress = netip.MustParseAddr(tt.node)

			cfg, err := render.Generate(in)
			if err != nil {
				t.Fatal(err)
			}

			// etcd's metrics need no credentials: they are served on the node's address only.
			if got := cfg.Cluster().Etcd().ExtraArgs()[argMetrics]; !slices.Equal(got, []string{tt.etcd}) {
				t.Fatalf("etcd serves its metrics on %v, want %s", got, tt.etcd)
			}

			if got := cfg.K8sControllerManagerConfig().ExtraArgs()[argBind]; !slices.Equal(got, []string{"0.0.0.0"}) {
				t.Fatalf("the controller manager binds to %v", got)
			}

			if got := cfg.K8sSchedulerConfig().ExtraArgs()[argBind]; !slices.Equal(got, []string{"0.0.0.0"}) {
				t.Fatalf("the scheduler binds to %v", got)
			}

			// The components still run the image the config was generated with.
			if cfg.K8sControllerManagerConfig().Image() == "" || cfg.K8sSchedulerConfig().Image() == "" {
				t.Fatal("switching the metrics on lost a component's image")
			}

			plain := input(t)
			plain.Contract = tt.contract

			if mustRender(t, in).Hash == mustRender(t, plain).Hash {
				t.Fatal("the switch must change the config, or nothing would be rolled out")
			}
		})
	}
}

func TestControlPlaneMetricsLeaveTheUsersOwnSettingAlone(t *testing.T) {
	in := input(t, `cluster:
  etcd:
    extraArgs:
      listen-metrics-urls: http://127.0.0.1:2381
---
apiVersion: v1alpha1
kind: KubeSchedulerConfig
extraArgs:
  bind-address: 10.0.0.11
`)
	in.MetricsAddress = netip.MustParseAddr("10.0.0.11")

	cfg, err := render.Generate(in)
	if err != nil {
		t.Fatal(err)
	}

	if got := cfg.Cluster().Etcd().ExtraArgs()[argMetrics]; !slices.Equal(got, []string{"http://127.0.0.1:2381"}) {
		t.Fatalf("the user's layer must win for etcd: %v", got)
	}

	if got := cfg.K8sSchedulerConfig().ExtraArgs()[argBind]; !slices.Equal(got, []string{"10.0.0.11"}) {
		t.Fatalf("the user's layer must win for the scheduler: %v", got)
	}
}

// A worker runs none of the three.
func TestControlPlaneMetricsChangeNothingOnAWorker(t *testing.T) {
	plain := input(t)
	plain.Role = machine.TypeWorker

	switched := plain
	switched.MetricsAddress = netip.MustParseAddr("10.0.0.21")

	if mustRender(t, switched).Hash != mustRender(t, plain).Hash {
		t.Fatal("a worker's config must not change")
	}
}

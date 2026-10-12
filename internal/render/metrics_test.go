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
	"slices"
	"testing"

	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	"github.com/jarensaa/talos-operator/internal/render"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

const argBind = "bind-address"

// The metrics of the controller manager and the scheduler are served where a Prometheus reaches
// them, in the form of config the contract has: a 1.14 config keeps the components in documents
// of their own and may not also name them in the v1alpha1 config, an older one has nothing else.
func TestControlPlaneMetricsAreSwitchedOnInTheFormTheContractHas(t *testing.T) {
	for _, tt := range []struct {
		name     string
		contract *config.VersionContract
	}{
		{"documents", config.TalosVersion1_14},
		{"v1alpha1", config.TalosVersion1_13},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := talostest.Input(t)
			in.Contract = tt.contract
			in.ControlPlaneMetrics = true

			cfg, err := render.Generate(in)
			if err != nil {
				t.Fatal(err)
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

			// etcd takes a changed argument only when it is restarted, which no rollout does: a
			// config that named one would say something the node does not do.
			if len(cfg.Cluster().Etcd().ExtraArgs()) != 0 {
				t.Fatalf("etcd was given arguments: %v", cfg.Cluster().Etcd().ExtraArgs())
			}

			plain := talostest.Input(t)
			plain.Contract = tt.contract

			if mustRender(t, in).Hash == mustRender(t, plain).Hash {
				t.Fatal("the switch must change the config, or nothing would be rolled out")
			}
		})
	}
}

func TestControlPlaneMetricsLeaveTheUsersOwnSettingAlone(t *testing.T) {
	in := talostest.Input(t, `apiVersion: v1alpha1
kind: KubeSchedulerConfig
extraArgs:
  bind-address: 10.0.0.11
`)
	in.ControlPlaneMetrics = true

	cfg, err := render.Generate(in)
	if err != nil {
		t.Fatal(err)
	}

	if got := cfg.K8sSchedulerConfig().ExtraArgs()[argBind]; !slices.Equal(got, []string{"10.0.0.11"}) {
		t.Fatalf("the user's layer must win for the scheduler: %v", got)
	}

	if got := cfg.K8sControllerManagerConfig().ExtraArgs()[argBind]; !slices.Equal(got, []string{"0.0.0.0"}) {
		t.Fatalf("the controller manager binds to %v", got)
	}
}

// A worker runs neither.
func TestControlPlaneMetricsChangeNothingOnAWorker(t *testing.T) {
	plain := talostest.Input(t)
	plain.Role = machine.TypeWorker

	switched := plain
	switched.ControlPlaneMetrics = true

	if mustRender(t, switched).Hash != mustRender(t, plain).Hash {
		t.Fatal("a worker's config must not change")
	}
}

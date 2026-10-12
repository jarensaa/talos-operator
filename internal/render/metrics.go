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

package render

import (
	"github.com/siderolabs/talos/pkg/machinery/config"
)

// metricsPatch is the config that makes a control plane serve the metrics of the controller
// manager and the scheduler where a Prometheus in the cluster can reach them (docs/scope.md
// §14.2). Talos binds both to localhost. They check who asks, and their probes ask on localhost,
// so they listen on every interface.
//
// The components are configured where the contract has their config: in their own documents
// from 1.14 on, in the v1alpha1 config before. A config must not have both.
//
// etcd is not part of it. Its metrics take an argument too, and Talos hands etcd its arguments
// only when it starts it (internal/app/machined/pkg/system/services/etcd.go:111 in v1.14): a
// config that names another one changes nothing until etcd is restarted, which no rollout does.
func metricsPatch(contract *config.VersionContract) []byte {
	if contract.MultidocKubernetesConfigSupported() {
		return []byte(`apiVersion: v1alpha1
kind: KubeControllerManagerConfig
extraArgs:
  bind-address: 0.0.0.0
---
apiVersion: v1alpha1
kind: KubeSchedulerConfig
extraArgs:
  bind-address: 0.0.0.0
`)
	}

	return []byte(`cluster:
  controllerManager:
    extraArgs:
      bind-address: 0.0.0.0
  scheduler:
    extraArgs:
      bind-address: 0.0.0.0
`)
}

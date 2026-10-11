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
	"fmt"
	"net"
	"net/netip"
	"strconv"

	"github.com/siderolabs/talos/pkg/machinery/config"
)

// EtcdMetricsPort is where etcd serves its metrics in plain HTTP once a control plane is asked
// to expose them. Talos's own listeners are 2379 and 2380, and 2383 for what needs a client
// certificate.
const EtcdMetricsPort = 2381

// metricsPatch is the config that makes a control plane serve the metrics of etcd, the
// controller manager and the scheduler where a Prometheus in the cluster can reach them
// (docs/scope.md §14.2). Talos binds the two components to localhost, and serves etcd's metrics
// only to who holds an etcd client certificate.
//
// etcd's are served without authentication, so on the node's own address and no other. The
// components check who asks, and their probes ask on localhost: they listen everywhere.
//
// The components are configured where the contract has their config: in their own documents
// from 1.14 on, in the v1alpha1 config before. A config must not have both.
func metricsPatch(contract *config.VersionContract, node netip.Addr) []byte {
	url := "http://" + net.JoinHostPort(node.String(), strconv.Itoa(EtcdMetricsPort))

	if contract.MultidocKubernetesConfigSupported() {
		return fmt.Appendf(nil, `cluster:
  etcd:
    extraArgs:
      listen-metrics-urls: %s
---
apiVersion: v1alpha1
kind: KubeControllerManagerConfig
extraArgs:
  bind-address: 0.0.0.0
---
apiVersion: v1alpha1
kind: KubeSchedulerConfig
extraArgs:
  bind-address: 0.0.0.0
`, url)
	}

	return fmt.Appendf(nil, `cluster:
  etcd:
    extraArgs:
      listen-metrics-urls: %s
  controllerManager:
    extraArgs:
      bind-address: 0.0.0.0
  scheduler:
    extraArgs:
      bind-address: 0.0.0.0
`, url)
}

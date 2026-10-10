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

	"github.com/prometheus/client_golang/prometheus"
	clusterapi "github.com/siderolabs/talos/pkg/machinery/api/cluster"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// COSI resource API methods (github.com/cosi-project/runtime/api/v1alpha1).
const (
	cosiGetMethod   = "/cosi.resource.State/Get"
	cosiListMethod  = "/cosi.resource.State/List"
	cosiWatchMethod = "/cosi.resource.State/Watch"
)

// readOnlyMethods is the complete set of RPCs a Reader connection may issue. It is the second
// line of defence behind the Reader interface: even a bug that reached for the raw client
// cannot mutate a node in observe mode (docs/scope.md §8.1, invariant 6).
var readOnlyMethods = map[string]bool{
	machineapi.MachineService_Version_FullMethodName:        true,
	machineapi.MachineService_EtcdMemberList_FullMethodName: true,
	machineapi.MachineService_EtcdStatus_FullMethodName:     true,
	clusterapi.ClusterService_HealthCheck_FullMethodName:    true,
	cosiGetMethod:   true,
	cosiListMethod:  true,
	cosiWatchMethod: true,
}

var rejectedRPCs = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "talos_operator_rejected_rpcs_total",
	Help: "Talos API calls blocked by the read-only allowlist. Must stay 0 in production.",
}, []string{"method"})

func init() {
	metrics.Registry.MustRegister(rejectedRPCs)
}

// checkReadOnly allows allowlisted methods, and ApplyConfiguration only as a dry run in mode TRY:
// the adoption proof asks Talos itself to diff a config without applying it (docs/scope.md §7.1).
// Any other mode is refused even as a dry run, because Talos cancels a pending TRY rollback
// before it checks DryRun (v1alpha1_server.go:203-205 in Talos v1.14), which would disarm the
// rollback that protects an in-flight config change (§8.5).
func checkReadOnly(method string, req any) error {
	if readOnlyMethods[method] {
		return nil
	}

	if method == machineapi.MachineService_ApplyConfiguration_FullMethodName {
		if apply, ok := req.(*machineapi.ApplyConfigurationRequest); ok && apply.GetDryRun() &&
			apply.GetMode() == machineapi.ApplyConfigurationRequest_TRY {
			return nil
		}
	}

	rejectedRPCs.WithLabelValues(method).Inc()

	return status.Errorf(codes.PermissionDenied, "talos-operator: %s is not allowed on this connection", method)
}

// access is what a connection may do. Each level has its own connection, so the allowlist is fixed
// for the connection's lifetime.
type access int

const (
	// accessReadOnly allows readOnlyMethods and dry-run applies in mode TRY.
	accessReadOnly access = iota
	// accessConfig additionally allows applying machine config, in the modes checkConfig admits.
	accessConfig
)

func (a access) check(method string, req any) error {
	if a == accessConfig {
		return checkConfig(method, req)
	}

	return checkReadOnly(method, req)
}

// checkConfig is the allowlist of a ConfigWriter connection: everything a Reader may do, plus
// ApplyConfiguration in mode NO_REBOOT or TRY. AUTO, REBOOT and STAGED are refused: the operator
// never lets Talos decide to reboot, and never leaves a staged config behind. Nothing else that
// mutates a node (reboot, upgrade, reset, etcd membership) is reachable through it.
func checkConfig(method string, req any) error {
	if method == machineapi.MachineService_ApplyConfiguration_FullMethodName {
		if apply, ok := req.(*machineapi.ApplyConfigurationRequest); ok && !apply.GetDryRun() {
			switch apply.GetMode() { //nolint:exhaustive // every other mode is refused below
			case machineapi.ApplyConfigurationRequest_NO_REBOOT, machineapi.ApplyConfigurationRequest_TRY:
				return nil
			}
		}
	}

	return checkReadOnly(method, req)
}

func (a access) unaryInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		if err := a.check(method, req); err != nil {
			return err
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamInterceptor checks the method before the stream opens. No streaming RPC is conditionally
// allowed, so the request message does not need inspecting.
func (a access) streamInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if err := a.check(method, nil); err != nil {
			return nil, err
		}

		return streamer(ctx, desc, cc, method, opts...)
	}
}

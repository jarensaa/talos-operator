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
	machineapi.MachineService_Logs_FullMethodName:           true,
	machineapi.MachineService_Dmesg_FullMethodName:          true,
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
//
// MachineService.Read reads any file of a node, its secrets included. It is let through for one
// path: the firmware's Secure Boot db, which holds certificates and hashes and nothing secret
// (§7.3).
func checkReadOnly(method string, req any) error {
	if readOnlyMethods[method] {
		return nil
	}

	if method == machineapi.MachineService_Read_FullMethodName {
		if read, ok := req.(*machineapi.ReadRequest); ok && read.GetPath() == SecureBootDBPath {
			return nil
		}
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

// upgradeMethods are the RPCs an Upgrader connection may issue besides the read-only ones: pulling
// an image, installing it to the inactive boot slot, handing off etcd leadership, and rebooting.
// The deprecated MachineService.Upgrade is not among them, and neither is anything that resets a
// node, changes etcd membership or applies config.
var upgradeMethods = map[string]bool{
	machineapi.ImageService_Pull_FullMethodName:                    true,
	machineapi.LifecycleService_Upgrade_FullMethodName:             true,
	machineapi.MachineService_EtcdForfeitLeadership_FullMethodName: true,
	machineapi.MachineService_Reboot_FullMethodName:                true,
}

// checkUpgrade is the allowlist of an Upgrader connection.
func checkUpgrade(method string, req any) error {
	if upgradeMethods[method] {
		return nil
	}

	return checkReadOnly(method, req)
}

// access is what a connection may do. Each level has its own connection, so the allowlist is fixed
// for the connection's lifetime.
type access int

const (
	// accessReadOnly allows readOnlyMethods and dry-run applies in mode TRY.
	accessReadOnly access = iota
	// accessConfig additionally allows applying machine config, in the modes checkConfig admits.
	accessConfig
	// accessUpgrade additionally allows upgradeMethods.
	accessUpgrade
	// accessMembership additionally allows what a removal is made of (checkMembership).
	accessMembership
	// accessUpkeep additionally allows what the upkeep of etcd is made of (checkUpkeep).
	accessUpkeep
	// accessMaintenance is the connection to a machine in maintenance mode: it allows reading
	// resources and handing the machine its config, and nothing a Reader may do besides.
	accessMaintenance
)

func (a access) check(method string, req any) error {
	switch a {
	case accessConfig:
		return checkConfig(method, req)
	case accessUpgrade:
		return checkUpgrade(method, req)
	case accessMembership:
		return checkMembership(method, req)
	case accessUpkeep:
		return checkUpkeep(method, req)
	case accessMaintenance:
		return checkMaintenance(method, req)
	case accessReadOnly:
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

// requestDecides are the streaming RPCs that are allowed for some requests only. Their request is
// checked when it is sent, which is after the stream opened: nothing has reached the node's
// handler by then, and nothing does when the request is refused.
var requestDecides = map[string]bool{
	machineapi.MachineService_Read_FullMethodName: true,
}

// streamInterceptor checks the method before the stream opens, and for the RPCs in requestDecides
// the request before it is sent.
func (a access) streamInterceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if !requestDecides[method] {
			if err := a.check(method, nil); err != nil {
				return nil, err
			}

			return streamer(ctx, desc, cc, method, opts...)
		}

		// A stream whose request is refused is given up at once, not when its caller's context ends.
		ctx, cancel := context.WithCancel(ctx)

		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			cancel()

			return nil, err
		}

		return &checkedStream{ClientStream: stream, cancel: cancel, check: func(req any) error { return a.check(method, req) }}, nil
	}
}

// checkedStream sends only the requests its check lets through.
type checkedStream struct {
	grpc.ClientStream

	cancel context.CancelFunc
	check  func(req any) error
}

func (s *checkedStream) SendMsg(m any) error {
	if err := s.check(m); err != nil {
		s.cancel()

		return err
	}

	return s.ClientStream.SendMsg(m)
}

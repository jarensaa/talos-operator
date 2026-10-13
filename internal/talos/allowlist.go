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
	Help: "Talos API calls the allowlist of the operator's own connection refused. Must stay 0 in production.",
}, []string{"method"})

func init() {
	metrics.Registry.MustRegister(rejectedRPCs)
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
	// accessCount is how many kinds of access there are, and not one of them. A new kind goes
	// above it, and into allowlists.
	accessCount
)

// An allowlist says of one call whether its connection lets it through: nothing if it does, and
// otherwise why not. It only answers. Counting and refusing are access.check's, so that no way of
// refusing a call can go uncounted.
type allowlist func(method string, req any) string

// allowlists is the allowlist of every kind of access.
var allowlists = [accessCount]allowlist{
	accessReadOnly:    checkReadOnly,
	accessConfig:      checkConfig,
	accessUpgrade:     checkUpgrade,
	accessMembership:  checkMembership,
	accessUpkeep:      checkUpkeep,
	accessMaintenance: checkMaintenance,
}

// check is what the interceptors of a connection ask before a call leaves. A refused call is
// counted in talos_operator_rejected_rpcs_total and answered with PermissionDenied, whichever
// allowlist refused it and for whatever reason. An access without an allowlist allows nothing.
func (a access) check(method string, req any) error {
	why := "this connection has no allowlist, and so allows nothing"

	if a >= 0 && a < accessCount && allowlists[a] != nil {
		why = allowlists[a](method, req)
	}

	if why == "" {
		return nil
	}

	rejectedRPCs.WithLabelValues(method).Inc()

	return status.Error(codes.PermissionDenied, "talos-operator: "+why)
}

// notAllowed is the answer of an allowlist that has nothing more to say about a call.
func notAllowed(method string) string {
	return method + " is not allowed on this connection"
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
func checkReadOnly(method string, req any) string {
	if readOnlyMethods[method] {
		return ""
	}

	if method == machineapi.MachineService_Read_FullMethodName {
		if read, ok := req.(*machineapi.ReadRequest); ok && read.GetPath() == SecureBootDBPath {
			return ""
		}
	}

	if method == machineapi.MachineService_ApplyConfiguration_FullMethodName {
		if apply, ok := req.(*machineapi.ApplyConfigurationRequest); ok && apply.GetDryRun() &&
			apply.GetMode() == machineapi.ApplyConfigurationRequest_TRY {
			return ""
		}
	}

	return notAllowed(method)
}

// checkConfig is the allowlist of a ConfigWriter connection: everything a Reader may do, plus
// ApplyConfiguration in mode NO_REBOOT or TRY. AUTO, REBOOT and STAGED are refused: the operator
// never lets Talos decide to reboot, and never leaves a staged config behind. Nothing else that
// mutates a node (reboot, upgrade, reset, etcd membership) is reachable through it.
func checkConfig(method string, req any) string {
	if method == machineapi.MachineService_ApplyConfiguration_FullMethodName {
		if apply, ok := req.(*machineapi.ApplyConfigurationRequest); ok && !apply.GetDryRun() {
			switch apply.GetMode() { //nolint:exhaustive // every other mode is refused below
			case machineapi.ApplyConfigurationRequest_NO_REBOOT, machineapi.ApplyConfigurationRequest_TRY:
				return ""
			}
		}
	}

	return checkReadOnly(method, req)
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
func checkUpgrade(method string, req any) string {
	if upgradeMethods[method] {
		return ""
	}

	return checkReadOnly(method, req)
}

// dialOptions puts the allowlist on a connection: what the access does not allow never leaves.
func (a access) dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(a.unaryInterceptor()),
		grpc.WithChainStreamInterceptor(a.streamInterceptor()),
	}
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

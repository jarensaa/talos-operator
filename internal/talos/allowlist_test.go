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
	"slices"
	"strings"
	"testing"

	cosiv1alpha1 "github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	clusterapi "github.com/siderolabs/talos/pkg/machinery/api/cluster"
	inspectapi "github.com/siderolabs/talos/pkg/machinery/api/inspect"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	securityapi "github.com/siderolabs/talos/pkg/machinery/api/security"
	signerapi "github.com/siderolabs/talos/pkg/machinery/api/signer"
	storageapi "github.com/siderolabs/talos/pkg/machinery/api/storage"
	timeapi "github.com/siderolabs/talos/pkg/machinery/api/time"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// allowCase is one call put to the allowlist of one kind of connection.
type allowCase struct {
	name   string
	method string
	req    any
	allow  bool
}

// checkAllowlist puts every case to the allowlist of a. What is refused is refused with
// PermissionDenied and counted, once; what is allowed is not counted.
func checkAllowlist(t *testing.T, a access, tests []allowCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := testutil.ToFloat64(rejectedRPCs.WithLabelValues(tt.method))

			err := a.check(tt.method, tt.req)

			counted := testutil.ToFloat64(rejectedRPCs.WithLabelValues(tt.method)) - before

			if tt.allow {
				if err != nil {
					t.Fatalf("expected allowed, got %v", err)
				}

				if counted != 0 {
					t.Fatal("allowed call must not count as rejected")
				}

				return
			}

			if status.Code(err) != codes.PermissionDenied {
				t.Fatalf("expected PermissionDenied, got %v", err)
			}

			if counted != 1 {
				t.Fatalf("a rejected call must be counted once, counted %v", counted)
			}
		})
	}
}

// apply is a request to apply a machine config.
func apply(mode machineapi.ApplyConfigurationRequest_Mode, dryRun bool) *machineapi.ApplyConfigurationRequest {
	return &machineapi.ApplyConfigurationRequest{Mode: mode, DryRun: dryRun}
}

// dial is how a connection with access a is opened: with its allowlist on it.
func dial(a access) []grpc.DialOption { return a.dialOptions() }

// accessNames names every kind of access, for what the tests below say. A kind without a name
// fails them: it was added without being put through them.
var accessNames = [accessCount]string{
	accessReadOnly:    "read-only",
	accessConfig:      "config",
	accessUpgrade:     "upgrade",
	accessMembership:  "membership",
	accessUpkeep:      "upkeep",
	accessMaintenance: "maintenance",
}

// everyAccess is every kind of access there is. It follows the enum, so a kind that is added is
// put through the matrix below without anybody remembering to.
func everyAccess(t *testing.T) []access {
	t.Helper()

	all := make([]access, 0, accessCount)

	for a := range accessCount {
		if accessNames[a] == "" || allowlists[a] == nil {
			t.Fatalf("access %d has no name in accessNames or no allowlist in allowlists", a)
		}

		all = append(all, a)
	}

	return all
}

func names(kinds []access) string {
	out := make([]string, 0, len(kinds))
	for _, a := range kinds {
		out = append(out, accessNames[a])
	}

	if len(out) == 0 {
		return "none"
	}

	return strings.Join(out, ", ")
}

// The connections that carry the cluster's credentials: every one but the maintenance connection.
var credentialed = []access{accessReadOnly, accessConfig, accessUpgrade, accessMembership, accessUpkeep}

// allowedOn is the specification of the allowlists: every call any connection lets through, and
// exactly the connections that do. Every other connection must refuse it, and a call that is not
// here must be refused by all of them (TestNoConnectionAllowsWhatIsNotSpecified).
//
// A kind of access that is added allows nothing here until it is written into a row.
var allowedOn = []struct {
	name   string
	method string
	req    any
	on     []access
}{
	// What only reads. A machine in maintenance mode is asked for its resources and nothing else.
	{"reading a resource", cosiGetMethod, nil, append(slices.Clone(credentialed), accessMaintenance)},
	{"listing resources", cosiListMethod, nil, append(slices.Clone(credentialed), accessMaintenance)},
	{"watching resources", cosiWatchMethod, nil, credentialed},
	{"the version", machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{}, credentialed},
	{"etcd's members", machineapi.MachineService_EtcdMemberList_FullMethodName, &machineapi.EtcdMemberListRequest{}, credentialed},
	{"etcd's status", machineapi.MachineService_EtcdStatus_FullMethodName, &emptypb.Empty{}, credentialed},
	{"a service's log", machineapi.MachineService_Logs_FullMethodName, &machineapi.LogsRequest{}, credentialed},
	{"the kernel's log", machineapi.MachineService_Dmesg_FullMethodName, &machineapi.DmesgRequest{}, credentialed},
	{"the cluster's health check", clusterapi.ClusterService_HealthCheck_FullMethodName, &clusterapi.HealthCheckRequest{}, credentialed},
	// Read reads any file of a node, its secrets included. The firmware's Secure Boot db, which
	// holds none, is the one path it is let through for.
	{"reading the Secure Boot db", machineapi.MachineService_Read_FullMethodName, &machineapi.ReadRequest{Path: SecureBootDBPath},
		credentialed},
	{"reading another firmware variable", machineapi.MachineService_Read_FullMethodName,
		&machineapi.ReadRequest{Path: "/sys/firmware/efi/efivars/PK-8be4df61-93ca-11d2-aa0d-00e098032b8c"}, nil},
	{"reading a file that holds secrets", machineapi.MachineService_Read_FullMethodName,
		&machineapi.ReadRequest{Path: "/system/state/config.yaml"}, nil},
	{"reading without a request", machineapi.MachineService_Read_FullMethodName, nil, nil},
	// The one apply that changes nothing: a dry run, in the one mode that leaves a pending try alone.
	{"a dry run in try mode", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_TRY, true), credentialed},

	// Applying config is the config connection's. A machine in maintenance mode takes a config from
	// anyone, without a reboot: the connections that carry the cluster's credentials must not be
	// the ones to hand it over, and a try would be taken back to no config at all.
	{"an apply without a reboot", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_NO_REBOOT, false), []access{accessConfig, accessMaintenance}},
	{"a try", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_TRY, false), []access{accessConfig}},
	// Talos would decide about a reboot itself, or leave a config staged for the next boot; and a
	// dry run in any mode but try cancels a pending try's rollback.
	{"an apply that lets Talos decide", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_AUTO, false), nil},
	{"a staged apply", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_STAGED, false), nil},
	//nolint:staticcheck // the deprecated mode must stay refused
	{"an apply with a reboot", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_REBOOT, false), nil},
	{"a dry run without a reboot", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_NO_REBOOT, true), nil},
	{"a dry run that lets Talos decide", machineapi.MachineService_ApplyConfiguration_FullMethodName,
		apply(machineapi.ApplyConfigurationRequest_AUTO, true), nil},
	{"an apply without a request", machineapi.MachineService_ApplyConfiguration_FullMethodName, nil, nil},

	// What an upgrade is made of.
	{"pulling an image", machineapi.ImageService_Pull_FullMethodName, &machineapi.ImageServicePullRequest{}, []access{accessUpgrade}},
	{"installing to the inactive slot", machineapi.LifecycleService_Upgrade_FullMethodName,
		&machineapi.LifecycleServiceUpgradeRequest{}, []access{accessUpgrade}},
	{"a reboot", machineapi.MachineService_Reboot_FullMethodName, &machineapi.RebootRequest{}, []access{accessUpgrade}},

	// A control plane hands off etcd leadership before it reboots, before it leaves, and before it
	// is defragmented: three connections, each for its own reason.
	{"forfeiting etcd leadership", machineapi.MachineService_EtcdForfeitLeadership_FullMethodName,
		&machineapi.EtcdForfeitLeadershipRequest{}, []access{accessUpgrade, accessMembership, accessUpkeep}},

	// What a removal is made of: its one reset, and no other.
	{"the reset of a removal", machineapi.MachineService_Reset_FullMethodName, reset(nil), []access{accessMembership}},
	{"removing an etcd member", machineapi.MachineService_EtcdRemoveMemberByID_FullMethodName,
		&machineapi.EtcdRemoveMemberByIDRequest{}, []access{accessMembership}},
	{"a reset of the whole system disk", machineapi.MachineService_Reset_FullMethodName,
		reset(func(r *machineapi.ResetRequest) { r.SystemPartitionsToWipe = nil }), nil},
	{"a reset that is not graceful", machineapi.MachineService_Reset_FullMethodName,
		reset(func(r *machineapi.ResetRequest) { r.Graceful = false }), nil},
	{"a reset without a request", machineapi.MachineService_Reset_FullMethodName, nil, nil},

	// What the upkeep of etcd is made of. Defragmenting takes a member away for a moment.
	{"defragmenting etcd", machineapi.MachineService_EtcdDefragment_FullMethodName, &emptypb.Empty{}, []access{accessUpkeep}},
	// No connection lists or disarms an alarm of etcd: one that is disarmed without its cause being
	// gone is one that is hidden (docs/scope.md §12.27).
	{"listing etcd's alarms", machineapi.MachineService_EtcdAlarmList_FullMethodName, &emptypb.Empty{}, nil},
	{"disarming etcd's alarms", machineapi.MachineService_EtcdAlarmDisarm_FullMethodName, &emptypb.Empty{}, nil},
}

// CLAUDE.md asks of every mutating call a test that the connections it is not for refuse it. This
// is that test, for all of them and for every kind of connection there is.
func TestEveryCallIsAllowedOnExactlyTheConnectionsItIsFor(t *testing.T) {
	all := everyAccess(t)

	for _, tt := range allowedOn {
		t.Run(tt.name, func(t *testing.T) {
			var got []access

			for _, a := range all {
				err := a.check(tt.method, tt.req)

				switch {
				case err == nil:
					got = append(got, a)
				case status.Code(err) != codes.PermissionDenied:
					t.Fatalf("a %s connection must refuse with PermissionDenied, got %v", accessNames[a], err)
				}
			}

			want := slices.Clone(tt.on)
			slices.Sort(want)

			if !slices.Equal(got, want) {
				t.Fatalf("%s is allowed on these connections: %s; it must be allowed on exactly these: %s",
					tt.method, names(got), names(want))
			}
		})
	}
}

// talosAPI is every method of the Talos API, and of the resource API beside it.
func talosAPI() []string {
	var methods []string

	for _, service := range []grpc.ServiceDesc{
		machineapi.MachineService_ServiceDesc, machineapi.ImageService_ServiceDesc, machineapi.LifecycleService_ServiceDesc,
		machineapi.LVMService_ServiceDesc, machineapi.MDService_ServiceDesc, machineapi.DebugService_ServiceDesc,
		clusterapi.ClusterService_ServiceDesc, storageapi.StorageService_ServiceDesc, inspectapi.InspectService_ServiceDesc,
		timeapi.TimeService_ServiceDesc, securityapi.SecurityService_ServiceDesc, signerapi.SignerService_ServiceDesc,
		cosiv1alpha1.State_ServiceDesc,
	} {
		for _, m := range service.Methods {
			methods = append(methods, "/"+service.ServiceName+"/"+m.MethodName)
		}

		for _, s := range service.Streams {
			methods = append(methods, "/"+service.ServiceName+"/"+s.StreamName)
		}
	}

	return methods
}

// The matrix above is complete: of everything the Talos API offers, no connection lets a call
// through that the matrix does not name. A method added to an allowlist without a row fails here.
func TestNoConnectionAllowsWhatIsNotSpecified(t *testing.T) {
	specified := map[string]bool{}
	for _, tt := range allowedOn {
		specified[tt.method] = true
	}

	api := talosAPI()

	// The names the matrix is written with are names the API has, so the API listed here is the one
	// the allowlists are about.
	for method := range specified {
		if !slices.Contains(api, method) {
			t.Errorf("%s is in the matrix and is not a method of the Talos API", method)
		}
	}

	for _, method := range api {
		if specified[method] {
			continue
		}

		for _, a := range everyAccess(t) {
			if status.Code(a.check(method, nil)) != codes.PermissionDenied {
				t.Errorf("a %s connection allows %s, which the matrix does not name", accessNames[a], method)
			}
		}
	}
}

// talos_operator_rejected_rpcs_total is every call an allowlist refused: whichever connection it
// was on, and whichever of the reasons it was refused for.
func TestEveryRefusalIsCounted(t *testing.T) {
	refused := func(t *testing.T, a access, method string, req any, why string) {
		t.Helper()

		before := testutil.ToFloat64(rejectedRPCs.WithLabelValues(method))

		err := a.check(method, req)
		if status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), why) {
			t.Fatalf("expected PermissionDenied saying %q, got %v", why, err)
		}

		if counted := testutil.ToFloat64(rejectedRPCs.WithLabelValues(method)) - before; counted != 1 {
			t.Fatalf("the refusal must be counted once, counted %v", counted)
		}
	}

	bootstrap := machineapi.MachineService_Bootstrap_FullMethodName

	for _, a := range everyAccess(t) {
		t.Run(accessNames[a], func(t *testing.T) {
			refused(t, a, bootstrap, &machineapi.BootstrapRequest{}, bootstrap+" is not allowed on")
		})
	}

	// The refusals that say more than that, and used to go uncounted.
	t.Run("a reset that is not a removal's", func(t *testing.T) {
		refused(t, accessMembership, machineapi.MachineService_Reset_FullMethodName, &machineapi.ResetRequest{Graceful: true, Reboot: true},
			"this reset is not the one a removal is allowed")
	})

	t.Run("what a machine in maintenance mode is not asked", func(t *testing.T) {
		refused(t, accessMaintenance, machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{},
			"is not allowed on a maintenance connection")
	})

	// A kind of access nobody wrote an allowlist for allows nothing, and says so.
	t.Run("an access without an allowlist", func(t *testing.T) {
		refused(t, accessCount, machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{}, "no allowlist")
	})
}

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
	"slices"
	"strings"
	"testing"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/nethelpers"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	timeres "github.com/siderolabs/talos/pkg/machinery/resources/time"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestCheckUpkeep(t *testing.T) {
	checkAllowlist(t, accessUpkeep, []allowCase{
		{"defragment", machineapi.MachineService_EtcdDefragment_FullMethodName, &emptypb.Empty{}, true},
		{"forfeit leadership", machineapi.MachineService_EtcdForfeitLeadership_FullMethodName, &machineapi.EtcdForfeitLeadershipRequest{}, true},
		{"reading logs", machineapi.MachineService_Logs_FullMethodName, nil, true},
		// An alarm that is disarmed without its cause being gone is an alarm that is hidden. And
		// nothing here has a use for the list of them: an operator inside the cluster can write
		// nothing down while etcd has one raised (docs/scope.md §14.6).
		{"listing alarms", machineapi.MachineService_EtcdAlarmList_FullMethodName, &emptypb.Empty{}, false},
		{"disarming alarms", machineapi.MachineService_EtcdAlarmDisarm_FullMethodName, &emptypb.Empty{}, false},
		{"removing a member", machineapi.MachineService_EtcdRemoveMemberByID_FullMethodName, &machineapi.EtcdRemoveMemberByIDRequest{}, false},
		{"leaving etcd", machineapi.MachineService_EtcdLeaveCluster_FullMethodName, &machineapi.EtcdLeaveClusterRequest{}, false},
		{"a reset", machineapi.MachineService_Reset_FullMethodName, reset(nil), false},
		{"a reboot", machineapi.MachineService_Reboot_FullMethodName, &machineapi.RebootRequest{}, false},
		{"apply", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			&machineapi.ApplyConfigurationRequest{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT}, false},
	})
}

func TestMachineryReadsWhatTalosSaysAboutANode(t *testing.T) {
	api, c := serve(t, dial(accessReadOnly)...)
	r := reader(c)
	ctx := context.Background()

	warning := runtime.NewDiagnostic(runtime.NamespaceName, "address-overlap")
	warning.TypedSpec().Message = "host and Kubernetes pod/service CIDR addresses overlap"
	create(t, api, warning)

	clock := timeres.NewStatus()
	clock.TypedSpec().Synced = false
	create(t, api, clock)

	link := func(name string, index uint32, change func(*network.LinkStatusSpec)) {
		res := network.NewLinkStatus(network.NamespaceName, name)
		res.TypedSpec().Type, res.TypedSpec().Index = nethelpers.LinkEther, index
		res.TypedSpec().OperationalState = nethelpers.OperStateUp
		change(res.TypedSpec())
		create(t, api, res)
	}
	link("bond0", 9, func(s *network.LinkStatusSpec) { s.Kind = "bond" })
	link("eth0", 2, func(s *network.LinkStatusSpec) { s.SpeedMegabits, s.MasterIndex, s.SlaveKind = 10000, 9, "bond" })
	link("eth1", 3, func(s *network.LinkStatusSpec) {
		s.OperationalState, s.MasterIndex, s.SlaveKind = nethelpers.OperStateDown, 9, "bond"
	})
	link("lo", 1, func(s *network.LinkStatusSpec) { s.Type = nethelpers.LinkLoopbck })

	volume := func(name string, phase block.VolumePhase, message string) {
		res := block.NewVolumeStatus(block.NamespaceName, name)
		res.TypedSpec().Phase, res.TypedSpec().ErrorMessage = phase, message
		create(t, api, res)
	}
	volume("STATE", block.VolumePhaseReady, "")
	volume("EPHEMERAL", block.VolumePhaseFailed, "no space left on the disk")

	diagnostics, err := r.Diagnostics(ctx, apiNode)
	if err != nil || len(diagnostics) != 1 || diagnostics[0].ID != "address-overlap" || !strings.Contains(diagnostics[0].Message, "overlap") {
		t.Fatalf("diagnostics: got %+v, %v", diagnostics, err)
	}

	if synced, err := r.TimeSynced(ctx, apiNode); err != nil || synced {
		t.Fatalf("time: got %t, %v", synced, err)
	}

	// A bond is as healthy as its members: only what is a physical link is listed, with the bond
	// it belongs to.
	links, err := r.Links(ctx, apiNode)
	if err != nil || !slices.Equal(links, []Link{{Name: "eth0", Up: true, SpeedMbps: 10000, Bond: "bond0"}, {Name: "eth1", Bond: "bond0"}}) {
		t.Fatalf("links: got %+v, %v", links, err)
	}

	volumes, err := r.Volumes(ctx, apiNode)
	want := []Volume{{Name: "EPHEMERAL", Phase: "failed", Error: "no space left on the disk"}, {Name: "STATE", Phase: VolumeReady}}

	if err != nil || !slices.Equal(volumes, want) {
		t.Fatalf("volumes: got %+v, %v", volumes, err)
	}
}

func TestMachineryReadsTheEndOfLogs(t *testing.T) {
	api, c := serve(t, dial(accessReadOnly)...)
	r := reader(c)
	ctx := context.Background()

	api.logs = map[string][]string{
		"kubelet": {"first line\nsecond", " line\n"},
		"dmesg":   {"kern: info: booted\n"},
		// More than is kept: the end is what says what went wrong.
		"etcd": {strings.Repeat("old line\n", maxLog/9), strings.Repeat("x", maxLog/2) + "\n", "the last line\n"},
	}

	if got, err := r.Logs(ctx, apiNode, "kubelet", 100); err != nil || string(got) != "first line\nsecond line\n" {
		t.Fatalf("got %q, %v", got, err)
	}

	if got, err := r.Dmesg(ctx, apiNode); err != nil || string(got) != "kern: info: booted\n" {
		t.Fatalf("got %q, %v", got, err)
	}

	got, err := r.Logs(ctx, apiNode, "etcd", 100)
	if err != nil || len(got) > maxLog || !strings.HasSuffix(string(got), "the last line\n") || strings.HasPrefix(string(got), "ld line") {
		t.Fatalf("got %d bytes ending %q, %v", len(got), got[max(0, len(got)-20):], err)
	}

	// Talos records every call it answers. The reads that succeeded, every observation of every
	// node, take no room: what happened at the failure stays, however many came after it.
	read := "2026/10/18 18:51:00 OK [/cosi.resource.State/List] 2.1ms stream Success (:authority=10.5.0.2:50000)\n"
	apply := "2026/10/18 18:46:26 OK [/machine.MachineService/ApplyConfiguration] 120ms unary Success (:authority=10.5.0.2:50000)\n"
	refused := "2026/10/18 18:52:00 PermissionDenied [/cosi.resource.State/Get] 1ms unary not allowed (:authority=10.5.0.2:50000)\n"
	failure := "2026/10/18 18:46:27 service[kubelet](Failed): Failed running service\n"
	api.logs["machined"] = []string{apply, failure, strings.Repeat(read, 2*maxLog/len(read)), refused}

	got, err = r.Logs(ctx, apiNode, "machined", -1)
	if err != nil || string(got) != apply+failure+refused {
		t.Fatalf("the answered reads must be left out and everything else kept, got %d bytes: %q, %v", len(got), got[:min(len(got), 300)], err)
	}

	// Reading is all a Reader's connection does about etcd.
	if _, err := c.EtcdDefragment(client.WithNode(ctx, apiNode.String())); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a read-only connection must refuse to defragment, got %v", err)
	}

	if _, err := c.EtcdAlarmDisarm(client.WithNode(ctx, apiNode.String())); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a read-only connection must refuse to disarm alarms, got %v", err)
	}
}

func TestKeeperDefragmentsTheMemberItIsAskedFor(t *testing.T) {
	api, c := serve(t, dial(accessUpkeep)...)
	keeper := &machineryKeeper{forfeiter{*reader(c)}}
	ctx := context.Background()

	if err := keeper.DefragmentEtcd(ctx, apiNode); err != nil || api.defrags != 1 {
		t.Fatalf("got %d defragmentations, %v", api.defrags, err)
	}

	if got := api.nodes[machineapi.MachineService_EtcdDefragment_FullMethodName]; !slices.Equal(got, []string{apiNode.String()}) {
		t.Fatalf("the defragmentation was addressed to %v", got)
	}

	if _, err := c.EtcdAlarmDisarm(client.WithNode(ctx, apiNode.String())); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("the upkeep connection must refuse to disarm alarms, got %v", err)
	}

	if err := c.Reboot(client.WithNode(ctx, apiNode.String())); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("the upkeep connection must refuse a reboot, got %v", err)
	}
}

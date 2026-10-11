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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"slices"
	"strings"
	"testing"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/hardware"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// reset is the reset of a removal, with one thing changed.
func reset(change func(*machineapi.ResetRequest)) *machineapi.ResetRequest {
	req := &machineapi.ResetRequest{
		Graceful: true,
		Reboot:   true,
		Mode:     machineapi.ResetRequest_SYSTEM_DISK,
		SystemPartitionsToWipe: []*machineapi.ResetPartitionSpec{
			{Label: "STATE", Wipe: true}, {Label: "EPHEMERAL", Wipe: true},
		},
	}

	if change != nil {
		change(req)
	}

	return req
}

// A removal wipes what Talos keeps of the cluster on the machine, and nothing that is the
// owner's: the connection itself refuses a reset in any other shape.
func TestCheckMembership(t *testing.T) {
	tests := []struct {
		name   string
		method string
		req    any
		allow  bool
	}{
		{"the reset of a removal", machineapi.MachineService_Reset_FullMethodName, reset(nil), true},
		{"forfeit leadership", machineapi.MachineService_EtcdForfeitLeadership_FullMethodName, &machineapi.EtcdForfeitLeadershipRequest{}, true},
		{"remove an etcd member", machineapi.MachineService_EtcdRemoveMemberByID_FullMethodName, &machineapi.EtcdRemoveMemberByIDRequest{}, true},
		{"reading the version", machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{}, true},

		// talosctl's default: the whole system disk, after which the machine does not boot.
		{"a reset of the whole system disk", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.SystemPartitionsToWipe = nil }), false},
		{"a reset in mode all", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.Mode = machineapi.ResetRequest_ALL }), false},
		{"a reset of user disks only", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.Mode = machineapi.ResetRequest_USER_DISKS }), false},
		{"a reset that names a user disk", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.UserDisksToWipe = []string{"/dev/sdb"} }), false},
		{"a reset that also wipes the boot partition", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) {
				r.SystemPartitionsToWipe = append(r.SystemPartitionsToWipe, &machineapi.ResetPartitionSpec{Label: "BOOT", Wipe: true})
			}), false},
		{"a reset that leaves the state", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.SystemPartitionsToWipe = r.SystemPartitionsToWipe[1:] }), false},
		{"a reset that names a partition without wiping it", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.SystemPartitionsToWipe[0].Wipe = false }), false},
		// A control plane that is not asked to leave etcd stays a member that never answers.
		{"a reset that is not graceful", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.Graceful = false }), false},
		// Only a machine that comes back can be seen to have forgotten the cluster.
		{"a reset that powers the machine off", machineapi.MachineService_Reset_FullMethodName,
			reset(func(r *machineapi.ResetRequest) { r.Reboot = false }), false},
		{"a reset without a request", machineapi.MachineService_Reset_FullMethodName, nil, false},

		{"apply", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			&machineapi.ApplyConfigurationRequest{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT}, false},
		{"a reboot", machineapi.MachineService_Reboot_FullMethodName, &machineapi.RebootRequest{}, false},
		{"shutdown", machineapi.MachineService_Shutdown_FullMethodName, &machineapi.ShutdownRequest{}, false},
		{"install", machineapi.LifecycleService_Upgrade_FullMethodName, &machineapi.LifecycleServiceUpgradeRequest{}, false},
		{"pull", machineapi.ImageService_Pull_FullMethodName, &machineapi.ImageServicePullRequest{}, false},
		// A graceful reset leaves etcd by itself; nothing else has a reason to.
		{"etcd leave", machineapi.MachineService_EtcdLeaveCluster_FullMethodName, &machineapi.EtcdLeaveClusterRequest{}, false},
		{"wipe a block device", "/storage.StorageService/BlockDeviceWipe", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := accessMembership.check(tt.method, tt.req)
			if tt.allow && err != nil {
				t.Fatalf("expected allowed, got %v", err)
			}

			if !tt.allow && status.Code(err) != codes.PermissionDenied {
				t.Fatalf("expected PermissionDenied, got %v", err)
			}
		})
	}

	// And what a removal may do, the other connections may not: not even its one reset.
	for _, a := range []access{accessReadOnly, accessConfig, accessUpgrade, accessMaintenance} {
		for method, req := range map[string]any{
			machineapi.MachineService_Reset_FullMethodName:                reset(nil),
			machineapi.MachineService_EtcdRemoveMemberByID_FullMethodName: &machineapi.EtcdRemoveMemberByIDRequest{},
		} {
			if status.Code(a.check(method, req)) != codes.PermissionDenied {
				t.Fatalf("%s must be refused on a connection with access %d", method, a)
			}
		}
	}
}

func TestCheckMaintenance(t *testing.T) {
	apply := func(m machineapi.ApplyConfigurationRequest_Mode, dryRun bool) *machineapi.ApplyConfigurationRequest {
		return &machineapi.ApplyConfigurationRequest{Mode: m, DryRun: dryRun}
	}

	tests := []struct {
		name   string
		method string
		req    any
		allow  bool
	}{
		{"reading a resource", cosiGetMethod, nil, true},
		{"listing resources", cosiListMethod, nil, true},
		{"handing over the config", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			apply(machineapi.ApplyConfigurationRequest_NO_REBOOT, false), true},

		// A try on a machine without a config would be taken back to no config at all.
		{"a try", machineapi.MachineService_ApplyConfiguration_FullMethodName, apply(machineapi.ApplyConfigurationRequest_TRY, false), false},
		{"a staged config", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			apply(machineapi.ApplyConfigurationRequest_STAGED, false), false},
		{"letting Talos decide", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			apply(machineapi.ApplyConfigurationRequest_AUTO, false), false},
		{"apply without a request", machineapi.MachineService_ApplyConfiguration_FullMethodName, nil, false},
		// What a machine in maintenance mode lets anyone do, and the operator has no use for.
		{"wipe a block device", "/storage.StorageService/BlockDeviceWipe", nil, false},
		{"watching resources", cosiWatchMethod, nil, false},
		{"a reset", machineapi.MachineService_Reset_FullMethodName, reset(nil), false},
		{"a reboot", machineapi.MachineService_Reboot_FullMethodName, &machineapi.RebootRequest{}, false},
		{"install", machineapi.LifecycleService_Install_FullMethodName, &machineapi.LifecycleServiceInstallRequest{}, false},
		{"the version", machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := accessMaintenance.check(tt.method, tt.req)
			if tt.allow && err != nil {
				t.Fatalf("expected allowed, got %v", err)
			}

			if !tt.allow && status.Code(err) != codes.PermissionDenied {
				t.Fatalf("expected PermissionDenied, got %v", err)
			}
		})
	}

	// A machine in maintenance mode takes a config from anyone. The connections that carry the
	// cluster's credentials must not be the ones to hand it over.
	for _, a := range []access{accessReadOnly, accessUpgrade, accessMembership} {
		req := apply(machineapi.ApplyConfigurationRequest_NO_REBOOT, false)
		if status.Code(a.check(machineapi.MachineService_ApplyConfiguration_FullMethodName, req)) != codes.PermissionDenied {
			t.Fatalf("an apply must be refused on a connection with access %d", a)
		}
	}
}

func dial(a access) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(a.unaryInterceptor()),
		grpc.WithChainStreamInterceptor(a.streamInterceptor()),
	}
}

// What the operator really sends when a machine leaves.
func TestRemoverSendsTheResetOfARemovalAndNothingElse(t *testing.T) {
	api, c := serve(t, dial(accessMembership)...)
	remover := &machineryRemover{*reader(c)}
	ctx := context.Background()

	if err := remover.Reset(ctx, apiNode); err != nil {
		t.Fatal(err)
	}

	if len(api.resets) != 1 || !removalReset(api.resets[0]) {
		t.Fatalf("got %+v", api.resets)
	}

	if got := api.nodes[machineapi.MachineService_Reset_FullMethodName]; !slices.Equal(got, []string{apiNode.String()}) {
		t.Fatalf("the reset was addressed to %v", got)
	}

	if err := remover.RemoveEtcdMember(ctx, apiNode, 0xabc); err != nil || !slices.Equal(api.removals, []uint64{0xabc}) {
		t.Fatalf("got %v, %v", api.removals, err)
	}

	// A member that is gone already is what was asked for.
	api.removeErr = status.Error(codes.NotFound, "member not found")

	if err := remover.RemoveEtcdMember(ctx, apiNode, 0xabc); err != nil {
		t.Fatalf("a member that is not there must not be an error: %v", err)
	}

	api.removeErr = status.Error(codes.Unavailable, "no leader")

	if err := remover.RemoveEtcdMember(ctx, apiNode, 0xabc); err == nil {
		t.Fatal("a removal that failed must say so")
	}

	// The connection lets no other reset through, whoever builds the request.
	whole := &machineapi.ResetRequest{Graceful: true, Reboot: true}
	if err := c.ResetGeneric(client.WithNode(ctx, apiNode.String()), whole); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a reset of the whole system disk must be refused, got %v", err)
	}

	if err := c.Reboot(client.WithNode(ctx, apiNode.String())); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a removal connection must refuse a reboot, got %v", err)
	}

	if len(api.resets) != 1 {
		t.Fatalf("nothing else may have reached the node: %+v", api.resets)
	}
}

const (
	candidateUUID = "aaaaaaaa-bbbb-4ccc-8ddd-000000000009"
	// selector is the install disk selector the operator's own rendering produces.
	installConfig = "apiVersion: v1alpha1\nkind: UnattendedInstallConfig\ninstaller:\n  image: factory.example.com/installer:v1.14.2\n" +
		"provisioning:\n  diskSelector:\n    match: %s\n"
	linkConfig = "apiVersion: v1alpha1\nkind: LinkConfig\nname: %s\nup: true\n"
)

func candidate(t *testing.T, disks ...string) (*testAPI, *machineryMaintenance) {
	t.Helper()

	api, c := serve(t, dial(accessMaintenance)...)

	machine := runtime.NewMachineStatus()
	machine.TypedSpec().Stage = runtime.MachineStageMaintenance
	machine.TypedSpec().Status.Ready = true
	create(t, api, machine)

	info := hardware.NewSystemInformation(hardware.SystemInformationID)
	info.TypedSpec().UUID = candidateUUID
	create(t, api, info)

	for _, name := range disks {
		disk := block.NewDisk(block.NamespaceName, name)
		disk.TypedSpec().DevPath = "/dev/" + name
		disk.TypedSpec().Size = 100e9
		disk.TypedSpec().Transport = "virtio"
		create(t, api, disk)
	}

	create(t, api, network.NewLinkStatus(network.NamespaceName, "eth0"))

	return api, &machineryMaintenance{machineryReader: *reader(c), addr: apiNode}
}

func rendered(documents ...string) []byte {
	return []byte(strings.Join(append([]string{strings.Replace(workerConfig, "%s", "new", 1)}, documents...), "---\n"))
}

func TestMaintenanceReadsTheMachineAndHandsItItsConfig(t *testing.T) {
	api, m := candidate(t, "vda")
	ctx := context.Background()

	got, err := m.Probe(ctx)
	if err != nil || got != (Candidate{Stage: StageMaintenance, UUID: candidateUUID}) {
		t.Fatalf("got %+v, %v", got, err)
	}

	cfg := rendered(strings.Replace(installConfig, "%s", `disk.dev_path == "/dev/vda"`, 1))
	if err := m.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}

	if len(api.applies) != 1 || api.applies[0].GetMode() != machineapi.ApplyConfigurationRequest_NO_REBOOT ||
		api.applies[0].GetDryRun() || string(api.applies[0].GetData()) != string(cfg) {
		t.Fatalf("got %+v", api.applies)
	}

	// The connection is good for that and for nothing else.
	if err := m.client.Reboot(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a maintenance connection must refuse a reboot, got %v", err)
	}

	if _, err := m.client.Version(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a maintenance connection has no use for the version, got %v", err)
	}
}

// Talos installs to the first disk a selector matches and says nothing about the others, so a
// selector has to match exactly one before the machine is given a config that carries it.
func TestMaintenanceFits(t *testing.T) {
	install := func(match string) string { return strings.Replace(installConfig, "%s", match, 1) }
	link := func(name string) string { return strings.Replace(linkConfig, "%s", name, 1) }

	for _, tt := range []struct {
		name   string
		disks  []string
		config []byte
		want   []string
	}{
		{"the one disk", []string{"vda"}, rendered(install(`disk.dev_path == "/dev/vda"`), link("eth0")), nil},
		{"one of two disks", []string{"vda", "vdb"}, rendered(install(`disk.dev_path == "/dev/vdb"`)), nil},
		{"a config that installs nothing", []string{"vda"}, rendered(), nil},
		{"a disk the machine does not have", []string{"vda"}, rendered(install(`disk.dev_path == "/dev/sda"`)),
			[]string{"the install disk selector matches none of the machine's disks (/dev/vda)"}},
		{"a selector that matches two disks", []string{"vda", "vdb"}, rendered(install(`disk.transport == "virtio"`)),
			[]string{"the install disk selector matches 2 disks (/dev/vda, /dev/vdb), and Talos would install to the first"}},
		{"a link the machine does not have", []string{"vda"}, rendered(install(`disk.dev_path == "/dev/vda"`), link("eth0"), link("enp3s0")),
			[]string{"the config configures the link enp3s0, which the machine does not have"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, m := candidate(t, tt.disks...)

			got, err := m.Fits(context.Background(), tt.config)
			if err != nil || !slices.Equal(got, tt.want) {
				t.Fatalf("got %q, %v", got, err)
			}
		})
	}

	_, m := candidate(t, "vda")
	if _, err := m.Fits(context.Background(), []byte("not: [a config")); err == nil {
		t.Fatal("a config that does not parse cannot be said to fit")
	}
}

// A machine without a config proves nothing about itself. What one conversation can have is that
// its every call reaches the key the first one did.
func TestKeyPin(t *testing.T) {
	conn := func(key string) tls.ConnectionState {
		return tls.ConnectionState{PeerCertificates: []*x509.Certificate{{RawSubjectPublicKeyInfo: []byte(key)}}}
	}

	pin := &keyPin{}

	if err := pin.verify(tls.ConnectionState{}); err == nil {
		t.Fatal("a server without a certificate was accepted")
	}

	for _, key := range []string{"first", "first"} {
		if err := pin.verify(conn(key)); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
	}

	if err := pin.verify(conn("another")); !errors.Is(err, errOtherKey) {
		t.Fatalf("another key was accepted: %v", err)
	}

	// And it stays with the first.
	if err := pin.verify(conn("first")); err != nil {
		t.Fatal(err)
	}
}

func TestRenderedHostname(t *testing.T) {
	const hostname = "apiVersion: v1alpha1\nkind: HostnameConfig\n%s\n"

	for _, tt := range []struct {
		name, document, want string
	}{
		{"a name", "auto: off\nhostname: node-e", "node-e"},
		{"a machine that names itself", "auto: stable", ""},
	} {
		got, err := RenderedHostname(rendered(strings.Replace(hostname, "%s", tt.document, 1)))
		if err != nil || got != tt.want {
			t.Errorf("%s: got %q, %v", tt.name, got, err)
		}
	}

	if got, err := RenderedHostname(rendered()); err != nil || got != "" {
		t.Errorf("no hostname document: got %q, %v", got, err)
	}

	if _, err := RenderedHostname([]byte("not: [a config")); err == nil {
		t.Error("a config that does not parse has no hostname to tell")
	}
}

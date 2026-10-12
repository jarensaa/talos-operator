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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const installerImage = "factory.example.com/metal-installer/aaa:" + apiVersion

func progress(message string) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{
		Response: &machineapi.LifecycleServiceInstallProgress_Message{Message: message},
	}}
}

func exitCode(code int32) *machineapi.LifecycleServiceUpgradeResponse {
	return &machineapi.LifecycleServiceUpgradeResponse{Progress: &machineapi.LifecycleServiceInstallProgress{
		Response: &machineapi.LifecycleServiceInstallProgress_ExitCode{ExitCode: code},
	}}
}

func pulled(name string) *machineapi.ImageServicePullResponse {
	return &machineapi.ImageServicePullResponse{Response: &machineapi.ImageServicePullResponse_Name{Name: name}}
}

func pulling() *machineapi.ImageServicePullResponse {
	return &machineapi.ImageServicePullResponse{
		Response: &machineapi.ImageServicePullResponse_PullProgress{PullProgress: &machineapi.ImageServicePullProgress{}},
	}
}

// The requests an upgrade is made of, as they reach a node: the installer pulled to and installed
// from Talos's own image store, each call addressed to the node and to nobody else.
func TestUpgraderSendsWhatAnUpgradeIsMadeOf(t *testing.T) {
	api, c := serve(t, dial(accessUpgrade)...)
	api.pull = []*machineapi.ImageServicePullResponse{pulling(), pulling(), pulled(installerImage)}
	api.install = []*machineapi.LifecycleServiceUpgradeResponse{progress("writing the boot slot"), exitCode(0)}

	u := &machineryUpgrader{*reader(c)}
	ctx := context.Background()

	if err := u.PullImage(ctx, apiNode, installerImage); err != nil {
		t.Fatal(err)
	}

	if err := u.Install(ctx, apiNode, installerImage); err != nil {
		t.Fatal(err)
	}

	if err := u.ForfeitEtcdLeadership(ctx, apiNode); err != nil {
		t.Fatal(err)
	}

	if err := u.Reboot(ctx, apiNode); err != nil {
		t.Fatal(err)
	}

	system := func(c *common.ContainerdInstance) bool {
		return c.GetDriver() == common.ContainerDriver_CONTAINERD && c.GetNamespace() == common.ContainerdNamespace_NS_SYSTEM
	}

	if len(api.pulls) != 1 || api.pulls[0].GetImageRef() != installerImage || !system(api.pulls[0].GetContainerd()) {
		t.Fatalf("pull: %+v", api.pulls)
	}

	if len(api.installs) != 1 || api.installs[0].GetSource().GetImageName() != installerImage || !system(api.installs[0].GetContainerd()) {
		t.Fatalf("install: %+v", api.installs)
	}

	// A graceful reboot: the default mode, never a forced one.
	if len(api.reboots) != 1 || api.reboots[0].GetMode() != machineapi.RebootRequest_DEFAULT || api.forfeits != 1 {
		t.Fatalf("reboot: %+v, forfeits: %d", api.reboots, api.forfeits)
	}

	for _, method := range []string{
		machineapi.ImageService_Pull_FullMethodName, machineapi.LifecycleService_Upgrade_FullMethodName,
		machineapi.MachineService_EtcdForfeitLeadership_FullMethodName, machineapi.MachineService_Reboot_FullMethodName,
	} {
		if got := api.addressedTo(method); len(got) != 1 || got[0] != apiNode.String() {
			t.Fatalf("%s must be addressed to the node: %v", method, got)
		}
	}
}

// An installer's failure is a message on the stream, not an error of the call, and a stream that
// just ends has not succeeded either.
func TestUpgraderTellsAFailedInstallFromOneThatFinished(t *testing.T) {
	api, c := serve(t, dial(accessUpgrade)...)
	u := &machineryUpgrader{*reader(c)}
	ctx := context.Background()

	api.install = []*machineapi.LifecycleServiceUpgradeResponse{progress("no space left on device"), exitCode(1)}

	var failed *InstallError
	if err := u.Install(ctx, apiNode, installerImage); !errors.As(err, &failed) || failed.ExitCode != 1 ||
		!strings.Contains(failed.Error(), "no space left") {
		t.Fatalf("a non-zero exit code is an InstallError with the installer's last word: %v", err)
	}

	api.install = []*machineapi.LifecycleServiceUpgradeResponse{progress("writing the boot slot")}
	if err := u.Install(ctx, apiNode, installerImage); err == nil || errors.As(err, &failed) && failed.ExitCode == 0 {
		t.Fatalf("an install that ends without an exit code did not finish: %v", err)
	}

	api.pull = []*machineapi.ImageServicePullResponse{pulling()}
	if err := u.PullImage(ctx, apiNode, installerImage); err == nil {
		t.Fatal("a pull that ends without naming the image did not finish")
	}
}

func TestCheckUpgrade(t *testing.T) {
	checkAllowlist(t, accessUpgrade, []allowCase{
		{"pull", machineapi.ImageService_Pull_FullMethodName, &machineapi.ImageServicePullRequest{}, true},
		{"install", machineapi.LifecycleService_Upgrade_FullMethodName, &machineapi.LifecycleServiceUpgradeRequest{}, true},
		{"forfeit leadership", machineapi.MachineService_EtcdForfeitLeadership_FullMethodName, &machineapi.EtcdForfeitLeadershipRequest{}, true},
		{"a reboot", machineapi.MachineService_Reboot_FullMethodName, &machineapi.RebootRequest{}, true},
		{"reading the version", machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{}, true},
		{"dry run in try mode", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			apply(machineapi.ApplyConfigurationRequest_TRY, true), true},
		// An upgrade connection does not apply config: that is the config connection's.
		{"apply", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			apply(machineapi.ApplyConfigurationRequest_NO_REBOOT, false), false},
		{"try", machineapi.MachineService_ApplyConfiguration_FullMethodName, apply(machineapi.ApplyConfigurationRequest_TRY, false), false},
		// A fresh install wipes the disk it is given.
		{"install from scratch", machineapi.LifecycleService_Install_FullMethodName, &machineapi.LifecycleServiceInstallRequest{}, false},
		//nolint:staticcheck // the legacy upgrade RPC takes force and stage, and must stay refused
		{"legacy upgrade", machineapi.MachineService_Upgrade_FullMethodName, &machineapi.UpgradeRequest{}, false},
		{"a reset", machineapi.MachineService_Reset_FullMethodName, &machineapi.ResetRequest{}, false},
		{"rollback", machineapi.MachineService_Rollback_FullMethodName, &machineapi.RollbackRequest{}, false},
		{"shutdown", machineapi.MachineService_Shutdown_FullMethodName, &machineapi.ShutdownRequest{}, false},
		{"etcd leave", machineapi.MachineService_EtcdLeaveCluster_FullMethodName, &machineapi.EtcdLeaveClusterRequest{}, false},
		{"etcd remove member", machineapi.MachineService_EtcdRemoveMemberByID_FullMethodName, &machineapi.EtcdRemoveMemberByIDRequest{}, false},
		{"image import", machineapi.ImageService_Import_FullMethodName, nil, false},
		{"image remove", machineapi.ImageService_Remove_FullMethodName, &machineapi.ImageServiceRemoveRequest{}, false},
	})
}

// The interceptors really are on the connection: what the access does not allow never leaves.
func TestUpgradeConnectionRefusesWhatIsNotAnUpgrade(t *testing.T) {
	api, c := serve(t, dial(accessUpgrade)...)
	ctx := client.WithNode(context.Background(), apiNode.String())
	cfg := []byte(strings.Replace(workerConfig, "%s", "new", 1))

	_, err := c.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{Data: cfg, Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("an upgrade connection must refuse to apply config, got %v", err)
	}

	if _, err := c.LifecycleClient.Install(ctx, &machineapi.LifecycleServiceInstallRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("an upgrade connection must refuse a fresh install, got %v", err)
	}

	if len(api.applies) != 0 {
		t.Fatalf("nothing may have reached the node: %+v", api.applies)
	}
}

func TestUpgraderLeavesTheCallersDeadlineOnLongCalls(t *testing.T) {
	api, c := serve(t, dial(accessUpgrade)...)
	api.pull = []*machineapi.ImageServicePullResponse{pulled(installerImage)}

	// The reader's own timeout is for reads. A pull takes as long as a download takes.
	u := &machineryUpgrader{machineryReader{client: c, timeout: time.Nanosecond}}

	if err := u.PullImage(context.Background(), apiNode, installerImage); err != nil {
		t.Fatalf("a pull must not be cut short by the timeout of a read: %v", err)
	}
}

func TestUpgradeProblem(t *testing.T) {
	const (
		current = "1.37.0"
		older   = "v1.14.1"
	)

	tests := []struct {
		name               string
		from, to, k8s, why string
	}{
		{"a patch release", older, apiVersion, current, ""},
		{"the same version, another installer", apiVersion, apiVersion, current, ""},
		{"a minor release", "v1.13.9", apiVersion, "1.36.0", ""},
		{"without a known Kubernetes version", older, apiVersion, "", ""},
		{"a downgrade", apiVersion, older, current, "downgrades are not supported"},
		{"newer than this operator", apiVersion, "v1.15.0", current, "newer than this operator"},
		{"older than supported", "v1.12.4", "v1.12.6", "1.34.0", "older than the minimum"},
		{"a jump Talos does not allow", "v1.10.5", apiVersion, "1.34.0", "does not upgrade"},
		{"a Kubernetes the target does not run", older, apiVersion, "1.20.0", "does not support Kubernetes"},
		{garbage, older, "latest", current, "unparsable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := UpgradeProblem(tt.from, tt.to, tt.k8s)
			if (tt.why == "") != (got == "") || !strings.Contains(got, tt.why) {
				t.Fatalf("got %q, want it to mention %q", got, tt.why)
			}
		})
	}
}

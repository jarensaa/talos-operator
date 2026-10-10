//go:build e2e

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
	"net/netip"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

// e2eTarget loads the talosconfig of the cluster created by `make e2e-cluster-up`;
// E2E_TALOSCONFIG points at it.
func e2eTarget(t *testing.T) Target {
	t.Helper()

	path := os.Getenv("E2E_TALOSCONFIG")
	if path == "" {
		t.Skip("E2E_TALOSCONFIG not set; run `make e2e-cluster-up` first")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	c, err := creds.Load(data, "", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	return Target{Key: "e2e", Credentials: c, Endpoints: c.Endpoints}
}

func TestE2EVersionOnEveryEndpoint(t *testing.T) {
	target := e2eTarget(t)
	c := target.Credentials

	f := NewFactory()
	defer f.Close()

	r, err := f.Reader(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}

	for _, ep := range c.Endpoints {
		v, err := r.Version(context.Background(), ep.Addr)
		if err != nil {
			t.Fatalf("version of %s: %v", ep.Addr, err)
		}

		if !strings.HasPrefix(v.Tag, "v1.") {
			t.Fatalf("unexpected tag %q on %s", v.Tag, ep.Addr)
		}
	}
}

// TestE2EReadOnlyConnectionRejectsMutation proves the allowlist sits on the real connection:
// a reboot attempted through the raw machinery client never leaves the operator.
func TestE2EReadOnlyConnectionRejectsMutation(t *testing.T) {
	target := e2eTarget(t)

	f := NewFactory()
	defer f.Close()

	c, err := f.client(context.Background(), target, accessReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	node := target.Endpoints[0].Addr.String()

	err = c.Reboot(client.WithNode(context.Background(), node))
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied from the allowlist, got %v", err)
	}
}

// TestE2EConfigConnectionOnlyAppliesConfig proves the same for the connection config: Manage
// uses: it can apply machine config without a reboot and nothing else. A reboot, and an apply in
// a mode that could reboot the node or leave a staged config behind, never leave the operator.
func TestE2EConfigConnectionOnlyAppliesConfig(t *testing.T) {
	target := e2eTarget(t)

	f := NewFactory()
	defer f.Close()

	c, err := f.client(context.Background(), target, accessConfig)
	if err != nil {
		t.Fatal(err)
	}

	ctx := client.WithNode(context.Background(), target.Endpoints[0].Addr.String())

	if err := c.Reboot(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied from the allowlist, got %v", err)
	}

	// The request never reaches the node, so its content does not matter.
	for _, mode := range []machineapi.ApplyConfigurationRequest_Mode{
		machineapi.ApplyConfigurationRequest_AUTO, machineapi.ApplyConfigurationRequest_STAGED,
		machineapi.ApplyConfigurationRequest_REBOOT, //nolint:staticcheck // deprecated, and refused like the others
	} {
		_, err := c.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{Data: []byte("refused"), Mode: mode})
		if status.Code(err) != codes.PermissionDenied || !IsRejected(err) {
			t.Fatalf("mode %s: expected PermissionDenied from the allowlist, got %v", mode, err)
		}
	}
}

// e2eNodes returns the harness's control planes and workers (from .e2e/env).
func e2eNodes(t *testing.T) (controlPlanes, workers []netip.Addr) {
	t.Helper()

	parse := func(env string) []netip.Addr {
		var out []netip.Addr

		for f := range strings.FieldsSeq(os.Getenv(env)) {
			out = append(out, netip.MustParseAddr(f))
		}

		return out
	}

	return parse("E2E_CONTROLPLANES"), parse("E2E_WORKERS")
}

// TestE2EObservers reads every Reader method from every node of the harness cluster, the way
// the observe controllers will.
func TestE2EObservers(t *testing.T) {
	target := e2eTarget(t)
	controlPlanes, workers := e2eNodes(t)

	f := NewFactory()
	defer f.Close()

	ctx := context.Background()

	r, err := f.Reader(ctx, target)
	if err != nil {
		t.Fatal(err)
	}

	for _, node := range controlPlanes {
		t.Run(node.String(), func(t *testing.T) {
			checkNodeObservers(ctx, t, r, node, "controlplane")
			checkEtcdObservers(ctx, t, r, node, len(controlPlanes))
		})
	}

	for _, node := range workers {
		t.Run(node.String(), func(t *testing.T) {
			checkNodeObservers(ctx, t, r, node, "worker")
		})
	}

	members, err := r.Members(ctx, controlPlanes[0])
	if err != nil || len(members) != len(controlPlanes)+len(workers) {
		t.Fatalf("members: %+v err=%v", members, err)
	}

	health, err := r.HealthCheck(ctx, controlPlanes[0], ClusterInfo{ControlPlanes: controlPlanes, Workers: workers}, 2*time.Minute)
	if err != nil || !health.Healthy {
		t.Fatalf("health: %+v err=%v", health, err)
	}
}

func checkNodeObservers(ctx context.Context, t *testing.T, r Reader, node netip.Addr, wantType string) {
	t.Helper()

	if ms, err := r.MachineStatus(ctx, node); err != nil || ms.Stage != "running" || !ms.Ready {
		t.Fatalf("machine status: %+v err=%v", ms, err)
	}

	if id, err := r.BootID(ctx, node); err != nil || id == "" {
		t.Fatalf("boot ID: %q err=%v", id, err)
	}

	if si, err := r.SystemInformation(ctx, node); err != nil || si.UUID == "" {
		t.Fatalf("system information: %+v err=%v", si, err)
	}

	if sc, err := r.ImageFactorySchematic(ctx, node); err != nil || len(sc.ID) != 64 {
		t.Fatalf("schematic: %+v err=%v", sc, err)
	}

	if name, err := r.Nodename(ctx, node); err != nil || !strings.HasPrefix(name, "e2e-") {
		t.Fatalf("node name: %q err=%v", name, err)
	}

	if st, err := r.UnattendedInstallStatus(ctx, node); err != nil || st.Phase != "installed" {
		t.Fatalf("install status: %+v err=%v", st, err)
	}

	svcs, err := r.Services(ctx, node)
	if err != nil || !slices.ContainsFunc(svcs, func(s ServiceStatus) bool { return s.ID == kubeletService && s.Running }) {
		t.Fatalf("services: %+v err=%v", svcs, err)
	}

	if mt, err := r.MachineType(ctx, node); err != nil || mt != wantType {
		t.Fatalf("machine type: %q err=%v", mt, err)
	}

	cfgs, err := r.MachineConfig(ctx, node)
	if err != nil {
		t.Fatalf("machine config: %v", err)
	}

	if cfgs.PendingConfigChange() {
		t.Fatal("a settled node reports a pending staged config")
	}

	if _, err := ParseMachineConfig(cfgs.Active); err != nil {
		t.Fatalf("live config does not parse: %v", err)
	}
}

func checkEtcdObservers(ctx context.Context, t *testing.T, r Reader, node netip.Addr, controlPlanes int) {
	t.Helper()

	if members, err := r.EtcdMembers(ctx, node); err != nil || len(members) != controlPlanes {
		t.Fatalf("etcd members: %+v err=%v", members, err)
	}

	if st, err := r.EtcdStatus(ctx, node); err != nil || st.MemberID == 0 || st.Leader == 0 {
		t.Fatalf("etcd status: %+v err=%v", st, err)
	}
}

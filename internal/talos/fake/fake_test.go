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

package fake_test

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/talos/fake"
)

const startVersion = "v1.14.0"

var (
	cp1 = netip.MustParseAddr("10.0.0.11")
	cp2 = netip.MustParseAddr("10.0.0.12")
)

func scenario() *fake.Scenario {
	return fake.NewScenario(
		fake.Node{Addr: cp1, Version: talos.VersionInfo{Tag: startVersion, Arch: "amd64"}},
		fake.Node{Addr: cp2, Version: talos.VersionInfo{Tag: startVersion, Arch: "amd64"}},
	)
}

func TestVersionAndCallRecording(t *testing.T) {
	s := scenario()
	r := s.Reader()

	v, err := r.Version(context.Background(), cp1)
	if err != nil {
		t.Fatal(err)
	}

	if v.Tag != startVersion {
		t.Fatalf("tag: %s", v.Tag)
	}

	calls := s.CallsFor(fake.MethodVersion)
	if len(calls) != 1 || calls[0].Node != cp1 {
		t.Fatalf("calls: %+v", calls)
	}

	s.AssertNoMutations(t)
}

func TestUpdateNodeIsVisibleToLaterCalls(t *testing.T) {
	s := scenario()

	s.UpdateNode(cp2, func(n *fake.Node) { n.Version.Tag = "v1.14.2" })

	v, err := s.Reader().Version(context.Background(), cp2)
	if err != nil {
		t.Fatal(err)
	}

	if v.Tag != "v1.14.2" {
		t.Fatalf("tag after update: %s", v.Tag)
	}
}

func TestErrorInjection(t *testing.T) {
	s := scenario()
	r := s.Reader()
	injected := status.Error(codes.Unavailable, "node rebooting")

	s.FailNext(fake.MethodVersion, cp1, injected)

	if _, err := r.Version(context.Background(), cp1); !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}

	// Other nodes are unaffected.
	if _, err := r.Version(context.Background(), cp2); err != nil {
		t.Fatal(err)
	}

	s.FailNext(fake.MethodVersion, cp1, nil)

	if _, err := r.Version(context.Background(), cp1); err != nil {
		t.Fatalf("after clearing: %v", err)
	}
}

func TestUnknownNodeIsUnavailable(t *testing.T) {
	_, err := scenario().Reader().Version(context.Background(), netip.MustParseAddr("10.0.0.9"))
	if status.Code(errors.Unwrap(err)) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

func TestCancelledContextIsNotRecorded(t *testing.T) {
	s := scenario()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := s.Reader().Version(ctx, cp1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if len(s.Calls()) != 0 {
		t.Fatal("a call that never reached the node must not be recorded")
	}
}

func clusterScenario() (*fake.Scenario, netip.Addr, netip.Addr) {
	cp := netip.MustParseAddr("10.5.0.2")
	worker := netip.MustParseAddr("10.5.0.3")

	s := fake.NewScenario(
		fake.Node{Addr: cp, MachineType: fake.TypeControlPlane, Nodename: "cp-1", EtcdMemberID: 0xa1,
			Version:       talos.VersionInfo{Tag: startVersion},
			InstallStatus: &talos.InstallStatus{Phase: "installed"}},
		fake.Node{Addr: worker, MachineType: fake.TypeWorker, Nodename: "worker-1",
			Version: talos.VersionInfo{Tag: startVersion}},
	)

	return s, cp, worker
}

func TestDerivedClusterViews(t *testing.T) {
	s, cp, worker := clusterScenario()
	r := s.Reader()
	ctx := context.Background()

	members, err := r.Members(ctx, cp)
	if err != nil || len(members) != 2 || members[0].Hostname != "cp-1" || members[1].Addresses[0] != worker {
		t.Fatalf("members derived from nodes: %+v err=%v", members, err)
	}

	etcd, err := r.EtcdMembers(ctx, cp)
	if err != nil || len(etcd) != 1 || etcd[0].ID != 0xa1 {
		t.Fatalf("etcd members derived from control planes: %+v err=%v", etcd, err)
	}

	st, err := r.EtcdStatus(ctx, cp)
	if err != nil || st.Leader != 0xa1 {
		t.Fatalf("etcd status: %+v err=%v", st, err)
	}
}

func TestWorkerHasNoEtcd(t *testing.T) {
	s, _, worker := clusterScenario()
	r := s.Reader()

	if _, err := r.EtcdMembers(context.Background(), worker); status.Code(errors.Unwrap(err)) != codes.FailedPrecondition {
		t.Fatalf("etcd members on a worker: %v", err)
	}

	if _, err := r.EtcdStatus(context.Background(), worker); status.Code(errors.Unwrap(err)) != codes.FailedPrecondition {
		t.Fatalf("etcd status on a worker: %v", err)
	}
}

func TestMissingResourcesAreNotFound(t *testing.T) {
	s, cp, worker := clusterScenario()
	r := s.Reader()

	if st, err := r.UnattendedInstallStatus(context.Background(), cp); err != nil || st.Phase != "installed" {
		t.Fatalf("install status on cp: %+v %v", st, err)
	}

	if _, err := r.UnattendedInstallStatus(context.Background(), worker); !errors.Is(err, talos.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for a node without install status, got %v", err)
	}

	if _, err := r.ImageFactorySchematic(context.Background(), worker); !errors.Is(err, talos.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for a node without schematic, got %v", err)
	}
}

func TestHealthIsScriptable(t *testing.T) {
	s, cp, _ := clusterScenario()

	s.SetHealth(talos.HealthResult{Messages: []string{"waiting for etcd"}})

	got, err := s.Reader().HealthCheck(context.Background(), cp, talos.ClusterInfo{}, time.Minute)
	if err != nil || got.Healthy || got.Messages[0] != "waiting for etcd" {
		t.Fatalf("health: %+v err=%v", got, err)
	}
}

// The configs of the apply tests: a node's own, and two others to apply.
var (
	original = minimalConfig("original")
	changed  = minimalConfig("changed")
	other    = minimalConfig("other")
)

// minimalConfig is the smallest machine config machinery parses, told apart by a node label.
func minimalConfig(label string) []byte {
	return []byte("version: v1alpha1\nmachine:\n  type: worker\n  nodeLabels:\n    config: " + label + "\ncluster: {}\n")
}

func applyScenario() (*fake.Scenario, talos.ConfigWriter) {
	s := fake.NewScenario(fake.Node{Addr: cp1, Config: talos.MachineConfigs{Active: original}})

	return s, s.Writer()
}

func configs(t *testing.T, s *fake.Scenario) (active, persistent string) {
	t.Helper()

	n := s.Node(cp1)

	return string(n.Config.Active), string(n.Config.Persistent)
}

func TestTryChangesOnlyTheActiveConfigAndRevertsByItself(t *testing.T) {
	s, w := applyScenario()
	ctx := context.Background()

	if err := w.ApplyConfig(ctx, cp1, changed, talos.ApplyTry, time.Minute); err != nil {
		t.Fatal(err)
	}

	if active, persistent := configs(t, s); active != string(changed) || persistent != "" {
		t.Fatalf("a try must change the active config only: active changed=%t, persistent=%q", active == string(changed), persistent)
	}

	if !s.TryPending(cp1) {
		t.Fatal("a try must be pending")
	}

	if !s.ExpireTry(cp1) {
		t.Fatal("the timer must fire")
	}

	if active, _ := configs(t, s); active != string(original) || s.TryPending(cp1) {
		t.Fatal("an unconfirmed try must end on the previous config")
	}

	if s.ExpireTry(cp1) {
		t.Fatal("the timer only fires once")
	}
}

func TestConfirmMakesATriedConfigPersistentAndStopsTheTimer(t *testing.T) {
	s, w := applyScenario()
	ctx := context.Background()

	if err := w.ApplyConfig(ctx, cp1, changed, talos.ApplyTry, time.Minute); err != nil {
		t.Fatal(err)
	}

	if err := w.ApplyConfig(ctx, cp1, changed, talos.ApplyNoReboot, 0); err != nil {
		t.Fatal(err)
	}

	if active, persistent := configs(t, s); active != string(changed) || persistent != string(changed) {
		t.Fatal("a confirmed config is active and persistent")
	}

	if s.ExpireTry(cp1) {
		t.Fatal("a confirm stops the rollback timer")
	}

	calls := s.CallsFor(fake.MethodApplyConfiguration)
	if len(calls) != 2 || calls[0].Mode != talos.ApplyTry || calls[1].Mode != talos.ApplyNoReboot || calls[0].DryRun {
		t.Fatalf("applies must be recorded with their mode: %+v", calls)
	}
}

// The hazard the rollout planner is built around: Talos falls back to whatever was active when
// the try arrived (RollbackToConfigAfter in Talos v1.14).
func TestASecondTryFallsBackToTheFirst(t *testing.T) {
	s, w := applyScenario()
	ctx := context.Background()

	for _, cfg := range [][]byte{changed, other} {
		if err := w.ApplyConfig(ctx, cp1, cfg, talos.ApplyTry, time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	s.ExpireTry(cp1)

	if active, _ := configs(t, s); active != string(changed) {
		t.Fatal("the second try must fall back to the first tried config, not to the original: that is what Talos does")
	}
}

func TestANodeCutOffByItsConfigComesBackWhenTheTryExpires(t *testing.T) {
	s, w := applyScenario()
	ctx := context.Background()

	s.UpdateNode(cp1, func(n *fake.Node) {
		n.UnreachableWhen = func(active []byte) bool { return string(active) == string(changed) }
	})

	if err := w.ApplyConfig(ctx, cp1, changed, talos.ApplyTry, time.Minute); err != nil {
		t.Fatal(err)
	}

	if _, err := s.Reader().Version(ctx, cp1); status.Code(err) != codes.Unavailable {
		t.Fatalf("the node must be cut off, got %v", err)
	}

	// Not even a confirm reaches it.
	if err := w.ApplyConfig(ctx, cp1, changed, talos.ApplyNoReboot, 0); status.Code(err) != codes.Unavailable {
		t.Fatalf("a cut-off node cannot be confirmed, got %v", err)
	}

	s.ExpireTry(cp1)

	if _, err := s.Reader().Version(ctx, cp1); err != nil {
		t.Fatalf("the node must answer again on its previous config: %v", err)
	}
}

func TestApplyRefusesWhatTalosWould(t *testing.T) {
	_, w := applyScenario()
	ctx := context.Background()

	if err := w.ApplyConfig(ctx, cp1, []byte("not: [a config"), talos.ApplyTry, time.Minute); !talos.IsRejected(err) {
		t.Fatalf("a config that does not parse must be rejected, got %v", err)
	}

	if err := w.ApplyConfig(ctx, cp1, changed, talos.ApplyTry, 0); err == nil {
		t.Fatal("a try needs a timeout")
	}

	if err := w.ApplyConfig(ctx, cp1, changed, talos.ApplyMode(0), 0); err == nil {
		t.Fatal("only try and no-reboot exist")
	}
}

func TestDryRunsAreNotMutations(t *testing.T) {
	s, w := applyScenario()
	ctx := context.Background()

	if _, err := w.DryRunApply(ctx, cp1, changed); err != nil {
		t.Fatal(err)
	}

	s.AssertNoMutations(t)

	if active, _ := configs(t, s); active != string(original) {
		t.Fatal("a dry run changes nothing")
	}
}

// The fake upgrades the way a node does: an install only fills the other boot slot, and nothing
// about the node changes until it has rebooted and booted again.
func TestAnInstallTakesEffectOnlyThroughAReboot(t *testing.T) {
	s, cp, worker := clusterScenario()
	u := s.Upgrader()
	ctx := context.Background()

	const image = "factory.example.com/metal-installer/bbb:v1.14.9"

	if err := u.Install(ctx, worker, image); err == nil {
		t.Fatal("an image that was not pulled cannot be installed")
	}

	if err := u.PullImage(ctx, worker, image); err != nil {
		t.Fatal(err)
	}

	if err := u.Install(ctx, worker, image); err != nil {
		t.Fatal(err)
	}

	before := s.Node(worker)
	if before.Version.Tag != startVersion || s.Down(worker) {
		t.Fatalf("an install changes nothing on the running node: %+v", before.Version)
	}

	if err := u.Reboot(ctx, worker); err != nil {
		t.Fatal(err)
	}

	if !s.Down(worker) {
		t.Fatal("a rebooted node is down until it boots")
	}

	if _, err := s.Reader().Version(ctx, worker); status.Code(err) != codes.Unavailable {
		t.Fatalf("a node that is down answers nothing: %v", err)
	}

	if _, err := s.Reader().Version(ctx, cp); err != nil {
		t.Fatalf("the other node is not affected: %v", err)
	}

	if !s.Boot(worker) || s.Boot(worker) {
		t.Fatal("Boot brings a node back once")
	}

	after := s.Node(worker)
	if after.Version.Tag != "v1.14.9" || after.Schematic.ID != "bbb" || after.BootID == before.BootID {
		t.Fatalf("after the reboot the node runs what was installed, under another boot ID: %+v", after)
	}

	if got := s.CallsFor(fake.MethodInstall); len(got) != 2 || got[1].Image != image {
		t.Fatalf("installs are recorded with their image: %+v", got)
	}
}

func TestANodeThatFallsBackKeepsItsSystemAndATriedConfigDoesNotSurviveAReboot(t *testing.T) {
	s, _, worker := clusterScenario()
	u := s.Upgrader()
	ctx := context.Background()

	const image = "factory.example.com/metal-installer/bbb:v1.14.9"

	s.UpdateNode(worker, func(n *fake.Node) {
		n.FallsBack = true
		n.Config = talos.MachineConfigs{Active: minimalConfig("a"), Persistent: minimalConfig("a")}
	})

	if err := s.Writer().ApplyConfig(ctx, worker, minimalConfig("b"), talos.ApplyTry, time.Minute); err != nil {
		t.Fatal(err)
	}

	for _, call := range []func() error{
		func() error { return u.PullImage(ctx, worker, image) },
		func() error { return u.Install(ctx, worker, image) },
		func() error { return u.Reboot(ctx, worker) },
	} {
		if err := call(); err != nil {
			t.Fatal(err)
		}
	}

	s.Boot(worker)

	after := s.Node(worker)
	if after.Version.Tag != startVersion {
		t.Fatalf("a node that falls back runs what it ran: %s", after.Version.Tag)
	}

	if string(after.Config.Active) != string(minimalConfig("a")) || s.TryPending(worker) {
		t.Fatal("a reboot boots the persistent config: a tried one is gone")
	}
}

func TestAFailingInstallerLeavesNothingToBoot(t *testing.T) {
	s, _, worker := clusterScenario()
	u := s.Upgrader()
	ctx := context.Background()

	const image = "factory.example.com/metal-installer/bbb:v1.14.9"

	s.UpdateNode(worker, func(n *fake.Node) { n.InstallExitCode = 1 })

	if err := u.PullImage(ctx, worker, image); err != nil {
		t.Fatal(err)
	}

	var failed *talos.InstallError
	if err := u.Install(ctx, worker, image); !errors.As(err, &failed) || failed.ExitCode != 1 {
		t.Fatalf("a failing installer is an InstallError: %v", err)
	}

	if err := u.Reboot(ctx, worker); err != nil {
		t.Fatal(err)
	}

	s.Boot(worker)

	if got := s.Node(worker).Version.Tag; got != startVersion {
		t.Fatalf("nothing was installed, so the node boots what it had: %s", got)
	}
}

func TestForfeitingEtcdLeadershipMovesItToAnotherMember(t *testing.T) {
	cp1, cp2 := netip.MustParseAddr("10.5.0.2"), netip.MustParseAddr("10.5.0.3")
	s := fake.NewScenario(
		fake.Node{Addr: cp1, MachineType: fake.TypeControlPlane, Nodename: "first", EtcdMemberID: 0xa1},
		fake.Node{Addr: cp2, MachineType: fake.TypeControlPlane, Nodename: "second", EtcdMemberID: 0xa2},
	)
	ctx := context.Background()

	leader := func(node netip.Addr) uint64 {
		st, err := s.Reader().EtcdStatus(ctx, node)
		if err != nil {
			t.Fatal(err)
		}

		return st.Leader
	}

	if leader(cp1) != 0xa1 {
		t.Fatal("the first control plane leads to begin with")
	}

	// A member that does not lead has nothing to give up.
	if err := s.Upgrader().ForfeitEtcdLeadership(ctx, cp2); err != nil || leader(cp1) != 0xa1 {
		t.Fatalf("only the leader forfeits: %v", err)
	}

	if err := s.Upgrader().ForfeitEtcdLeadership(ctx, cp1); err != nil || leader(cp2) != 0xa2 {
		t.Fatalf("the leader hands over to another member: %v", err)
	}
}

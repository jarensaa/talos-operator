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

// Package fake is an in-memory Talos cluster for controller tests. It implements the talos
// Reader over a scriptable Scenario and records every call, so tests can assert both what the
// operator observed and that observe mode never attempted a mutation.
package fake

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/siderolabs/talos/pkg/machinery/config/configdiff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jarensaa/talos-operator/internal/talos"
)

// Method names recorded in Calls. Mutating methods are added here as the Writer grows.
const (
	MethodVersion                 = "Version"
	MethodMachineStatus           = "MachineStatus"
	MethodBootID                  = "BootID"
	MethodSystemInformation       = "SystemInformation"
	MethodImageFactorySchematic   = "ImageFactorySchematic"
	MethodNodename                = "Nodename"
	MethodUnattendedInstallStatus = "UnattendedInstallStatus"
	MethodServices                = "Services"
	MethodMachineType             = "MachineType"
	MethodMachineConfig           = "MachineConfig"
	MethodMembers                 = "Members"
	MethodEtcdMembers             = "EtcdMembers"
	MethodEtcdStatus              = "EtcdStatus"
	MethodHealthCheck             = "HealthCheck"
	MethodApplyConfiguration      = "ApplyConfiguration"
	MethodImagePull               = "ImagePull"
	MethodInstall                 = "Install"
	MethodEtcdForfeitLeadership   = "EtcdForfeitLeadership"
	MethodReboot                  = "Reboot"
	MethodKubernetesImagePull     = "KubernetesImagePull"
	MethodKubernetesUpgradeChecks = "KubernetesUpgradeChecks"
	// What a removal and a machine in maintenance mode are sent.
	MethodReset            = "Reset"
	MethodEtcdRemoveMember = "EtcdRemoveMemberByID"
	MethodMaintenanceProbe = "MaintenanceProbe"
	MethodMaintenanceFits  = "MaintenanceFits"
	MethodMaintenanceApply = "MaintenanceApply"
	// What the upkeep of etcd sends, and what is read of a node's health and logs.
	MethodEtcdDefragment = "EtcdDefragment"
	MethodNodeHealth     = "NodeHealth"
	MethodLogs           = "Logs"
)

// Machine types as Talos reports them: talos.MachineTypeControlPlane and talos.MachineTypeWorker,
// under the names the tests have for them.
var (
	TypeControlPlane = talos.MachineTypeControlPlane
	TypeWorker       = talos.MachineTypeWorker
)

// mutatingMethods lists recorded methods that change node state. A dry-run apply is recorded
// under ApplyConfiguration with DryRun set, which AssertNoMutations allows.
var mutatingMethods = map[string]bool{
	MethodApplyConfiguration: true, MethodImagePull: true, MethodInstall: true, MethodEtcdForfeitLeadership: true, MethodReboot: true,
	MethodKubernetesImagePull: true, MethodReset: true, MethodEtcdRemoveMember: true, MethodMaintenanceApply: true,
	MethodEtcdDefragment: true,
}

func (n *Node) unhealthy() bool { return n.UnhealthyWhen != nil && n.UnhealthyWhen(n.Config.Active) }

// Node is one simulated Talos node. Every field drives the answer of the matching Reader method.
type Node struct {
	Addr              netip.Addr
	MachineType       string
	Version           talos.VersionInfo
	MachineStatus     talos.MachineStatus
	BootID            string
	SystemInformation talos.SystemInformation
	Schematic         talos.Schematic
	Nodename          string
	// InstallStatus nil means the node has no unattended install status (ErrNotFound).
	InstallStatus *talos.InstallStatus
	Services      []talos.ServiceStatus
	Config        talos.MachineConfigs
	// DryRunReportsChanges makes dry-run applies report changes whatever is sent, as when the
	// server's diff sees something the operator's per-document comparison does not.
	DryRunReportsChanges bool
	// UnreachableWhen cuts the node off while it returns true for the active config: every call to
	// the node fails as unavailable, as when a config breaks its network. A reverted try brings
	// it back.
	UnreachableWhen func(active []byte) bool
	// UnhealthyWhen makes the node unhealthy while it returns true for the active config: the
	// node answers and reports itself not ready, and the cluster's health check fails on it, as
	// when a config stops its kubelet.
	UnhealthyWhen func(active []byte) bool
	// tryPrevious is the config a pending try falls back to: whatever was active when the try
	// was applied, exactly as Talos captures it.
	tryPrevious []byte
	// InstallExitCode makes the installer fail with this exit code when it is not zero.
	InstallExitCode int32
	// FallsBack makes the node come back from its next reboot on the system it ran before, as
	// Talos does by itself when the newly installed one does not boot.
	FallsBack bool
	// pulled are the images on the node; installed is the installer image whose system sits in
	// the inactive boot slot, waiting for a reboot.
	pulled    map[string]bool
	installed string
	// down is true between a reboot and Boot: the node answers nothing.
	down bool
	// EtcdMemberID identifies the node's etcd member; 0 means none (workers).
	EtcdMemberID uint64
	EtcdLearner  bool
	// What Talos says about the node besides whether it is ready. A node with none of it is one
	// with nothing to say: no warnings, a clock in sync, no links or volumes listed.
	Diagnostics  []talos.Diagnostic
	TimeUnsynced bool
	Links        []talos.Link
	Volumes      []talos.Volume
	// EtcdDBSize and EtcdDBInUse are the size of the member's database file and what of it
	// holds data. Defragmenting makes the first the second.
	EtcdDBSize, EtcdDBInUse int64
	// Logs are the node's logs, by service; "dmesg" is the kernel's.
	Logs map[string]string
	// Maintenance is true for a machine that has no config and waits for one: it answers nobody
	// who comes with the cluster's credentials, is no member of anything, and takes a config
	// from whoever hands it one over a maintenance connection. Everything else the Node says is
	// what it will be once it has joined.
	Maintenance bool
	// FitProblems is what a config does not fit of the machine, as its disks and links would say.
	FitProblems []string
	// joining is true from the moment a machine in maintenance mode took its config until it
	// boots: it is installing, and not a member yet.
	joining bool
}

// Call is one recorded Reader call.
type Call struct {
	Method string
	Node   netip.Addr
	// DryRun is true for dry-run applies, which are allowed in observe mode.
	DryRun bool
	// Mode is the mode of a real apply; zero otherwise.
	Mode talos.ApplyMode
	// Image is the image of a pull or an install.
	Image string
}

// Scenario is a simulated cluster. It is safe for concurrent use.
type Scenario struct {
	mu     sync.Mutex
	nodes  map[netip.Addr]*Node
	errors map[errorKey]error
	calls  []Call

	// members and etcdMembers override the views derived from nodes when non-nil.
	members     []talos.Member
	etcdMembers []talos.EtcdMember
	etcdLeader  uint64
	health      talos.HealthResult
	// removed is what the Kubernetes upgrade checks find, by the version checked.
	removed map[string][]string
}

type errorKey struct {
	method string
	node   netip.Addr
}

// NewScenario returns a healthy cluster with the given nodes.
func NewScenario(nodes ...Node) *Scenario {
	s := &Scenario{
		nodes:  map[netip.Addr]*Node{},
		errors: map[errorKey]error{},
		health: talos.HealthResult{Healthy: true},
	}

	for i := range nodes {
		n := nodes[i]
		s.nodes[n.Addr] = &n

		if s.etcdLeader == 0 && n.EtcdMemberID != 0 {
			s.etcdLeader = n.EtcdMemberID
		}
	}

	return s
}

// Node returns a copy of a node's current state; the zero Node if it does not exist.
func (s *Scenario) Node(addr netip.Addr) Node {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n, ok := s.nodes[addr]; ok {
		return *n
	}

	return Node{}
}

// UpdateNode mutates a node in place, e.g. to simulate an upgrade between reconciles.
func (s *Scenario) UpdateNode(addr netip.Addr, update func(*Node)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n, ok := s.nodes[addr]; ok {
		update(n)
	}
}

// SetMembers overrides the discovery member list (default: derived from nodes).
func (s *Scenario) SetMembers(members []talos.Member) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.members = members
}

// SetEtcdMembers overrides the etcd member list (default: derived from control-plane nodes).
func (s *Scenario) SetEtcdMembers(members []talos.EtcdMember) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.etcdMembers = members
}

// SetHealth sets the result of HealthCheck.
func (s *Scenario) SetHealth(result talos.HealthResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.health = result
}

// SetRemovedInKubernetes makes the upgrade checks for Kubernetes version find the given problems
// on the nodes; none clears them.
func (s *Scenario) SetRemovedInKubernetes(version string, problems ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.removed == nil {
		s.removed = map[string][]string{}
	}

	s.removed[version] = problems
}

// FailNext makes every subsequent call of method on node return err until cleared with a nil err.
func (s *Scenario) FailNext(method string, node netip.Addr, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := errorKey{method: method, node: node}
	if err == nil {
		delete(s.errors, key)

		return
	}

	s.errors[key] = err
}

// Calls returns a copy of all recorded calls.
func (s *Scenario) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.calls)
}

// CallsFor returns recorded calls of one method.
func (s *Scenario) CallsFor(method string) []Call {
	var out []Call

	for _, c := range s.Calls() {
		if c.Method == method {
			out = append(out, c)
		}
	}

	return out
}

// TestingT is the subset of testing.TB AssertNoMutations needs; GinkgoT() satisfies it too.
type TestingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

// AssertNoMutations fails the test if any recorded call could have changed node state.
func (s *Scenario) AssertNoMutations(t TestingT) {
	t.Helper()

	for _, c := range s.Calls() {
		if mutatingMethods[c.Method] && !c.DryRun {
			t.Fatalf("observe mode issued a mutating call: %s on %s", c.Method, c.Node)
		}
	}
}

// ExpireTry fires the node's try-mode timer, as machined does when the timeout passes without a
// confirming apply: the config that was active when the try was applied becomes active again.
// It reports whether a try was pending.
func (s *Scenario) ExpireTry(addr netip.Addr) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[addr]
	if !ok || n.tryPrevious == nil {
		return false
	}

	n.Config.Active = n.tryPrevious
	n.tryPrevious = nil

	return true
}

// TryPending reports whether the node has an unconfirmed try.
func (s *Scenario) TryPending(addr netip.Addr) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[addr]

	return ok && n.tryPrevious != nil
}

// Down reports whether the node was rebooted and has not booted again.
func (s *Scenario) Down(addr netip.Addr) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[addr]

	return ok && n.down
}

// Boot brings a rebooted node back, as its firmware and Talos would: with a new boot ID, on the
// system an install left in the inactive slot (unless the node FallsBack), and on its persistent
// config, because a tried one does not survive a reboot. It reports whether the node was down.
func (s *Scenario) Boot(addr netip.Addr) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[addr]
	if !ok || !n.down {
		return false
	}

	n.down, n.joining = false, false
	n.BootID += "+"

	if n.installed != "" && !n.FallsBack {
		if schematic, version, ok := ParseInstaller(n.installed); ok {
			n.Schematic.ID, n.Version.Tag = schematic, version
		}
	}

	n.installed = ""

	if n.tryPrevious != nil {
		n.Config.Active, n.tryPrevious = n.tryPrevious, nil
	}

	return true
}

// ParseInstaller splits an Image Factory installer image, <host>/<platform>-installer/<schematic>:<version>,
// into its schematic and version.
func ParseInstaller(image string) (schematic, version string, ok bool) {
	slash, colon := strings.LastIndex(image, "/"), strings.LastIndex(image, ":")
	if slash < 0 || colon < slash {
		return "", "", false
	}

	return image[slash+1 : colon], image[colon+1:], true
}

// Remover returns a talos.Remover backed by the scenario.
func (s *Scenario) Remover() talos.Remover { return &remover{reader{s: s}} }

// Maintenance returns the maintenance connection to the machine at addr.
func (s *Scenario) Maintenance(addr netip.Addr) talos.Maintenance {
	return &maintenance{s: s, addr: addr}
}

// Add puts another node on the network.
func (s *Scenario) Add(n Node) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodes[n.Addr] = &n
}

// Forget takes a node out of the scenario altogether, as when its machine is switched off for
// good and the cluster's discovery has dropped it.
func (s *Scenario) Forget(addr netip.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.nodes, addr)
}

// Kill makes a node answer nothing from now on, with no reboot to come back from. It stays a
// member of whatever it was a member of, as a machine that died does.
func (s *Scenario) Kill(addr netip.Addr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n, ok := s.nodes[addr]; ok {
		n.down = true
	}
}

// remover is the Remover of a scenario.
type remover struct{ reader }

// ForfeitEtcdLeadership implements talos.Remover.
func (r *remover) ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error {
	return (&upgrader{r.reader}).ForfeitEtcdLeadership(ctx, node)
}

// Reset implements talos.Remover: the node leaves etcd, forgets its config and reboots. It comes
// back from Boot as a machine in maintenance mode.
func (r *remover) Reset(ctx context.Context, node netip.Addr) error {
	if _, err := r.s.record(ctx, MethodReset, node); err != nil {
		return err
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	n := r.s.nodes[node]
	if n.EtcdMemberID != 0 && r.s.etcdLeader == n.EtcdMemberID {
		r.s.etcdLeader = 0

		for _, other := range r.s.sortedNodesLocked() {
			if other.EtcdMemberID != 0 && other.EtcdMemberID != n.EtcdMemberID && !other.Maintenance {
				r.s.etcdLeader = other.EtcdMemberID

				break
			}
		}
	}

	n.down, n.Maintenance, n.EtcdMemberID, n.EtcdLearner = true, true, 0, false
	n.Config = talos.MachineConfigs{}
	n.tryPrevious = nil

	return nil
}

// RemoveEtcdMember implements talos.Remover: whichever node has the member loses it, dead or not.
func (r *remover) RemoveEtcdMember(ctx context.Context, peer netip.Addr, id uint64) error {
	asked, err := r.s.record(ctx, MethodEtcdRemoveMember, peer)
	if err != nil {
		return err
	}

	if asked.MachineType != TypeControlPlane {
		return fmt.Errorf("%s on %s: %w", MethodEtcdRemoveMember, peer, status.Error(codes.FailedPrecondition, "fake: not a control plane"))
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	for _, n := range r.s.nodes {
		if n.EtcdMemberID == id {
			n.EtcdMemberID, n.EtcdLearner = 0, false
		}
	}

	return nil
}

// maintenance is the maintenance connection to one machine of a scenario.
type maintenance struct {
	s    *Scenario
	addr netip.Addr
}

// candidate logs a call and returns a copy of the machine, if it waits in maintenance mode.
func (m *maintenance) candidate(ctx context.Context, method string) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.s.mu.Lock()
	defer m.s.mu.Unlock()

	m.s.calls = append(m.s.calls, Call{Method: method, Node: m.addr})

	if err := m.s.errors[errorKey{method: method, node: m.addr}]; err != nil {
		return nil, fmt.Errorf("%s on %s: %w", method, m.addr, err)
	}

	n, ok := m.s.nodes[m.addr]

	switch {
	case !ok, n.down:
		return nil, fmt.Errorf("%s on %s: %w", method, m.addr, status.Error(codes.Unavailable, "fake: nothing answers"))
	case !n.Maintenance:
		// A configured node asks for a client certificate, and a maintenance connection has none.
		return nil, fmt.Errorf("%s on %s: %w", method, m.addr, status.Error(codes.Unavailable, "fake: the node wants a client certificate"))
	}

	copied := *n

	return &copied, nil
}

// Probe implements talos.Maintenance.
func (m *maintenance) Probe(ctx context.Context) (talos.Candidate, error) {
	n, err := m.candidate(ctx, MethodMaintenanceProbe)
	if err != nil {
		return talos.Candidate{}, err
	}

	return talos.Candidate{Stage: talos.StageMaintenance, UUID: n.SystemInformation.UUID}, nil
}

// Fits implements talos.Maintenance.
func (m *maintenance) Fits(ctx context.Context, _ []byte) ([]string, error) {
	n, err := m.candidate(ctx, MethodMaintenanceFits)
	if err != nil {
		return nil, err
	}

	return slices.Clone(n.FitProblems), nil
}

// ApplyConfig implements talos.Maintenance: the machine takes the config and goes away to
// install itself. It is back, as what its Node says, after Boot.
func (m *maintenance) ApplyConfig(ctx context.Context, machineConfig []byte) error {
	if _, err := m.candidate(ctx, MethodMaintenanceApply); err != nil {
		return err
	}

	m.s.mu.Lock()
	defer m.s.mu.Unlock()

	n := m.s.nodes[m.addr]
	n.Maintenance, n.joining, n.down = false, true, true
	n.Config = talos.MachineConfigs{Active: slices.Clone(machineConfig), Persistent: slices.Clone(machineConfig)}

	return nil
}

// Close implements talos.Maintenance.
func (m *maintenance) Close() error { return nil }

// Upgrader returns a talos.Upgrader backed by the scenario.
func (s *Scenario) Upgrader() talos.Upgrader {
	return &upgrader{reader{s: s}}
}

// Writer returns a talos.ConfigWriter backed by the scenario.
func (s *Scenario) Writer() talos.ConfigWriter {
	return &writer{reader{s: s}}
}

// Reader returns a talos.Reader backed by the scenario.
func (s *Scenario) Reader() talos.Reader {
	return &reader{s: s}
}

// record logs a call and returns a copy of the node, or the injected / unavailable error.
func (s *Scenario) record(ctx context.Context, method string, node netip.Addr) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls = append(s.calls, Call{Method: method, Node: node, DryRun: method == MethodApplyConfiguration && dryRun(ctx)})

	if err := s.errors[errorKey{method: method, node: node}]; err != nil {
		return nil, fmt.Errorf("%s on %s: %w", method, node, err)
	}

	n, ok := s.nodes[node]
	if !ok {
		return nil, fmt.Errorf("%s on %s: %w", method, node, status.Error(codes.Unavailable, "fake: no such node"))
	}

	if n.down {
		return nil, fmt.Errorf("%s on %s: %w", method, node, status.Error(codes.Unavailable, "fake: node is rebooting"))
	}

	if n.Maintenance {
		// It asks for no client certificate and has no CA to check one against.
		return nil, fmt.Errorf("%s on %s: %w", method, node, status.Error(codes.Unavailable, "fake: the machine is in maintenance mode"))
	}

	if n.UnreachableWhen != nil && n.UnreachableWhen(n.Config.Active) {
		return nil, fmt.Errorf("%s on %s: %w", method, node, status.Error(codes.Unavailable, "fake: node is cut off"))
	}

	copied := *n

	return &copied, nil
}

func (s *Scenario) membersLocked() []talos.Member {
	if s.members != nil {
		return slices.Clone(s.members)
	}

	out := make([]talos.Member, 0, len(s.nodes))
	for _, n := range s.sortedNodesLocked() {
		if n.Maintenance || n.joining {
			continue
		}

		out = append(out, talos.Member{
			Hostname:        n.Nodename,
			MachineType:     n.MachineType,
			OperatingSystem: "Talos (" + n.Version.Tag + ")",
			Addresses:       []netip.Addr{n.Addr},
		})
	}

	return out
}

func (s *Scenario) etcdMembersLocked() []talos.EtcdMember {
	if s.etcdMembers != nil {
		return slices.Clone(s.etcdMembers)
	}

	var out []talos.EtcdMember

	for _, n := range s.sortedNodesLocked() {
		if n.EtcdMemberID != 0 && !n.Maintenance && !n.joining {
			out = append(out, talos.EtcdMember{ID: n.EtcdMemberID, Hostname: n.Nodename, IsLearner: n.EtcdLearner})
		}
	}

	return out
}

func (s *Scenario) sortedNodesLocked() []*Node {
	nodes := make([]*Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}

	slices.SortFunc(nodes, func(a, b *Node) int { return a.Addr.Compare(b.Addr) })

	return nodes
}

type reader struct {
	s *Scenario
}

var _ talos.Reader = (*reader)(nil)

func (r *reader) Version(ctx context.Context, node netip.Addr) (talos.VersionInfo, error) {
	n, err := r.s.record(ctx, MethodVersion, node)
	if err != nil {
		return talos.VersionInfo{}, err
	}

	return n.Version, nil
}

func (r *reader) MachineStatus(ctx context.Context, node netip.Addr) (talos.MachineStatus, error) {
	n, err := r.s.record(ctx, MethodMachineStatus, node)
	if err != nil {
		return talos.MachineStatus{}, err
	}

	ms := n.MachineStatus
	if n.unhealthy() {
		ms.Ready = false
	}

	return ms, nil
}

func (r *reader) BootID(ctx context.Context, node netip.Addr) (string, error) {
	n, err := r.s.record(ctx, MethodBootID, node)
	if err != nil {
		return "", err
	}

	return n.BootID, nil
}

func (r *reader) SystemInformation(ctx context.Context, node netip.Addr) (talos.SystemInformation, error) {
	n, err := r.s.record(ctx, MethodSystemInformation, node)
	if err != nil {
		return talos.SystemInformation{}, err
	}

	return n.SystemInformation, nil
}

func (r *reader) ImageFactorySchematic(ctx context.Context, node netip.Addr) (talos.Schematic, error) {
	n, err := r.s.record(ctx, MethodImageFactorySchematic, node)
	if err != nil {
		return talos.Schematic{}, err
	}

	if n.Schematic.ID == "" {
		return talos.Schematic{}, fmt.Errorf("schematic on %s: %w", node, talos.ErrNotFound)
	}

	return n.Schematic, nil
}

func (r *reader) Nodename(ctx context.Context, node netip.Addr) (string, error) {
	n, err := r.s.record(ctx, MethodNodename, node)
	if err != nil {
		return "", err
	}

	return n.Nodename, nil
}

func (r *reader) UnattendedInstallStatus(ctx context.Context, node netip.Addr) (talos.InstallStatus, error) {
	n, err := r.s.record(ctx, MethodUnattendedInstallStatus, node)
	if err != nil {
		return talos.InstallStatus{}, err
	}

	if n.InstallStatus == nil {
		return talos.InstallStatus{}, fmt.Errorf("install status on %s: %w", node, talos.ErrNotFound)
	}

	return *n.InstallStatus, nil
}

func (r *reader) Services(ctx context.Context, node netip.Addr) ([]talos.ServiceStatus, error) {
	n, err := r.s.record(ctx, MethodServices, node)
	if err != nil {
		return nil, err
	}

	return slices.Clone(n.Services), nil
}

func (r *reader) MachineType(ctx context.Context, node netip.Addr) (string, error) {
	n, err := r.s.record(ctx, MethodMachineType, node)
	if err != nil {
		return "", err
	}

	return n.MachineType, nil
}

func (r *reader) MachineConfig(ctx context.Context, node netip.Addr) (talos.MachineConfigs, error) {
	n, err := r.s.record(ctx, MethodMachineConfig, node)
	if err != nil {
		return talos.MachineConfigs{}, err
	}

	return talos.MachineConfigs{
		Active:     slices.Clone(n.Config.Active),
		Persistent: slices.Clone(n.Config.Persistent),
	}, nil
}

func (r *reader) Members(ctx context.Context, node netip.Addr) ([]talos.Member, error) {
	if _, err := r.s.record(ctx, MethodMembers, node); err != nil {
		return nil, err
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	return r.s.membersLocked(), nil
}

func (r *reader) EtcdMembers(ctx context.Context, node netip.Addr) ([]talos.EtcdMember, error) {
	n, err := r.s.record(ctx, MethodEtcdMembers, node)
	if err != nil {
		return nil, err
	}

	if n.MachineType != TypeControlPlane {
		return nil, fmt.Errorf("etcd members on %s: %w", node, status.Error(codes.FailedPrecondition, "not a control plane node"))
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	return r.s.etcdMembersLocked(), nil
}

func (r *reader) EtcdStatus(ctx context.Context, node netip.Addr) (talos.EtcdStatus, error) {
	n, err := r.s.record(ctx, MethodEtcdStatus, node)
	if err != nil {
		return talos.EtcdStatus{}, err
	}

	if n.EtcdMemberID == 0 {
		return talos.EtcdStatus{}, fmt.Errorf("etcd status on %s: %w", node, status.Error(codes.FailedPrecondition, "etcd is not running"))
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	return talos.EtcdStatus{
		MemberID: n.EtcdMemberID, Leader: r.s.etcdLeader, IsLearner: n.EtcdLearner, DBSize: n.EtcdDBSize, DBSizeInUse: n.EtcdDBInUse,
	}, nil
}

// Diagnostics implements talos.Reader.
func (r *reader) Diagnostics(ctx context.Context, node netip.Addr) ([]talos.Diagnostic, error) {
	n, err := r.s.record(ctx, MethodNodeHealth, node)
	if err != nil {
		return nil, err
	}

	return slices.Clone(n.Diagnostics), nil
}

// TimeSynced implements talos.Reader.
func (r *reader) TimeSynced(ctx context.Context, node netip.Addr) (bool, error) {
	n, err := r.s.record(ctx, MethodNodeHealth, node)
	if err != nil {
		return false, err
	}

	return !n.TimeUnsynced, nil
}

// Links implements talos.Reader.
func (r *reader) Links(ctx context.Context, node netip.Addr) ([]talos.Link, error) {
	n, err := r.s.record(ctx, MethodNodeHealth, node)
	if err != nil {
		return nil, err
	}

	return slices.Clone(n.Links), nil
}

// Volumes implements talos.Reader.
func (r *reader) Volumes(ctx context.Context, node netip.Addr) ([]talos.Volume, error) {
	n, err := r.s.record(ctx, MethodNodeHealth, node)
	if err != nil {
		return nil, err
	}

	return slices.Clone(n.Volumes), nil
}

// Logs implements talos.Reader.
func (r *reader) Logs(ctx context.Context, node netip.Addr, service string, _ int32) ([]byte, error) {
	n, err := r.s.record(ctx, MethodLogs, node)
	if err != nil {
		return nil, err
	}

	return []byte(n.Logs[service]), nil
}

// Dmesg implements talos.Reader.
func (r *reader) Dmesg(ctx context.Context, node netip.Addr) ([]byte, error) {
	return r.Logs(ctx, node, "dmesg", 0)
}

// Keeper returns a talos.Keeper backed by the scenario.
func (s *Scenario) Keeper() talos.Keeper { return &keeper{reader{s: s}} }

// keeper is the Keeper of a scenario.
type keeper struct{ reader }

// ForfeitEtcdLeadership implements talos.Keeper.
func (k *keeper) ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error {
	return (&upgrader{k.reader}).ForfeitEtcdLeadership(ctx, node)
}

// DefragmentEtcd implements talos.Keeper: the member's file shrinks to what it holds.
func (k *keeper) DefragmentEtcd(ctx context.Context, node netip.Addr) error {
	n, err := k.s.record(ctx, MethodEtcdDefragment, node)
	if err != nil {
		return err
	}

	if n.EtcdMemberID == 0 {
		return fmt.Errorf("%s on %s: %w", MethodEtcdDefragment, node, status.Error(codes.FailedPrecondition, "fake: not a member of etcd"))
	}

	k.s.mu.Lock()
	defer k.s.mu.Unlock()

	k.s.nodes[node].EtcdDBSize = k.s.nodes[node].EtcdDBInUse

	return nil
}

func (r *reader) HealthCheck(ctx context.Context, node netip.Addr, _ talos.ClusterInfo, _ time.Duration) (talos.HealthResult, error) {
	if _, err := r.s.record(ctx, MethodHealthCheck, node); err != nil {
		return talos.HealthResult{}, err
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	// Talos checks the cluster as a whole: one node that is not ready fails it.
	for _, n := range r.s.sortedNodesLocked() {
		if n.unhealthy() {
			return talos.HealthResult{Messages: []string{n.Nodename + " is not ready"}}, nil
		}
	}

	return talos.HealthResult{Healthy: r.s.health.Healthy, Messages: slices.Clone(r.s.health.Messages)}, nil
}

// KubernetesUpgradeChecks implements talos.Reader: every node is asked, and one that does not
// answer fails the checks.
func (r *reader) KubernetesUpgradeChecks(ctx context.Context, _, to string, nodes talos.ClusterInfo) ([]string, error) {
	for _, node := range slices.Concat(nodes.ControlPlanes, nodes.Workers) {
		if _, err := r.s.record(ctx, MethodKubernetesUpgradeChecks, node); err != nil {
			return nil, err
		}
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	return slices.Clone(r.s.removed[to]), nil
}

// Factory is a fake target.ClientFactory that serves the scenario's Reader and ConfigWriter and
// records every target it was asked for, so tests can assert credentials and endpoint selection.
type Factory struct {
	Scenario *Scenario

	mu              sync.Mutex
	targets         []talos.Target
	writerTargets   []talos.Target
	upgraderTargets []talos.Target
	removerTargets  []talos.Target
	keeperTargets   []talos.Target
}

// Keeper implements target.ClientFactory.
func (f *Factory) Keeper(_ context.Context, t talos.Target) (talos.Keeper, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.keeperTargets = append(f.keeperTargets, t)

	return f.Scenario.Keeper(), nil
}

// KeeperTargets returns the targets a Keeper was requested for.
func (f *Factory) KeeperTargets() []talos.Target {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.keeperTargets)
}

// Remover implements target.ClientFactory.
func (f *Factory) Remover(_ context.Context, t talos.Target) (talos.Remover, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removerTargets = append(f.removerTargets, t)

	return f.Scenario.Remover(), nil
}

// RemoverTargets returns the targets a Remover was requested for.
func (f *Factory) RemoverTargets() []talos.Target {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.removerTargets)
}

// Maintenance implements target.ClientFactory.
func (f *Factory) Maintenance(_ context.Context, addr netip.Addr) (talos.Maintenance, error) {
	return f.Scenario.Maintenance(addr), nil
}

// Upgrader implements target.ClientFactory.
func (f *Factory) Upgrader(_ context.Context, t talos.Target) (talos.Upgrader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.upgraderTargets = append(f.upgraderTargets, t)

	return f.Scenario.Upgrader(), nil
}

// UpgraderTargets returns the targets an Upgrader was requested for.
func (f *Factory) UpgraderTargets() []talos.Target {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.upgraderTargets)
}

// ConfigWriter implements target.ClientFactory.
func (f *Factory) ConfigWriter(_ context.Context, t talos.Target) (talos.ConfigWriter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.writerTargets = append(f.writerTargets, t)

	return f.Scenario.Writer(), nil
}

// WriterTargets returns the targets a ConfigWriter was requested for.
func (f *Factory) WriterTargets() []talos.Target {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.writerTargets)
}

// Reader implements target.ReaderFactory.
func (f *Factory) Reader(_ context.Context, t talos.Target) (talos.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.targets = append(f.targets, t)

	return f.Scenario.Reader(), nil
}

// Targets returns the targets requested so far.
func (f *Factory) Targets() []talos.Target {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.targets)
}

type dryRunKey struct{}

// dryRun reports whether ctx belongs to a dry-run apply, so record can mark the call.
func dryRun(ctx context.Context) bool {
	v, _ := ctx.Value(dryRunKey{}).(bool)

	return v
}

// DryRunApply answers like Talos: it diffs the request against the node's active config with
// machinery's configdiff, which is what the server does (generateDiff in Talos v1.14).
func (r *reader) DryRunApply(ctx context.Context, node netip.Addr, machineConfig []byte) (talos.DryRunResult, error) {
	n, err := r.s.record(context.WithValue(ctx, dryRunKey{}, true), MethodApplyConfiguration, node)
	if err != nil {
		return talos.DryRunResult{}, err
	}

	if n.DryRunReportsChanges {
		return talos.DryRunResult{}, nil
	}

	active, err := talos.ParseMachineConfig(n.Config.Active)
	if err != nil {
		return talos.DryRunResult{}, fmt.Errorf("fake: active config of %s: %w", node, err)
	}

	requested, err := talos.ParseMachineConfig(machineConfig)
	if err != nil {
		return talos.DryRunResult{}, status.Error(codes.InvalidArgument, "fake: the requested config does not parse")
	}

	diff, err := configdiff.DiffConfigs(active, requested)
	if err != nil {
		return talos.DryRunResult{}, err
	}

	return talos.DryRunResult{NoChanges: diff == ""}, nil
}

// writer implements talos.ConfigWriter with Talos's apply semantics
// (v1alpha1_server.go:281-311 and RollbackToConfigAfter in Talos v1.14).
type writer struct {
	reader
}

// ApplyConfig implements talos.ConfigWriter.
func (w *writer) ApplyConfig(ctx context.Context, node netip.Addr, machineConfig []byte, mode talos.ApplyMode,
	tryTimeout time.Duration,
) error {
	if _, err := w.s.record(ctx, MethodApplyConfiguration, node); err != nil {
		return err
	}

	if _, err := talos.ParseMachineConfig(machineConfig); err != nil {
		return status.Error(codes.InvalidArgument, "fake: the config does not parse")
	}

	w.s.mu.Lock()
	defer w.s.mu.Unlock()

	w.s.calls[len(w.s.calls)-1].Mode = mode

	n := w.s.nodes[node]

	switch mode {
	case talos.ApplyNoReboot:
		// Any non-try apply cancels the rollback timer.
		n.tryPrevious = nil
		n.Config.Persistent = slices.Clone(machineConfig)
		n.Config.Active = slices.Clone(machineConfig)
	case talos.ApplyTry:
		if tryTimeout <= 0 {
			return status.Error(codes.InvalidArgument, "fake: a try needs a timeout")
		}

		// The rollback target is whatever is active now, and an earlier timer is cancelled. A
		// second try of the same config therefore falls back to the tried config itself.
		n.tryPrevious = slices.Clone(n.Config.Active)
		n.Config.Active = slices.Clone(machineConfig)
	default:
		return status.Error(codes.InvalidArgument, "fake: unsupported apply mode")
	}

	return nil
}

// upgrader implements talos.Upgrader the way a node behaves: an install only fills the inactive
// boot slot, and nothing about the node changes until it reboots and is booted again (Boot).
type upgrader struct {
	reader
}

var _ talos.Upgrader = (*upgrader)(nil)

func (u *upgrader) image(ctx context.Context, method string, node netip.Addr, image string) (*Node, error) {
	if _, err := u.s.record(ctx, method, node); err != nil {
		return nil, err
	}

	u.s.mu.Lock()
	defer u.s.mu.Unlock()

	u.s.calls[len(u.s.calls)-1].Image = image

	return u.s.nodes[node], nil
}

// PullImage implements talos.Upgrader.
func (u *upgrader) PullImage(ctx context.Context, node netip.Addr, image string) error {
	n, err := u.image(ctx, MethodImagePull, node, image)
	if err != nil {
		return err
	}

	u.s.mu.Lock()
	defer u.s.mu.Unlock()

	if n.pulled == nil {
		n.pulled = map[string]bool{}
	}

	n.pulled[image] = true

	return nil
}

// PullKubernetesImage implements talos.Upgrader.
func (u *upgrader) PullKubernetesImage(ctx context.Context, node netip.Addr, component, image string) error {
	// Only what Talos runs from a node's config. kube-proxy is not among it: nothing pulls it here.
	if err := talos.RunsKubernetesComponent(component); err != nil {
		return err
	}

	_, err := u.image(ctx, MethodKubernetesImagePull, node, image)

	return err
}

// Install implements talos.Upgrader.
func (u *upgrader) Install(ctx context.Context, node netip.Addr, image string) error {
	n, err := u.image(ctx, MethodInstall, node, image)
	if err != nil {
		return err
	}

	u.s.mu.Lock()
	defer u.s.mu.Unlock()

	switch {
	case !n.pulled[image]:
		return status.Error(codes.NotFound, "fake: the image was not pulled")
	case n.InstallExitCode != 0:
		return &talos.InstallError{ExitCode: n.InstallExitCode, Detail: "fake: the installer failed"}
	}

	n.installed = image

	return nil
}

// ForfeitEtcdLeadership implements talos.Upgrader: the next control plane takes over.
func (u *upgrader) ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error {
	n, err := u.s.record(ctx, MethodEtcdForfeitLeadership, node)
	if err != nil {
		return err
	}

	u.s.mu.Lock()
	defer u.s.mu.Unlock()

	if n.EtcdMemberID == 0 || u.s.etcdLeader != n.EtcdMemberID {
		return nil
	}

	for _, other := range u.s.sortedNodesLocked() {
		if other.EtcdMemberID != 0 && other.EtcdMemberID != n.EtcdMemberID {
			u.s.etcdLeader = other.EtcdMemberID

			break
		}
	}

	return nil
}

// Reboot implements talos.Upgrader. The node is down until Boot.
func (u *upgrader) Reboot(ctx context.Context, node netip.Addr) error {
	if _, err := u.s.record(ctx, MethodReboot, node); err != nil {
		return err
	}

	u.s.mu.Lock()
	defer u.s.mu.Unlock()

	u.s.nodes[node].down = true

	return nil
}

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
)

// Machine types as Talos reports them.
const (
	TypeControlPlane = "controlplane"
	TypeWorker       = "worker"
)

// mutatingMethods lists recorded methods that change node state. A dry-run apply is recorded
// under ApplyConfiguration with DryRun set, which AssertNoMutations allows.
var mutatingMethods = map[string]bool{MethodApplyConfiguration: true}

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
	// tryPrevious is the config a pending try falls back to: whatever was active when the try
	// was applied, exactly as Talos captures it.
	tryPrevious []byte
	// EtcdMemberID identifies the node's etcd member; 0 means none (workers).
	EtcdMemberID uint64
	EtcdLearner  bool
}

// Call is one recorded Reader call.
type Call struct {
	Method string
	Node   netip.Addr
	// DryRun is true for dry-run applies, which are allowed in observe mode.
	DryRun bool
	// Mode is the mode of a real apply; zero otherwise.
	Mode talos.ApplyMode
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
		if n.EtcdMemberID != 0 {
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

	return n.MachineStatus, nil
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

	return talos.EtcdStatus{MemberID: n.EtcdMemberID, Leader: r.s.etcdLeader, IsLearner: n.EtcdLearner}, nil
}

func (r *reader) HealthCheck(ctx context.Context, node netip.Addr, _ talos.ClusterInfo, _ time.Duration) (talos.HealthResult, error) {
	if _, err := r.s.record(ctx, MethodHealthCheck, node); err != nil {
		return talos.HealthResult{}, err
	}

	r.s.mu.Lock()
	defer r.s.mu.Unlock()

	return talos.HealthResult{Healthy: r.s.health.Healthy, Messages: slices.Clone(r.s.health.Messages)}, nil
}

// Factory is a fake target.ClientFactory that serves the scenario's Reader and ConfigWriter and
// records every target it was asked for, so tests can assert credentials and endpoint selection.
type Factory struct {
	Scenario *Scenario

	mu            sync.Mutex
	targets       []talos.Target
	writerTargets []talos.Target
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

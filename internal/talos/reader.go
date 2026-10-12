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

// Package talos is the operator's only gateway to the Talos API (docs/scope.md §4.1).
//
// Observe mode is write-proof by construction: controllers only ever hold a Reader, whose
// methods are all read-only, and every Reader connection additionally runs behind a gRPC
// interceptor that rejects any method outside a read-only allowlist.
package talos

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"
)

// ErrNotFound is returned (wrapped) when a node does not have the requested resource, e.g. no
// unattended install status on a node installed before UnattendedInstallConfig existed.
var ErrNotFound = errors.New("not found")

// Reader is the read-only view of a Talos cluster. Methods take the node to query and return
// operator-owned types, so machinery types never leak into controllers and fakes stay simple.
//
// Every method added here must be read-only and its RPC must be on the allowlist in allowlist.go.
type Reader interface {
	// Version returns the Talos version running on node.
	Version(ctx context.Context, node netip.Addr) (VersionInfo, error)
	// MachineStatus returns the node's boot stage and readiness.
	MachineStatus(ctx context.Context, node netip.Addr) (MachineStatus, error)
	// BootID returns the node's current boot ID; it changes on every reboot.
	BootID(ctx context.Context, node netip.Addr) (string, error)
	// SystemInformation returns SMBIOS identity, used to bind TalosMachines to nodes.
	SystemInformation(ctx context.Context, node netip.Addr) (SystemInformation, error)
	// ImageFactorySchematic returns the schematic the node booted from.
	ImageFactorySchematic(ctx context.Context, node netip.Addr) (Schematic, error)
	// Nodename returns the node's Kubernetes node name.
	Nodename(ctx context.Context, node netip.Addr) (string, error)
	// UnattendedInstallStatus returns the install status; ErrNotFound if the node has none.
	UnattendedInstallStatus(ctx context.Context, node netip.Addr) (InstallStatus, error)
	// Services returns the state of every Talos service on the node.
	Services(ctx context.Context, node netip.Addr) ([]ServiceStatus, error)
	// MachineType returns "controlplane" or "worker".
	MachineType(ctx context.Context, node netip.Addr) (string, error)
	// MachineConfig returns the node's active machine config and, if one was applied in this boot,
	// the persistent one, as raw bytes. The resource is sensitive: it needs the os:admin role.
	// Parse with ParseMachineConfig.
	MachineConfig(ctx context.Context, node netip.Addr) (MachineConfigs, error)
	// Members returns cluster members as seen by node through discovery.
	Members(ctx context.Context, node netip.Addr) ([]Member, error)
	// EtcdMembers returns the etcd member list as seen by a control-plane node.
	EtcdMembers(ctx context.Context, node netip.Addr) ([]EtcdMember, error)
	// EtcdStatus returns the local etcd member's status on a control-plane node.
	EtcdStatus(ctx context.Context, node netip.Addr) (EtcdStatus, error)
	// HealthCheck runs Talos's cluster health checks on node (server-side), waiting at most timeout.
	HealthCheck(ctx context.Context, node netip.Addr, info ClusterInfo, timeout time.Duration) (HealthResult, error)
	// DryRunApply asks node whether applying machineConfig would change its active config, without
	// applying it. It always uses mode TRY: Talos cancels a pending TRY rollback for any other
	// mode, even in a dry run. Only the verdict is returned, because the server's diff is an
	// unredacted, order-sensitive text diff of the whole config.
	DryRunApply(ctx context.Context, node netip.Addr, machineConfig []byte) (DryRunResult, error)
	// KubernetesUpgradeChecks reads how the Kubernetes components on the given nodes are started,
	// and describes every flag, feature gate and admission plugin that the upgrade from
	// Kubernetes from to to removes. Empty means nothing stands in the way; an error, that it
	// could not be told.
	KubernetesUpgradeChecks(ctx context.Context, from, to string, nodes ClusterInfo) ([]string, error)

	// Diagnostics returns the warnings Talos raises about node's configuration right now.
	Diagnostics(ctx context.Context, node netip.Addr) ([]Diagnostic, error)
	// TimeSynced reports whether node's clock is synchronised.
	TimeSynced(ctx context.Context, node netip.Addr) (bool, error)
	// Links returns node's physical network links.
	Links(ctx context.Context, node netip.Addr) ([]Link, error)
	// Volumes returns Talos's volumes on node.
	Volumes(ctx context.Context, node netip.Addr) ([]Volume, error)
	// SecureBoot reports whether node's firmware enforces Secure Boot.
	SecureBoot(ctx context.Context, node netip.Addr) (bool, error)
	// SecureBootDB returns what node's firmware trusts to boot: the certificates and image hashes
	// in its signature database. It is read from the one file of a node the operator may read.
	SecureBootDB(ctx context.Context, node netip.Addr) (SecureBootDB, error)
	// Logs returns the end of the log of one of node's Talos services: its last lines, and of
	// those the last 128 KiB.
	Logs(ctx context.Context, node netip.Addr, service string, lines int32) ([]byte, error)
	// Dmesg returns the end of node's kernel log.
	Dmesg(ctx context.Context, node netip.Addr) ([]byte, error)
}

// Upgrader is the Talos access of upgrades: Enabled: everything a Reader can do, plus the calls an
// upgrade of a node is made of (docs/scope.md §7.3). It cannot apply machine config, reset a node
// or change etcd membership.
//
// The sequence is the one talosctl follows since Talos 1.13 (cmd/talosctl/cmd/talos/upgrade.go:
// 93-174): pull, install to the inactive boot slot, and only then reboot. Draining is Kubernetes'
// business and happens in between, by the caller.
type Upgrader interface {
	Reader

	// PullImage fetches the installer image to node. It takes as long as the download does, so the
	// caller's context bounds it.
	PullImage(ctx context.Context, node netip.Addr, image string) error
	// PullKubernetesImage fetches the image of a Kubernetes component to node, to where that
	// component is run from, so that a Kubernetes upgrade restarts it without waiting for a
	// download (docs/scope.md §7.4).
	PullKubernetesImage(ctx context.Context, node netip.Addr, component, image string) error
	// Install writes the system from a pulled installer image to node's inactive boot slot. The node
	// keeps running what it runs until it reboots. A failed install is reported as an
	// *InstallError; the caller's context bounds the call.
	Install(ctx context.Context, node netip.Addr, image string) error
	// ForfeitEtcdLeadership makes node hand etcd leadership to another member, if it is the leader.
	ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error
	// Reboot asks node to reboot. It returns once the node accepted the request.
	Reboot(ctx context.Context, node netip.Addr) error
}

// InstallError is an installer that ran and failed. The node is unchanged.
type InstallError struct {
	ExitCode int32
	// Detail is the installer's last message, if it said anything.
	Detail string
}

func (e *InstallError) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("the installer exited with code %d", e.ExitCode)
	}

	return fmt.Sprintf("the installer exited with code %d: %s", e.ExitCode, e.Detail)
}

// DryRunResult is Talos's verdict on a dry-run apply.
type DryRunResult struct {
	// NoChanges is true when the server's diff against the active config is empty.
	NoChanges bool
}

// VersionInfo is the Talos version of one node.
type VersionInfo struct {
	// Tag is the Talos release, e.g. v1.14.2.
	Tag string
	// SHA is the source commit.
	SHA string
	// Arch is the machine architecture, e.g. amd64.
	Arch string
}

// MachineStatus is the node's position in its boot sequence.
type MachineStatus struct {
	// Stage is the Talos machine stage, e.g. "running" or "maintenance".
	Stage string
	// Ready is true once every readiness condition is met.
	Ready bool
	// UnmetConditions explain why the node is not ready, as "name: reason".
	UnmetConditions []string
}

// SystemInformation is the node's SMBIOS identity.
type SystemInformation struct {
	UUID         string
	SerialNumber string
	Manufacturer string
	ProductName  string
}

// Schematic is the Image Factory schematic the node booted from.
type Schematic struct {
	ID     string
	Flavor string
	// FactoryURL is the Image Factory API URL the schematic came from.
	FactoryURL string
}

// InstallStatus is the node's unattended install progress.
type InstallStatus struct {
	// Phase is pending, installing, installed, waiting-for-reboot or failed.
	Phase string
	Image string
	Error string
}

// ServiceStatus is one Talos service's state.
type ServiceStatus struct {
	ID      string
	Running bool
	Healthy bool
	// Unknown is true when the service has no health check.
	Unknown bool
}

// MachineConfigs holds a node's machine config as Talos stores it.
type MachineConfigs struct {
	// Active is the config the running system uses.
	Active []byte
	// Persistent is the config last applied to disk in this boot, or nil when nothing was applied
	// since the node booted from its on-disk config (Talos only creates the resource on apply:
	// internal/app/machined/pkg/controllers/config/acquire.go:224 in v1.14). When set, it differs
	// from Active while a staged apply is pending.
	//
	// A TRY apply changes only Active, so during its rollback window (default 1m) Persistent can
	// be nil or equal to the pre-TRY config while Active differs.
	Persistent []byte
}

// PendingConfigChange reports whether a staged apply is waiting for the next reboot.
func (m MachineConfigs) PendingConfigChange() bool {
	return m.Persistent != nil && HashConfig(m.Persistent) != HashConfig(m.Active)
}

// Member is one cluster member from Talos discovery.
type Member struct {
	NodeID          string
	Hostname        string
	MachineType     string
	OperatingSystem string
	Addresses       []netip.Addr
}

// EtcdMember is one etcd cluster member.
type EtcdMember struct {
	ID         uint64
	Hostname   string
	PeerURLs   []string
	ClientURLs []string
	IsLearner  bool
}

// EtcdStatus is the status of one etcd member.
type EtcdStatus struct {
	MemberID    uint64
	Leader      uint64
	DBSize      int64
	DBSizeInUse int64
	RaftIndex   uint64
	IsLearner   bool
	Errors      []string
}

// ClusterInfo lists the nodes the health check should expect.
type ClusterInfo struct {
	ControlPlanes []netip.Addr
	Workers       []netip.Addr
}

// HealthResult is the outcome of a server-side health check.
type HealthResult struct {
	Healthy bool
	// Messages are the check's progress messages, the last of which explains a failure.
	Messages []string
}

// ApplyMode is how a machine config is applied.
type ApplyMode int

const (
	// ApplyNoReboot makes the config active and persistent, without rebooting.
	ApplyNoReboot ApplyMode = iota + 1
	// ApplyTry makes the config active only. Talos restores the previous config by itself when the
	// try timeout expires, unless an ApplyNoReboot confirms it first.
	ApplyTry
)

// ConfigWriter is the Talos access of config: Manage: everything a Reader can do, plus applying
// machine config. It cannot reboot, upgrade or reset a node.
type ConfigWriter interface {
	Reader

	// ApplyConfig applies machineConfig to node. tryTimeout is used by ApplyTry only.
	//
	// Never send ApplyTry to a node whose active config is not the one to fall back to. Talos
	// takes the config that is active at that moment as the rollback target and cancels any
	// earlier timer (RollbackToConfigAfter, v1alpha1_runtime.go:92-108 in Talos v1.14), so trying
	// a config a second time would "roll back" to the tried config itself.
	ApplyConfig(ctx context.Context, node netip.Addr, machineConfig []byte, mode ApplyMode, tryTimeout time.Duration) error
}

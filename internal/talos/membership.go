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
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"sync"

	"github.com/cosi-project/runtime/pkg/safe"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	configconfig "github.com/siderolabs/talos/pkg/machinery/config/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/types/block/blockhelpers"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StageMaintenance is the stage of a machine that has no config and waits for one.
const StageMaintenance = "maintenance"

// Remover is the Talos access of membership: Enabled for a machine that leaves the cluster:
// everything a Reader can do, plus the three calls a removal is made of (docs/scope.md §7.7). It
// cannot apply config, upgrade or reboot a node.
type Remover interface {
	Reader

	// ForfeitEtcdLeadership makes node hand etcd leadership to another member, if it is the leader.
	ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error
	// Reset asks node to leave the cluster and forget it: it leaves etcd if it is a member, wipes
	// Talos's own STATE and EPHEMERAL partitions, and reboots, after which it has no config and
	// waits in maintenance mode. No other partition and no other disk is touched; this client
	// cannot ask for more. It returns once the node accepted the request.
	Reset(ctx context.Context, node netip.Addr) error
	// RemoveEtcdMember removes the member with the given ID from etcd, asking peer, which must be
	// another, healthy control plane. A member that is not there is not an error.
	RemoveEtcdMember(ctx context.Context, peer netip.Addr, id uint64) error
}

// Candidate is what a machine in maintenance mode says of itself. Nothing in it is authenticated:
// a machine without a config has no identity it could prove (docs/scope.md §7.6).
type Candidate struct {
	// Stage is the machine's stage; StageMaintenance for one that waits for a config.
	Stage string
	// UUID is the hardware UUID the machine reports.
	UUID string
}

// Maintenance is the access to one machine that waits in maintenance mode, at one address, without
// credentials: such a machine has none to check, and none to present. It is handed out only while
// membership is Enabled.
//
// A Maintenance pins the key of whoever answers its first call and refuses to talk to anyone
// else afterwards. What Probe read and what ApplyConfig is sent to are therefore the same
// machine, as far as an unauthenticated connection can tell.
type Maintenance interface {
	// Probe reads the machine's stage and hardware identity.
	Probe(ctx context.Context) (Candidate, error)
	// Fits reports why machineConfig does not fit the machine, or nothing: its install disk
	// selector must match exactly one of the machine's disks (Talos installs to the first match
	// without a word), and every physical link it configures must exist.
	Fits(ctx context.Context, machineConfig []byte) ([]string, error)
	// ApplyConfig hands the machine its config. The machine then leaves maintenance mode,
	// installs itself and joins the cluster the config names; this connection is of no use
	// after that.
	ApplyConfig(ctx context.Context, machineConfig []byte) error
	// Close ends the connection.
	Close() error
}

// membershipMethods are the RPCs a Remover connection may issue besides the read-only ones.
var membershipMethods = map[string]bool{
	machineapi.MachineService_EtcdForfeitLeadership_FullMethodName: true,
	machineapi.MachineService_EtcdRemoveMemberByID_FullMethodName:  true,
}

// checkMembership is the allowlist of a Remover connection. A Reset is only let through in the
// one shape a removal uses: graceful, with a reboot, wiping exactly STATE and EPHEMERAL of the
// system disk. A reset of the whole system disk, of a user disk, or one that leaves etcd without
// saying so, is refused here, whatever the caller meant.
func checkMembership(method string, req any) string {
	if membershipMethods[method] {
		return ""
	}

	if method == machineapi.MachineService_Reset_FullMethodName {
		if reset, ok := req.(*machineapi.ResetRequest); ok && removalReset(reset) {
			return ""
		}

		return "this reset is not the one a removal is allowed"
	}

	return checkReadOnly(method, req)
}

// removalReset reports whether req is exactly the reset of a removal.
func removalReset(req *machineapi.ResetRequest) bool {
	if !req.GetGraceful() || !req.GetReboot() || len(req.GetUserDisksToWipe()) > 0 ||
		req.GetMode() != machineapi.ResetRequest_SYSTEM_DISK {
		return false
	}

	labels := make([]string, 0, len(req.GetSystemPartitionsToWipe()))

	for _, partition := range req.GetSystemPartitionsToWipe() {
		if !partition.GetWipe() {
			return false
		}

		labels = append(labels, partition.GetLabel())
	}

	slices.Sort(labels)

	return slices.Equal(labels, []string{constants.EphemeralPartitionLabel, constants.StatePartitionLabel})
}

// checkMaintenance is the allowlist of a Maintenance connection: reading resources, and applying
// a config without a reboot, which is the only mode that makes a machine in maintenance mode take
// one. The machine itself allows an unauthenticated caller little more.
func checkMaintenance(method string, req any) string {
	switch method {
	case cosiGetMethod, cosiListMethod:
		return ""
	case machineapi.MachineService_ApplyConfiguration_FullMethodName:
		if apply, ok := req.(*machineapi.ApplyConfigurationRequest); ok && !apply.GetDryRun() &&
			apply.GetMode() == machineapi.ApplyConfigurationRequest_NO_REBOOT {
			return ""
		}
	}

	return method + " is not allowed on a maintenance connection"
}

// machineryRemover implements Remover with the machinery client.
type machineryRemover struct{ machineryReader }

var _ Remover = (*machineryRemover)(nil)

// ForfeitEtcdLeadership implements Remover.
func (m *machineryRemover) ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error {
	ctx, cancel := m.callContext(ctx, node)
	defer cancel()

	if _, err := m.client.EtcdForfeitLeadership(ctx, &machineapi.EtcdForfeitLeadershipRequest{}); err != nil {
		return fmt.Errorf("forfeit etcd leadership on %s: %w", node, err)
	}

	return nil
}

// Reset implements Remover. The request is the one talosctl sends for
// `reset --graceful --reboot --wipe-mode system-disk --system-labels-to-wipe STATE,EPHEMERAL`
// (cmd/talosctl/cmd/talos/reset.go:168-206): with labels given, Talos wipes those partitions and
// nothing else (v1alpha1_sequencer.go:338-349).
func (m *machineryRemover) Reset(ctx context.Context, node netip.Addr) error {
	ctx, cancel := m.callContext(ctx, node)
	defer cancel()

	err := m.client.ResetGeneric(ctx, &machineapi.ResetRequest{
		Graceful: true,
		Reboot:   true,
		Mode:     machineapi.ResetRequest_SYSTEM_DISK,
		SystemPartitionsToWipe: []*machineapi.ResetPartitionSpec{
			{Label: constants.StatePartitionLabel, Wipe: true},
			{Label: constants.EphemeralPartitionLabel, Wipe: true},
		},
	})
	if err != nil {
		return fmt.Errorf("reset %s: %w", node, err)
	}

	return nil
}

// RemoveEtcdMember implements Remover.
func (m *machineryRemover) RemoveEtcdMember(ctx context.Context, peer netip.Addr, id uint64) error {
	ctx, cancel := m.callContext(ctx, peer)
	defer cancel()

	err := m.client.EtcdRemoveMemberByID(ctx, &machineapi.EtcdRemoveMemberByIDRequest{MemberId: id})
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("remove etcd member %x through %s: %w", id, peer, err)
	}

	return nil
}

// errOtherKey is a connection to someone else than the first call reached.
var errOtherKey = errors.New("the machine at this address presents another key than the one that was identified")

// keyPin remembers the public key of the first server a connection reached, and accepts no other.
// A machine in maintenance mode signs its certificate itself, so there is nothing to verify it
// against; what can be had is that every call of one Maintenance reaches the same key.
type keyPin struct {
	mu  sync.Mutex
	key [sha256.Size]byte
	set bool
}

func (p *keyPin) verify(state tls.ConnectionState) error {
	if len(state.PeerCertificates) == 0 {
		return errors.New("the machine presented no certificate")
	}

	key := sha256.Sum256(state.PeerCertificates[0].RawSubjectPublicKeyInfo)

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.set {
		p.key, p.set = key, true

		return nil
	}

	if p.key != key {
		return errOtherKey
	}

	return nil
}

// machineryMaintenance implements Maintenance with the machinery client.
type machineryMaintenance struct {
	machineryReader

	addr netip.Addr
}

var _ Maintenance = (*machineryMaintenance)(nil)

// newMaintenance connects to the machine at addr the way talosctl does with --insecure
// (client.WithMaintenanceMode, pkg/machinery/client/options.go:178): TLS without a client
// certificate and without verifying the server's, to that one address. Nothing is sent until the
// first call.
func newMaintenance(ctx context.Context, addr netip.Addr, f *Factory) (*machineryMaintenance, error) {
	pin := &keyPin{}

	c, err := client.New(ctx,
		client.WithTLSConfig(&tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // a machine without a config has no certificate anyone could verify
			VerifyConnection:   pin.verify,
		}),
		client.WithEndpoints(addr.String()),
		client.WithGRPCDialOptions(
			grpc.WithChainUnaryInterceptor(accessMaintenance.unaryInterceptor()),
			grpc.WithChainStreamInterceptor(accessMaintenance.streamInterceptor()),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create a maintenance client for %s: %w", addr, err)
	}

	return &machineryMaintenance{machineryReader: machineryReader{client: c, timeout: f.callTimeout()}, addr: addr}, nil
}

// Probe implements Maintenance.
func (m *machineryMaintenance) Probe(ctx context.Context) (Candidate, error) {
	machine, err := m.MachineStatus(ctx, m.addr)
	if err != nil {
		return Candidate{}, err
	}

	info, err := m.SystemInformation(ctx, m.addr)
	if err != nil {
		return Candidate{}, err
	}

	return Candidate{Stage: machine.Stage, UUID: info.UUID}, nil
}

// Fits implements Maintenance.
func (m *machineryMaintenance) Fits(ctx context.Context, machineConfig []byte) ([]string, error) {
	cfg, err := configloader.NewFromBytes(machineConfig)
	if err != nil {
		return nil, fmt.Errorf("parse the rendered config: %w", err)
	}

	ctx, cancel := m.callContext(ctx, m.addr)
	defer cancel()

	var problems []string

	if install := cfg.UnattendedInstallConfig(); install != nil {
		selector := install.VolumeSelector()

		// Talos's own matching, against the machine's own disks (unattended_install.go:222-247 in
		// Talos v1.14).
		matched, err := blockhelpers.MatchDisks(ctx, m.client.COSI, &selector)
		if err != nil {
			return nil, fmt.Errorf("match the install disk on %s: %w", m.addr, err)
		}

		paths := make([]string, 0, len(matched))
		for _, disk := range matched {
			paths = append(paths, disk.TypedSpec().DevPath)
		}

		slices.Sort(paths)

		switch len(paths) {
		case 1:
		case 0:
			all, err := m.disks(ctx)
			if err != nil {
				return nil, err
			}

			problems = append(problems, fmt.Sprintf("the install disk selector matches none of the machine's disks (%s)", strings.Join(all, ", ")))
		default:
			problems = append(problems, fmt.Sprintf("the install disk selector matches %d disks (%s), and Talos would install to the first",
				len(paths), strings.Join(paths, ", ")))
		}
	}

	wanted := physicalLinks(cfg)
	if len(wanted) == 0 {
		return problems, nil
	}

	links, err := safe.StateListAll[*network.LinkStatus](ctx, m.client.COSI)
	if err != nil {
		return nil, fmt.Errorf("list the links of %s: %w", m.addr, err)
	}

	have := map[string]bool{}
	for link := range links.All() {
		have[link.Metadata().ID()] = true
	}

	for _, name := range wanted {
		if !have[name] {
			problems = append(problems, "the config configures the link "+name+", which the machine does not have")
		}
	}

	return problems, nil
}

func (m *machineryMaintenance) disks(ctx context.Context) ([]string, error) {
	disks, err := safe.StateListAll[*block.Disk](ctx, m.client.COSI)
	if err != nil {
		return nil, fmt.Errorf("list the disks of %s: %w", m.addr, err)
	}

	var out []string

	for disk := range disks.All() {
		if spec := disk.TypedSpec(); !spec.CDROM && !spec.Readonly {
			out = append(out, spec.DevPath)
		}
	}

	slices.Sort(out)

	return out, nil
}

// physicalLinks lists the physical links a config configures by their own name. A link that is
// configured under an alias is found by the alias's selector, on the machine, and is not asked
// for here.
func physicalLinks(cfg configconfig.Config) []string {
	aliases := map[string]bool{}
	for _, alias := range cfg.NetworkLinkAliasConfigs() {
		aliases[alias.Name()] = true
	}

	var out []string

	for _, link := range cfg.NetworkCommonLinkConfigs() {
		if _, physical := link.(configconfig.NetworkPhysicalLinkConfig); physical && !aliases[link.Name()] {
			out = append(out, link.Name())
		}
	}

	slices.Sort(out)

	return slices.Compact(out)
}

// ApplyConfig implements Maintenance.
func (m *machineryMaintenance) ApplyConfig(ctx context.Context, machineConfig []byte) error {
	ctx, cancel := m.callContext(ctx, m.addr)
	defer cancel()

	_, err := m.client.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{
		Data: machineConfig,
		Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT,
	})
	if err != nil {
		return fmt.Errorf("apply config to the machine at %s: %w", m.addr, err)
	}

	return nil
}

// Close implements Maintenance.
func (m *machineryMaintenance) Close() error { return m.client.Close() }

// RenderedHostname is the hostname a machine config names, or nothing when the machine is left
// to name itself: that name is derived from the machine and cannot be told before it boots.
func RenderedHostname(machineConfig []byte) (string, error) {
	cfg, err := configloader.NewFromBytes(machineConfig)
	if err != nil {
		return "", fmt.Errorf("parse the rendered config: %w", err)
	}

	if hostname := cfg.NetworkHostnameConfig(); hostname != nil {
		return hostname.Hostname(), nil
	}

	return "", nil
}

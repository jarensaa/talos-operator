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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"

	"github.com/cosi-project/runtime/pkg/safe"
	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"github.com/siderolabs/talos/pkg/machinery/nethelpers"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	timeres "github.com/siderolabs/talos/pkg/machinery/resources/time"
)

// What Talos says about a node besides whether it is ready (docs/scope.md §14.2), and the last
// of a node's logs. All of it is read, with the role a Reader has.

// Diagnostic is a warning Talos raises about a node's own configuration. It is there only while
// the warning is.
type Diagnostic struct {
	// ID names the warning, e.g. address-overlap.
	ID      string
	Message string
}

// Link is the state of one physical network link.
type Link struct {
	Name string
	// Up is whether the link is operational.
	Up bool
	// SpeedMbps is zero when the link reports none.
	SpeedMbps int64
	// Bond is the name of the bond the link is a member of, or empty.
	Bond string
}

// Volume is the state of one of Talos's volumes.
type Volume struct {
	// Name is the volume's ID, e.g. STATE or EPHEMERAL.
	Name string
	// Phase is the volume's phase as Talos names it; VolumeReady is the one that is fine.
	Phase string
	// Error is what Talos says went wrong, if anything.
	Error string
}

// VolumeReady is the phase of a volume that is in order.
const VolumeReady = "ready"

// maxLog bounds what is kept of one log: the end of it, where what went wrong is.
const maxLog = 128 << 10

// Diagnostics implements Reader.
func (r *machineryReader) Diagnostics(ctx context.Context, node netip.Addr) ([]Diagnostic, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	list, err := safe.StateListAll[*runtime.Diagnostic](ctx, r.client.COSI)
	if err != nil {
		return nil, fmt.Errorf("diagnostics on %s: %w", node, err)
	}

	out := make([]Diagnostic, 0, list.Len())
	for d := range list.All() {
		out = append(out, Diagnostic{ID: d.Metadata().ID(), Message: d.TypedSpec().Message})
	}

	slices.SortFunc(out, func(a, b Diagnostic) int { return cmpString(a.ID, b.ID) })

	return out, nil
}

// TimeSynced implements Reader. A node whose time sync is switched off is not out of sync: its
// clock is somebody else's business.
func (r *machineryReader) TimeSynced(ctx context.Context, node netip.Addr) (bool, error) {
	res, err := cosiGet[*timeres.Status](r, ctx, node, "time status", timeres.StatusID)
	if err != nil {
		return false, err
	}

	return res.TypedSpec().Synced || res.TypedSpec().SyncDisabled, nil
}

// Links implements Reader.
func (r *machineryReader) Links(ctx context.Context, node netip.Addr) ([]Link, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	list, err := safe.StateListAll[*network.LinkStatus](ctx, r.client.COSI)
	if err != nil {
		return nil, fmt.Errorf("links on %s: %w", node, err)
	}

	names := map[uint32]string{}
	for link := range list.All() {
		names[link.TypedSpec().Index] = link.Metadata().ID()
	}

	var out []Link

	for link := range list.All() {
		spec := link.TypedSpec()
		if !spec.Physical() {
			continue
		}

		l := Link{
			Name:      link.Metadata().ID(),
			Up:        spec.OperationalState == nethelpers.OperStateUp,
			SpeedMbps: int64(spec.SpeedMegabits),
		}

		if spec.MasterIndex != 0 && spec.SlaveKind == "bond" {
			l.Bond = names[spec.MasterIndex]
		}

		out = append(out, l)
	}

	slices.SortFunc(out, func(a, b Link) int { return cmpString(a.Name, b.Name) })

	return out, nil
}

// Volumes implements Reader.
func (r *machineryReader) Volumes(ctx context.Context, node netip.Addr) ([]Volume, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	list, err := safe.StateListAll[*block.VolumeStatus](ctx, r.client.COSI)
	if err != nil {
		return nil, fmt.Errorf("volumes on %s: %w", node, err)
	}

	out := make([]Volume, 0, list.Len())

	for volume := range list.All() {
		spec := volume.TypedSpec()
		out = append(out, Volume{Name: volume.Metadata().ID(), Phase: spec.Phase.String(), Error: spec.ErrorMessage})
	}

	slices.SortFunc(out, func(a, b Volume) int { return cmpString(a.Name, b.Name) })

	return out, nil
}

// Logs implements Reader: the end of the log of one of the node's services.
func (r *machineryReader) Logs(ctx context.Context, node netip.Addr, service string, lines int32) ([]byte, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	stream, err := r.client.Logs(ctx, constants.SystemContainerdNamespace, common.ContainerDriver_CONTAINERD, service, false, lines)
	if err != nil {
		return nil, fmt.Errorf("logs of %s on %s: %w", service, node, err)
	}

	return theEnd(stream, "logs of "+service, node)
}

// Dmesg implements Reader: the end of the node's kernel log.
func (r *machineryReader) Dmesg(ctx context.Context, node netip.Addr) ([]byte, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	stream, err := r.client.Dmesg(ctx, false, false)
	if err != nil {
		return nil, fmt.Errorf("kernel log of %s: %w", node, err)
	}

	return theEnd(stream, "kernel log", node)
}

// theEnd reads a log stream to its end and keeps the last maxLog bytes of it, from the start of a
// line.
func theEnd(stream client.MachineStream, what string, node netip.Addr) ([]byte, error) {
	reader, err := client.ReadStream(stream)
	if err != nil {
		return nil, fmt.Errorf("%s on %s: %w", what, node, err)
	}

	defer reader.Close() //nolint:errcheck // a stream that was read to its end, or is abandoned

	var kept []byte

	buf := make([]byte, 32<<10)

	for {
		n, err := reader.Read(buf)

		kept = append(kept, buf[:n]...)
		if len(kept) > 2*maxLog {
			kept = slices.Clone(kept[len(kept)-maxLog:])
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("%s on %s: %w", what, node, err)
		}
	}

	if len(kept) > maxLog {
		kept = kept[len(kept)-maxLog:]

		if i := bytes.IndexByte(kept, '\n'); i >= 0 {
			kept = kept[i+1:]
		}
	}

	return kept, nil
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

// Keeper is the Talos access of maintenance: Enabled: everything a Reader can do, plus what the
// upkeep of etcd is made of (docs/scope.md §14.6). It cannot apply config, upgrade, reboot or
// reset a node, change etcd's members, or disarm an alarm.
type Keeper interface {
	Reader

	// ForfeitEtcdLeadership makes node hand etcd leadership to another member, if it is the leader.
	ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error
	// DefragmentEtcd defragments the etcd member of node, and no other. The member answers
	// nobody while it lasts, and it lasts as long as it takes: the caller's context bounds it.
	DefragmentEtcd(ctx context.Context, node netip.Addr) error
}

// upkeepMethods are the RPCs a Keeper connection may issue besides the read-only ones.
var upkeepMethods = map[string]bool{
	machineapi.MachineService_EtcdDefragment_FullMethodName:        true,
	machineapi.MachineService_EtcdForfeitLeadership_FullMethodName: true,
}

// checkUpkeep is the allowlist of a Keeper connection.
func checkUpkeep(method string, req any) string {
	if upkeepMethods[method] {
		return ""
	}

	return checkReadOnly(method, req)
}

// machineryKeeper implements Keeper with the machinery client.
type machineryKeeper struct{ machineryReader }

var _ Keeper = (*machineryKeeper)(nil)

// ForfeitEtcdLeadership implements Keeper.
func (k *machineryKeeper) ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error {
	ctx, cancel := k.callContext(ctx, node)
	defer cancel()

	if _, err := k.client.EtcdForfeitLeadership(ctx, &machineapi.EtcdForfeitLeadershipRequest{}); err != nil {
		return fmt.Errorf("forfeit etcd leadership on %s: %w", node, err)
	}

	return nil
}

// DefragmentEtcd implements Keeper. Talos defragments the member of the node that is asked
// (v1alpha1_server.go:2172-2195 in Talos v1.14), and answers when etcd is done.
func (k *machineryKeeper) DefragmentEtcd(ctx context.Context, node netip.Addr) error {
	if _, err := k.client.EtcdDefragment(client.WithNode(ctx, node.String())); err != nil {
		return fmt.Errorf("defragment etcd on %s: %w", node, err)
	}

	return nil
}

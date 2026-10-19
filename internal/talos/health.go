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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"github.com/siderolabs/talos/pkg/machinery/nethelpers"
	"github.com/siderolabs/talos/pkg/machinery/resources/block"
	"github.com/siderolabs/talos/pkg/machinery/resources/network"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	timeres "github.com/siderolabs/talos/pkg/machinery/resources/time"
	"google.golang.org/grpc/codes"
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

// maxLog bounds what is kept of one log: the end of it, where what went wrong is.
const maxLog = 128 << 10

// Diagnostics implements Reader.
func (r *machineryReader) Diagnostics(ctx context.Context, node netip.Addr) ([]Diagnostic, error) {
	list, err := cosiList[*runtime.Diagnostic](r, ctx, node, "diagnostics on")
	if err != nil {
		return nil, err
	}

	out := make([]Diagnostic, 0, list.Len())
	for d := range list.All() {
		out = append(out, Diagnostic{ID: d.Metadata().ID(), Message: d.TypedSpec().Message})
	}

	slices.SortFunc(out, func(a, b Diagnostic) int { return strings.Compare(a.ID, b.ID) })

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
	list, err := cosiList[*network.LinkStatus](r, ctx, node, "links on")
	if err != nil {
		return nil, err
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

	slices.SortFunc(out, func(a, b Link) int { return strings.Compare(a.Name, b.Name) })

	return out, nil
}

// Volumes implements Reader.
func (r *machineryReader) Volumes(ctx context.Context, node netip.Addr) ([]Volume, error) {
	list, err := cosiList[*block.VolumeStatus](r, ctx, node, "volumes on")
	if err != nil {
		return nil, err
	}

	out := make([]Volume, 0, list.Len())

	for volume := range list.All() {
		spec := volume.TypedSpec()
		out = append(out, Volume{Name: volume.Metadata().ID(), Phase: spec.Phase.String(), Error: spec.ErrorMessage})
	}

	slices.SortFunc(out, func(a, b Volume) int { return strings.Compare(a.Name, b.Name) })

	return out, nil
}

// Logs implements Reader: the end of the log of one of the node's services, without the lines
// that only say a read was answered.
func (r *machineryReader) Logs(ctx context.Context, node netip.Addr, service string, lines int32) ([]byte, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	stream, err := r.client.Logs(ctx, constants.SystemContainerdNamespace, common.ContainerDriver_CONTAINERD, service, false, lines)
	if err != nil {
		return nil, fmt.Errorf("logs of %s on %s: %w", service, node, err)
	}

	return theEnd(stream, "logs of "+service, node, func(line []byte) bool { return !answeredRead(line) })
}

// accessLine is how Talos logs an API call it answered: the status code, the method in brackets,
// how long it took, and whether it was unary or a stream (pkg/grpc/middleware/log/log.go:92,118 in
// Talos v1.14), after the logger's own prefix.
var accessLine = regexp.MustCompile(`(?:^|\s)(\w+) \[(/[\w.]+/\w+)\] \S+ (?:unary|stream) `)

// answeredRead reports whether line is Talos's record of a read that succeeded. Every observation
// of every node is one, this operator's included: they are most of machined's and apid's logs, and
// the end of a log that is kept would hold nothing else. A write, and a call that failed, are kept.
func answeredRead(line []byte) bool {
	m := accessLine.FindSubmatch(line)

	return m != nil && string(m[1]) == codes.OK.String() && readOnlyMethods[string(m[2])]
}

// Dmesg implements Reader: the end of the node's kernel log.
func (r *machineryReader) Dmesg(ctx context.Context, node netip.Addr) ([]byte, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	stream, err := r.client.Dmesg(ctx, false, false)
	if err != nil {
		return nil, fmt.Errorf("kernel log of %s: %w", node, err)
	}

	return theEnd(stream, "kernel log", node, nil)
}

// theEnd reads a log stream to its end and keeps the last maxLog bytes of it, from the start of a
// line. keep, when set, says which lines are kept at all: those it leaves out take no room.
func theEnd(stream client.MachineStream, what string, node netip.Addr, keep func(line []byte) bool) ([]byte, error) {
	reader, err := client.ReadStream(stream)
	if err != nil {
		return nil, fmt.Errorf("%s on %s: %w", what, node, err)
	}

	defer reader.Close() //nolint:errcheck // a stream that was read to its end, or is abandoned

	var kept []byte

	lines := bufio.NewReaderSize(reader, 32<<10)

	for {
		line, err := lines.ReadBytes('\n')

		if len(line) > 0 && (keep == nil || keep(line)) {
			kept = append(kept, line...)
			if len(kept) > 2*maxLog {
				kept = slices.Clone(kept[len(kept)-maxLog:])
			}
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

// machineryKeeper implements Keeper on a connection opened with accessUpkeep.
type machineryKeeper struct{ forfeiter }

var _ Keeper = (*machineryKeeper)(nil)

// DefragmentEtcd implements Keeper. Talos defragments the member of the node that is asked
// (v1alpha1_server.go:2172-2195 in Talos v1.14), and answers when etcd is done.
func (k *machineryKeeper) DefragmentEtcd(ctx context.Context, node netip.Addr) error {
	if _, err := k.client.EtcdDefragment(client.WithNode(ctx, node.String())); err != nil {
		return fmt.Errorf("defragment etcd on %s: %w", node, err)
	}

	return nil
}

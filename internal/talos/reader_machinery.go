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
	"fmt"
	"io"
	"net/netip"
	"strings"
	"time"

	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/resource/meta"
	"github.com/cosi-project/runtime/pkg/resource/protobuf"
	"github.com/cosi-project/runtime/pkg/safe"
	"github.com/cosi-project/runtime/pkg/state"
	clusterapi "github.com/siderolabs/talos/pkg/machinery/api/cluster"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	configpb "github.com/siderolabs/talos/pkg/machinery/api/resource/config"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/resources/cluster"
	"github.com/siderolabs/talos/pkg/machinery/resources/config"
	"github.com/siderolabs/talos/pkg/machinery/resources/hardware"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"github.com/siderolabs/talos/pkg/machinery/resources/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// machineryReader implements Reader with the machinery client. Every call targets one node via
// client.WithNode, so the endpoint never has to be the node being queried.
type machineryReader struct {
	client  *client.Client
	timeout time.Duration
}

var _ Reader = (*machineryReader)(nil)

func (r *machineryReader) callContext(ctx context.Context, node netip.Addr) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel := context.WithTimeout(ctx, r.timeout)

		return client.WithNode(ctx, node.String()), cancel
	}

	return client.WithNode(ctx, node.String()), func() {}
}

// cosiGet reads a singleton COSI resource from node, mapping missing resources to ErrNotFound.
func cosiGet[T meta.ResourceWithRD](r *machineryReader, ctx context.Context, node netip.Addr, what string, id resource.ID) (T, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	res, err := safe.StateGetByID[T](ctx, r.client.COSI, id)
	if err != nil {
		var zero T

		if state.IsNotFoundError(err) {
			return zero, fmt.Errorf("%s on %s: %w", what, node, ErrNotFound)
		}

		return zero, fmt.Errorf("%s on %s: %w", what, node, err)
	}

	return res, nil
}

// Version implements Reader.
func (r *machineryReader) Version(ctx context.Context, node netip.Addr) (VersionInfo, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	resp, err := r.client.Version(ctx)
	if err != nil {
		return VersionInfo{}, fmt.Errorf("version of %s: %w", node, err)
	}

	msgs := resp.GetMessages()
	if len(msgs) == 0 || msgs[0].GetVersion() == nil {
		return VersionInfo{}, fmt.Errorf("version of %s: empty response", node)
	}

	v := msgs[0].GetVersion()

	return VersionInfo{Tag: v.GetTag(), SHA: v.GetSha(), Arch: v.GetArch()}, nil
}

// MachineStatus implements Reader.
func (r *machineryReader) MachineStatus(ctx context.Context, node netip.Addr) (MachineStatus, error) {
	res, err := cosiGet[*runtime.MachineStatus](r, ctx, node, "machine status", runtime.MachineStatusID)
	if err != nil {
		return MachineStatus{}, err
	}

	spec := res.TypedSpec()
	out := MachineStatus{Stage: spec.Stage.String(), Ready: spec.Status.Ready}

	for _, c := range spec.Status.UnmetConditions {
		out.UnmetConditions = append(out.UnmetConditions, c.Name+": "+c.Reason)
	}

	return out, nil
}

// BootID implements Reader.
func (r *machineryReader) BootID(ctx context.Context, node netip.Addr) (string, error) {
	res, err := cosiGet[*runtime.BootID](r, ctx, node, "boot ID", runtime.BootIDID)
	if err != nil {
		return "", err
	}

	return res.TypedSpec().BootID, nil
}

// SystemInformation implements Reader.
func (r *machineryReader) SystemInformation(ctx context.Context, node netip.Addr) (SystemInformation, error) {
	res, err := cosiGet[*hardware.SystemInformation](r, ctx, node, "system information", hardware.SystemInformationID)
	if err != nil {
		return SystemInformation{}, err
	}

	spec := res.TypedSpec()

	return SystemInformation{
		UUID:         spec.UUID,
		SerialNumber: spec.SerialNumber,
		Manufacturer: spec.Manufacturer,
		ProductName:  spec.ProductName,
	}, nil
}

// ImageFactorySchematic implements Reader.
func (r *machineryReader) ImageFactorySchematic(ctx context.Context, node netip.Addr) (Schematic, error) {
	res, err := cosiGet[*runtime.ImageFactorySchematic](r, ctx, node, "image factory schematic", runtime.ImageFactorySchematicID)
	if err != nil {
		return Schematic{}, err
	}

	spec := res.TypedSpec()

	return Schematic{ID: spec.SchematicID, Flavor: spec.Flavor, FactoryURL: spec.APIURL}, nil
}

// Nodename implements Reader.
func (r *machineryReader) Nodename(ctx context.Context, node netip.Addr) (string, error) {
	res, err := cosiGet[*k8s.Nodename](r, ctx, node, "node name", k8s.NodenameID)
	if err != nil {
		return "", err
	}

	return res.TypedSpec().Nodename, nil
}

// UnattendedInstallStatus implements Reader.
func (r *machineryReader) UnattendedInstallStatus(ctx context.Context, node netip.Addr) (InstallStatus, error) {
	res, err := cosiGet[*runtime.UnattendedInstallStatus](r, ctx, node, "unattended install status", runtime.UnattendedInstallStatusID)
	if err != nil {
		return InstallStatus{}, err
	}

	spec := res.TypedSpec()

	return InstallStatus{Phase: spec.Phase.String(), Image: spec.Image, Error: spec.Error}, nil
}

// Services implements Reader.
func (r *machineryReader) Services(ctx context.Context, node netip.Addr) ([]ServiceStatus, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	list, err := safe.StateListAll[*v1alpha1.Service](ctx, r.client.COSI)
	if err != nil {
		return nil, fmt.Errorf("services on %s: %w", node, err)
	}

	out := make([]ServiceStatus, 0, list.Len())

	for svc := range list.All() {
		spec := svc.TypedSpec()
		out = append(out, ServiceStatus{
			ID:      svc.Metadata().ID(),
			Running: spec.Running,
			Healthy: spec.Healthy,
			Unknown: spec.Unknown,
		})
	}

	return out, nil
}

// MachineType implements Reader.
func (r *machineryReader) MachineType(ctx context.Context, node netip.Addr) (string, error) {
	res, err := cosiGet[*config.MachineType](r, ctx, node, "machine type", config.MachineTypeID)
	if err != nil {
		return "", err
	}

	return res.MachineType().String(), nil
}

// MachineConfig implements Reader. The resources are read without COSI's typed unmarshalling,
// which would parse the config with this operator's machinery (failing on unknown document
// kinds before ParseMachineConfig can fail closed) and re-encode it.
func (r *machineryReader) MachineConfig(ctx context.Context, node netip.Addr) (MachineConfigs, error) {
	active, err := r.rawMachineConfig(ctx, node, config.ActiveID)
	if err != nil {
		return MachineConfigs{}, err
	}

	persistent, err := r.rawMachineConfig(ctx, node, config.PersistentID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return MachineConfigs{}, err
	}

	return MachineConfigs{Active: active, Persistent: persistent}, nil
}

func (r *machineryReader) rawMachineConfig(ctx context.Context, node netip.Addr, id resource.ID) ([]byte, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	res, err := r.client.COSI.Get(ctx,
		resource.NewMetadata(config.NamespaceName, config.MachineConfigType, id, resource.VersionUndefined),
		state.WithGetUnmarshalOptions(state.WithSkipProtobufUnmarshal()),
	)
	if err != nil {
		if state.IsNotFoundError(err) {
			return nil, fmt.Errorf("machine config %s on %s: %w", id, node, ErrNotFound)
		}

		// Never wrap anything that could carry config content; the gRPC status is safe.
		return nil, fmt.Errorf("machine config %s on %s: %w", id, node, err)
	}

	body, err := machineConfigBody(res)
	if err != nil {
		return nil, fmt.Errorf("machine config %s on %s: %w", id, node, err)
	}

	return body, nil
}

// machineConfigBody extracts the config bytes from a MachineConfig resource read without typed
// unmarshalling. The protobuf spec carries them exactly as Talos holds them
// (resources/config/machine_config.go MarshalProto); the YAML spec is a re-encoding of them as a
// string scalar and must not be parsed as config.
func machineConfigBody(res resource.Resource) ([]byte, error) {
	pb, ok := res.(*protobuf.Resource)
	if !ok {
		return nil, fmt.Errorf("unexpected resource type %T", res)
	}

	raw, err := pb.Marshal()
	if err != nil {
		return nil, errors.New("decode resource")
	}

	var spec configpb.MachineConfigSpec
	if err := proto.Unmarshal(raw.GetSpec().GetProtoSpec(), &spec); err != nil {
		return nil, errors.New("decode spec")
	}

	if len(spec.GetYamlMarshalled()) == 0 {
		return nil, errors.New("empty")
	}

	return spec.GetYamlMarshalled(), nil
}

// Members implements Reader.
func (r *machineryReader) Members(ctx context.Context, node netip.Addr) ([]Member, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	list, err := safe.StateListAll[*cluster.Member](ctx, r.client.COSI)
	if err != nil {
		return nil, fmt.Errorf("members seen by %s: %w", node, err)
	}

	out := make([]Member, 0, list.Len())

	for m := range list.All() {
		spec := m.TypedSpec()
		out = append(out, Member{
			NodeID:          spec.NodeID,
			Hostname:        spec.Hostname,
			MachineType:     spec.MachineType.String(),
			OperatingSystem: spec.OperatingSystem,
			Addresses:       append([]netip.Addr(nil), spec.Addresses...),
		})
	}

	return out, nil
}

// EtcdMembers implements Reader.
func (r *machineryReader) EtcdMembers(ctx context.Context, node netip.Addr) ([]EtcdMember, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	resp, err := r.client.EtcdMemberList(ctx, &machineapi.EtcdMemberListRequest{})
	if err != nil {
		return nil, fmt.Errorf("etcd members seen by %s: %w", node, err)
	}

	msgs := resp.GetMessages()
	if len(msgs) == 0 {
		return nil, fmt.Errorf("etcd members seen by %s: empty response", node)
	}

	members := msgs[0].GetMembers()
	out := make([]EtcdMember, 0, len(members))

	for _, m := range members {
		out = append(out, EtcdMember{
			ID:         m.GetId(),
			Hostname:   m.GetHostname(),
			PeerURLs:   m.GetPeerUrls(),
			ClientURLs: m.GetClientUrls(),
			IsLearner:  m.GetIsLearner(),
		})
	}

	return out, nil
}

// EtcdStatus implements Reader.
func (r *machineryReader) EtcdStatus(ctx context.Context, node netip.Addr) (EtcdStatus, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	resp, err := r.client.EtcdStatus(ctx)
	if err != nil {
		return EtcdStatus{}, fmt.Errorf("etcd status on %s: %w", node, err)
	}

	msgs := resp.GetMessages()
	if len(msgs) == 0 || msgs[0].GetMemberStatus() == nil {
		return EtcdStatus{}, fmt.Errorf("etcd status on %s: empty response", node)
	}

	s := msgs[0].GetMemberStatus()

	return EtcdStatus{
		MemberID:    s.GetMemberId(),
		Leader:      s.GetLeader(),
		DBSize:      s.GetDbSize(),
		DBSizeInUse: s.GetDbSizeInUse(),
		RaftIndex:   s.GetRaftIndex(),
		IsLearner:   s.GetIsLearner(),
		Errors:      s.GetErrors(),
	}, nil
}

// HealthCheck implements Reader. The checks run on node itself (ClusterService.HealthCheck), so
// the operator reuses Talos's check suite without importing the main talos module (docs/scope.md
// §3.12). An unhealthy cluster is a result, not an error; errors mean the check could not run.
// With single-node routing, failures arrive as gRPC errors (response metadata errors only apply
// to the deprecated one-to-many proxying).
func (r *machineryReader) HealthCheck(ctx context.Context, node netip.Addr, info ClusterInfo, timeout time.Duration) (HealthResult, error) {
	// The stream must outlive the server-side wait, so it gets its own deadline.
	ctx, cancel := context.WithTimeout(ctx, timeout+r.timeout)
	defer cancel()

	ctx = client.WithNode(ctx, node.String())

	stream, err := r.client.ClusterHealthCheck(ctx, timeout, &clusterapi.ClusterInfo{
		ControlPlaneNodes: addrStrings(info.ControlPlanes),
		WorkerNodes:       addrStrings(info.Workers),
	})
	if err != nil {
		return HealthResult{}, fmt.Errorf("health check on %s: %w", node, err)
	}

	var result HealthResult

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			result.Healthy = true

			return result, nil
		}

		if err != nil {
			// Transport and authorization failures mean the check never ran.
			switch status.Code(err) {
			case codes.Unavailable, codes.Canceled, codes.PermissionDenied, codes.Unauthenticated:
				return HealthResult{}, fmt.Errorf("health check on %s: %w", node, err)
			default:
				// The server ends the stream with an error when a check fails or times out.
				result.Messages = append(result.Messages, status.Convert(err).Message())

				return result, nil
			}
		}

		result.Messages = append(result.Messages, msg.GetMessage())
	}
}

func addrStrings(addrs []netip.Addr) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, a.String())
	}

	return out
}

// dryRunNoChanges ends the dry-run summary when the server's diff is empty
// (generateDiff, v1alpha1_server.go:323-334 in Talos v1.14).
const dryRunNoChanges = "Config diff:\n\nNo changes."

// DryRunApply implements Reader.
func (r *machineryReader) DryRunApply(ctx context.Context, node netip.Addr, machineConfig []byte) (DryRunResult, error) {
	ctx, cancel := r.callContext(ctx, node)
	defer cancel()

	resp, err := r.client.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{
		Data:   machineConfig,
		Mode:   machineapi.ApplyConfigurationRequest_TRY,
		DryRun: true,
	})
	if err != nil {
		return DryRunResult{}, fmt.Errorf("dry-run apply on %s: %w", node, err)
	}

	return parseDryRun(resp)
}

// parseDryRun reads the verdict from a dry-run response. It fails closed on a response it does not
// recognise rather than guessing, and never returns the diff text.
func parseDryRun(resp *machineapi.ApplyConfigurationResponse) (DryRunResult, error) {
	msgs := resp.GetMessages()
	if len(msgs) != 1 {
		return DryRunResult{}, fmt.Errorf("dry-run apply: expected one response message, got %d", len(msgs))
	}

	details := msgs[0].GetModeDetails()
	if !strings.Contains(details, "Config diff:") {
		return DryRunResult{}, errors.New("dry-run apply: the response carries no config diff; unsupported Talos version?")
	}

	return DryRunResult{NoChanges: strings.HasSuffix(strings.TrimRight(details, "\n"), dryRunNoChanges)}, nil
}

// machineryWriter implements ConfigWriter on a connection opened with accessConfig.
type machineryWriter struct {
	machineryReader
}

var _ ConfigWriter = (*machineryWriter)(nil)

// ApplyConfig implements ConfigWriter.
func (w *machineryWriter) ApplyConfig(ctx context.Context, node netip.Addr, machineConfig []byte, mode ApplyMode,
	tryTimeout time.Duration,
) error {
	req := &machineapi.ApplyConfigurationRequest{Data: machineConfig}

	switch mode {
	case ApplyNoReboot:
		req.Mode = machineapi.ApplyConfigurationRequest_NO_REBOOT
	case ApplyTry:
		if tryTimeout <= 0 {
			return errors.New("apply config: a try needs a timeout")
		}

		req.Mode = machineapi.ApplyConfigurationRequest_TRY
		req.TryModeTimeout = durationpb.New(tryTimeout)
	default:
		return fmt.Errorf("apply config: unsupported mode %d", mode)
	}

	ctx, cancel := w.callContext(ctx, node)
	defer cancel()

	if _, err := w.client.ApplyConfiguration(ctx, req); err != nil {
		return fmt.Errorf("apply config to %s: %w", node, err)
	}

	return nil
}

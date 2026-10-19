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
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cosiv1alpha1 "github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/inmem"
	"github.com/cosi-project/runtime/pkg/state/impl/namespaced"
	"github.com/cosi-project/runtime/pkg/state/protobuf/server"
	clusterapi "github.com/siderolabs/talos/pkg/machinery/api/cluster"
	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/siderolabs/talos/pkg/machinery/resources/cluster"
	"github.com/siderolabs/talos/pkg/machinery/resources/config"
	"github.com/siderolabs/talos/pkg/machinery/resources/hardware"
	"github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"github.com/siderolabs/talos/pkg/machinery/resources/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// What the test node reports about itself.
const (
	apiVersion     = "v1.14.2"
	apiWorker      = "worker-1"
	kubeletService = "kubelet"
)

// apiNode is the node every test asks for. The test server is the endpoint; the node only
// travels in the request metadata, as it does through a real apid.
var apiNode = netip.MustParseAddr("10.5.0.3")

// testAPI is a Talos API served in-process: the machine and cluster services, scripted, and a
// real COSI state server over an in-memory state. The machinery client talks to it over a unix
// socket exactly as it talks to apid, so these tests cover what the fake Reader cannot: the
// requests the operator really sends and how it reads what really comes back.
type testAPI struct {
	machineapi.UnimplementedMachineServiceServer
	clusterapi.UnimplementedClusterServiceServer

	state state.State

	mu sync.Mutex
	// nodes records, per full method name, the node each call was addressed to.
	nodes map[string][]string
	// applies records every ApplyConfiguration request that reached the server.
	applies []*machineapi.ApplyConfigurationRequest
	// healthRequests records every HealthCheck request.
	healthRequests []*clusterapi.HealthCheckRequest

	// Scripted answers.
	version     *machineapi.VersionResponse
	applyErr    error
	applyAnswer *machineapi.ApplyConfigurationResponse
	etcdMembers *machineapi.EtcdMemberListResponse
	etcdStatus  *machineapi.EtcdStatusResponse
	etcdErr     error
	health      func(stream grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error

	// What an upgrade sends, and the scripted streams it gets back.
	pulls    []*machineapi.ImageServicePullRequest
	installs []*machineapi.LifecycleServiceUpgradeRequest
	reboots  []*machineapi.RebootRequest
	forfeits int
	pull     []*machineapi.ImageServicePullResponse
	install  []*machineapi.LifecycleServiceUpgradeResponse

	// What a removal sends.
	resets    []*machineapi.ResetRequest
	removals  []uint64
	removeErr error

	// What the upkeep sends, and what a node's logs hold.
	defrags int
	logs    map[string][]string

	// The files a node has, and the paths that were read of it.
	files map[string][]byte
	reads []string
}

func (a *testAPI) Read(req *machineapi.ReadRequest, stream grpc.ServerStreamingServer[common.Data]) error {
	a.mu.Lock()
	a.reads = append(a.reads, req.GetPath())
	content, ok := a.files[req.GetPath()]
	a.mu.Unlock()

	if !ok {
		return status.Error(codes.NotFound, "no such file")
	}

	return stream.Send(&common.Data{Bytes: content})
}

func (a *testAPI) EtcdDefragment(context.Context, *emptypb.Empty) (*machineapi.EtcdDefragmentResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.defrags++

	return &machineapi.EtcdDefragmentResponse{}, nil
}

func (a *testAPI) Logs(req *machineapi.LogsRequest, stream grpc.ServerStreamingServer[common.Data]) error {
	for _, chunk := range a.logs[req.GetId()] {
		if err := stream.Send(&common.Data{Bytes: []byte(chunk)}); err != nil {
			return err
		}
	}

	return nil
}

func (a *testAPI) Dmesg(_ *machineapi.DmesgRequest, stream grpc.ServerStreamingServer[common.Data]) error {
	for _, chunk := range a.logs["dmesg"] {
		if err := stream.Send(&common.Data{Bytes: []byte(chunk)}); err != nil {
			return err
		}
	}

	return nil
}

func (a *testAPI) Reset(_ context.Context, req *machineapi.ResetRequest) (*machineapi.ResetResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.resets = append(a.resets, req)

	return &machineapi.ResetResponse{}, nil
}

func (a *testAPI) EtcdRemoveMemberByID(_ context.Context, req *machineapi.EtcdRemoveMemberByIDRequest,
) (*machineapi.EtcdRemoveMemberByIDResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.removals = append(a.removals, req.GetMemberId())

	return &machineapi.EtcdRemoveMemberByIDResponse{}, a.removeErr
}

// imageAPI and lifecycleAPI serve the two services whose method names the machine service also
// has, from the same script.
type imageAPI struct {
	machineapi.UnimplementedImageServiceServer

	*testAPI
}

type lifecycleAPI struct {
	machineapi.UnimplementedLifecycleServiceServer

	*testAPI
}

func (i imageAPI) Pull(req *machineapi.ImageServicePullRequest,
	stream grpc.ServerStreamingServer[machineapi.ImageServicePullResponse],
) error {
	a := i.testAPI

	a.mu.Lock()
	a.pulls = append(a.pulls, req)
	a.mu.Unlock()

	for _, msg := range a.pull {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (l lifecycleAPI) Upgrade(req *machineapi.LifecycleServiceUpgradeRequest,
	stream grpc.ServerStreamingServer[machineapi.LifecycleServiceUpgradeResponse],
) error {
	a := l.testAPI

	a.mu.Lock()
	a.installs = append(a.installs, req)
	a.mu.Unlock()

	for _, msg := range a.install {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}

	return nil
}

func (a *testAPI) Reboot(_ context.Context, req *machineapi.RebootRequest) (*machineapi.RebootResponse, error) {
	a.mu.Lock()
	a.reboots = append(a.reboots, req)
	a.mu.Unlock()

	return &machineapi.RebootResponse{}, nil
}

func (a *testAPI) EtcdForfeitLeadership(context.Context, *machineapi.EtcdForfeitLeadershipRequest,
) (*machineapi.EtcdForfeitLeadershipResponse, error) {
	a.mu.Lock()
	a.forfeits++
	a.mu.Unlock()

	return &machineapi.EtcdForfeitLeadershipResponse{}, nil
}

func (a *testAPI) record(ctx context.Context, method string) {
	md, _ := metadata.FromIncomingContext(ctx)

	a.mu.Lock()
	defer a.mu.Unlock()

	a.nodes[method] = append(a.nodes[method], strings.Join(md.Get("node"), ","))
}

func (a *testAPI) addressedTo(method string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	return slices.Clone(a.nodes[method])
}

func (a *testAPI) Version(context.Context, *emptypb.Empty) (*machineapi.VersionResponse, error) {
	return a.version, nil
}

func (a *testAPI) ApplyConfiguration(_ context.Context, req *machineapi.ApplyConfigurationRequest,
) (*machineapi.ApplyConfigurationResponse, error) {
	a.mu.Lock()
	a.applies = append(a.applies, req)
	a.mu.Unlock()

	if a.applyErr != nil {
		return nil, a.applyErr
	}

	if a.applyAnswer != nil {
		return a.applyAnswer, nil
	}

	return &machineapi.ApplyConfigurationResponse{Messages: []*machineapi.ApplyConfiguration{{Mode: req.GetMode()}}}, nil
}

func (a *testAPI) EtcdMemberList(context.Context, *machineapi.EtcdMemberListRequest) (*machineapi.EtcdMemberListResponse, error) {
	return a.etcdMembers, a.etcdErr
}

func (a *testAPI) EtcdStatus(context.Context, *emptypb.Empty) (*machineapi.EtcdStatusResponse, error) {
	return a.etcdStatus, a.etcdErr
}

func (a *testAPI) HealthCheck(req *clusterapi.HealthCheckRequest, stream grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error {
	a.mu.Lock()
	a.healthRequests = append(a.healthRequests, req)
	a.mu.Unlock()

	return a.health(stream)
}

// serve starts the API and returns a machinery client connected to it, with the given dial
// options (the allowlist interceptors, for the tests about those).
func serve(t *testing.T, opts ...grpc.DialOption) (*testAPI, *client.Client) {
	t.Helper()

	api := &testAPI{
		state: state.WrapCore(namespaced.NewState(inmem.Build)),
		nodes: map[string][]string{},
	}

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			api.record(ctx, info.FullMethod)

			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(s any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			api.record(ss.Context(), info.FullMethod)

			return handler(s, ss)
		}),
	)

	machineapi.RegisterMachineServiceServer(srv, api)
	machineapi.RegisterImageServiceServer(srv, imageAPI{testAPI: api})
	machineapi.RegisterLifecycleServiceServer(srv, lifecycleAPI{testAPI: api})
	clusterapi.RegisterClusterServiceServer(srv, api)
	cosiv1alpha1.RegisterStateServer(srv, server.NewState(api.state))

	// Unix socket paths are short-lived and short: t.TempDir() can exceed the 104 bytes macOS allows.
	dir, err := os.MkdirTemp("", "talos-api")
	if err != nil {
		t.Fatal(err)
	}

	socket := filepath.Join(dir, "apid.sock")

	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}

	go srv.Serve(listener) //nolint:errcheck // ends with the test

	dial := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)

	c, err := client.New(context.Background(), client.WithUnixSocket(socket), client.WithGRPCDialOptions(dial...))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = c.Close() //nolint:errcheck // best effort at the end of a test
		srv.Stop()
		_ = os.RemoveAll(dir) //nolint:errcheck // best effort at the end of a test
	})

	return api, c
}

func reader(c *client.Client) *machineryReader {
	return &machineryReader{client: c, timeout: 5 * time.Second}
}

func create(t *testing.T, api *testAPI, res resource.Resource) {
	t.Helper()

	if err := api.state.Create(context.Background(), res); err != nil {
		t.Fatal(err)
	}
}

// workerConfig is a small machine config machinery parses.
const workerConfig = "version: v1alpha1\nmachine:\n  type: worker\n  nodeLabels:\n    config: %s\ncluster: {}\n"

func machineConfig(t *testing.T, label string, id resource.ID) *config.MachineConfig {
	t.Helper()

	provider, err := configloader.NewFromBytes([]byte(strings.Replace(workerConfig, "%s", label, 1)))
	if err != nil {
		t.Fatal(err)
	}

	return config.NewMachineConfigWithID(provider, id)
}

func TestMachineryReadsNodeState(t *testing.T) {
	api, c := serve(t)
	r := reader(c)
	ctx := context.Background()

	api.version = &machineapi.VersionResponse{Messages: []*machineapi.Version{{
		Version: &machineapi.VersionInfo{Tag: apiVersion, Sha: "abc123", Arch: "amd64"},
	}}}

	machineStatus := runtime.NewMachineStatus()
	machineStatus.TypedSpec().Stage = runtime.MachineStageRunning
	machineStatus.TypedSpec().Status = runtime.MachineStatusStatus{
		UnmetConditions: []runtime.UnmetCondition{{Name: "services", Reason: "kubelet is not healthy"}},
	}
	create(t, api, machineStatus)

	bootID := runtime.NewBootID()
	bootID.TypedSpec().BootID = "boot-7"
	create(t, api, bootID)

	sysinfo := hardware.NewSystemInformation(hardware.SystemInformationID)
	sysinfo.TypedSpec().UUID = "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"
	sysinfo.TypedSpec().SerialNumber = "SN1"
	create(t, api, sysinfo)

	schematic := runtime.NewImageFactorySchematic(runtime.NamespaceName, runtime.ImageFactorySchematicID)
	schematic.TypedSpec().SchematicID = "376567988ad3"
	schematic.TypedSpec().APIURL = "https://factory.talos.dev"
	create(t, api, schematic)

	nodename := k8s.NewNodename(k8s.NamespaceName, k8s.NodenameID)
	nodename.TypedSpec().Nodename = apiWorker
	create(t, api, nodename)

	machineType := config.NewMachineType()
	machineType.SetMachineType(machine.TypeControlPlane)
	create(t, api, machineType)

	for id, running := range map[string]bool{kubeletService: true, "etcd": false} {
		svc := v1alpha1.NewService(id)
		svc.TypedSpec().Running, svc.TypedSpec().Healthy = running, running
		create(t, api, svc)
	}

	if v, err := r.Version(ctx, apiNode); err != nil || v != (VersionInfo{Tag: apiVersion, SHA: "abc123", Arch: "amd64"}) {
		t.Fatalf("version: %+v, %v", v, err)
	}

	ms, err := r.MachineStatus(ctx, apiNode)
	if err != nil || ms.Stage != "running" || ms.Ready || !slices.Equal(ms.UnmetConditions, []string{"services: kubelet is not healthy"}) {
		t.Fatalf("machine status: %+v, %v", ms, err)
	}

	if id, err := r.BootID(ctx, apiNode); err != nil || id != "boot-7" {
		t.Fatalf("boot ID: %q, %v", id, err)
	}

	si, err := r.SystemInformation(ctx, apiNode)
	if err != nil || si.UUID != "aaaaaaaa-bbbb-4ccc-8ddd-000000000001" || si.SerialNumber != "SN1" {
		t.Fatalf("system information: %+v, %v", si, err)
	}

	sc, err := r.ImageFactorySchematic(ctx, apiNode)
	if err != nil || sc.ID != "376567988ad3" || sc.FactoryURL != "https://factory.talos.dev" {
		t.Fatalf("schematic: %+v, %v", sc, err)
	}

	if name, err := r.Nodename(ctx, apiNode); err != nil || name != apiWorker {
		t.Fatalf("nodename: %q, %v", name, err)
	}

	if mt, err := r.MachineType(ctx, apiNode); err != nil || mt != "controlplane" {
		t.Fatalf("machine type: %q, %v", mt, err)
	}

	services, err := r.Services(ctx, apiNode)
	if err != nil || len(services) != 2 {
		t.Fatalf("services: %+v, %v", services, err)
	}

	slices.SortFunc(services, func(a, b ServiceStatus) int { return strings.Compare(a.ID, b.ID) })

	if services[0] != (ServiceStatus{ID: "etcd"}) || services[1] != (ServiceStatus{ID: kubeletService, Running: true, Healthy: true}) {
		t.Fatalf("services: %+v", services)
	}

	// Every call names the node it is for; the endpoint is never assumed to be the node.
	for method, nodes := range api.nodes {
		for _, n := range nodes {
			if n != apiNode.String() {
				t.Errorf("%s was addressed to %q, not %s", method, n, apiNode)
			}
		}
	}

	if len(api.nodes) < 2 {
		t.Fatalf("expected calls to the machine and state services, saw %v", api.nodes)
	}
}

func TestMachineryMapsMissingResourcesToNotFound(t *testing.T) {
	_, c := serve(t)
	r := reader(c)
	ctx := context.Background()

	checks := map[string]func() error{
		"schematic":      func() error { _, err := r.ImageFactorySchematic(ctx, apiNode); return err },
		"install status": func() error { _, err := r.UnattendedInstallStatus(ctx, apiNode); return err },
		"machine status": func() error { _, err := r.MachineStatus(ctx, apiNode); return err },
		"machine config": func() error { _, err := r.MachineConfig(ctx, apiNode); return err },
	}

	for what, check := range checks {
		err := check()
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: expected ErrNotFound, got %v", what, err)
		}

		// A node that says "I have none" answered: it is neither unreachable nor refusing.
		if IsUnreachable(err) || IsRejected(err) {
			t.Errorf("%s: a missing resource is an answer: %v", what, err)
		}

		if err != nil && !strings.Contains(err.Error(), apiNode.String()) {
			t.Errorf("%s: the error must name the node: %v", what, err)
		}
	}
}

func TestMachineryReadsMachineConfigsAsTalosHoldsThem(t *testing.T) {
	api, c := serve(t)
	r := reader(c)
	ctx := context.Background()

	active := machineConfig(t, "active", config.ActiveID)
	create(t, api, active)

	want, err := active.Provider().Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Booted from disk: no persistent config resource, which is not an error.
	cfgs, err := r.MachineConfig(ctx, apiNode)
	if err != nil {
		t.Fatal(err)
	}

	if string(cfgs.Active) != string(want) || cfgs.Persistent != nil || cfgs.PendingConfigChange() {
		t.Fatalf("active must be the bytes Talos holds, persistent absent: %q / %q", cfgs.Active, cfgs.Persistent)
	}

	// The bytes are a config again, not a re-encoding of one.
	if _, err := ParseMachineConfig(cfgs.Active); err != nil {
		t.Fatalf("the config read back must parse: %v", err)
	}

	// A try in flight, or a staged config: persistent differs from active.
	create(t, api, machineConfig(t, "persistent", config.PersistentID))

	cfgs, err = r.MachineConfig(ctx, apiNode)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(cfgs.Persistent), "config: persistent") || !cfgs.PendingConfigChange() {
		t.Fatalf("persistent config must be read and seen to differ: %q", cfgs.Persistent)
	}
}

func TestMachineryReadsMembersAndEtcd(t *testing.T) {
	api, c := serve(t)
	r := reader(c)
	ctx := context.Background()

	for i, name := range []string{apiControlPlane, "cp-2", apiWorker} {
		m := cluster.NewMember(cluster.NamespaceName, name)
		m.TypedSpec().Hostname = name
		m.TypedSpec().NodeID = "id-" + name
		m.TypedSpec().Addresses = []netip.Addr{netip.AddrFrom4([4]byte{10, 5, 0, byte(2 + i)})}
		m.TypedSpec().MachineType = machine.TypeControlPlane

		if name == apiWorker {
			m.TypedSpec().MachineType = machine.TypeWorker
		}

		create(t, api, m)
	}

	members, err := r.Members(ctx, apiNode)
	if err != nil || len(members) != 3 {
		t.Fatalf("members: %+v, %v", members, err)
	}

	slices.SortFunc(members, func(a, b Member) int { return strings.Compare(a.Hostname, b.Hostname) })

	if members[0].MachineType != "controlplane" || members[2].MachineType != "worker" ||
		members[2].Addresses[0] != netip.MustParseAddr("10.5.0.4") || members[1].NodeID != "id-cp-2" {
		t.Fatalf("members: %+v", members)
	}

	// Three voters and a member that was added but never started: an ID, no name.
	api.etcdMembers = &machineapi.EtcdMemberListResponse{Messages: []*machineapi.EtcdMembers{{Members: []*machineapi.EtcdMember{
		{Id: 0xa1, Hostname: apiControlPlane, PeerUrls: []string{"https://10.5.0.2:2380"}},
		{Id: 0xa2, Hostname: "cp-2"},
		{Id: 0xa3, Hostname: "cp-3", IsLearner: true},
		{Id: 0xbeef},
	}}}}

	etcd, err := r.EtcdMembers(ctx, apiNode)
	if err != nil || len(etcd) != 4 {
		t.Fatalf("etcd members: %+v, %v", etcd, err)
	}

	if etcd[0].ID != 0xa1 || etcd[0].PeerURLs[0] != "https://10.5.0.2:2380" || !etcd[2].IsLearner ||
		etcd[3].Hostname != "" || etcd[3].ID != 0xbeef {
		t.Fatalf("etcd members: %+v", etcd)
	}

	api.etcdStatus = &machineapi.EtcdStatusResponse{Messages: []*machineapi.EtcdStatus{{MemberStatus: &machineapi.EtcdMemberStatus{
		MemberId: 0xa2, Leader: 0xa1, RaftIndex: 42, Errors: []string{"alarm: NOSPACE"},
	}}}}

	es, err := r.EtcdStatus(ctx, apiNode)
	if err != nil || es.MemberID != 0xa2 || es.Leader != 0xa1 || es.RaftIndex != 42 || !slices.Equal(es.Errors, []string{"alarm: NOSPACE"}) {
		t.Fatalf("etcd status: %+v, %v", es, err)
	}

	// An answer without content is not an answer.
	api.etcdMembers = &machineapi.EtcdMemberListResponse{}
	api.etcdStatus = &machineapi.EtcdStatusResponse{Messages: []*machineapi.EtcdStatus{{}}}

	if _, err := r.EtcdMembers(ctx, apiNode); err == nil {
		t.Fatal("an empty member list response must be an error, not an empty cluster")
	}

	if _, err := r.EtcdStatus(ctx, apiNode); err == nil {
		t.Fatal("an etcd status response without a status must be an error")
	}

	// A worker runs no etcd: Talos says so, and the node did answer.
	api.etcdErr = status.Error(codes.FailedPrecondition, "etcd is not running")

	_, err = r.EtcdStatus(ctx, apiNode)
	if status.Code(err) != codes.FailedPrecondition || IsUnreachable(err) {
		t.Fatalf("expected the node's own refusal, got %v", err)
	}
}

func TestMachineryAppliesExactlyWhatWasAsked(t *testing.T) {
	api, c := serve(t)
	w := &machineryWriter{*reader(c)}
	ctx := context.Background()
	cfg := []byte(strings.Replace(workerConfig, "%s", "new", 1))

	if err := w.ApplyConfig(ctx, apiNode, cfg, ApplyTry, 4*time.Minute); err != nil {
		t.Fatal(err)
	}

	if err := w.ApplyConfig(ctx, apiNode, cfg, ApplyNoReboot, 0); err != nil {
		t.Fatal(err)
	}

	if len(api.applies) != 2 {
		t.Fatalf("expected two applies, saw %d", len(api.applies))
	}

	try, confirm := api.applies[0], api.applies[1]

	if try.GetMode() != machineapi.ApplyConfigurationRequest_TRY || try.GetDryRun() || string(try.GetData()) != string(cfg) ||
		try.GetTryModeTimeout().AsDuration() != 4*time.Minute {
		t.Fatalf("a try must carry the config, the mode and the timeout it was given: %+v", try)
	}

	if confirm.GetMode() != machineapi.ApplyConfigurationRequest_NO_REBOOT || confirm.GetDryRun() || confirm.GetTryModeTimeout() != nil {
		t.Fatalf("a confirm is a plain no-reboot apply: %+v", confirm)
	}

	addressed := api.addressedTo(machineapi.MachineService_ApplyConfiguration_FullMethodName)
	if got := addressed; !slices.Equal(got, []string{apiNode.String(), apiNode.String()}) {
		t.Fatalf("applies must be addressed to the node: %v", got)
	}

	// Nothing but a try and a no-reboot apply can be asked for, and a try never without a
	// timeout: Talos would fall back to its own default of one minute.
	for name, err := range map[string]error{
		"a try without a timeout": w.ApplyConfig(ctx, apiNode, cfg, ApplyTry, 0),
		"an unknown mode":         w.ApplyConfig(ctx, apiNode, cfg, ApplyMode(0), time.Minute),
	} {
		if err == nil {
			t.Errorf("%s must be refused", name)
		}
	}

	if len(api.applies) != 2 {
		t.Fatal("a refused apply must not reach the node")
	}

	// What Talos says to a config it does not accept reaches the caller as a rejection.
	api.applyErr = status.Error(codes.InvalidArgument, "unknown keys found during decoding")

	if err := w.ApplyConfig(ctx, apiNode, cfg, ApplyTry, time.Minute); !IsRejected(err) || IsUnreachable(err) {
		t.Fatalf("expected a rejection, got %v", err)
	}

	// While another apply runs, Talos answers FailedPrecondition: that passes.
	api.applyErr = status.Error(codes.FailedPrecondition, "another apply configuration is already in progress")

	if err := w.ApplyConfig(ctx, apiNode, cfg, ApplyTry, time.Minute); err == nil || IsRejected(err) {
		t.Fatalf("expected an error that may pass, got %v", err)
	}
}

func TestMachineryDryRunsInTryModeAndReadsOnlyTheVerdict(t *testing.T) {
	api, c := serve(t)
	r := reader(c)
	ctx := context.Background()
	cfg := []byte(strings.Replace(workerConfig, "%s", "new", 1))

	api.applyAnswer = dryRunSummary("No changes.")

	got, err := r.DryRunApply(ctx, apiNode, cfg)
	if err != nil || !got.NoChanges {
		t.Fatalf("dry run: %+v, %v", got, err)
	}

	// Any other mode would cancel a pending rollback, even in a dry run (docs/scope.md §8.5.1).
	req := api.applies[0]
	if !req.GetDryRun() || req.GetMode() != machineapi.ApplyConfigurationRequest_TRY || string(req.GetData()) != string(cfg) {
		t.Fatalf("a dry run must be sent in mode TRY: %+v", req)
	}

	api.applyAnswer = dryRunSummary("--- a\n+++ b\n-token: old\n+token: new\n")

	if got, err = r.DryRunApply(ctx, apiNode, cfg); err != nil || got.NoChanges {
		t.Fatalf("a diff is not no changes: %+v, %v", got, err)
	}

	// An answer without a diff cannot be told from "no changes" by guessing.
	api.applyAnswer = &machineapi.ApplyConfigurationResponse{Messages: []*machineapi.ApplyConfiguration{{ModeDetails: "Applied"}}}

	if _, err = r.DryRunApply(ctx, apiNode, cfg); err == nil {
		t.Fatal("a dry-run answer without a diff must be an error")
	}
}

func TestMachineryHealthCheckTellsAFailingClusterFromACheckThatDidNotRun(t *testing.T) {
	api, c := serve(t)
	r := reader(c)
	ctx := context.Background()

	info := ClusterInfo{
		ControlPlanes: []netip.Addr{netip.MustParseAddr("10.5.0.2")},
		Workers:       []netip.Addr{netip.MustParseAddr("10.5.0.3"), netip.MustParseAddr("10.5.0.4")},
	}

	progress := func(messages ...string) func(grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error {
		return func(stream grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error {
			for _, m := range messages {
				if err := stream.Send(&clusterapi.HealthCheckProgress{Message: m}); err != nil {
					return err
				}
			}

			return nil
		}
	}

	// Every check passed: the stream just ends.
	api.health = progress("waiting for etcd to be healthy: OK", "waiting for all k8s nodes to report ready: OK")

	got, err := r.HealthCheck(ctx, apiNode, info, 30*time.Second)
	if err != nil || !got.Healthy || len(got.Messages) != 2 {
		t.Fatalf("healthy cluster: %+v, %v", got, err)
	}

	req := api.healthRequests[0]
	if req.GetWaitTimeout().AsDuration() != 30*time.Second ||
		!slices.Equal(req.GetClusterInfo().GetControlPlaneNodes(), []string{"10.5.0.2"}) ||
		!slices.Equal(req.GetClusterInfo().GetWorkerNodes(), []string{"10.5.0.3", "10.5.0.4"}) {
		t.Fatalf("the check must be told which nodes to expect, and for how long to wait: %+v", req)
	}

	// A check failed: Talos ends the stream with an error. That is a verdict.
	for _, code := range []codes.Code{codes.Unknown, codes.Internal, codes.FailedPrecondition} {
		api.health = func(stream grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error {
			if err := progress("waiting for etcd to be healthy: OK")(stream); err != nil {
				return err
			}

			return status.Error(code, "etcd member 10.5.0.2 is not healthy")
		}

		got, err = r.HealthCheck(ctx, apiNode, info, time.Second)
		if err != nil || got.Healthy {
			t.Fatalf("%s: an unhealthy cluster is a result, not an error: %+v, %v", code, got, err)
		}

		if last := got.Messages[len(got.Messages)-1]; last != "etcd member 10.5.0.2 is not healthy" {
			t.Fatalf("%s: the last message must say what failed: %q", code, last)
		}
	}

	// A check ran out of time: the error says only that, and the line Talos sent as it gave up says
	// which check it was and what it last found.
	api.health = func(stream grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error {
		if err := progress("waiting for etcd to be healthy: OK",
			"waiting for all k8s nodes to report ready: node 10.5.0.3 is not ready")(stream); err != nil {
			return err
		}

		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	}

	got, err = r.HealthCheck(ctx, apiNode, info, 5*time.Minute)
	if err != nil || got.Healthy {
		t.Fatalf("a check that timed out is a result, not an error: %+v, %v", got, err)
	}

	if last, want := got.Messages[len(got.Messages)-1],
		"the health check timed out after 5m0s waiting for all k8s nodes to report ready: node 10.5.0.3 is not ready"; last != want {
		t.Fatalf("a timeout must say which check ran out of time, and after how long:\n got %q\nwant %q", last, want)
	}

	api.health = func(grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error {
		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	}

	got, _ = r.HealthCheck(ctx, apiNode, info, time.Second)
	if last := got.Messages[len(got.Messages)-1]; last != "the health check timed out after 1s before any check reported" {
		t.Fatalf("a timeout before any progress must say so: %q", last)
	}

	// The check never ran: that is an error, and never a verdict in either direction.
	for _, code := range []codes.Code{codes.Unavailable, codes.PermissionDenied, codes.Unauthenticated, codes.Canceled} {
		api.health = func(grpc.ServerStreamingServer[clusterapi.HealthCheckProgress]) error {
			return status.Error(code, "nope")
		}

		got, err = r.HealthCheck(ctx, apiNode, info, time.Second)
		if err == nil || got.Healthy || len(got.Messages) != 0 {
			t.Fatalf("%s: a check that did not run must be an error: %+v, %v", code, got, err)
		}
	}
}

// The allowlist sits on the connection, not in the methods above: a caller that reached past the
// Reader to the raw client still could not mutate a node.
func TestMachineryConnectionsRefuseWhatTheirAccessDoesNotAllow(t *testing.T) {
	cfg := []byte(strings.Replace(workerConfig, "%s", "new", 1))
	ctx := client.WithNode(context.Background(), apiNode.String())

	api, readOnly := serve(t, dial(accessReadOnly)...)

	for _, mode := range []machineapi.ApplyConfigurationRequest_Mode{
		machineapi.ApplyConfigurationRequest_TRY, machineapi.ApplyConfigurationRequest_NO_REBOOT,
	} {
		_, err := readOnly.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{Data: cfg, Mode: mode})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("a read-only connection must refuse a %s apply, got %v", mode, err)
		}
	}

	if err := readOnly.Reboot(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a read-only connection must refuse a reboot, got %v", err)
	}

	// The one apply it may send: a dry run, in mode TRY.
	if _, err := reader(readOnly).DryRunApply(ctx, apiNode, cfg); err == nil || status.Code(err) == codes.PermissionDenied {
		t.Fatalf("a TRY dry run must pass the allowlist (and then fail on the scripted answer): %v", err)
	}

	if len(api.applies) != 1 || !api.applies[0].GetDryRun() {
		t.Fatalf("only the dry run may have reached the node: %+v", api.applies)
	}

	api, writing := serve(t, dial(accessConfig)...)
	w := &machineryWriter{*reader(writing)}

	if err := w.ApplyConfig(ctx, apiNode, cfg, ApplyTry, time.Minute); err != nil {
		t.Fatalf("a config connection must let a try through: %v", err)
	}

	for _, mode := range []machineapi.ApplyConfigurationRequest_Mode{
		machineapi.ApplyConfigurationRequest_AUTO, machineapi.ApplyConfigurationRequest_STAGED,
	} {
		_, err := writing.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{Data: cfg, Mode: mode})
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("a config connection must refuse a %s apply, got %v", mode, err)
		}
	}

	if err := writing.Reboot(ctx); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a config connection must refuse a reboot, got %v", err)
	}

	if len(api.applies) != 1 || api.applies[0].GetMode() != machineapi.ApplyConfigurationRequest_TRY {
		t.Fatalf("only the try may have reached the node: %+v", api.applies)
	}
}

func TestMachineryBoundsEveryCallAndLeavesACallersDeadlineAlone(t *testing.T) {
	r := &machineryReader{timeout: 30 * time.Second}

	ctx, cancel := r.callContext(context.Background(), apiNode)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 30*time.Second {
		t.Fatalf("a call without a deadline gets the default: %v, %v", deadline, ok)
	}

	short, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()

	ctx, cancel = r.callContext(short, apiNode)
	defer cancel()

	if deadline, _ := ctx.Deadline(); time.Until(deadline) > time.Second {
		t.Fatalf("a caller's shorter deadline must stand: %v", time.Until(deadline))
	}

	md, _ := metadata.FromOutgoingContext(ctx)
	if got := md.Get("node"); !slices.Equal(got, []string{apiNode.String()}) {
		t.Fatalf("the call must name its node: %v", got)
	}
}

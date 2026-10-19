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
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"time"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/jarensaa/talos-operator/internal/talos/creds"
	"github.com/jarensaa/talos-operator/internal/talos/talostest"
)

// readerMethods is the reviewed set of Reader methods. Adding a method to Reader fails this test
// until the method is confirmed read-only and its RPC is on readOnlyMethods.
//
// Reviewed: every method below reads state through an allowlisted RPC (COSI Get/List, Version,
// EtcdMemberList, EtcdStatus, ClusterService.HealthCheck).
var readerMethods = []string{
	"BootID", "DryRunApply", "EtcdMembers", "EtcdStatus", "HealthCheck", "ImageFactorySchematic", "KubernetesUpgradeChecks", "MachineConfig",
	"MachineStatus", "MachineType", "Members", "Nodename", "Services", "SystemInformation",
	"UnattendedInstallStatus", "Version",
	// What Talos and etcd say about themselves, and the end of a node's logs: all read.
	"Diagnostics", "Dmesg", "Links", "Logs", "TimeSynced", "Volumes",
	// What a node's firmware says about Secure Boot: a resource, and the one file the allowlist
	// lets MachineService.Read through for.
	"SecureBoot", "SecureBootDB",
}

func TestReaderMethodSetIsReviewed(t *testing.T) {
	typ := reflect.TypeFor[Reader]()

	got := make([]string, 0, typ.NumMethod())
	for i := range typ.NumMethod() {
		got = append(got, typ.Method(i).Name)
	}

	slices.Sort(got)

	want := slices.Clone(readerMethods)
	slices.Sort(want)

	if !slices.Equal(got, want) {
		t.Fatalf("Reader methods %v differ from the reviewed set %v", got, want)
	}
}

func TestCheckReadOnly(t *testing.T) {
	checkAllowlist(t, accessReadOnly, []allowCase{
		{"version", machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{}, true},
		{"cosi get", cosiGetMethod, nil, true},
		{"reboot", machineapi.MachineService_Reboot_FullMethodName, &machineapi.RebootRequest{}, false},
		{"reset", machineapi.MachineService_Reset_FullMethodName, &machineapi.ResetRequest{}, false},
		{"apply dry run in try mode", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			&machineapi.ApplyConfigurationRequest{DryRun: true, Mode: machineapi.ApplyConfigurationRequest_TRY}, true},
		// Talos cancels a pending TRY rollback for every other mode, dry run or not.
		{"apply dry run in no-reboot mode", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			&machineapi.ApplyConfigurationRequest{DryRun: true, Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT}, false},
		{"apply dry run in auto mode", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			&machineapi.ApplyConfigurationRequest{DryRun: true, Mode: machineapi.ApplyConfigurationRequest_AUTO}, false},
		{"apply in try mode", machineapi.MachineService_ApplyConfiguration_FullMethodName,
			&machineapi.ApplyConfigurationRequest{Mode: machineapi.ApplyConfigurationRequest_TRY}, false},
		{"apply", machineapi.MachineService_ApplyConfiguration_FullMethodName, &machineapi.ApplyConfigurationRequest{}, false},
		{"apply without typed request", machineapi.MachineService_ApplyConfiguration_FullMethodName, nil, false},
		{"bootstrap", machineapi.MachineService_Bootstrap_FullMethodName, &machineapi.BootstrapRequest{}, false},
		// Read reads any file of a node. The firmware's Secure Boot db is the one it may be asked for.
		{"read the Secure Boot db", machineapi.MachineService_Read_FullMethodName, &machineapi.ReadRequest{Path: SecureBootDBPath}, true},
		{"read another firmware variable", machineapi.MachineService_Read_FullMethodName,
			&machineapi.ReadRequest{Path: "/sys/firmware/efi/efivars/PK-8be4df61-93ca-11d2-aa0d-00e098032b8c"}, false},
		{"read a file that holds secrets", machineapi.MachineService_Read_FullMethodName,
			&machineapi.ReadRequest{Path: "/system/state/config.yaml"}, false},
		{"read a path that only starts like the db", machineapi.MachineService_Read_FullMethodName,
			&machineapi.ReadRequest{Path: SecureBootDBPath + "/../../../../../system/state/config.yaml"}, false},
		{"read without typed request", machineapi.MachineService_Read_FullMethodName, nil, false},
	})
}

// A ConfigWriter connection may apply config without rebooting or in try mode, and still nothing
// else that mutates a node.
func TestCheckConfig(t *testing.T) {
	const method = machineapi.MachineService_ApplyConfiguration_FullMethodName

	checkAllowlist(t, accessConfig, []allowCase{
		{"apply no-reboot", method, apply(machineapi.ApplyConfigurationRequest_NO_REBOOT, false), true},
		{"apply try", method, apply(machineapi.ApplyConfigurationRequest_TRY, false), true},
		{"dry run in try mode", method, apply(machineapi.ApplyConfigurationRequest_TRY, true), true},
		// Talos would decide about a reboot itself, or leave a config staged for the next boot.
		{"apply auto", method, apply(machineapi.ApplyConfigurationRequest_AUTO, false), false},
		{"apply staged", method, apply(machineapi.ApplyConfigurationRequest_STAGED, false), false},
		//nolint:staticcheck // the deprecated mode must stay refused
		{"apply reboot", method, apply(machineapi.ApplyConfigurationRequest_REBOOT, false), false},
		// A dry run in another mode still cancels a pending try rollback.
		{"dry run in no-reboot mode", method, apply(machineapi.ApplyConfigurationRequest_NO_REBOOT, true), false},
		{"apply without typed request", method, nil, false},
		{"version", machineapi.MachineService_Version_FullMethodName, &emptypb.Empty{}, true},
		{"reboot", machineapi.MachineService_Reboot_FullMethodName, &machineapi.RebootRequest{}, false},
		{"reset", machineapi.MachineService_Reset_FullMethodName, &machineapi.ResetRequest{}, false},
		//nolint:staticcheck // the legacy upgrade RPC must stay refused
		{"upgrade", machineapi.MachineService_Upgrade_FullMethodName, &machineapi.UpgradeRequest{}, false},
		{"etcd leave", machineapi.MachineService_EtcdLeaveCluster_FullMethodName, &machineapi.EtcdLeaveClusterRequest{}, false},
		{"bootstrap", machineapi.MachineService_Bootstrap_FullMethodName, &machineapi.BootstrapRequest{}, false},
	})
}

func TestReaderAndConfigWriterUseSeparateConnections(t *testing.T) {
	f := NewFactory()
	defer f.Close()

	ctx := context.Background()
	target := Target{Key: "uid-1", Credentials: testCredentials(t), Endpoints: []creds.Endpoint{ep("10.5.0.2")}}

	ro, err := f.client(ctx, target, accessReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	rw, err := f.client(ctx, target, accessConfig)
	if err != nil {
		t.Fatal(err)
	}

	if ro == rw {
		t.Fatal("a Reader must never share the ConfigWriter's connection, whose allowlist is wider")
	}
}

func TestUnaryInterceptorNeverInvokesRejectedCall(t *testing.T) {
	invoked := false
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		invoked = true

		return nil
	}

	err := accessReadOnly.unaryInterceptor()(context.Background(), machineapi.MachineService_Reboot_FullMethodName,
		&machineapi.RebootRequest{}, nil, nil, invoker)
	if status.Code(err) != codes.PermissionDenied || invoked {
		t.Fatalf("err=%v invoked=%v", err, invoked)
	}

	if err := accessReadOnly.unaryInterceptor()(context.Background(), machineapi.MachineService_Version_FullMethodName,
		&emptypb.Empty{}, nil, nil, invoker); err != nil || !invoked {
		t.Fatalf("allowed call: err=%v invoked=%v", err, invoked)
	}
}

func TestStreamInterceptorRejectsBeforeOpening(t *testing.T) {
	opened := false
	streamer := func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
		opened = true

		return nil, nil
	}

	_, err := accessReadOnly.streamInterceptor()(context.Background(), &grpc.StreamDesc{}, nil,
		machineapi.MachineService_Upgrade_FullMethodName, streamer)
	if status.Code(err) != codes.PermissionDenied || opened {
		t.Fatalf("err=%v opened=%v", err, opened)
	}

	_, err = accessReadOnly.streamInterceptor()(context.Background(), &grpc.StreamDesc{}, nil, cosiWatchMethod, streamer)
	if err != nil || !opened {
		t.Fatalf("allowed stream: err=%v opened=%v", err, opened)
	}
}

// sentStream records what was sent on it.
type sentStream struct {
	grpc.ClientStream

	sent []any
}

func (s *sentStream) SendMsg(m any) error {
	s.sent = append(s.sent, m)

	return nil
}

// Read is allowed for one path, and what it is asked for is only known when the request is sent:
// a request for anything else never leaves, and its stream is given up.
func TestStreamInterceptorChecksTheRequestOfARead(t *testing.T) {
	var (
		under    = &sentStream{}
		streamed context.Context
	)

	var streamer grpc.Streamer = func(
		ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		streamed = ctx

		return under, nil
	}

	open := func(a access) grpc.ClientStream {
		t.Helper()

		stream, err := a.streamInterceptor()(context.Background(), &grpc.StreamDesc{}, nil,
			machineapi.MachineService_Read_FullMethodName, streamer)
		if err != nil {
			t.Fatal(err)
		}

		return stream
	}

	stream := open(accessReadOnly)

	if err := stream.SendMsg(&machineapi.ReadRequest{Path: "/system/state/config.yaml"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a read of another file must be refused, got %v", err)
	}

	if len(under.sent) != 0 || streamed.Err() == nil {
		t.Fatalf("a refused request must not be sent, and its stream must be given up: sent %v, stream %v", under.sent, streamed.Err())
	}

	stream = open(accessReadOnly)

	if err := stream.SendMsg(&machineapi.ReadRequest{Path: SecureBootDBPath}); err != nil || len(under.sent) != 1 || streamed.Err() != nil {
		t.Fatalf("the read of the Secure Boot db must be sent: err %v, sent %v, stream %v", err, under.sent, streamed.Err())
	}

	// A machine in maintenance mode is read through resources and nothing else.
	if err := open(accessMaintenance).SendMsg(&machineapi.ReadRequest{Path: SecureBootDBPath}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a maintenance connection reads no file, got %v", err)
	}
}

func ep(s string) creds.Endpoint {
	e, err := creds.ParseEndpoint(s)
	if err != nil {
		panic(err)
	}

	return e
}

func TestSelectEndpoints(t *testing.T) {
	cps := []creds.Endpoint{ep("10.0.0.11"), ep("10.0.0.12"), ep("10.0.0.13")}

	got, err := SelectEndpoints(cps, SelectOptions{Exclude: netip.MustParseAddr("10.0.0.11")})
	if err != nil {
		t.Fatal(err)
	}

	want := []creds.Endpoint{ep("10.0.0.12"), ep("10.0.0.13")}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if _, err := SelectEndpoints([]creds.Endpoint{ep("10.0.0.11")}, SelectOptions{
		Exclude: netip.MustParseAddr("10.0.0.11"),
	}); !errors.Is(err, ErrNoEndpoints) {
		t.Fatalf("expected ErrNoEndpoints, got %v", err)
	}
}

func TestParseEndpointRejectsHostnames(t *testing.T) {
	for _, s := range []string{"cluster.example.com", "cluster.example.com:50000", "10.0.0.1:0", "10.0.0.1:abc"} {
		if _, err := creds.ParseEndpoint(s); err == nil {
			t.Fatalf("%q: expected error", s)
		}
	}

	for _, s := range []string{"10.0.0.1", "10.0.0.1:50000", "fd00::1", "[fd00::1]:50000"} {
		if _, err := creds.ParseEndpoint(s); err != nil {
			t.Fatalf("%q: %v", s, err)
		}
	}
}

func testCredentials(t *testing.T) *creds.Credentials {
	t.Helper()

	c, err := creds.Load(talostest.AdminTalosconfig(t, "10.5.0.2"), "", time.Now())
	if err != nil {
		t.Fatal(err)
	}

	return c
}

func TestFactoryCachesPerTargetAndEndpoints(t *testing.T) {
	f := NewFactory()
	defer f.Close()

	clock := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	f.now = func() time.Time { return clock }

	ctx := context.Background()
	c := testCredentials(t)
	target := Target{Key: "uid-1", Credentials: c, Endpoints: []creds.Endpoint{ep("10.5.0.2"), ep("10.5.0.3")}}

	first, err := f.client(ctx, target, accessReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	again, err := f.client(ctx, target, accessReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	if again != first {
		t.Fatal("expected the cached client for an unchanged target")
	}

	// A rollout keeps the node it changes out of the endpoints while another controller still
	// asks with all of them. Neither may close the other's client.
	outside := target
	outside.Endpoints = []creds.Endpoint{ep("10.5.0.3")}

	moved, err := f.client(ctx, outside, accessReadOnly)
	if err != nil {
		t.Fatal(err)
	}

	if moved == first {
		t.Fatal("expected a separate client for other endpoints")
	}

	if again, err = f.client(ctx, target, accessReadOnly); err != nil || again != first {
		t.Fatalf("a client must survive another being asked for: %v", err)
	}

	// A client nobody asks for any more goes away, as after a certificate rotation.
	clock = clock.Add(idleClientTTL)

	if _, err := f.client(ctx, outside, accessReadOnly); err != nil {
		t.Fatal(err)
	}

	if len(f.clients) != 1 {
		t.Fatalf("an idle client must be closed and forgotten, %d left", len(f.clients))
	}

	f.Close()

	if len(f.clients) != 0 {
		t.Fatal("Close must close and forget every cached client")
	}
}

func TestIsRejected(t *testing.T) {
	wrapped := func(code codes.Code) error {
		return fmt.Errorf("apply config to 10.5.0.2: %w", status.Error(code, "detail"))
	}

	for _, code := range []codes.Code{codes.InvalidArgument, codes.PermissionDenied, codes.Unimplemented} {
		if !IsRejected(wrapped(code)) {
			t.Errorf("%s must count as a rejection: sending the same request again cannot succeed", code)
		}
	}

	// FailedPrecondition is what Talos answers while another apply is in progress.
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.FailedPrecondition, codes.Internal} {
		if IsRejected(wrapped(code)) {
			t.Errorf("%s may pass and must be retried, not treated as a rejection", code)
		}
	}

	if IsRejected(errors.New("connection reset")) || IsRejected(nil) {
		t.Error("errors without a status are not rejections")
	}
}

func TestIsUnreachable(t *testing.T) {
	wrapped := func(err error) error { return fmt.Errorf("version of 10.5.0.3: %w", err) }

	for name, err := range map[string]error{
		"no route":        wrapped(status.Error(codes.Unavailable, "connection error")),
		"server deadline": wrapped(status.Error(codes.DeadlineExceeded, "context deadline exceeded")),
		"client deadline": wrapped(context.DeadlineExceeded),
	} {
		if !IsUnreachable(err) {
			t.Errorf("%s: a node that does not answer is unreachable", name)
		}
	}

	for name, err := range map[string]error{
		"not found":         wrapped(ErrNotFound),
		"permission denied": wrapped(status.Error(codes.PermissionDenied, "not authorized")),
		"refused config":    wrapped(status.Error(codes.InvalidArgument, "unknown field")),
		"no error":          nil,
	} {
		if IsUnreachable(err) {
			t.Errorf("%s: a node that answers is reachable, whatever it says", name)
		}
	}
}

func TestFactoryRejectsIncompleteTargets(t *testing.T) {
	f := NewFactory()
	defer f.Close()

	ctx := context.Background()

	if _, err := f.Reader(ctx, Target{Credentials: testCredentials(t), Endpoints: []creds.Endpoint{ep("10.5.0.2")}}); err == nil {
		t.Fatal("expected error for a missing key")
	}

	if _, err := f.Reader(ctx, Target{Key: "k", Endpoints: []creds.Endpoint{ep("10.5.0.2")}}); err == nil {
		t.Fatal("expected error for missing credentials")
	}

	if _, err := f.Reader(ctx, Target{Key: "k", Credentials: testCredentials(t)}); !errors.Is(err, ErrNoEndpoints) {
		t.Fatalf("expected ErrNoEndpoints, got %v", err)
	}
}

// dryRunSummary is the ModeDetails Talos v1.14 returns for a dry run in mode TRY
// (v1alpha1_server.go:258-270 and generateDiff).
func dryRunSummary(diff string) *machineapi.ApplyConfigurationResponse {
	return &machineapi.ApplyConfigurationResponse{Messages: []*machineapi.ApplyConfiguration{{
		Mode:        machineapi.ApplyConfigurationRequest_TRY,
		ModeDetails: "Dry run summary:\nApplied configuration without a reboot (skipped in dry-run).\nConfig diff:\n\n" + diff,
	}}}
}

func TestParseDryRunReadsOnlyTheVerdict(t *testing.T) {
	got, err := parseDryRun(dryRunSummary("No changes."))
	if err != nil || !got.NoChanges {
		t.Fatalf("empty diff: %+v, %v", got, err)
	}

	got, err = parseDryRun(dryRunSummary("--- a\n+++ b\n@@ -1 +1 @@\n-token: old\n+token: new\n"))
	if err != nil || got.NoChanges {
		t.Fatalf("diff reported as no changes: %+v, %v", got, err)
	}

	// A diff that merely mentions the phrase is still a diff.
	got, err = parseDryRun(dryRunSummary("--- a\n+++ b\n-x: No changes.\n+x: y\n"))
	if err != nil || got.NoChanges {
		t.Fatalf("diff mentioning the phrase: %+v, %v", got, err)
	}

	noDiff := &machineapi.ApplyConfigurationResponse{Messages: []*machineapi.ApplyConfiguration{{ModeDetails: "Applied configuration"}}}
	if _, err := parseDryRun(noDiff); err == nil {
		t.Fatal("an unrecognised response must fail closed")
	}

	if _, err := parseDryRun(&machineapi.ApplyConfigurationResponse{}); err == nil {
		t.Fatal("an empty response must fail closed")
	}
}

func TestServicesDownNamesWhatANodeNeedsAndDoesNotRun(t *testing.T) {
	running := func(id string) ServiceStatus { return ServiceStatus{ID: id, Running: true, Healthy: true} }

	all := []ServiceStatus{running(ServiceCRI), running(ServiceKubelet), running(ServiceEtcd), running("apid")}
	if down := ServicesDown(MachineTypeControlPlane, all); len(down) != 0 {
		t.Fatalf("a control plane running everything has nothing down: %v", down)
	}

	// Talos lists only services that run: a kubelet it lost is missing.
	if down := ServicesDown(MachineTypeWorker, []ServiceStatus{running(ServiceCRI)}); !slices.Equal(down, []string{"kubelet does not run"}) {
		t.Fatalf("a missing kubelet does not run: %v", down)
	}

	// etcd is needed on a control plane only.
	if down := ServicesDown(MachineTypeWorker, []ServiceStatus{running(ServiceCRI), running(ServiceKubelet)}); len(down) != 0 {
		t.Fatalf("a worker needs no etcd: %v", down)
	}

	withoutEtcd := []ServiceStatus{running(ServiceCRI), running(ServiceKubelet)}
	if down := ServicesDown(MachineTypeControlPlane, withoutEtcd); !slices.Equal(down, []string{"etcd does not run"}) {
		t.Fatalf("a control plane needs etcd: %v", down)
	}

	unhealthy := []ServiceStatus{running(ServiceCRI), {ID: ServiceKubelet, Running: true}}
	if down := ServicesDown(MachineTypeWorker, unhealthy); !slices.Equal(down, []string{"kubelet is not healthy"}) {
		t.Fatalf("a kubelet that fails its health check is down: %v", down)
	}

	// A service without a health check is as healthy as running makes it.
	unchecked := []ServiceStatus{{ID: ServiceCRI, Running: true, Unknown: true}, running(ServiceKubelet)}
	if down := ServicesDown(MachineTypeWorker, unchecked); len(down) != 0 {
		t.Fatalf("a running service without a health check is not down: %v", down)
	}
}

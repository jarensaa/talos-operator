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

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
)

// machineryUpgrader implements Upgrader on a connection opened with accessUpgrade.
type machineryUpgrader struct {
	machineryReader
}

var _ Upgrader = (*machineryUpgrader)(nil)

// systemContainerd is where Talos keeps its own images, the installer among them: the "system"
// namespace talosctl upgrade defaults to (cmd/talosctl/cmd/talos/upgrade.go:361 in Talos v1.14).
func systemContainerd() *common.ContainerdInstance {
	return &common.ContainerdInstance{Driver: common.ContainerDriver_CONTAINERD, Namespace: common.ContainerdNamespace_NS_SYSTEM}
}

// PullImage implements Upgrader. The stream reports progress and ends with the image's name.
func (u *machineryUpgrader) PullImage(ctx context.Context, node netip.Addr, image string) error {
	stream, err := u.client.ImageClient.Pull(client.WithNode(ctx, node.String()), &machineapi.ImageServicePullRequest{
		Containerd: systemContainerd(),
		ImageRef:   image,
	})
	if err != nil {
		return fmt.Errorf("pull %s on %s: %w", image, node, err)
	}

	pulled := false

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("pull %s on %s: %w", image, node, err)
		}

		if msg.GetName() != "" {
			pulled = true
		}
	}

	if !pulled {
		return fmt.Errorf("pull %s on %s: the node ended the pull without naming the image", image, node)
	}

	return nil
}

// Install implements Upgrader. The exit code of the installer arrives as a message, not as an
// error of the call (api/machine/lifecycle.proto: "Server SHOULD NOT respond with error, even if
// the value is non-zero"), so a stream that ends without one did not finish either.
func (u *machineryUpgrader) Install(ctx context.Context, node netip.Addr, image string) error {
	stream, err := u.client.LifecycleClient.Upgrade(client.WithNode(ctx, node.String()), &machineapi.LifecycleServiceUpgradeRequest{
		Containerd: systemContainerd(),
		Source:     &machineapi.InstallArtifactsSource{ImageName: image},
	})
	if err != nil {
		return fmt.Errorf("install %s on %s: %w", image, node, err)
	}

	var (
		last     string
		exitCode *int32
	)

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return fmt.Errorf("install %s on %s: %w", image, node, err)
		}

		switch progress := msg.GetProgress().GetResponse().(type) {
		case *machineapi.LifecycleServiceInstallProgress_Message:
			last = progress.Message
		case *machineapi.LifecycleServiceInstallProgress_ExitCode:
			exitCode = &progress.ExitCode
		}
	}

	switch {
	case exitCode == nil:
		return fmt.Errorf("install %s on %s: the node ended the install without an exit code", image, node)
	case *exitCode != 0:
		return fmt.Errorf("install %s on %s: %w", image, node, &InstallError{ExitCode: *exitCode, Detail: last})
	}

	return nil
}

// ForfeitEtcdLeadership implements Upgrader.
func (u *machineryUpgrader) ForfeitEtcdLeadership(ctx context.Context, node netip.Addr) error {
	ctx, cancel := u.callContext(ctx, node)
	defer cancel()

	if _, err := u.client.EtcdForfeitLeadership(ctx, &machineapi.EtcdForfeitLeadershipRequest{}); err != nil {
		return fmt.Errorf("forfeit etcd leadership on %s: %w", node, err)
	}

	return nil
}

// Reboot implements Upgrader. The default mode: a graceful reboot, never a forced one.
func (u *machineryUpgrader) Reboot(ctx context.Context, node netip.Addr) error {
	ctx, cancel := u.callContext(ctx, node)
	defer cancel()

	if err := u.client.Reboot(ctx); err != nil {
		return fmt.Errorf("reboot %s: %w", node, err)
	}

	return nil
}

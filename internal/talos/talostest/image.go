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

package talostest

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/siderolabs/talos/pkg/machinery/api/common"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/client"
)

// ImportImage loads an image tarball (as `docker save` writes it) into the container runtime the
// kubelet of node uses, and returns the name the image has there. It is how the e2e harness gets
// an image that exists nowhere else onto its nodes, with the cluster admin's talosconfig; the
// operator itself never imports images.
func ImportImage(ctx context.Context, talosconfigPath, node string, tarball io.Reader) (string, error) {
	c, err := client.New(ctx, client.WithConfigFromFile(talosconfigPath))
	if err != nil {
		return "", fmt.Errorf("talos client: %w", err)
	}

	defer c.Close() //nolint:errcheck // nothing to do about a client that does not close

	stream, err := c.ImageClient.Import(client.WithNode(ctx, node))
	if err != nil {
		return "", fmt.Errorf("open import stream to %s: %w", node, err)
	}

	// The first message says where the image goes; the rest is the tarball in chunks
	// (cmd/talosctl/cmd/talos/image.go:408-456 in Talos v1.14).
	if err := stream.Send(&machineapi.ImageServiceImportRequest{
		Request: &machineapi.ImageServiceImportRequest_Containerd{
			Containerd: &common.ContainerdInstance{
				Driver:    common.ContainerDriver_CRI,
				Namespace: common.ContainerdNamespace_NS_CRI,
			},
		},
	}); err != nil {
		return "", fmt.Errorf("start import on %s: %w", node, err)
	}

	buf := make([]byte, 32*1024)

	for {
		n, err := tarball.Read(buf)

		if n > 0 {
			chunk := &machineapi.ImageServiceImportRequest{
				Request: &machineapi.ImageServiceImportRequest_ImageChunk{ImageChunk: &common.Data{Bytes: buf[:n]}},
			}
			if err := stream.Send(chunk); err != nil {
				return "", fmt.Errorf("send image to %s: %w", node, err)
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return "", fmt.Errorf("read image tarball: %w", err)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", fmt.Errorf("import on %s: %w", node, err)
	}

	return resp.GetName(), nil
}

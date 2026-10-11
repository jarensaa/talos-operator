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

package controller

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/netip"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos"
)

// What is kept of a machine at which an operation failed (docs/scope.md §14.6). Why a node did
// not come up under a new config is in the logs of its services and of its kernel, and those are
// gone with the next boot, or scrolled away by the time somebody looks. So the operator takes
// them when it halts, into one Secret per cluster, and the failure names it.
//
// It is a Secret because a log can quote what a config holds. It is never logged, and the next
// failure overwrites it: what it is for is the failure somebody is looking at.

const (
	// bundleLogLines is how much of each log is asked for: its end.
	bundleLogLines = 500
	// bundleRetry is how long collecting is tried again after a failure, for a node that does not
	// answer yet or a Secret that cannot be written yet. After it, the failure says what was missing.
	bundleRetry = 5 * time.Minute
	// notCollected starts what status.operation.failure.bundle says when there is no bundle.
	notCollected = "not collected: "

	bundleFailure  = "failure.txt"
	bundleServices = "services.txt"
	bundleKernel   = "dmesg.log.gz"
	serviceEtcd    = "etcd"
)

// bundleServiceLogs are the services whose logs say why a node did not come up. etcd is asked
// of control planes only.
var bundleServiceLogs = []string{"machined", "apid", "kubelet", "cri", serviceEtcd}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=update

func supportBundleName(tc *talosv1alpha1.TalosCluster) string { return tc.Name + "-support-bundle" }

// collectBundle collects what the machine of a failed operation says, once per failure, and
// records in the failure where it is. It changes nothing on any node: everything is read, with
// the connection every observation uses.
func (r *TalosClusterReconciler) collectBundle(ctx context.Context, tc *talosv1alpha1.TalosCluster, obs observation) {
	op := tc.Status.Operation
	if op == nil || op.Failure == nil || op.Failure.Bundle != "" {
		return
	}

	f := op.Failure
	log := logf.FromContext(ctx)

	// later leaves the bundle for a later pass while the failure is fresh, and says why there is
	// none once it is not.
	later := func(why string) {
		if f.At != nil && r.now().Sub(f.At.Time) < bundleRetry {
			log.Info("no support bundle yet, will try again", "machine", f.Machine, "reason", why)

			return
		}

		f.Bundle = notCollected + why
	}

	if f.Machine == "" {
		f.Bundle = notCollected + "the failure is not of a machine"

		return
	}

	tm := &talosv1alpha1.TalosMachine{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: f.Machine}, tm); err != nil {
		later("the TalosMachine could not be read")

		return
	}

	node, err := netip.ParseAddr(tm.Spec.Address)
	if err != nil || obs.reader == nil {
		later("the cluster's Talos API was not reached")

		return
	}

	pool := &talosv1alpha1.TalosMachinePool{}
	isControlPlane := r.Get(ctx, client.ObjectKey{Namespace: tc.Namespace, Name: tm.Spec.PoolRef.Name}, pool) == nil && controlPlane(pool)

	data, missing := gather(ctx, obs.reader, node, isControlPlane)
	if len(data) == 0 {
		later("the node did not answer")

		return
	}

	data[bundleFailure] = fmt.Appendf(nil,
		"operation: %s (%s)\nmachine: %s\nstep: %s\nreason: %s\nmessage: %s\nfailed at: %s\ncollected at: %s\nnot read: %s\n",
		op.ID, op.Type, f.Machine, f.Step, f.Reason, f.Message, timeOf(f.At).UTC().Format(time.RFC3339),
		r.now().UTC().Format(time.RFC3339), orNone(strings.Join(missing, ", ")))

	name := supportBundleName(tc)

	switch foreign, err := r.writeBundle(ctx, tc, name, data); {
	case foreign:
		f.Bundle = notCollected + "a Secret named " + name + " exists and is not this cluster's support bundle"
	case err != nil:
		log.Error(err, "the support bundle could not be stored", "secret", name)
		later("the Secret " + name + " could not be written")
	default:
		f.Bundle = name

		log.Info("collected a support bundle from the machine the operation failed at", "machine", f.Machine, "secret", name,
			"notRead", missing)
	}
}

// gather reads the state of the node's services and the end of their logs and of the kernel's.
// It returns what it got, keyed as the Secret holds it, and what it did not get.
func gather(ctx context.Context, reader talos.Reader, node netip.Addr, isControlPlane bool) (map[string][]byte, []string) {
	reads := newNodeReads(ctx, node)
	data := map[string][]byte{}

	reads.read("services", func(ctx context.Context) error {
		services, err := reader.Services(ctx, node)
		if err != nil {
			return err
		}

		var out strings.Builder

		for _, s := range services {
			fmt.Fprintf(&out, "%s running=%t healthy=%t healthUnknown=%t\n", s.ID, s.Running, s.Healthy, s.Unknown)
		}

		data[bundleServices] = []byte(out.String())

		return nil
	})

	for _, service := range bundleServiceLogs {
		if service == serviceEtcd && !isControlPlane {
			continue
		}

		reads.read("log of "+service, func(ctx context.Context) error {
			raw, err := reader.Logs(ctx, node, service, bundleLogLines)
			if err != nil {
				return err
			}

			data[service+".log.gz"] = compressed(raw)

			return nil
		})
	}

	reads.read("kernel log", func(ctx context.Context) error {
		raw, err := reader.Dmesg(ctx, node)
		if err != nil {
			return err
		}

		data[bundleKernel] = compressed(raw)

		return nil
	})

	return data, reads.failed
}

// compressed gzips a log. Logs repeat themselves, and a Secret holds a megabyte.
func compressed(raw []byte) []byte {
	var buf bytes.Buffer

	w := gzip.NewWriter(&buf)
	// Writing to a buffer does not fail.
	_, _ = w.Write(raw) //nolint:errcheck // see above
	_ = w.Close()       //nolint:errcheck // see above

	return buf.Bytes()
}

// writeBundle stores the bundle in the cluster's one Secret for it, replacing the last. A Secret
// of that name that is not the cluster's own is left alone, and reported as foreign.
func (r *TalosClusterReconciler) writeBundle(ctx context.Context, tc *talosv1alpha1.TalosCluster, name string, data map[string][]byte,
) (foreign bool, err error) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: tc.Namespace,
			Labels:    map[string]string{talosv1alpha1.LabelCluster: tc.Name},
		},
		Type: corev1.SecretTypeOpaque,
		Data: data,
	}

	// Unlike the cluster's PKI, a bundle is of no use without the cluster it is about.
	if err := controllerutil.SetControllerReference(tc, secret, r.Scheme); err != nil {
		return false, fmt.Errorf("own the support bundle: %w", err)
	}

	existing := &corev1.Secret{}

	switch err := r.Get(ctx, client.ObjectKeyFromObject(secret), existing); {
	case apierrors.IsNotFound(err):
		return false, r.Create(ctx, secret)
	case err != nil:
		return false, fmt.Errorf("get the support bundle: %w", err)
	case !metav1.IsControlledBy(existing, tc):
		return true, nil
	}

	existing.Data = data

	return false, r.Update(ctx, existing)
}

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
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/talos/creds"
)

// Without a KubeTalosAPIAccessConfig, Talos installs no talos.dev CRD; the API server then knows
// no such kind. envtest always has the CRD, so this case uses a client that lacks it.
func TestRequestCredentialsReportsDisabledTalosAPIAccess(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	if err := talosv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	noKind := func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if obj.GetObjectKind().GroupVersionKind() == talosServiceAccountGVK {
			return &meta.NoKindMatchError{GroupKind: talosServiceAccountGVK.GroupKind(), SearchedVersions: []string{talosServiceAccountGVK.Version}}
		}

		return c.Get(ctx, key, obj, opts...)
	}

	r := &TalosClusterReconciler{
		Client: fakeclient.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{Get: noKind}).Build(),
		Scheme: scheme,
	}

	tc := &talosv1alpha1.TalosCluster{ObjectMeta: metav1.ObjectMeta{Name: "happy", Namespace: "talos-operator"}}

	err := r.requestCredentials(context.Background(), tc)
	if got := creds.Reason(err); got != talosv1alpha1.ReasonTalosAPIAccessDisabled {
		t.Fatalf("reason: got %s (%v)", got, err)
	}

	if want := "KubeTalosAPIAccessConfig"; err == nil || !contains(err.Error(), want, "talos-operator") {
		t.Fatalf("the message must say how to enable access for the namespace: %v", err)
	}
}

func contains(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}

	return true
}

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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	// The time zones of maintenance windows, whatever the image the operator runs in has.
	_ "time/tzdata"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	talosv1alpha1 "github.com/jarensaa/talos-operator/api/v1alpha1"
	"github.com/jarensaa/talos-operator/internal/catalog"
	"github.com/jarensaa/talos-operator/internal/controller"
	"github.com/jarensaa/talos-operator/internal/talos"
	"github.com/jarensaa/talos-operator/internal/target"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(talosv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// options are the manager's command-line flags.
type options struct {
	metricsAddr     string
	metricsCertPath string
	metricsCertName string
	metricsCertKey  string
	secureMetrics   bool
	enableHTTP2     bool
	probeAddr       string
	leaderElection  bool
	watchNamespace  string
	observeInterval time.Duration
	// versionSources says whether upstream is asked what exists, and kubernetesReleases where
	// Kubernetes publishes its releases.
	versionSources     bool
	kubernetesReleases string
	zap                zap.Options
}

func parseFlags() *options {
	o := &options{}

	flag.StringVar(&o.metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&o.probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&o.leaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&o.secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&o.metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate. Without it a self-signed one is generated.")
	flag.StringVar(&o.metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&o.metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&o.enableHTTP2, "enable-http2", false, "If set, HTTP/2 will be enabled for the metrics server")
	flag.StringVar(&o.watchNamespace, "watch-namespace", "",
		"Namespace to watch for TalosClusters, TalosMachines and their Secrets. "+
			"The Helm chart sets it to the release namespace so Secret access stays namespaced; "+
			"empty watches all namespaces (local development only).")

	flag.DurationVar(&o.observeInterval, "observe-interval", controller.DefaultObserveInterval,
		"How often a cluster and its machines are observed when nothing else brings a reconcile.")

	flag.BoolVar(&o.versionSources, "version-sources", false,
		"Ask Image Factory, the Kubernetes release site and the registries of the Kubernetes images what exists: "+
			"status then says which releases are available, and an upgrade to something that does not exist is refused "+
			"before a node is asked to pull it. Without it the operator talks to nothing but the cluster it manages.")
	flag.StringVar(&o.kubernetesReleases, "kubernetes-releases", catalog.DefaultKubernetesReleases,
		"Where Kubernetes publishes its releases. Image Factory is the one a TalosCluster's installer names.")

	// Production logging (JSON, info level, no stack traces on warnings) unless --zap-devel asks
	// for the development format; `make run` does.
	o.zap.BindFlags(flag.CommandLine)
	flag.Parse()

	return o
}

// metricsOptions configures the metrics server. It returns the watcher of its serving
// certificate, if one was given, for the manager to run.
func metricsOptions(o *options) (metricsserver.Options, *certwatcher.CertWatcher, error) {
	var tlsOpts []func(*tls.Config)

	// HTTP/2 stays off unless asked for: it is what the Stream Cancellation and Rapid Reset
	// vulnerabilities need (GHSA-qppj-fm5r-hxr3, GHSA-4374-p667-p6c8).
	if !o.enableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *tls.Config) { c.NextProtos = []string{"http/1.1"} })
	}

	opts := metricsserver.Options{
		BindAddress:   o.metricsAddr,
		SecureServing: o.secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if o.secureMetrics {
		// Scrapers are authenticated and authorized through the Kubernetes API; the chart grants
		// the rights that needs (templates/rbac.yaml).
		opts.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	if o.metricsCertPath == "" {
		return opts, nil, nil
	}

	setupLog.Info("serving metrics with the provided certificate",
		"metrics-cert-path", o.metricsCertPath, "metrics-cert-name", o.metricsCertName, "metrics-cert-key", o.metricsCertKey)

	watcher, err := certwatcher.New(
		filepath.Join(o.metricsCertPath, o.metricsCertName),
		filepath.Join(o.metricsCertPath, o.metricsCertKey),
	)
	if err != nil {
		return opts, nil, fmt.Errorf("watch the metrics certificate: %w", err)
	}

	opts.TLSOpts = append(opts.TLSOpts, func(c *tls.Config) { c.GetCertificate = watcher.GetCertificate })

	return opts, watcher, nil
}

// setupControllers registers every controller with the manager.
func setupControllers(mgr ctrl.Manager, resolver *target.Resolver, o *options) error {
	evictor, err := controller.NewPodEvictor(mgr.GetConfig())
	if err != nil {
		return err
	}

	// One source for every cluster, so that what was asked a moment ago is not asked again.
	var upstream catalog.Source
	if o.versionSources {
		upstream = &catalog.Cached{Source: &catalog.HTTP{KubernetesReleases: o.kubernetesReleases}}
	}

	if err := (&controller.TalosClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Resolver: resolver,
		Recorder: mgr.GetEventRecorder("talos-operator"),
		// Uncached reads: node heartbeat Leases, which live outside the watched namespace, and the
		// TalosCluster itself right before a write to Talos.
		APIReader:       mgr.GetAPIReader(),
		ObserveInterval: o.observeInterval,
		Evictor:         evictor,
		// The chart passes the node's name down; without it the operator simply does not know
		// which node an upgrade should leave for last.
		NodeName: os.Getenv("NODE_NAME"),
		Catalog:  upstream,
		// The cluster the operator runs in is the cluster it manages (docs/scope.md §4.2).
		KubernetesChecks: controller.KubernetesCheckerFunc(func(ctx context.Context, from, to string) ([]string, error) {
			return talos.RemovedAPIObjects(ctx, mgr.GetConfig(), from, to)
		}),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("TalosCluster controller: %w", err)
	}

	if err := (&controller.TalosMachineReconciler{
		Client:          mgr.GetClient(),
		Scheme:          mgr.GetScheme(),
		Resolver:        resolver,
		ObserveInterval: o.observeInterval,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("TalosMachine controller: %w", err)
	}

	if err := (&controller.TalosMachinePoolReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("TalosMachinePool controller: %w", err)
	}

	if err := (&controller.TalosMachineConfigReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("TalosMachineConfig controller: %w", err)
	}
	// +kubebuilder:scaffold:builder

	return nil
}

func run(o *options) error {
	metricsServerOptions, metricsCertWatcher, err := metricsOptions(o)
	if err != nil {
		return err
	}

	cacheOptions := cache.Options{}
	if o.watchNamespace != "" {
		// Cluster-scoped objects such as Nodes are unaffected by this restriction.
		cacheOptions.DefaultNamespaces = map[string]cache.Config{o.watchNamespace: {}}
	}

	// There is no webhook server: admission rules are ValidatingAdmissionPolicies, which the API
	// server evaluates itself (docs/scope.md §12.8).
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Cache:                  cacheOptions,
		Metrics:                metricsServerOptions,
		HealthProbeBindAddress: o.probeAddr,
		LeaderElection:         o.leaderElection,
		LeaderElectionID:       "talos-operator.jarensaa.github.io",
		// Release the lease on shutdown so a standby takes over immediately when a rollout drains
		// the leader's node (docs/scope.md §4.2). Safe because main exits right after the
		// manager stops and does no cleanup afterwards.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("create the manager: %w", err)
	}

	talosFactory := talos.NewFactory()
	defer talosFactory.Close()

	if err := setupControllers(mgr, &target.Resolver{Client: mgr.GetClient(), Factory: talosFactory}, o); err != nil {
		return err
	}

	if metricsCertWatcher != nil {
		if err := mgr.Add(metricsCertWatcher); err != nil {
			return fmt.Errorf("add the metrics certificate watcher: %w", err)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("set up the health check: %w", err)
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("set up the ready check: %w", err)
	}

	setupLog.Info("starting manager")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("run the manager: %w", err)
	}

	return nil
}

func main() {
	// The one subcommand: the check a repository runs before a spec reaches a cluster. It is in
	// this binary so that the image a cluster runs is the image that checks its manifests.
	if len(os.Args) > 1 && os.Args[1] == "validate" {
		os.Exit(validateCommand(context.Background(), os.Args[2:], os.Stdout, os.Stderr))
	}

	o := parseFlags()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&o.zap)))

	if err := run(o); err != nil {
		setupLog.Error(err, "the manager stopped")
		os.Exit(1)
	}
}

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

// Package classify decides how risky a machine config change is (docs/scope.md §6.3). It is pure:
// it compares two normalized configs and names the class of the difference and what put it there.
// The class decides whether the operator applies the change at all, and whether a human must
// approve it first. Whatever is applied is applied the same way, in Talos's try mode (§8.5.1),
// because a change of any class can leave a node unable to report that it is broken.
// Nothing here talks to Talos.
package classify

import (
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v4"

	"github.com/jarensaa/talos-operator/internal/render"
)

// Class is the risk class of a config change. Classes are ordered: a change spanning several
// classes takes the most severe one.
type Class string

const (
	// None means the configs are equal.
	None Class = ""
	// LiveSafe changes apply without a reboot and cannot cut a node off: labels, kubelet
	// arguments, sysctls, registries, time.
	LiveSafe Class = "LiveSafe"
	// ControlPlaneRestart changes restart control-plane components; they go one control plane at a
	// time behind the etcd and static-pod checks.
	ControlPlaneRestart Class = "ControlPlaneRestart"
	// Connectivity changes can cut a node off: links, addresses, routes, name resolution, the
	// host firewall.
	Connectivity Class = "Connectivity"
	// Storage changes touch volumes and need a human's approval.
	Storage Class = "Storage"
	// Unknown is everything this package has no rule for. It needs approval, so a document kind
	// Talos adds later is never treated as safe by default.
	Unknown Class = "Unknown"
	// Installer changes name another installer image: another Talos version, or another schematic.
	// They take effect through an upgrade, which the operator performs when upgrades are enabled
	// (§7.3). As a config change it is applied once the node runs what the image installs, and
	// then it changes nothing on the node: see Installed.
	Installer Class = "Installer"
	// Kubernetes changes name another image of a Kubernetes component: the API server, controller
	// manager, scheduler, kube-proxy or kubelet. A config rollout never applies them: a Kubernetes
	// upgrade does, one component at a time and the API servers first (§7.4).
	Kubernetes Class = "Kubernetes"
	// BootTime changes only take effect when a node is installed: kernel arguments, the install
	// disk, disk encryption. The operator does not reinstall a node for them.
	BootTime Class = "BootTime"
	// Protected fields never change: cluster identity, CAs, tokens, the machine type.
	Protected Class = "Protected"
)

// severity orders the classes.
var severity = map[Class]int{
	None: 0, LiveSafe: 1, ControlPlaneRestart: 2, Connectivity: 3, Storage: 4, Unknown: 5, Kubernetes: 6, Installer: 7, BootTime: 8,
	Protected: 9,
}

// Worse reports whether c is more severe than other.
func (c Class) Worse(other Class) bool { return severity[c] > severity[other] }

// NeedsApproval reports whether a human must approve the change before it is applied.
func (c Class) NeedsApproval() bool { return c == Storage || c == Unknown }

// Blocked reports whether a config rollout does not apply the change: because the operator never
// makes it, or because an upgrade has to bring the node there first.
func (c Class) Blocked() bool {
	return c == Protected || c == BootTime || c == Installer || c == Kubernetes
}

// Change is the classification of the difference between two configs.
type Change struct {
	Class Class
	// Reasons names what put the change in its class: document keys, and field paths of the
	// v1alpha1 document. Only the reasons of the most severe class are listed.
	Reasons []string
}

// kindClasses classifies whole config documents by kind. A kind not listed is Unknown.
var kindClasses = map[string]Class{
	// Cluster identity and PKI.
	"KubeClusterConfig": Protected, "KubeNetworkConfig": Protected, "KubeAPIServerCAConfig": Protected,
	"KubeAggregatorCAConfig": Protected, "KubeServiceAccountConfig": Protected, "IssuerServiceAccountConfig": Protected,
	"AcceptedServiceAccountConfig": Protected, "KubeEtcdEncryptionConfig": Protected, "DiscoveryIdentityConfig": Protected,
	// A control plane's hostname is its etcd member name.
	"HostnameConfig": Protected,

	// Only a reinstall changes these.
	"UnattendedInstallConfig": BootTime,
	// A revert does not undo these, and a reboot is what would: Talos loads a kernel module and never
	// unloads one (controllers/runtime/kernel_module_spec.go:74 in Talos v1.14), takes a PCI device
	// from its driver for another (controllers/hardware/pci_driver_rebind.go:151-160), and puts the
	// container plane in its own namespace or out of it only when it starts (sandboxd/enabled.go:42-55).
	// machine.kernel, kernel modules included, is BootTime too.
	"KernelModuleConfig": BootTime, "PCIDriverRebindConfig": BootTime, "SecurityProfileConfig": BootTime,

	// Links, addresses, routes, name resolution and the host firewall.
	"BondConfig": Connectivity, "BridgeConfig": Connectivity, "DHCPv4Config": Connectivity, "DHCPv6Config": Connectivity,
	"DummyLinkConfig": Connectivity, "EthernetConfig": Connectivity, "LinkAliasConfig": Connectivity, "LinkConfig": Connectivity,
	"VLANConfig": Connectivity, "VRFConfig": Connectivity, "VethConfig": Connectivity, "WireguardConfig": Connectivity,
	"ResolverConfig": Connectivity, "RouteConfig": Connectivity, "RoutingRuleConfig": Connectivity,
	"BlackholeRouteConfig": Connectivity, "BGPInstanceConfig": Connectivity, "Layer2VIPConfig": Connectivity,
	"HCloudVIPConfig": Connectivity, "NetworkDefaultActionConfig": Connectivity, "NetworkRuleConfig": Connectivity,
	"KubeSpanConfig": Connectivity, "KubeSpanEndpointsConfig": Connectivity, "KubePrismConfig": Connectivity,
	"HostDNSConfig": Connectivity, "StaticHostConfig": Connectivity, "NodeIPConfig": Connectivity,
	// Members find each other, and the operator finds them, through discovery.
	"DiscoveryServiceConfig": Connectivity,
	// How workloads reach the Talos API, this operator's own access by ServiceAccount among them, and
	// the node's link to Omni.
	"KubeTalosAPIAccessConfig": Connectivity, "SideroLinkConfig": Connectivity,

	"VolumeConfig": Storage, "UserVolumeConfig": Storage, "RawVolumeConfig": Storage, "ExistingVolumeConfig": Storage,
	"ExternalVolumeConfig": Storage, "SwapVolumeConfig": Storage, "LVMLogicalVolumeConfig": Storage,
	"LVMVolumeGroupConfig": Storage, "RAIDArrayConfig": Storage,
	// The image cache is a volume of its own (controllers/cri/image_cache_config.go:160-170).
	"ImageCacheConfig": Storage,

	"KubeAPIServerConfig": ControlPlaneRestart, "KubeControllerManagerConfig": ControlPlaneRestart,
	"KubeSchedulerConfig": ControlPlaneRestart, "KubeAdmissionControlConfig": ControlPlaneRestart,
	"KubeAuditPolicyConfig": ControlPlaneRestart, "KubeAuthenticationConfig": ControlPlaneRestart,
	"KubeAuthorizerConfig": ControlPlaneRestart,

	"KubeNodeConfig": LiveSafe, "KubeletConfig": LiveSafe, "KubeCredentialProviderConfig": LiveSafe,
	// Talos applies the kube-proxy manifest once, when the cluster is bootstrapped, and never again
	// (§12.20): what the config says about kube-proxy changes nothing that runs. Its image still
	// belongs to the Kubernetes version (kubernetesKinds).
	"KubeProxyConfig": LiveSafe,
	"SysctlConfig":    LiveSafe, "SysfsConfig": LiveSafe, "TimeSyncConfig": LiveSafe, "RegistryMirrorConfig": LiveSafe,
	"RegistryAuthConfig": LiveSafe, "RegistryTLSConfig": LiveSafe, "EnvironmentConfig": LiveSafe, "EtcFileConfig": LiveSafe,
	"UdevRulesConfig": LiveSafe, "KmsgLogConfig": LiveSafe, "EventSinkConfig": LiveSafe, "WatchdogTimerConfig": LiveSafe,
	"OOMConfig": LiveSafe, "TCPProbeConfig": LiveSafe, "HTTPProbeConfig": LiveSafe,
	"FilesystemTrimConfig": LiveSafe, "FilesystemScrubConfig": LiveSafe, "TrustedRootsConfig": LiveSafe,
	"ImageVerificationConfig": LiveSafe,
	// zswap is kernel parameters, as sysctls are (controllers/block/zswap_config.go:79-104).
	"ZswapConfig": LiveSafe,
	// Talos restarts containerd when its config or the base runtime spec changes
	// (controllers/cri/service.go:96-110), as it does for registries: machine.files, which carries
	// the legacy form of a CRI customization, is LiveSafe too. Containers, extension services and
	// static pods are what Talos runs itself, as machine.pods says for static pods.
	"CRICustomizationConfig": LiveSafe, "CRIBaseRuntimeSpecConfig": LiveSafe, "ContainerConfig": LiveSafe,
	"ExtensionServiceConfig": LiveSafe, "KubeStaticPodConfig": LiveSafe,
	// Talos applies a manifest to Kubernetes once and skips what it applied before
	// (controllers/k8s/manifest_apply.go:322), as it does kube-proxy's (§12.20).
	"KubeCoreDNSConfig": LiveSafe, "KubeFlannelCNIConfig": LiveSafe, "KubeInlineManifestConfig": LiveSafe,
	"KubeExternalManifestConfig": LiveSafe,
}

// pathRule classifies a field of the legacy v1alpha1 document by path prefix.
type pathRule struct {
	prefix string
	class  Class
}

// pathRules is matched by longest prefix; a path matching no rule is Unknown.
var pathRules = sortRules([]pathRule{
	{"machine.type", Protected}, {"machine.token", Protected}, {"machine.ca", Protected}, {"machine.acceptedCAs", Protected},
	{"cluster.id", Protected}, {"cluster.secret", Protected}, {"cluster.token", Protected}, {"cluster.ca", Protected},
	{"cluster.acceptedCAs", Protected}, {"cluster.aggregatorCA", Protected}, {"cluster.serviceAccount", Protected},
	{"cluster.aescbcEncryptionSecret", Protected}, {"cluster.secretboxEncryptionSecret", Protected},
	{"cluster.etcd.ca", Protected}, {"cluster.clusterName", Protected}, {"cluster.controlPlane.endpoint", Protected},
	{"cluster.network.podSubnets", Protected}, {"cluster.network.serviceSubnets", Protected},
	{"cluster.network.dnsDomain", Protected},

	{v1alpha1InstallImage, Installer},
	{"machine.kubelet.image", Kubernetes}, {"cluster.apiServer.image", Kubernetes},
	{"cluster.controllerManager.image", Kubernetes}, {"cluster.scheduler.image", Kubernetes},
	{"cluster.proxy.image", Kubernetes}, {"cluster.proxy", LiveSafe},
	{"machine.install", BootTime}, {"machine.kernel", BootTime}, {"machine.systemDiskEncryption", BootTime},

	{"machine.disks", Storage},

	{"machine.network", Connectivity}, {"machine.features.hostDNS", Connectivity}, {"machine.features.kubePrism", Connectivity},

	{"cluster.apiServer", ControlPlaneRestart}, {"cluster.controllerManager", ControlPlaneRestart},
	{"cluster.scheduler", ControlPlaneRestart}, {"cluster.etcd", ControlPlaneRestart},

	{"machine.kubelet", LiveSafe}, {"machine.nodeLabels", LiveSafe}, {"machine.nodeAnnotations", LiveSafe},
	{"machine.nodeTaints", LiveSafe}, {"machine.sysctls", LiveSafe}, {"machine.sysfs", LiveSafe},
	{"machine.registries", LiveSafe}, {"machine.time", LiveSafe}, {"machine.env", LiveSafe}, {"machine.logging", LiveSafe},
	{"machine.certSANs", LiveSafe}, {"machine.files", LiveSafe}, {"machine.udev", LiveSafe}, {"machine.pods", LiveSafe},
	{"machine.seccompProfiles", LiveSafe}, {"cluster.adminKubeconfig", LiveSafe},
	{"cluster.allowSchedulingOnControlPlanes", LiveSafe},
})

func sortRules(rules []pathRule) []pathRule {
	sort.SliceStable(rules, func(i, j int) bool { return len(rules[i].prefix) > len(rules[j].prefix) })

	return rules
}

// Option changes how a difference is classified.
type Option func(*options)

type options struct {
	installed bool
}

// Installed says that the node already runs what the desired config's installer image installs.
// Naming that image in the node's config is then bookkeeping, and as safe as a label.
func Installed() Option { return func(o *options) { o.installed = true } }

// installerImagePaths are where a machine config names the installer image: in the
// UnattendedInstallConfig document, and in the v1alpha1 document of older configs.
const (
	unattendedInstallKind  = "UnattendedInstallConfig"
	unattendedInstallImage = "installer.image"
	v1alpha1InstallImage   = "machine.install.image"
)

// Classify compares live with desired. An empty Change means they are equal.
func Classify(live, desired *render.Config, opts ...Option) (Change, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	installer := Installer
	if o.installed {
		installer = LiveSafe
	}

	byClass := map[Class][]string{}

	note := func(class Class, reason string) {
		if class == Installer {
			class = installer
		}

		byClass[class] = append(byClass[class], reason)
	}

	keys := make(map[render.DocKey]bool, len(live.Docs)+len(desired.Docs))
	for k := range live.Docs {
		keys[k] = true
	}

	for k := range desired.Docs {
		keys[k] = true
	}

	for k := range keys {
		a, inLive := live.Docs[k]
		b, inDesired := desired.Docs[k]

		if inLive && inDesired && string(a) == string(b) {
			continue
		}

		// A document only one of the configs has is classified as a whole.
		whole := kindClass(k)

		if rule, byField := fieldRuleFor(k); byField && inLive && inDesired {
			paths, err := changedPaths(a, b)
			if err != nil {
				return Change{}, fmt.Errorf("compare %s: %w", k, err)
			}

			if classes, ok := rule.classes(paths); ok {
				for i, p := range paths {
					note(classes[i], string(k)+": "+p)
				}

				continue
			}

			whole = rule.whole
		}

		note(whole, string(k))
	}

	var out Change

	for class, reasons := range byClass {
		if class.Worse(out.Class) {
			out = Change{Class: class, Reasons: reasons}
		}
	}

	slices.Sort(out.Reasons)

	return out, nil
}

// fieldRule classifies a document that both configs have by the fields that differ between them,
// instead of as a whole.
type fieldRule struct {
	// class is the class of one field. A field that has none leaves the document to be classified
	// as a whole.
	class func(path string) (Class, bool)
	// whole is the class of the document when its fields do not say: one of them has no class, or
	// the documents differ in a way the field comparison does not see.
	whole Class
}

// fieldRuleFor returns the rule of the three kinds of document that have one.
func fieldRuleFor(key render.DocKey) (fieldRule, bool) {
	kind := key.Kind()

	switch {
	case key == render.RootKey:
		// Every field of the legacy root document has a class: Unknown, if no rule names it.
		return fieldRule{class: func(path string) (Class, bool) { return pathClass(path), true }, whole: Unknown}, true
	case kubernetesKinds[kind]:
		// The image of a Kubernetes component is the Kubernetes upgrade's to change. Everything
		// else in its document is classified as the document is.
		return fieldRule{
			class: func(path string) (Class, bool) {
				if path == componentImage {
					return Kubernetes, true
				}

				return kindClass(key), true
			},
			whole: kindClass(key),
		}, true
	case kind == unattendedInstallKind:
		// The document that names the installer: a change of the image alone is the installer's
		// class, anything else in it only an install can bring about.
		return fieldRule{
			class: func(path string) (Class, bool) { return Installer, path == unattendedInstallImage },
			whole: kindClass(key),
		}, true
	}

	return fieldRule{}, false
}

// classes returns the class of each of the paths. It reports false when there are none, or when
// one of them has no class.
func (r fieldRule) classes(paths []string) ([]Class, bool) {
	out := make([]Class, 0, len(paths))

	for _, p := range paths {
		class, ok := r.class(p)
		if !ok {
			return nil, false
		}

		out = append(out, class)
	}

	return out, len(out) > 0
}

// kubernetesKinds are the documents that name the image of a Kubernetes component Talos runs,
// each in its "image" field (machinery config/types/k8s).
var kubernetesKinds = map[string]bool{
	"KubeAPIServerConfig": true, "KubeControllerManagerConfig": true, "KubeSchedulerConfig": true, "KubeletConfig": true,
	"KubeProxyConfig": true,
}

const componentImage = "image"

// kindClass classifies a document as a whole, by its kind.
func kindClass(key render.DocKey) Class {
	kind := key.Kind()
	if kind == "" {
		// The whole v1alpha1 document appearing or disappearing.
		return Protected
	}

	if class, ok := kindClasses[kind]; ok {
		return class
	}

	return Unknown
}

func pathClass(path string) Class {
	for _, rule := range pathRules {
		if path == rule.prefix || strings.HasPrefix(path, rule.prefix+".") {
			return rule.class
		}
	}

	return Unknown
}

// changedPaths returns the dotted paths of the fields that differ between two YAML documents. It
// descends through mappings only: a list or scalar that differs is reported at its own path.
func changedPaths(a, b []byte) ([]string, error) {
	var left, right map[string]any

	if err := yaml.Unmarshal(a, &left); err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(b, &right); err != nil {
		return nil, err
	}

	var paths []string

	diffMaps("", left, right, &paths)
	slices.Sort(paths)

	return paths, nil
}

func diffMaps(prefix string, a, b map[string]any, out *[]string) {
	keys := make(map[string]bool, len(a)+len(b))
	for k := range a {
		keys[k] = true
	}

	for k := range b {
		keys[k] = true
	}

	for k := range keys {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}

		av, aok := a[k]
		bv, bok := b[k]

		am, aIsMap := av.(map[string]any)
		bm, bIsMap := bv.(map[string]any)

		switch {
		case aok && bok && aIsMap && bIsMap:
			diffMaps(path, am, bm, out)
		case aok && bok && reflect.DeepEqual(av, bv):
		case aIsMap && !bok && len(am) > 0:
			// A removed subtree: report every field under it, so each is classified on its own.
			diffMaps(path, am, nil, out)
		case bIsMap && !aok && len(bm) > 0:
			diffMaps(path, nil, bm, out)
		default:
			*out = append(*out, path)
		}
	}
}

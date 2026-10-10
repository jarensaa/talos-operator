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
	// BootTime changes only take effect when a node is installed: kernel arguments, the install
	// disk, disk encryption. The operator does not reinstall a node for them.
	BootTime Class = "BootTime"
	// Protected fields never change: cluster identity, CAs, tokens, the machine type.
	Protected Class = "Protected"
)

// severity orders the classes.
var severity = map[Class]int{
	None: 0, LiveSafe: 1, ControlPlaneRestart: 2, Connectivity: 3, Storage: 4, Unknown: 5, Installer: 6, BootTime: 7, Protected: 8,
}

// Worse reports whether c is more severe than other.
func (c Class) Worse(other Class) bool { return severity[c] > severity[other] }

// NeedsApproval reports whether a human must approve the change before it is applied.
func (c Class) NeedsApproval() bool { return c == Storage || c == Unknown }

// Blocked reports whether a config rollout does not apply the change: because the operator never
// makes it, or because an upgrade has to bring the node there first.
func (c Class) Blocked() bool { return c == Protected || c == BootTime || c == Installer }

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

	"VolumeConfig": Storage, "UserVolumeConfig": Storage, "RawVolumeConfig": Storage, "ExistingVolumeConfig": Storage,
	"ExternalVolumeConfig": Storage, "SwapVolumeConfig": Storage, "LVMLogicalVolumeConfig": Storage,
	"LVMVolumeGroupConfig": Storage, "RAIDArrayConfig": Storage,

	"KubeAPIServerConfig": ControlPlaneRestart, "KubeControllerManagerConfig": ControlPlaneRestart,
	"KubeSchedulerConfig": ControlPlaneRestart, "KubeAdmissionControlConfig": ControlPlaneRestart,
	"KubeAuditPolicyConfig": ControlPlaneRestart, "KubeAuthenticationConfig": ControlPlaneRestart,
	"KubeAuthorizerConfig": ControlPlaneRestart,

	"KubeNodeConfig": LiveSafe, "KubeletConfig": LiveSafe, "KubeCredentialProviderConfig": LiveSafe,
	"SysctlConfig": LiveSafe, "SysfsConfig": LiveSafe, "TimeSyncConfig": LiveSafe, "RegistryMirrorConfig": LiveSafe,
	"RegistryAuthConfig": LiveSafe, "RegistryTLSConfig": LiveSafe, "EnvironmentConfig": LiveSafe, "EtcFileConfig": LiveSafe,
	"UdevRulesConfig": LiveSafe, "KmsgLogConfig": LiveSafe, "EventSinkConfig": LiveSafe, "WatchdogTimerConfig": LiveSafe,
	"OOMConfig": LiveSafe, "TCPProbeConfig": LiveSafe, "HTTPProbeConfig": LiveSafe,
	"FilesystemTrimConfig": LiveSafe, "FilesystemScrubConfig": LiveSafe,
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

// v1alpha1Key is the key of the legacy root document in a normalized config.
const v1alpha1Key render.DocKey = "v1alpha1"

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

		if k == v1alpha1Key && inLive && inDesired {
			paths, err := changedPaths(a, b)
			if err != nil {
				return Change{}, fmt.Errorf("compare %s: %w", k, err)
			}

			for _, p := range paths {
				note(pathClass(p), string(k)+": "+p)
			}

			if len(paths) == 0 {
				// The documents differ in a way the field comparison does not see.
				note(Unknown, string(k))
			}

			continue
		}

		// The document that names the installer: a change of the image alone is the installer's
		// class, anything else in it only an install can bring about.
		if isKind(k, unattendedInstallKind) && inLive && inDesired {
			paths, err := changedPaths(a, b)
			if err != nil {
				return Change{}, fmt.Errorf("compare %s: %w", k, err)
			}

			if len(paths) == 1 && paths[0] == unattendedInstallImage {
				note(Installer, string(k)+": "+paths[0])

				continue
			}
		}

		note(kindClass(k), string(k))
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

func isKind(key render.DocKey, kind string) bool {
	parts := strings.Split(string(key), "/")

	return len(parts) >= 2 && parts[1] == kind
}

// kindClass classifies a document key such as v1alpha1/BondConfig/bond0.
func kindClass(key render.DocKey) Class {
	parts := strings.Split(string(key), "/")
	if len(parts) < 2 {
		// The whole v1alpha1 document appearing or disappearing.
		return Protected
	}

	if class, ok := kindClasses[parts[1]]; ok {
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

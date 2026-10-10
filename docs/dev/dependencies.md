# Dependencies

Pinned set, verified on 2026-10-09 by building and vetting a module that imports every package the operator plans to use (beads `talos-w3y.9`).

| Module | Version | Why this version |
|--------|---------|------------------|
| Go toolchain | `go 1.26.5` | Minimum required by machinery v1.14.x and go-kubernetes v0.2.43. `GOTOOLCHAIN=auto` downloads it. |
| `github.com/siderolabs/talos/pkg/machinery` | `v1.14.2` | Latest patch of the Talos minor the target clusters run. Its minor must be ≥ the newest Talos minor managed (docs/scope.md §4.3). |
| `github.com/siderolabs/go-kubernetes` | `v0.2.43` | Upgrade path/checks, manifests, SSA. It requires machinery v1.14.2 itself. |
| `sigs.k8s.io/controller-runtime` | `v0.25.2` | First line built on `k8s.io/*` v0.37 (v0.24.x is on v0.36). |
| `k8s.io/api`, `apimachinery`, `client-go` | `v0.37.0` | Shared by machinery, go-kubernetes and controller-runtime. |

Packages verified to build together: machinery
- `client`, `client/config`
- `compatibility`, `role`
- `config`, `config/{generate,generate/secrets,generate/stdpatches,configpatcher,configdiff,configloader}`
- `resources/{runtime,config,cluster,hardware,k8s,etcd}`

and go-kubernetes `kubernetes/{upgrade,manifests,ssa}`, plus controller-runtime and `pkg/envtest`.

## Notes

- **No `replace` directives are needed.** machinery's go.mod replaces `github.com/mdlayher/ethtool` with a Sidero fork. Downstream modules ignore that, and none of the packages above need the fork's extra APIs: the build resolves upstream `ethtool v0.6.1` and compiles cleanly. Revisit if we import machinery network helpers.
- **Never import** `github.com/siderolabs/talos` (the main module) or anything under `cmd/talosctl`. See `CLAUDE.md`.
- **`GOPROXY=direct` does not work.** `github.com/google/cel-go v0.31.0`, which machinery requires, is served by `proxy.golang.org`, but its tag is no longer present on GitHub (`git ls-remote` stops at v0.30.0). Builds need a module proxy: the Go default `GOPROXY=https://proxy.golang.org,direct`. Checksums are still verified against `sum.golang.org`. CI uses the default.

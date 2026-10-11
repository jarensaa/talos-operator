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

## Keeping them current

Dependabot proposes the bumps (`.github/dependabot.yml`): weekly, a few days after a release came out, grouped so that what has to move together does. A pull request that changes `go.mod` runs the e2e like any other.

- **machinery and go-kubernetes, patch releases.** These carry what the operator knows about Talos and Kubernetes: which Kubernetes a Talos release runs, which steps an upgrade may take, what a Kubernetes release removed. The operator keeps no table of its own (docs/scope.md §7.5), so merging these is how that knowledge stays current. They are committed as `fix(deps)` and therefore released.
- **A new minor of machinery** is a new Talos to support, not a bump. It arrives as a pull request of its own and stays open until the work is done: the config version contract, the versions the e2e runs, and the table above.
- **`k8s.io/*` and controller-runtime** move together, in one pull request. The envtest binaries follow them: the `Makefile` reads their version from `go.mod`.
- **GitHub Actions and the release tooling** are committed as `ci` and `chore`, and release nothing.

**What merges by itself** (`.github/workflows/dependabot-merge.yml`), once every required check has passed:

| | Merges by itself | Waits for a person |
|---|---|---|
| Go modules | patch releases | a new minor or major |
| GitHub Actions, release tooling | patch and minor releases | a new major |

- The checks are the ones every pull request needs, the e2e on one control plane included. Three control planes run weekly, after the fact.
- A pull request's checks never run the steps that publish. A bump of an action that only publishing uses is first tried by the next release.
- **The merge starts no workflow on `main`.** GitHub starts none for what a workflow's own token does. Nothing is lost: the checks ran on the pull request on top of `main` as it was, and the bump is released with the next merge a person makes. Until a cluster depends on a bump being released promptly, that is soon enough; when one does, the merge needs an identity of its own (a GitHub App), which is a credential in a job on pull requests and a decision for the owner.

Not covered, and bumped by hand: the Go toolchain, the tools the `Makefile` pins (golangci-lint, which `lint.yml` names too, controller-gen and kustomize), and the `talosctl`, Talos and Kubernetes versions of the e2e (docs/dev/e2e.md).

## Notes

- **No `replace` directives are needed.** machinery's go.mod replaces `github.com/mdlayher/ethtool` with a Sidero fork. Downstream modules ignore that, and none of the packages above need the fork's extra APIs: the build resolves upstream `ethtool v0.6.1` and compiles cleanly. Revisit if we import machinery network helpers.
- **Never import** `github.com/siderolabs/talos` (the main module) or anything under `cmd/talosctl`. See `CLAUDE.md`.
- **`GOPROXY=direct` does not work.** `github.com/google/cel-go v0.31.0`, which machinery requires, is served by `proxy.golang.org`, but its tag is no longer present on GitHub (`git ls-remote` stops at v0.30.0). Builds need a module proxy: the Go default `GOPROXY=https://proxy.golang.org,direct`. Checksums are still verified against `sum.golang.org`. CI uses the default.

# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:6cd5cc61 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/SYNC_CONCEPTS.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->

## Project Overview

A Kubernetes operator that manages Talos Linux clusters declaratively through CRDs (`TalosCluster`, `TalosMachine`). The goal is a self-managed cluster: the operator runs inside the cluster it manages, adopts it safely, owns its machine config, rolls out Talos and Kubernetes upgrades, and adds or removes nodes.

**The design source of truth is [docs/scope.md](docs/scope.md).** Read the relevant section before implementing anything. If the implementation needs to diverge from it, update the doc in the same change.

Milestones M0–M9 are tracked as beads epics; run `bd ready` for unblocked work.

**Nothing about a real cluster belongs in this repository.** Docs, samples and test fixtures use invented names, addresses and hardware identifiers (`my-cluster`, `10.0.0.11`, `cluster.example.com`, `aaaaaaaa-bbbb-4ccc-8ddd-000000000001`). Never copy a value read from a live cluster or from someone's infrastructure repository into a file here, a commit message or a pull request.

## Guiding principles

### Manual fixes are never solutions
Never fix an issue by acting on the cluster outside the codebase (ad-hoc `talosctl`/`kubectl` changes, hand-created resources). Fix the code, chart or CRDs so the operator handles the case. The only manually created resources allowed are our own CRs and the credentials Secrets they reference. If a stop-gap is truly unavoidable, file a bead for it.

### Safety invariants are non-negotiable
They live in `docs/scope.md` §8 and are enforced in pure planners (`internal/plan`), not scattered through controllers. Key ones:
- One disruptive operation per cluster at a time (`status.operation` is the lock).
- At most one control-plane node not Ready or not a healthy etcd member.
- Never pass `force`. Never write Talos-owned META keys.
- No writes while a capability is not enabled in `spec.management`. Observe mode uses a read-only client interface, so writes are impossible by construction.
- Fail closed: on failure, halt and surface the problem. Never move on to the next node.
- Losing contact with a node must result in rollback (TRY-then-confirm, §8.5).
- Persist every step before its side effect; resume by observing facts (boot ID, config hashes), never from memory.

### Talos integration rules
- Talk to Talos only through `github.com/siderolabs/talos/pkg/machinery`, and only from `internal/talos`. Never exec `talosctl`. Never import the main `github.com/siderolabs/talos` module or `cmd/talosctl/...`.
- Use the `talosctl` source in the Go module cache (`~/go/pkg/mod/github.com/siderolabs/talos@v1.14.0`) as the reference for how to drive the API. Cite `file:line` in design notes.
- Talos API endpoints are node IPs, never the cluster DNS name. Reach the target node through a different node via `client.WithNode`.
- Only use strategic-merge config patches. JSON6902 does not work on multi-document configs.
- Every config change is applied in mode `TRY` and confirmed with `NO_REBOOT` only after verification (docs/scope.md §8.5.1). Never send `TRY` to a node whose active config is not the one to fall back to: Talos takes whatever is active as the rollback target. Dry runs always use mode `TRY`; any other mode cancels a pending rollback.
- Pin the config version contract explicitly. Never use `TalosVersionCurrent`.
- Never log rendered machine configs. Redact secret-bearing fields in diffs, status and Events.
- Read nodes through `nodeReads` (`internal/controller/nodereads.go`) when observing: every read is bounded and logged, and a node that does not answer is not asked again in that pass. A fact that was not read is unknown to every gate, never fine, and never left in status looking current.
- A failure of the Kubernetes API is not a fact about Talos (`target.ErrKubernetesAPI`): return it and let the pass be retried.

## Build & Test

Go 1.26.5 (required by machinery v1.14; `GOTOOLCHAIN=auto` fetches it). Builds need a module proxy: the default `GOPROXY=https://proxy.golang.org,direct` works and `direct` alone does not (docs/dev/dependencies.md).

```bash
make build            # manager binary
make test             # regenerate, fmt, vet, then unit + envtest
make test-unit        # unit + envtest only (what CI runs)
make lint             # golangci-lint, including e2e-tagged code
make manifests        # CRDs + RBAC; also syncs them and config/admission into charts/talos-operator
make helm-lint        # chart lint
```

e2e runs in CI (`.github/workflows/e2e.yml`) against a QEMU Talos cluster; see docs/dev/e2e.md. e2e tests must be named `TestE2E*` and carry the `e2e` build tag.

CI Go setup goes through `.github/actions/setup-go`, which restores the newest build cache saved from `main` (only the "Unit and envtest" job saves it, via `.github/actions/save-go-cache`). Do not re-enable setup-go's own cache: its single go.sum key freezes the build cache. Caches are written from `main` only, never from pull requests (a PR's cache is visible to that PR alone), and saves prune the entries they supersede (`.github/actions/prune-caches`). The image is built from binaries cross-compiled on the host (`make build-image-binaries`); the Dockerfile only packages them, so never compile inside Docker or add a BuildKit layer cache. Local runs are optional: `make e2e-cluster-up` / `make test-e2e` / `make e2e-cluster-down`, which prompt for sudo on macOS. Docker-based Talos clusters do not work for Talos 1.14 on Docker Desktop.

Releases are automatic. Every push to `main` runs `.github/workflows/test.yml`: verify, then semantic-release (`.releaserc.json`, `hack/release/`), then publish.
- **Versions come from Conventional Commits** since the last tag: `feat:` is a minor release, `fix:` a patch, and `!` or a `BREAKING CHANGE:` footer a major one. Other types (`ci:`, `docs:`, `test:`, `chore:`…) release nothing. Write every commit, and a squash-merged PR's title, in that form. Never push release tags by hand.
- **A release** publishes `ghcr.io/jarensaa/talos-operator:vX.Y.Z` (amd64+arm64, SBOM, provenance), the chart `oci://ghcr.io/jarensaa/charts/talos-operator` at `X.Y.Z`, and a GitHub Release whose notes semantic-release writes, with the artifacts (image digest, chart) added and the chart attached. The chart's default image tag is `v` + appVersion, so they always match.
- **Every push** publishes `:sha-<commit>` and `:main` images.
- Release tooling is Node (`package.json`, `pnpm-lock.yaml`, pnpm 10.28); `pnpm test:release` tests it.

Layout: `api/v1alpha1` (CRD types), `config/admission` (ValidatingAdmissionPolicies; there is no webhook, see docs/scope.md §12.8), `internal/talos` (the only Talos API gateway; `creds`, `fake`, `talostest`), `internal/target` (TalosCluster → clients), `internal/render` (machine config from spec), `internal/classify` (how risky a config change is), `internal/plan` (pure planners: what may happen next), `internal/controller` (observe, ask a planner, do the one thing it answers), `charts/talos-operator`, `hack/e2e`, `test/e2e`.

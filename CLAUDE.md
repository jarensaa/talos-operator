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

Milestones are tracked as beads epics; run `bd ready` for unblocked work. No milestone is open: what is left is in docs/scope.md §10 under "Later" (PKI rotation) and "Owned by no milestone" (restarting etcd for a changed argument, among others). Bootstrapping a cluster and the rest of the old M9 are out of scope (§12.26).

## This repository is public

Treat every push as publication. Anyone can read every file, every commit message and branch name, every pull request title, description and comment, and every line a CI job prints, and none of it can be taken back: a later commit does not remove what an earlier one published, and GitHub keeps a pull request's commits for good.

- **No secrets, ever.** No tokens, keys, certificates, kubeconfigs, talosconfigs, secrets bundles, age or SOPS keys, or `.env` files, in files or in text. A machine config from a real cluster is a secret: it carries the cluster's CA keys. Tests generate their key material when they run; none is checked in.
- **Nothing about a real cluster or its owner.** Docs, samples and test fixtures use invented names, addresses and hardware identifiers (`my-cluster`, `10.0.0.11`, `cluster.example.com`, `aaaaaaaa-bbbb-4ccc-8ddd-000000000001`, `eth0`). Never copy a value read from a live cluster or from someone's infrastructure repository: no cluster or node names, IP addresses, DNS names, NIC names, disk or system identifiers, no names of private repositories, no local filesystem paths, no personal email addresses. Describe the kind of thing, never the instance.
- **CI output is public too.** Do not let a job print a machine config, a secrets bundle, or the output of a command that contains one. Check what a command writes to stderr as well as stdout: `talosctl apply-config --dry-run` prints the whole config, keys included, on stderr. Artifacts uploaded on failure are as public as the log.
- **Workflows run for pull requests from anyone.** Never use `pull_request_target`, never hand a secret to a job that runs on a pull request, and keep releasing and publishing on pushes to `main` only. One job on pull requests has a token that may write: `dependabot-merge.yml`, for pull requests Dependabot opened, which runs nothing from the pull request. Never add a checkout or a build to it.
- **The beads database stays on the machine it lives on.** Its issue text is working notes, not vetted for publication: never commit `.beads/`, and never run a Dolt remote sync against this repository's remote.
- **If something sensitive does get pushed, stop and tell the owner at once.** Do not try to bury it under another commit. A leaked credential has to be revoked, not hidden.

## Guiding principles

### Manual fixes are never solutions
Never fix an issue by acting on the cluster outside the codebase (ad-hoc `talosctl`/`kubectl` changes, hand-created resources). Fix the code, chart or CRDs so the operator handles the case. The only manually created resources allowed are our own CRs and the credentials Secrets they reference. If a stop-gap is truly unavoidable, file a bead for it.

### Safety invariants are non-negotiable
They live in `docs/scope.md` §8 and are enforced in pure planners (`internal/plan`), not scattered through controllers. Key ones:
- One disruptive operation per cluster at a time (`status.operation` is the lock).
- At most one control-plane node not Ready or not a healthy etcd member.
- A node is touched only while every other node is known to be healthy (`plan.TouchGate`). The node's own health is not asked about, so a rollout can repair it; verification afterwards leaves nobody out (`plan.VerifyGate`).
- Never pass `force`. Never write Talos-owned META keys.
- While a cluster is being adopted, the operator writes its pools and machines by server-side apply **without force**, as `talos-operator-adoption`, and only fills empty fields of the TalosCluster. Once adopted (`status.adoption.adoptedAt`), it never writes a spec again; nothing unsets that (docs/scope.md §12.14).
- No writes while a capability is not enabled in `spec.management`. Observe mode uses a read-only client interface, so writes are impossible by construction.
- A node is not cordoned or rebooted while `plan.RebootGate` is closed, and a control plane never while etcd has exactly two members. A reboot is only asked of a node that still has the boot ID recorded before; a node that comes back on anything but the installed system is a failure.
- Drain through the eviction API (`controller.PodEvictor`), never by deleting pods, and never with the Kubernetes client's own retries on: they sleep through a budget's refusals inside a reconcile.
- Every kind of operation goes through one driver (`controller.drive`, deciding in `plan.NextTurn`): no node begins while paused or outside a maintenance window, a node that is being changed is always finished, and an operation that ran its `preRollout` hooks does not release the lock before its `postRollout` hooks ran. A new kind of operation supplies its turns to the driver; it does not walk its plan itself (docs/scope.md §6.4).
- The user's gates are asked wherever the cluster's own health is: before a node is touched and while a changed node is verified. Only a plain `true` opens one; an object that is missing or may not be read, and an expression that fails, close it.
- A config rollout never applies the image of a Kubernetes component, and never waits for one either: it compares and applies the rendered config with those images as the node has them (`withLiveKubernetes`). A Kubernetes upgrade changes them: one component on one node at a time, every API server first, never skipping a minor, each change tried and confirmed only once the component runs the new version. It cannot skip a node (docs/scope.md §7.4).
- The operator talks to nothing outside its cluster except through `internal/catalog`, and only when `versionSources.enabled` switches it on (off by default; the e2e deploy switches it on). It only asks what exists upstream: over HTTPS, rarely, identified, and never while an operation is in flight. Which versions go together and what a release removed are never asked: those rules come compiled in, from machinery and go-kubernetes. What it learns may refuse an upgrade and may choose a release on the way to the version a spec asks for; it never chooses the version. "Not known" is neither yes nor no: a source that does not answer refuses nothing in the operator, and fails the `validate` check (docs/scope.md §7.5, §12.22).
- One question needs no switch: which certificate the installer's Image Factory signs boot images with (`catalog.Signer`), asked when a Talos upgrade is about to start and a machine in its plan has Secure Boot on, and at no other time. Never add a second question to that interface, and never ask it at rest or on a timer.
- A Talos upgrade touches no node until every machine in its plan would boot the new system (`secureBootPreflight`, deciding in `plan.PlanUpgrade`): a machine whose firmware has Secure Boot on is not installed from an installer whose boot image is not signed, nor while its firmware's db does not trust the signer. One machine that would not refuses the upgrade of all of them, and here "not known" refuses too: a db that was not read or a factory that did not answer starts nothing (docs/scope.md §7.3, §12.28).
- The operator applies none of the manifests Talos applies at bootstrap, and patches no workload: kube-proxy and CoreDNS are not its to change. The kube-proxy image in a control plane's config is, as part of a Kubernetes upgrade (docs/scope.md §12.20).
- A machine is a member from `status.joinedAt` until `status.removedAt` (`isMember`). Rollouts, upgrades and the proof see members only; a machine that is declared and has not joined, or has left, holds nothing up and is the business of adding and removing alone (docs/scope.md §5.4, §7.6).
- A machine in maintenance mode is handed its config only when it said, in that pass and over that connection, that it is the machine declared, and the config fits it; never twice (`plan.NextJoinStep`). A machine is only added to a cluster at rest.
- A control plane leaves etcd only while three voting members remain; there is no override but a human's token-bound word that the machine is dead (`plan.RemovalGate`, `AnnotationConfirmDead`). A machine that left is done when it answers in maintenance mode again, and its Node is not deleted before that. A machine that does not answer is never guessed about (docs/scope.md §7.7).
- etcd is defragmented through the driver like every operation: every member when one is over the threshold, the leader last, a member only while every node is healthy (`plan.TouchGate` with nobody left out) and the next only after the cluster was verified, and not again within the interval (`plan.PlanDefragment`, docs/scope.md §14.6).
- A cluster at rest is not written to. What status holds of a node's health describes a moment and is gone when it was not read; a number that always changes, such as the size of an etcd database, is followed at a distance (`followedSize`).
- What a failed operation's machine said goes into one Secret, never a log, and never over a Secret that is not the operator's (`internal/controller/bundle.go`).
- Fail closed: on failure, halt and surface the problem. Never move on to the next node.
- Losing contact with a node must result in rollback (TRY-then-confirm, §8.5).
- Persist every step before its side effect; resume by observing facts (boot ID, config hashes), never from memory.

### Talos integration rules
- Talk to Talos only through `github.com/siderolabs/talos/pkg/machinery`, and only from `internal/talos`. Never exec `talosctl`. Never import the main `github.com/siderolabs/talos` module or `cmd/talosctl/...`.
- Use the `talosctl` source in the Go module cache (`~/go/pkg/mod/github.com/siderolabs/talos@v1.14.0`) as the reference for how to drive the API. Cite `file:line` in design notes.
- Talos API endpoints are node IPs, never the cluster DNS name. Reach the target node through a different node via `client.WithNode`.
- Six kinds of connection, each behind its own allowlist (`internal/talos/allowlist.go`, `membership.go`, `health.go`): reading, applying config (`ConfigWriter`, only while `config: Manage`), upgrading (`Upgrader`, only while `upgrades: Enabled`: pull, install to the inactive slot, hand off etcd leadership, reboot), removing (`Remover`, only while `membership: Enabled`: hand off etcd leadership, the one reset of a removal, removing a dead machine's etcd member), the maintenance connection (`Maintenance`, only while `membership: Enabled`: to one machine without a config, without the cluster's credentials, to read its hardware and hand it its config), and upkeep (`Keeper`, only while `maintenance: Enabled`: defragment the etcd member of one node, hand off etcd leadership). A new mutating call belongs to one of them, on its list, with a test that the others refuse it.
- One file of a node is read, and no other: the firmware's Secure Boot db (`talos.SecureBootDBPath`). `MachineService.Read` reads anything, a node's secrets included; the read allowlist lets it through for that path alone. Never add a path.
- No connection lists or disarms an alarm of etcd, and none restarts a service. While etcd has an alarm raised it refuses the writes an operator inside the cluster would act with, so upkeep is what prevents one (docs/scope.md §12.27).
- The `Remover` connection lets exactly one shape of `Reset` through: graceful, with a reboot, wiping `STATE` and `EPHEMERAL` of the system disk. Never widen it. Nothing else of a machine is ever wiped.
- A Kubernetes upgrade adds no kind of connection: it pulls images through the `Upgrader` and applies config through the `ConfigWriter`. The config it applies is the node's live config with one image replaced (`talos.WithKubernetesImage`, the `stdpatches` talosctl uses), never the rendered one.
- What go-kubernetes knows about upgrades (paths, removed flags, removed APIs) is used from `internal/talos/kubernetes.go` only. The APIs its checks list are granted in the chart (`files/upgrade-check-rules.yaml`); `TestChartGrantsTheUpgradeChecks` fails when a newer go-kubernetes asks for more.
- An upgrade changes the system first and its config second (docs/scope.md §7.3, §12.16). Never apply a config that names an installer the node does not run, and never put a tried config under a reboot.
- Only use strategic-merge config patches. JSON6902 does not work on multi-document configs.
- Every config change is applied in mode `TRY` and confirmed with `NO_REBOOT` only after verification (docs/scope.md §8.5.1). Never send `TRY` to a node whose active config is not the one to fall back to: Talos takes whatever is active as the rollback target. Dry runs always use mode `TRY`; any other mode cancels a pending rollback.
- Pin the config version contract explicitly. Never use `TalosVersionCurrent`.
- Never log rendered machine configs. Redact secret-bearing fields in diffs, status and Events.
- Metrics about a cluster are its resources' status, read at every scrape by `internal/exporter`: never a gauge a controller sets, which only the leader would have and which could say something else than status. To export something new, put it in status first. A dashboard panel may only ask for what the exporter describes (`TestDashboardsAskOnlyForWhatIsExported`). No condition message becomes a label. An alert is a rule in `charts/talos-operator/files/alerts.yaml` and needs a scenario in `internal/exporter/alerts_test.go` that raises it: `make alerts` writes the promtool test from the scenarios, `make alerts-test` runs it.
- Read nodes through `nodeReads` (`internal/controller/nodereads.go`) when observing: every read is bounded and logged, and a node that does not answer is not asked again in that pass. A fact that was not read is unknown to every gate, never fine, and never left in status looking current.
- A failure of the Kubernetes API is not a fact about Talos (`target.ErrKubernetesAPI`): return it and let the pass be retried.

## Build & Test

Go 1.26.5 (required by machinery v1.14; `GOTOOLCHAIN=auto` fetches it). Builds need a module proxy: the default `GOPROXY=https://proxy.golang.org,direct` works and `direct` alone does not (docs/dev/dependencies.md).

```bash
make build            # manager binary
make test             # tidy, regenerate, fmt, vet, then unit + envtest
make committed        # what CI checks first: go.mod tidy, generated files and formatting committed
make hooks            # git hooks (hack/hooks): that check before a commit; lint, chart and the pure
                      # packages' tests before a push. Install them in every checkout you commit from.
make test-unit        # unit + envtest only (what CI runs)
make lint             # golangci-lint, including e2e-tagged code
make manifests        # CRDs + RBAC; also syncs them and config/admission into charts/talos-operator
                      # (CRDs go to the chart's files/crds and are rendered as templates; never add a crds/ directory)
make helm-lint        # chart lint, and a check of what the chart renders (hack/chart/check.sh)
make alerts-test      # the chart's alert rules, checked and tested by promtool on what the exporter exports
```

e2e runs in CI (`.github/workflows/e2e.yml`) against a QEMU Talos cluster; see docs/dev/e2e.md. e2e tests must be named `TestE2E*` and carry the `e2e` build tag. The suite tests the operator as the chart deploys it, two replicas, and starts no controllers itself (`make e2e-deploy`, docs/scope.md §12.18). The cluster is created one Talos patch release and one Kubernetes minor below what the suite upgrades it to. A pull request's check has one control plane and one worker. The cluster has one machine more, left in maintenance mode, for the suite to add and remove (`MAINTENANCE_NODES`). A change to how control planes are handled (rollout order and gates, Talos and Kubernetes upgrades, etcd membership, adding or removing one) must also be shown on three: `gh workflow run e2e.yml --ref <branch> -f controlplanes=3 -f workers=1`, with the run linked in the pull request. A spec that needs several control planes skips on a smaller cluster and says why.

CI Go setup goes through `.github/actions/setup-go`, which restores the newest build cache saved from `main` (only the "Unit and envtest" job saves it, via `.github/actions/save-go-cache`). Do not re-enable setup-go's own cache: its single go.sum key freezes the build cache. Caches are written from `main` only, never from pull requests (a PR's cache is visible to that PR alone), and saves prune the entries they supersede (`.github/actions/prune-caches`). The image is built from binaries cross-compiled on the host (`make build-image-binaries`); the Dockerfile only packages them, so never compile inside Docker or add a BuildKit layer cache. Local runs are optional: `make e2e-cluster-up` / `make test-e2e` / `make e2e-cluster-down`, which prompt for sudo on macOS. Docker-based Talos clusters do not work for Talos 1.14 on Docker Desktop.

Releases are automatic. Every push to `main` runs `.github/workflows/test.yml`: verify, then semantic-release (`.releaserc.json`, `hack/release/`), then publish.
- **Versions come from Conventional Commits** since the last tag: `feat:` is a minor release, `fix:` a patch, and `!` or a `BREAKING CHANGE:` footer a major one. Other types (`ci:`, `docs:`, `test:`, `chore:`…) release nothing. Write every commit in that form, work in progress included: pull requests are merged with a merge commit, so it is the branch's commits that decide the release, not the pull request's title, and a feature committed as `wip:` is merged without ever being released. CI and the pre-push hook refuse a commit that is not one (`hack/release/commits.sh`). Never push release tags by hand.
- **A release** publishes `ghcr.io/jarensaa/talos-operator:vX.Y.Z` (amd64+arm64, SBOM, provenance), the chart `oci://ghcr.io/jarensaa/charts/talos-operator` at `X.Y.Z`, and a GitHub Release whose notes semantic-release writes, with the artifacts (image digest, chart) added and the chart attached. The chart's default image tag is `v` + appVersion, so they always match.
- **Every push** publishes `:sha-<commit>` and `:main` images.
- Release tooling is Node (`package.json`, `pnpm-lock.yaml`, pnpm 10.28); `pnpm test:release` tests it.
- Dependabot proposes dependency bumps weekly (`.github/dependabot.yml`, docs/dev/dependencies.md). A Go module bump is `fix(deps)` and is released: patch releases of machinery and go-kubernetes are how the compiled-in rules about Talos and Kubernetes stay current. Actions and release tooling are `ci` and `chore`. Every bump merges by itself once the required checks pass, except a new minor or major of the Talos libraries, which is a new Talos to support and waits for a person (`.github/workflows/dependabot-merge.yml`); that merge starts no workflow on `main`, so its release comes with the next merge a person makes.

Layout: `api/v1alpha1` (CRD types), `config/admission` (ValidatingAdmissionPolicies; there is no webhook, see docs/scope.md §12.8), `internal/talos` (the only Talos API gateway; `creds`, `fake`, `talostest`), `internal/target` (TalosCluster → clients), `internal/render` (machine config from spec), `internal/classify` (how risky a config change is), `internal/catalog` (what exists upstream: the one place that asks anything outside the cluster), `internal/validate` (the check `manager validate` runs on a repository's manifests), `internal/gates` (the user's gates: a bounded CEL expression over one object), `internal/exporter` (status as metrics), `internal/plan` (pure planners: what may happen next), `internal/controller` (observe, ask a planner, do the one thing it answers), `charts/talos-operator`, `hack/e2e`, `test/e2e`.

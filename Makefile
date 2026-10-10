# VERSION is the operator version used for image tags and the Helm chart appVersion.
VERSION ?= 0.0.1

# Image URL to use all building/pushing image targets
IMG ?= ghcr.io/jarensaa/talos-operator:v$(VERSION)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases
	@# The Helm chart consumes the generated CRDs and RBAC rules directly so it cannot drift.
	rm -f $(CHART_DIR)/crds/*.yaml
	cp config/crd/bases/*.yaml $(CHART_DIR)/crds/
	cp config/rbac/role.yaml $(CHART_DIR)/files/manager-role.yaml
	cp config/admission/policies.yaml $(CHART_DIR)/files/admission-policies.yaml

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet test-unit ## Regenerate, format, vet, then run unit and envtest tests.

# CI runs test-unit directly: it checks generated code and formatting in a separate step, and
# golangci-lint already runs vet.
.PHONY: test-unit
test-unit: setup-envtest ## Run unit and envtest tests only.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# e2e runs against a disposable QEMU Talos cluster (docs/dev/e2e.md).
# The QEMU provisioner needs root on macOS, so e2e-cluster-up/down prompt for sudo.
E2E_DIR ?= $(CURDIR)/.e2e

.PHONY: e2e-cluster-up
e2e-cluster-up: ## Create (or reuse) the QEMU e2e Talos cluster. Requires sudo.
	E2E_DIR=$(E2E_DIR) hack/e2e/cluster-up.sh

.PHONY: e2e-cluster-down
e2e-cluster-down: ## Destroy the QEMU e2e Talos cluster. Requires sudo.
	E2E_DIR=$(E2E_DIR) hack/e2e/cluster-down.sh

E2E_PACKAGES ?= ./internal/talos/... ./test/e2e/...

.PHONY: test-parity
test-parity: ## Compare render output with talosctl gen config (talosctl must match the machinery version).
	go test -tags parity -run Parity -count=1 ./internal/render/

.PHONY: test-e2e-build
test-e2e-build: ## Compile the e2e tests without running them (CI overlaps this with cluster creation).
	go test -tags e2e -count=1 -run '^$$' $(E2E_PACKAGES)

.PHONY: test-e2e
test-e2e: ## Run e2e tests against the cluster from e2e-cluster-up.
	@test -s $(E2E_DIR)/env || { echo "No e2e cluster; run 'make e2e-cluster-up' first."; exit 1; }
	@# Only tests named TestE2E* run: unit tests in the same packages already run in test-unit.
	@# -p 1: the packages share one cluster, and test/e2e takes a node offline for a while.
	. $(E2E_DIR)/env && go test -tags e2e -run '^TestE2E' $(E2E_PACKAGES) -v -count=1 -p 1 -timeout 45m

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go --leader-elect=false --zap-devel

# IMAGE_PLATFORMS are the platforms the manager image is published for.
IMAGE_PLATFORMS ?= linux/amd64,linux/arm64
comma := ,

# The image packages binaries built here rather than compiling inside Docker, so image builds reuse
# the local (and in CI, the restored) Go build cache.
.PHONY: build-image-binaries
build-image-binaries: ## Cross-compile the manager into dist/ for every platform in IMAGE_PLATFORMS.
	@for platform in $(subst $(comma), ,$(IMAGE_PLATFORMS)); do \
		os=$${platform%/*}; arch=$${platform#*/}; \
		echo "go build $$os/$$arch"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch go build -trimpath -o dist/manager-$$os-$$arch ./cmd || exit 1; \
	done

.PHONY: docker-build
docker-build: build-image-binaries ## Build the manager image for the local platform.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

.PHONY: docker-buildx
docker-buildx: build-image-binaries ## Build and push the manager image for every platform in IMAGE_PLATFORMS.
	- $(CONTAINER_TOOL) buildx create --name talos-operator-builder
	$(CONTAINER_TOOL) buildx use talos-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(IMAGE_PLATFORMS) --tag ${IMG} .
	- $(CONTAINER_TOOL) buildx rm talos-operator-builder

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml

##@ Helm

CHART_DIR ?= charts/talos-operator

.PHONY: helm-lint
helm-lint: manifests ## Lint the Helm chart.
	helm lint $(CHART_DIR)

.PHONY: helm-template
helm-template: manifests ## Render the Helm chart for inspection.
	helm template talos-operator $(CHART_DIR) --namespace talos-operator

.PHONY: helm-package
helm-package: manifests ## Package the Helm chart into dist/.
	@mkdir -p dist
	helm package $(CHART_DIR) --app-version $(VERSION) --version $(VERSION) -d dist/

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.3
CONTROLLER_TOOLS_VERSION ?= v0.22.0
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.14.0

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

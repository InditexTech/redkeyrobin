# SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
#
# SPDX-License-Identifier: Apache-2.0

.DEFAULT_GOAL := help
SHELL := /bin/bash

NAME                 := redkeyrobin
VERSION              := 1.0.0
REDIS_CLIENT_VERSION := 8.2.3
GOLANG_VERSION       := 1.25.7
DELVE_VERSION        := 1.25
PACKAGE              := github.com/inditextech/$(NAME)

# Image URL to use for building/pushing image.
IMG ?= redkey-robin:$(VERSION)

# .............................................................................
# DONT TOUCH THIS SECTION
# .............................................................................
# Build specific information
COMMIT?=$(shell git rev-parse HEAD)
DATE?=$(shell date +%FT%T%z)

# Go related variables.
GO = go
GOFMT = gofmt
GOLINT = staticcheck

# go source files, ignore vendor directory
SRC = $(shell find . -path ./vendor -prune -o -name '*.go' -print)
M = $(shell printf "\033[34;1m▶\033[0m")

MODULE=$(shell go list -m)
GO_COMPILE_FLAGS='-X $(MODULE)/cmd/server.GitCommit=$(COMMIT) -X $(MODULE)/cmd/server.BuildDate=$(DATE) -X $(MODULE)/cmd/server.VersionBuild=$(VERSION)'

# Test coverage files
TEST_COVERAGE_PROFILE_OUTPUT = ".local/coverage.out"
TEST_REPORT_OUTPUT = ".local/test_report.ndjson"
TEST_REPORT_OUTPUT_E2E = ".local/test_report_e2e.ndjson"

# .............................................................................
# / END SECTION
# .............................................................................

# .............................................................................
# / IMPORTANT VARIABLES
# .............................................................................
# CHANNELS define the bundle channels used in the bundle.
# Add a new line here if you would like to change its default config. (E.g CHANNELS = "preview,fast,stable")
# To re-generate a bundle for other specific channels without changing the standard setup, you can:
# - use the CHANNELS as arg of the bundle target (e.g make bundle CHANNELS=preview,fast,stable)
# - use environment variables to overwrite this value (e.g export CHANNELS="preview,fast,stable")
ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif

# DEFAULT_CHANNEL defines the default channel used in the bundle.
# Add a new line here if you would like to change its default config. (E.g DEFAULT_CHANNEL = "stable")
# To re-generate a bundle for any other default channel without changing the default setup, you can:
# - use the DEFAULT_CHANNEL as arg of the bundle target (e.g make bundle DEFAULT_CHANNEL=stable)
# - use environment variables to overwrite this value (e.g export DEFAULT_CHANNEL="stable")
ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Image URL to use for building/pushing image targets when using `dev` deployment profile.
# We always use version 0.1.0 for this purpose.
IMG_DEV ?= redkey-robin:0.1.0-dev

# Image URL to use for deploying the operator pod when using `debug` deployment profile.
# A base golang image is used with Delve installed, in order to be able to remotely debug the manager.
IMG_DEBUG ?= delve:1.24.0

# Image REF in bundle image
# Can be overwritten with make bundle IMAGE_REF=<some-registry>/<project-name-bundle>:<tag>
IMAGE_REF ?= $(IMG)

# Allowed deploying profiles.
PROFILES := dev debug pro

# Namespace where redkey robin is deployed.
NAMESPACE ?= redkey-operator

# Deploying profile used to generate the manifest files to deploy the operator.
# The files to generate the manifests are kustomized from the directory config/deploy-profile/<PROFILE>.
# By default, `dev` profile is used. It can be overwritten (e.g. make process-manifests PROFILE=debug).
# Only the values defined in `PROFILES` are allowed.
PROFILE ?= dev
ifeq ($(filter $(PROFILE),$(PROFILES)), )
$(error The profile specified ($(PROFILE)) is not supported)
endif
# .............................................................................
# / END SECTION
# .............................................................................

# .............................................................................
# PUBLIC TARGETS
# .............................................................................

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ##	Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ General
.PHONY: verify
verify: deps tidy checkfmt lint vet build test-cov ## Check the code

deps: ## Installs dependencies
	$(info $(M) installing dependencies...)
	GONOSUMDB=honnef.co/go/* GONOPROXY=honnef.co/go/* $(GO) install honnef.co/go/tools/cmd/staticcheck@v0.6.1

.PHONY: version
version:: ## Print the current version of the project.
	@echo "$(VERSION)"

.PHONY: version-next
version-next:: ## Bump to next development version
	@echo "Bumping to next development version"
	sed -ri 's/(.*)(VERSION\s*:=\s*)([0-9]+)\.([0-9]+)\.([0-9]+)(.*)/echo "\1\2\3.$$((\4+1)).0-SNAPSHOT\6"/ge' Makefile

.PHONY: version-set
version-set:: ## Set the project version to the given version, using the NEW_VERSION environment variable
	@echo "Setting version to $(NEW_VERSION)"
	sed -ri 's/(.*)(VERSION\s*:=\s*)([0-9]+\.[0-9]+\.[0-9]+)(-SNAPSHOT)(.*)/echo "\1\2$(NEW_VERSION)\5"/ge' Makefile

.PHONY: checkfmt
checkfmt: ## Check format validation
	$(info $(M) running gofmt checking code style...)
	@fmtRes=$$($(GOFMT) -d $(SRC)); \
	if [ -n "$${fmtRes}" ]; then \
		echo "gofmt checking failed!"; echo "$${fmtRes}"; echo; \
		echo "Please ensure you are using $$($(GO) version) for formatting code."; \
		exit 1; \
	fi

.PHONY: fmt
fmt: ## Run gofmt on all source files
	$(info $(M) running go fmt...)
	$(GOFMT) -l -w $(SRC)

.PHONY: lint
lint: deps ## Run golint
	$(info $(M) running staticcheck...)
	$(GOLINT) ./...

.PHONY: vet
vet: ## Run go vet
	$(info $(M) running go vet...)
	$(GO) vet ./...

.PHONY: clean
clean: ## Clean the build artifacts and Go cache
	$(info $(M) cleaning generated files...)
	rm -rf ./target
	rm -rf ./bin
	$(GO) clean --modcache


##@ Build
.PHONY: build
build: ##	Build program binary
	$(info $(M) building executable...)
	$(GO) build \
			-ldflags $(GO_COMPILE_FLAGS) \
			-tags release \
			-o bin/robin \
			./cmd/main.go

.PHONY: update-packages
update-packages: ##	Run go get -u
	$(info $(M) running go get -u...)
	$(GO) get -u


.PHONY: tidy
tidy: ##	Run go mod tidy
	$(info $(M) running go mod tidy...)
	$(GO) mod tidy

.PHONY: run
run: ##	Execute the program locally
	$(info $(M) running app...)
	CONFIGMAP_PATH=./config_test/configmap.local.yml SECRET_PATH=./config_test/secrets.local.yml $(GO) run ./cmd/main.go

dev-build: ##	Build robin binary.
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -o bin/robin ./cmd/main.go

docker-build: ##	Build docker image with the manager (uses `${IMG}` image name).
	docker build -t ${IMG} --build-arg REDIS_CLIENT_VERSION=${REDIS_CLIENT_VERSION} --build-arg GOLANG_VERSION=${GOLANG_VERSION} .

docker-push: ##	Push docker image with the manager (uses `${IMG}` image name).
	docker push ${IMG}

dev-docker-build:  ##	Build docker image with the manager for development (uses `${IMG_DEV}` image name).
	docker build -t ${IMG_DEV} --build-arg REDIS_CLIENT_VERSION=${REDIS_CLIENT_VERSION} --build-arg GOLANG_VERSION=${GOLANG_VERSION} .

dev-docker-push: ##	Push docker image with the manager for development (uses `${IMG_DEV}` image name).
	docker push ${IMG_DEV}

debug-docker-build: ##	Build docker image for debugging from debug.Dockerfile (uses `${IMG_DEBUG}` image name).
	docker build -t ${IMG_DEBUG} -f debug.Dockerfile --build-arg REDIS_CLIENT_VERSION=${REDIS_CLIENT_VERSION} --build-arg GOLANG_VERSION=${GOLANG_VERSION} --build-arg DELVE_VERSION=${DELVE_VERSION} .

debug-docker-push: ##	Push docker image for debugging from debug.Dockerfile (uses `${IMG_DEBUG}` image name).
	docker push ${IMG_DEBUG}


##@ Deployment
REDKEY_ROBIN=$(shell kubectl -n ${NAMESPACE} get po -l='redis.redkeycluster.operator/component=robin' -o=jsonpath='{.items[0].metadata.name}')
dev-deploy: ##		Build a new robin binary, copy the file to the webhook pod and run it.
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -gcflags="all=-N -l" -C robin -o ../bin/robin  main.go
	kubectl wait -n ${NAMESPACE} --for=condition=ready pod -l redis.redkeycluster.operator/component=robin
	kubectl cp ./bin/robin $(REDKEY_ROBIN):/robin -n ${NAMESPACE}
	kubectl exec -it po/$(REDKEY_ROBIN) -n ${NAMESPACE} exec /robin

debug: ##		Build a new robin binary, copy the file to the pod and run it in debug mode (listening on port 40000 for Delve connections).
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -gcflags="all=-N -l" -o bin/robin  ./cmd/main.go
	kubectl wait -n ${NAMESPACE} --for=condition=ready pod -l redis.redkeycluster.operator/component=robin
	kubectl cp ./bin/robin $(REDKEY_ROBIN):/robin -n ${NAMESPACE}
	kubectl exec -it po/$(REDKEY_ROBIN) -n ${NAMESPACE} -- dlv --listen=:40000 --headless=true --api-version=2 --accept-multiclient exec /robin --continue

port-forward: ##		Port forwarding of port 40000 to port 40002 for debugging robin with Delve.
	kubectl port-forward pod/$(REDKEY_ROBIN) 40002:40000 -n ${NAMESPACE}

port-forward-metrics: ##		Port forwarding of port 8080 for debugging the manager with Delve.
	kubectl port-forward pod/$(REDKEY_ROBIN) 8080:8080 -n ${NAMESPACE}

.PHONY: test-sonar
test-sonar: ## Execute the application test for Sonar (coverage + test report)
	$(info $(M) running tests and generating sonar report...)
	$(eval TEST_COVERAGE_PROFILE_OUTPUT_DIRNAME=$(shell dirname $(TEST_COVERAGE_PROFILE_OUTPUT)))
	$(eval TEST_REPORT_OUTPUT_DIRNAME=$(shell dirname $(TEST_REPORT_OUTPUT)))
	mkdir -p $(TEST_COVERAGE_PROFILE_OUTPUT_DIRNAME) $(TEST_REPORT_OUTPUT_DIRNAME)
	$(GO) test ./internal/*/ -coverprofile=$(TEST_COVERAGE_PROFILE_OUTPUT) -json > $(TEST_REPORT_OUTPUT)

.PHONY: test-cov
test-cov: ## Execute the application test with coverage
	$(info $(M) running tests and generating coverage report...)
	$(eval TEST_REPORT_OUTPUT_DIRNAME=$(shell dirname $(TEST_REPORT_OUTPUT)))
	mkdir -p $(TEST_REPORT_OUTPUT_DIRNAME)
	$(GO) test ./internal/*/ -coverprofile=$(TEST_COVERAGE_PROFILE_OUTPUT) -covermode=count

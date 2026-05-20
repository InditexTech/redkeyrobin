<!--
SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)

SPDX-License-Identifier: Apache-2.0
-->

# AGENTS.md

Reference guide for AI agents and automated tools working on the Redkey Robin codebase.

---

## Project Summary

**Redkey Robin** is the companion runtime used by Redkey Operator to process configuration rollouts for a single `RedkeyCluster`.

It runs as a standalone Go binary that:

- connects to the Kubernetes API,
- watches the `RedkeyClusterConfig` custom resources produced by Redkey Operator,
- selects the next actionable config revision for one cluster,
- advances config lifecycle state sequentially through the reconciliation loop, and
- exposes Prometheus metrics for the process.

Today, Robin focuses on orchestration of `RedkeyClusterConfig` progression rather than cluster deployment. It operates per cluster instance, using `--cluster-name` and `--namespace` to scope its work.

### Technology Stack

| Layer | Technology |
| ----- | ---------- |
| Language | Go 1.26.3 |
| Runtime style | Standalone controller-style daemon |
| Kubernetes client | [controller-runtime](https://sigs.k8s.io/controller-runtime) v0.24.0 |
| API dependency | `github.com/inditextech/redkeyoperator/api/v1beta1` via local `replace ../redkeyoperator` |
| Metrics | [Prometheus client_golang](https://github.com/prometheus/client_golang) |
| Testing | Go `testing`, [Ginkgo v2](https://github.com/onsi/ginkgo) + [Gomega](https://github.com/onsi/gomega), [envtest](https://sigs.k8s.io/controller-runtime/tools/setup-envtest) |
| Linting | [golangci-lint](https://github.com/golangci/golangci-lint) v2.1.0 |

---

## Build Commands

All common operations are driven by `make`. Tools that are not yet present are downloaded automatically into `bin/`.

### Prerequisites (installed manually)

- Go 1.26+
- Make
- Docker or Podman (only needed for image build targets)
- Access to the sibling `../redkeyoperator` checkout, because this module imports its API types through a local `replace`

### Dependency and formatting tasks

```shell
make tidy        # go mod tidy
make fmt         # go fmt ./...
make vet         # go vet ./...
make lint        # golangci-lint run
make lint-fix    # golangci-lint run --fix
make lint-config # validates golangci-lint configuration
```

### Binary build

```shell
make build       # go build -o bin/robin cmd/main.go
```

### Local execution

```shell
make run                                 # runs Robin locally
make run CLUSTER_NAME=mycluster NAMESPACE=mynamespace
```

Robin requires `--cluster-name` and `--namespace`. It also exposes `--metrics-bind-address`, `--reconcile-interval`, and `--reconcile-interval-on-error`.

### Container image

```shell
make docker-build              # builds image tagged as localhost:5005/redkey-robin:<VERSION>
make docker-push               # pushes the image
make docker-buildx             # cross-platform build (linux/amd64 + linux/arm64) and push
```

Override the image tag with `IMG=<registry>/<name>:<tag>`.

### Full CI verification

```shell
make verify      # tidy + fmt + vet + build + test
```

---

## Testing Instructions

### Mandatory validation for every change

For every change in this repository, before considering the task complete, run:

```shell
make lint
make test-all
```

This requirement is mandatory even if the change is small.

### Unit tests

```shell
make test
```

Runs unit tests for all packages except `e2e` and `test/integration`. Generates a coverage profile at `cover.out`.

```shell
make coverage    # opens HTML coverage report from cover.out
```

### Integration tests (envtest)

Requires the envtest binaries to be present. The target installs them automatically.

```shell
make test-integration
```

Integration tests use CRDs from the sibling operator repository at `../redkeyoperator/config/crd/bases` and run against envtest, not a real cluster.

### All tests (unit + integration)

```shell
make test-all
```

---

## Style Guidelines

### Code conventions

- Follow standard Go idioms and the [Effective Go](https://go.dev/doc/effective_go) guidelines.
- For every change, run `make lint` and `make test-all` before finishing the task.
- Runtime entrypoint lives in `cmd/main.go`.
- Core reconciliation logic lives in `internal/reconciler/`.
- Metrics HTTP server code lives in `internal/metrics/`.
- Tests mirror their subject file with a `_test.go` suffix in the same package for unit coverage.
- Integration tests live under `test/integration/` and depend on envtest plus CRDs from the sibling operator repository.
- Use structured logging via `log/slog`; do not use `fmt.Print*` for operational output.

### Architecture conventions

- A Robin process is scoped to one `RedkeyCluster`.
- The reconciliation loop polls `RedkeyClusterConfig` objects in sequence and should remain idempotent.
- State transitions that can continue synchronously may trigger an immediate re-poll, but waiting states (for example pods becoming Ready or cluster convergence) must use the configured interval; idle and error states use their respective configured intervals.
- Changes to CRD types come from the operator repository, not from Robin directly; keep the local module replacement aligned with the sibling checkout.
- REUSE compliance is required: every source file must have an `SPDX-FileCopyrightText` and `SPDX-License-Identifier` header.

### Dependency management

- Use `go mod tidy` after adding or removing dependencies.
- Do not vendor dependencies; the project relies on the Go module cache.
- Preserve the local `replace github.com/inditextech/redkeyoperator => ../redkeyoperator` unless the repository layout is intentionally changed.

---

## Commit and PR Management

### Commit format

Follow the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification:

```text
<type>(<optional scope>): <short description>

[optional body]

[optional footers]
Signed-off-by: Name <email>
```

Common types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`.

### Required commit properties

Every commit in a PR **must**:

1. Include a `Signed-off-by` trailer (`git commit -s`). This certifies agreement with the [CLA](./CLA.md).
2. Be GPG-signed with a verified key (`git commit -S`, or set `git config --local commit.gpgsign true`).
3. Use a verified email address associated with the GitHub account.

### Pull Request guidelines

- Open an issue before starting significant work and reference it in the PR (`Closes #<issue>`).
- Check existing issues and PRs to avoid duplicate work.
- Keep PRs focused; split unrelated changes into separate PRs.
- For every change, ensure `make lint` and `make test-all` pass locally before opening or updating a PR.
- Add or update tests for every code change.
- Document behaviour changes if they affect operator or runtime workflows.
- An automated check will validate commit signatures and CLA compliance on every PR.

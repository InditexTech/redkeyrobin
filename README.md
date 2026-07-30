<!--
SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL S.A. (INDITEX S.A.)

SPDX-License-Identifier: Apache-2.0
-->

# Redkey Robin

![Redkey logo](docs/images/redkey-logo-256.png)

Redis-side orchestration runtime for Redkey instances on Kubernetes.

Redkey Robin is the runtime component used by the [Redkey Operator](https://github.com/InditexTech/redkeyoperator) to execute Redis-side orchestration, health supervision, and metrics collection. It is deployed by the operator as a `Deployment` and works as the in-cluster companion for each managed Redkey instance, whether it runs in cluster or standalone mode.

[![GitHub License](https://img.shields.io/github/license/InditexTech/redkeyrobin)](LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/InditexTech/redkeyrobin)](https://github.com/InditexTech/redkeyrobin/releases)
[![Go Version](https://img.shields.io/github/go-mod/go-version/InditexTech/redkeyrobin)](go.mod)
[![Build Status](https://img.shields.io/github/actions/workflow/status/InditexTech/redkeyrobin/ci.yml?branch=main)](https://github.com/InditexTech/redkeyrobin/actions)

[![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=flat&logo=kubernetes&logoColor=white)](https://kubernetes.io/)
[![Operator SDK](https://img.shields.io/badge/Operator%20SDK-326CE5?style=flat&logo=kubernetes&logoColor=white)](https://sdk.operatorframework.io/)
[![Go](https://img.shields.io/badge/Go-00ADD8?style=flat&logo=go&logoColor=white)](https://golang.org/)
[![REUSE Compliance](https://img.shields.io/badge/REUSE-compliant-green)](https://reuse.software/)

[![GitHub Issues](https://img.shields.io/github/issues/InditexTech/redkeyrobin)](https://github.com/InditexTech/redkeyrobin/issues)
[![GitHub Pull Requests](https://img.shields.io/github/issues-pr/InditexTech/redkeyrobin)](https://github.com/InditexTech/redkeyrobin/pulls)
[![GitHub Stars](https://img.shields.io/github/stars/InditexTech/redkeyrobin?style=social)](https://github.com/InditexTech/redkeyrobin/stargazers)
[![GitHub Forks](https://img.shields.io/github/forks/InditexTech/redkeyrobin?style=social)](https://github.com/InditexTech/redkeyrobin/network/members)

[📖 Redkey Documentation](https://github.com/InditexTech/redkeyoperator/tree/main/docs) • [🛠 Developer Guide](https://github.com/InditexTech/redkeyoperator/blob/main/docs/developer-guide/development-guide.md) • [🤝 Contributing](./CONTRIBUTING.md) • [📝 License](./LICENSE)

---

## Key Features

- Reconciles bootstrap and configuration from Kubernetes and Redis state, covering cluster formation and topology validation in cluster mode and single-node setup in standalone mode
- Performs automated health checks and remediation, including cluster-mode concerns such as membership drift, uncovered slots, replica spread issues, cluster check findings, and slot imbalance
- Exposes Prometheus metrics for Redkey and Redis/Valkey state, together with runtime metrics used by Redkey Operator and observability tooling
- Hot-reloads operational settings from `RedkeyConfig` without restarting the Robin Pod, including reconciliation cadence, metrics collection, auth secret reference, and profiling state
- Provides a metrics HTTP endpoint plus optional `pprof` endpoints for diagnostics and performance analysis
- Works as the Redis-side runtime companion for Redkey Operator during normal operation, maintenance, and recovery flows

## Getting Started

Redkey Robin is intended to be deployed and configured through [Redkey Operator](https://github.com/InditexTech/redkeyoperator), not as a standalone component.

For development, build, deployment, and debugging workflows, follow the [Redkey Operator developer guide](https://github.com/InditexTech/redkeyoperator/blob/main/docs/developer-guide/development-guide.md) in the operator repository.

## Documentation

The canonical documentation for both Redkey Operator and Redkey Robin lives in the `redkeyoperator` repository:

- [Documentation index](https://github.com/InditexTech/redkeyoperator/blob/main/docs/README.md)
- [Redkey Robin guide](https://github.com/InditexTech/redkeyoperator/blob/main/docs/operator-guide/robin.md)
- [Cluster health checks and remediation](https://github.com/InditexTech/redkeyoperator/blob/main/docs/cluster-health-checks.md)
- [Dynamic configuration and hot reload](https://github.com/InditexTech/redkeyoperator/blob/main/docs/operator-guide/dynamic-configuration.md)
- [Metrics reference](https://github.com/InditexTech/redkeyoperator/blob/main/docs/metrics.md)
- [Observability guide](https://github.com/InditexTech/redkeyoperator/blob/main/docs/observability.md)

## Contributing

We welcome contributions. Please read [CONTRIBUTING.md](./CONTRIBUTING.md) and follow the [Code of Conduct](./CODE_OF_CONDUCT.md).

## License

This project is licensed under the [Apache-2.0 License](./LICENSE).

© 2025 INDUSTRIA DE DISEÑO TEXTIL S.A. (INDITEX S.A.)

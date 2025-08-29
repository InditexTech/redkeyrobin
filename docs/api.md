<!--
SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL S.A. (INDITEX S.A.)

SPDX-License-Identifier: Apache-2.0
-->

# Robin API

This document provides an overview of the Robin API, defining its endpoints and how it is implemented.


## API specification

The following endpoints are provided:

- `/v1/redkeycluster/status`: endpoint to get and update the current RedKey Cluster status from the RedKey Operator persective. The possible statuses will be "Initializing", "Ready", "Error", "Upgrading", "ScalingDown", "ScalingUp", "Maintenance" or "Unknown". Methods provided:
  - `GET`: returns a JSON with the current RedKey Cluster status in Robin. The JSON contains the field `status` (string).
  - `PUT`: updates the RedKey Cluster status. It expects a JSON with the field `status` (string). 
- `/v1/redkeycluster/replicas`: endpoint to get and update the RedKey Cluster replicas. Methods provided:
  - `GET`: returns a JSON with the number of current RedKey Cluster replicas. The JSON contains thes field `replicas` (integer) and `replicas_per_master` (integer).
  - `PUT`: updates the RedKey Cluster replicas. It expects a JSON with the field `replicas` (integer) and, optionally, `replicas_per_master` (integer). Once received, Robin will modify the cluster to have the desired replicas and replicas per master. 
- `/v1/cluster/status`: endpoint to get the current RedKey Cluster status from the Robin persective. The possible statuses will be "Ready", "Error", "Resharding", "Rebalancing", "Fixing" and "CheckingIntegrity", "Resetting" and a error status per each previos status ("MeetingError", "ForgettingError", etc). Methods provided:
  - `GET`: returns a JSON with the current RedKey Cluster status in Robin. The JSON contains the field `status` (string).
- `/v1/cluster/move`: endpoint to ask Robin to move slots between two nodes. It will be called by the RedKey Operator to empty (reshard) a node. Methods provided:
  - `PUT`: request to reshard a node. It expects a JSON with the fields `from` (string) and `to` (string), which are the node indexes between which to move slots (names can also be used), and, optionally, `slots` (integer), with the number of slots to move (if not provided, all slots of from node are moved).
- `/v1/cluster/check`: endpoint to perform a RedKey Cluster check. This is an auxiliar endpoint that can be invoked by the Operator or manually to actively check if RedKey Cluster is healthy. Methods provided:
  - `GET`: performs a `redis-cli --cluster check` over the RedKey Cluster and returns the result. 
- `/v1/cluster/fix`: endpoint to perform a RedKey Cluster fix, which involves doing a integrity check: cluster meet, cluster forget, cluster rebalance, cluster fix and cluster ratio assurance. This is an auxiliar endpoint that can be invoked by the Operator or manually to actively perform the fix operations. Methods provided:
  - `PUT`: performs the fix operations over the RedKey Cluster.
- `/v1/cluster/reset/{nodeIndex}`: endpoint to reset the node `{nodeIndex}`. Methods provided:
  - `PUT`: performs the node reset and returns the result.
- `/v1/cluster/nodes`: endpoint that returns the information that Robin has about the nodes. Methods provided:
  - `GET`: returns a JSON with a list of object, each containing `id` (string), `name` (string), `flags` (string), `slots` (array of objects with `start` (integer) and `end` (integer)), `ip` (string), `masterId` (string), `failures` (integer), `sent` (integer), `recv` (integer) and `linkStatus` (string).
- `/v1/cluster/recreate`: endpoint to force cluster recreations, rediscovering nodes. Methods provided:
  - `PUT`: performs the recreate operation.

This specification can also be observed in `api/openapi-rest.yml` file


### Asynchronous calls

Some operations described above are done asynchronously since they are time consuming. Robin launches a separate goroutine to perform them and the request will be immediately returned. The endpoint `/v1/cluster/status` can be then called to know what Robin is doing specifically. In particular, the asynchronous requests are:

- `PUT` to `/v1/cluster/replicas`: Robin will change the replicas, launch a separate goroutine to perform the associated operations and returns.
- `PUT` to `/v1/cluster/move`: Robin will launch a separate goroutine to do the resharding and returns.
- `PUT` to `/v1/cluster/fix`: Robin will launch a separate goroutine to perform the fix operations and returns.
- `PUT` to `/v1/cluster/recreate`: Robin will launch a separate goroutine to perform the recreate operation and return.


In order to have traceability, know what Robin is doing and not launching the same operation twice, Robin will maintain in its internal state a reference to the running operations. This way, the same operation will only be launched once even if it is invoked more than once. Callers can know this by analyzing the HTTP status code returned by these endpoints:

- 200 OK: the request is already done. This response will depend on the called endpoint:
  - `/v1/cluster/replicas`: the RedKey Cluster has already the requested replicas.
  - `/v1/cluster/move`: the node received at `from` is already empty, `from` node is a replica or `from` node has replicas (and one of them has been promoted to master).
  - `/v1/cluster/fix`: this endpoint will not returns this status code.
  - `/v1/cluster/recreate`: this endpoint will not returns this status code.
- 201 Created: the request triggers the action, that is, a new goroutine to perform the action has been launched.
- 202 Accepted: the requested action is already running, therefore, the request does not trigger a new goroutine. This status code is only used by `/v1/cluster/move`.

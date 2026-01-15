// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/inditextech/redkeyrobin/internal/util"
)

// RedKeyCluster represents a RedKey cluster
type RedKeyCluster struct {
	clusterBase
	nodes      map[string]*redis.RedisNode
	operations map[string][]RedisOperation
	channel    chan struct{}
	mux        sync.RWMutex

	clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
	operationFactory *OperationFactory

	outdatedOperationsRunning bool
}

// MigratingSlot represents a slot being migrated from one node to another
type MigratingSlot struct {
	Slot int
	From string
	To   string
}

// Default client factory for RedKeyCluster (global, no closure)
func defaultRedKeyClusterClientFactory(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
	redisClient := redis.NewRedisClient(ctx, addr, os.Getenv("REDISAUTH"), 0)
	if err := redisClient.CheckConnection(maxRetries, backoff); err != nil {
		return nil, err
	}
	return redisClient, nil
}

// NewRedKeyCluster creates a new RedKey cluster
func NewRedKeyCluster(ctx context.Context, conf *config.Configuration, channel chan struct{}) Cluster {
	return &RedKeyCluster{
		clusterBase: clusterBase{
			ctx:    ctx,
			logger: util.GetLogger("redkey-cluster"),
			conf:   conf,
			status: Unknown,
		},
		nodes:            make(map[string]*redis.RedisNode),
		operations:       make(map[string][]RedisOperation),
		channel:          channel,
		mux:              sync.RWMutex{},
		clientFactory:    defaultRedKeyClusterClientFactory,
		operationFactory: defaultOperationFactory(),
	}
}

// NewFakeRedKeyCluster creates a new fake RedKey cluster
func NewFakeRedKeyCluster(ctx context.Context, conf *config.Configuration, status string, nodes map[string]*redis.RedisNode, operations map[string][]RedisOperation, channel chan struct{}) *RedKeyCluster {
	return &RedKeyCluster{
		clusterBase: clusterBase{
			ctx:    ctx,
			logger: util.GetLogger("redkey-cluster"),
			conf:   conf,
			status: status,
		},
		nodes:            nodes,
		operations:       operations,
		channel:          channel,
		mux:              sync.RWMutex{},
		clientFactory:    defaultRedKeyClusterClientFactory,
		operationFactory: defaultOperationFactory(),
	}
}

// WithClientFactory allows to configure a custom function to obtain clients (used for testing)
func (rc *RedKeyCluster) WithClientFactory(factory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)) *RedKeyCluster {
	rc.clientFactory = factory
	return rc
}

// WithOperationFactory allows to configure a custom operation factory (used for testing)
func (rc *RedKeyCluster) WithOperationFactory(factory *OperationFactory) *RedKeyCluster {
	rc.operationFactory = factory
	return rc
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- GETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// GetNodes returns the Redis nodes in the cluster
func (rc *RedKeyCluster) GetNodes() []*redis.RedisNode {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	nodes := []*redis.RedisNode{}
	for _, node := range rc.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetNode returns the Redis node with the specified name or nil if it doesn't exist
func (rc *RedKeyCluster) GetNode(name string) *redis.RedisNode {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	// If the name is a number, add the cluster name as a prefix
	if _, err := strconv.Atoi(name); err == nil {
		name = fmt.Sprintf("%s-%s", rc.GetName(), name)
	}
	return rc.nodes[name]
}

// GetNodeById returns the Redis node with the specified ID or nil if it doesn't exist
func (rc *RedKeyCluster) GetNodeById(id string) *redis.RedisNode {
	for _, node := range rc.nodes {
		if node.ID == id {
			return node
		}
	}
	return nil
}

// GetNodeFromID returns the Redis node with the specified ID or nil if it doesn't exist
func (rc *RedKeyCluster) GetNodeFromID(nodeID string) *redis.RedisNode {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	for _, node := range rc.nodes {
		if node.ID == nodeID {
			return node
		}
	}
	return nil
}

// GetPrimaryNodes returns the primaries in the RedKey cluster
func (rc *RedKeyCluster) GetPrimaryNodes() []*redis.RedisNode {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	primaries := make([]*redis.RedisNode, 0)
	for _, node := range rc.nodes {
		if node.IsPrimary() {
			primaries = append(primaries, node)
		}
	}
	return primaries
}

// GetReplicaNodes returns the replicas in the RedKey cluster
func (rc *RedKeyCluster) GetReplicaNodes() []*redis.RedisNode {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	replicas := make([]*redis.RedisNode, 0)
	for _, node := range rc.nodes {
		if node.IsReplica() {
			replicas = append(replicas, node)
		}
	}
	return replicas
}

// GetReplicasOfNode returns the replicas of the specified node
func (rc *RedKeyCluster) GetReplicasOfNode(node *redis.RedisNode) []*redis.RedisNode {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	if node.IsReplica() {
		return []*redis.RedisNode{}
	}

	replicas := make([]*redis.RedisNode, 0)
	for _, other := range rc.nodes {
		if other.IsReplica() && other.PrimaryID == node.ID {
			replicas = append(replicas, other)
		}
	}
	return replicas
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- SETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// SetReplicas sets the number of primaries and replicas per primary in the RedKey cluster.
// It returns an OperationAlreadyDoneError if the current number of replicas is equal to the desired number of replicas.
func (rc *RedKeyCluster) SetReplicas(replicas int, replicasPerPrimary *int) error {
	currentReplicas := rc.GetPrimaries()
	currentReplicasPerPrimary := rc.GetReplicasPerPrimary()
	rc.logger.Info("Changing RedKey Cluster replicas", "current", currentReplicas, "desired", replicas)

	if replicasPerPrimary != nil && currentReplicasPerPrimary != *replicasPerPrimary {
		rc.logger.Info("Changing RedKey Cluster replicas per primary", "current", currentReplicasPerPrimary, "desired", *replicasPerPrimary)
		rc.conf.Redis.Cluster.ReplicasPerPrimary = *replicasPerPrimary
	}

	// Check if the current number of replicas is equal to the desired number of replicas
	if replicas == currentReplicas {
		if replicasPerPrimary == nil || *replicasPerPrimary == currentReplicasPerPrimary {
			return &OperationCompletedError{Operation: "SetReplicas"}
		}
	}

	// Set the desired replicas
	rc.conf.Redis.Cluster.Primaries = replicas

	// Write in the channel to trigger the reconciler
	rc.channel <- struct{}{}

	return nil
}

// SetRedKeyClusterStatus sets the status of the RedKey cluster
func (rc *RedKeyCluster) SetRedKeyClusterStatus(status string) error {
	currentStatus := rc.GetRedKeyClusterStatus()

	rc.logger.Info("Changing RedKey Cluster status", "current", currentStatus, "desired", status)
	rc.conf.Redis.Cluster.Status = status

	// Write in the channel to trigger the reconciler when previous status is Unknown
	if currentStatus == Unknown {
		rc.channel <- struct{}{}
	}

	return nil
}

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PUBLIC  ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// Init initializes the RedKey cluster. It should be called after creating a new RedKey cluster.
// It sets the Robin status from the RedKey Cluster status and creates the nodes and initializes them.
func (rc *RedKeyCluster) Init() error {
	// Initialize nodes
	for i := range rc.GetDesiredReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
		nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
		rc.addNode(nodeName, nodeAddr)
	}

	// Refresh nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("Error refreshing nodes info: %v", err)
	}

	// Launch go routine to remove outdated operations
	if !rc.outdatedOperationsRunning {
		go rc.removeOutdatedOperations()
	}

	return nil
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------- PUBLIC OPERATIONS -----------------------------------------
// ----------------------------------------------------------------------------------------------------

// Rebalance rebalances the RedKey cluster.
// It receives the weights of the nodes (map) and the async and force flags (bool).
// It launches the cluster rebalance operation and, depending on the async flag, waits for it to finish synchronously or asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a rebalance.
// It returns an OperationInProgressError if the cluster is already rebalancing, unless the force flag is set, in which case the ongoing rebalance operation is cancelled.
// It returns an OperationCompletedError if the cluster is already rebalanced.
func (rc *RedKeyCluster) Rebalance(async bool, weights map[string]int, force bool) error {
	// Check if there is an ongoing operation that conflicts with a rebalance
	conflict := rc.getConflictingOperation(Rebalancing)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be rebalanced right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "Rebalance", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster is already rebalancing or is already rebalanced
	if rc.IsRebalancing() {
		if force {
			rc.logger.Info("Cancelling ongoing rebalance operation")
			operation := rc.getOperation(Rebalancing, "Running")
			if operation != nil {
				operation.Cancel()
			}
		} else {
			rc.logger.Info("Cluster is already rebalancing")
			return &OperationInProgressError{Operation: "Rebalance"}
		}
	} else if !force && rc.IsBalanced() {
		rc.logger.Info("Cluster is balanced")
		return &OperationCompletedError{Operation: "Rebalance"}
	}

	// Launch cluster rebalance operation
	return rc.launchOperation(rc.operationFactory.NewRebalance(rc.ctx, rc, weights), Rebalancing, async)
}

// MoveSlots moves slots from one Redis node to another.
// It receives the origin node (from), the destination node (to) and the number of slots to move (slots).
// It launches the move operation and waits for it to finish asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a reshard.
// It returns an OperationInProgressError if there is already a reshard operation between the specified nodes.
// It returns an OperationCompletedError if the origin node is a replica, has no slots or has replicas (and a replica of the origin node is promoted).
func (rc *RedKeyCluster) MoveSlots(from, to *redis.RedisNode, slots int) error {
	// Check if there is an ongoing operation that conflicts with a move
	conflict := rc.getConflictingOperation(Resharding)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be resharded right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "Reshard", ConflictingWith: conflict.GetName()}
	}

	// Check if a move operation should be launched
	if rc.IsReshardingNodes(*from, *to) { // There is an ongoing move between the specified nodes
		rc.logger.Info("There is already a reshard operation between nodes", "from", from.Name, "to", to.Name)
		return &OperationInProgressError{Operation: "Reshard"}

	} else if from.IsReplica() { // The origin node is a replica
		rc.logger.Info("Origin node is a replica", "from", from.Name)
		return &OperationCompletedError{Operation: "Reshard", Reason: "Origin node is a replica"}

	} else if !from.HasSlots() { // The origin node has no slots
		rc.logger.Info("Origin node has no slots", "from", from.Name)
		return &OperationCompletedError{Operation: "Reshard", Reason: "Origin node has no slots"}

	} else if rc.NodeHasReplicas(from) { // The origin node has replicas
		rc.logger.Info("Origin node has replicas", "from", from.Name)

		// Promote a replica of the origin node
		if err := rc.promoteReplicaOfNode(rc.ctx, from); err != nil {
			return fmt.Errorf("error promoting replica of node '%s': %v", from.Name, err)
		}
		return &OperationCompletedError{Operation: "Reshard", Reason: "Promoted replica of origin node"}
	}

	// Launch move operation
	return rc.launchOperation(rc.operationFactory.NewMove(rc.ctx, rc, from, to, slots), Resharding, true)
}

// Check checks the RedKey cluster.
// It launches the cluster check operation and waits for it to finish asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a check.
// It returns a ClusterCheckResult with the results of the check.
func (rc *RedKeyCluster) Check() (*redis.ClusterCheckResult, error) {
	// Check if there is an ongoing operation that conflicts with a cluster check
	conflict := rc.getConflictingOperation(CheckCluster)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be checked right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return nil, &OperationConflictError{Operation: "CheckCluster", ConflictingWith: conflict.GetName()}
	}

	// Get Redis client and check connection
	redisClient, err := rc.getRedisClient(true)
	if err != nil {
		return nil, fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Get a timedout context for the cluster check
	checkCtx, cancel := context.WithTimeout(rc.ctx, rc.GetClusterCheckTimeout())
	defer cancel()

	// Launch check operation
	result, err := redisClient.ClusterCheck(checkCtx)
	if err != nil {
		return nil, fmt.Errorf("error checking cluster: %v", err)
	}

	return result, nil
}

// Fix fixes the RedKey cluster.
// It receives the async and force flags (bool).
// It launches the cluster fix operation and, depending on the async flag, waits for it to finish synchronously or asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a fix.
// It returns an OperationInProgressError if the cluster is already fixing, unless the force flag is set, in which case the ongoing fixing operation is cancelled.
func (rc *RedKeyCluster) Fix(async bool, force bool) error {
	// Check if there is an ongoing operation that conflicts with a fix
	conflict := rc.getConflictingOperation(Fixing)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be fixed right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "Fix", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster is already fixing
	if rc.IsFixing() {
		if force {
			rc.logger.Info("Cancelling ongoing fixing operation")
			operation := rc.getOperation(Fixing, "Running")
			if operation != nil {
				operation.Cancel()
			}
		} else {
			rc.logger.Info("Cluster is already fixing")
			return &OperationInProgressError{Operation: "Fix"}
		}
	}

	// Launch cluster fix operation
	return rc.launchOperation(rc.operationFactory.NewFix(rc.ctx, rc), Fixing, async)
}

// CheckIntegrity checks the integrity of the RedKey cluster
// It receives the async and force flags (bool).
// It launches the cluster check integrity operation and, depending on the async flag, waits for it to finish synchronously or asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a check integrity.
// It returns an OperationInProgressError if the cluster is already checking integrity, unless the force flag is set, in which case the ongoing checking integrity operation is cancelled.
func (rc *RedKeyCluster) CheckIntegrity(async, force bool) error {
	// Check if there is an ongoing operation that conflicts with a check integrity
	conflict := rc.getConflictingOperation(CheckingIntegrity)
	if conflict != nil {
		rc.logger.Info("Cluster integrity cannot be checked right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "CheckIntegrity", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster check integrity is being executed
	if rc.IsCheckingIntegrity() {
		if force {
			rc.logger.Info("Cancelling ongoing checking integrity operation")
			operation := rc.getOperation(CheckingIntegrity, "Running")
			if operation != nil {
				operation.Cancel()
			}
		} else {
			rc.logger.Info("Cluster check integrity is already being executed")
			return &OperationInProgressError{Operation: "CheckIntegrity"}
		}
	}

	// Launch cluster check integrity operation
	return rc.launchOperation(rc.operationFactory.NewCheckIntegrity(rc.ctx, rc), CheckingIntegrity, async)
}

// ScaleUp scales up the RedKey cluster
// It receives the force flag (bool).
// It launches the cluster scale up operation and waits for it to finish asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a scale up.
// It returns an OperationInProgressError if the cluster is already scaling up, unless the force flag is set, in which case a new scale up operation is launched.
func (rc *RedKeyCluster) ScaleUp(force bool) error {
	// Check if the cluster can be scaled up
	conflict := rc.getConflictingOperation(ScalingUp)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be scaled up right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "ScaleUp", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster is already scaling up
	if rc.IsScalingUp() && !force {
		rc.logger.Info("Cluster is already scaling up")
		return &OperationInProgressError{Operation: "ScaleUp"}
	}

	// Launch cluster scaling up operation
	return rc.launchOperation(rc.operationFactory.NewScaleUp(rc.ctx, rc), ScalingUp, false)
}

// ScaleDown scales down the RedKey cluster
// It receives the force flag (bool).
// It launches the cluster scale down operation and waits for it to finish asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a scale down.
// It returns an OperationInProgressError if the cluster is already scaling down, unless the force flag is set, in which case a new scale down operation is launched.
func (rc *RedKeyCluster) ScaleDown(force bool) error {
	// Check if there is an ongoing operation that conflicts with a scale down
	conflict := rc.getConflictingOperation(ScalingDown)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be scaled down right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "ScaleDown", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster is already scaling down
	if rc.IsScalingDown() && !force {
		rc.logger.Info("Cluster is already scaling down")
		return &OperationInProgressError{Operation: "ScaleDown"}
	}

	// Launch cluster scaling down operation
	return rc.launchOperation(rc.operationFactory.NewScaleDown(rc.ctx, rc), ScalingDown, false)
}

// Upgrade upgrades the RedKey cluster
// It receives the force flag (bool).
// It launches the cluster upgrade operation and waits for it to finish asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a upgrade.
// It returns an OperationInProgressError if the cluster is already upgrading, unless the force flag is set, in which case a new upgrade operation is launched.
func (rc *RedKeyCluster) Upgrade(force bool) error {
	// Check if there is an ongoing operation that conflicts with an upgrade
	conflict := rc.getConflictingOperation(Upgrading)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be upgraded right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "Upgrade", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster is already upgrading
	if rc.IsUpgrading() && !force {
		rc.logger.Info("Cluster is already upgrading")
		return &OperationInProgressError{Operation: "Upgrade"}
	}

	// Launch cluster upgrade operation
	return rc.launchOperation(rc.operationFactory.NewUpgrade(rc.ctx, rc), Upgrading, false)
}

// Check reset a Redis node in the cluster
// It receives the node to reset.
// It launches the cluster reset operation and waits for it to finish asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a reset node.
// It returns an OperationInProgressError if the cluster is already resetting the node.
func (rc *RedKeyCluster) ResetNode(node *redis.RedisNode) error {
	// Check if there is an ongoing operation that conflicts with a reset
	conflict := rc.getConflictingOperation(Resetting)
	if conflict != nil {
		rc.logger.Info("Cluster node cannot be reset right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "ResetNode", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster node is already being resetted
	if rc.IsResettingNode(*node) {
		rc.logger.Info("Cluster node is already being resetted", "node", node.Name)
		return &OperationInProgressError{Operation: "ResetNode"}
	}

	// Launch reset node operation
	return rc.launchOperation(rc.operationFactory.NewResetNode(rc.ctx, rc, node), Upgrading, false)
}

// RecreateCluster recreates the RedKey cluster
// It launches the cluster recreate operation and waits for it to finish asynchronously.
// It returns an OperationConflictError if there is already a conflicting operation with a recreate cluster.
// It returns an OperationInProgressError if the cluster is already recreating.
func (rc *RedKeyCluster) RecreateCluster() error {
	// Check if there is an ongoing operation that conflicts with a recreate
	conflict := rc.getConflictingOperation(Recreating)
	if conflict != nil {
		rc.logger.Info("Cluster cannot be recreated right now due to conflicting operation", "operation", conflict.GetName(), "status", conflict.GetStatus())
		return &OperationConflictError{Operation: "RecreateCluster", ConflictingWith: conflict.GetName()}
	}

	// Check if the cluster is already recreating
	if rc.IsRecreating() {
		rc.logger.Info("Cluster is already recreating")
		return &OperationInProgressError{Operation: "RecreateCluster"}
	}

	// Launch cluster recreate operation
	return rc.launchOperation(rc.operationFactory.NewRecreate(rc.ctx, rc), Recreating, true)
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- ASKERS ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// IsStandalone returns true if the RedKey cluster is standalone
func (rc *RedKeyCluster) IsStandalone() bool {
	return false
}

// IsBalanced returns true if the RedKey cluster is balanced
func (rc *RedKeyCluster) IsBalanced() bool {
	primaries := rc.GetPrimaryNodes()
	slotsPerPrimary := int(math.Ceil(float64(RedKeyClusterTotalSlots) / float64(len(primaries))))
	maximumSlots := slotsPerPrimary + (slotsPerPrimary * RedisNodesUnbalancedThreshold / 100)
	minimumSlots := slotsPerPrimary - (slotsPerPrimary * RedisNodesUnbalancedThreshold / 100)

	for _, primary := range primaries {
		primarySlots := primary.GetNumberOfSlots()
		// Cluster is not balanced if any of primary node has no slots
		if primarySlots == 0 {
			rc.logger.Info("Cluster needs rebalance: node has no slots", "node", primary.Name)
			return false
		}

		// Cluster is not balanced if the number of slots is not in the expected range
		if primarySlots < minimumSlots || primarySlots > maximumSlots {
			rc.logger.Info("Cluster needs rebalance: node slots are not in the expected range", "node", primary.Name, "slots", primarySlots, "minimumSlots", minimumSlots, "maximumSlots", maximumSlots)
			return false
		}
	}

	// Cluster is balanced otherwise
	return true
}

// IsRebalancing returns true if the cluster is rebalancing right now
func (rc *RedKeyCluster) IsRebalancing() bool {
	return rc.hasOperation(Rebalancing, "Running")
}

// IsReshardingNodes returns true if there is a reshard operation between the specified nodes
func (rc *RedKeyCluster) IsReshardingNodes(from, to redis.RedisNode) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Running", from, to)
}

// IsResharding returns true if the cluster is resharding right now
func (rc *RedKeyCluster) IsResharding() bool {
	return rc.hasOperation(Resharding, "Running")
}

// IsFixing returns true if the cluster is being fixing right now
func (rc *RedKeyCluster) IsFixing() bool {
	return rc.hasOperation(Fixing, "Running")
}

// IsCheckingIntegrity returns true if we are checkint the integrity of the cluster right now
func (rc *RedKeyCluster) IsCheckingIntegrity() bool {
	return rc.hasOperation(CheckingIntegrity, "Running")
}

// IsScalingUp returns true if the cluster is scaling up right now
func (rc *RedKeyCluster) IsScalingUp() bool {
	return rc.hasOperation(ScalingUp, "Running")
}

// IsScalingDown returns true if the cluster is scaling down right now
func (rc *RedKeyCluster) IsScalingDown() bool {
	return rc.hasOperation(ScalingDown, "Running")
}

// IsUpgrading returns true if the cluster is upgrading right now
func (rc *RedKeyCluster) IsUpgrading() bool {
	return rc.hasOperation(Upgrading, "Running")
}

// IsResettingNode returns true if the cluster nose is being resetted right now
func (rc *RedKeyCluster) IsResettingNode(node redis.RedisNode) bool {
	return rc.hasOperationInNode(Resetting, "Running", node)
}

// IsResetting returns true if the cluster is resetting right now
func (rc *RedKeyCluster) IsResetting() bool {
	return rc.hasOperation(Resetting, "Running")
}

// IsRecreating returns true if the cluster is recreating right now
func (rc *RedKeyCluster) IsRecreating() bool {
	return rc.hasOperation(Recreating, "Running")
}

// IsScaled returns true if the cluster is scaled. That is, if it has the desired number of replicas, has no missing slots and is balanced
func (rc *RedKeyCluster) IsScaled() bool {
	return rc.HasDesiredReplicas() && !rc.HasMissingSlots() && rc.IsBalanced()
}

// IsUpgraded returns true if the cluster is correctly configured for an upgrade. That is, if it has the desired number of replicas and has no missing slots
func (rc *RedKeyCluster) IsUpgraded() bool {
	return rc.HasDesiredReplicas() && !rc.HasMissingSlots()
}

// CanBeUpgraded returns true if there is not a conflicting operation with a cluster upgrade. That is, if the cluster is not rebalancing, resharding, fixing,
// checking integrity, scaling up, scaling down or resetting
func (rc *RedKeyCluster) CanBeUpgraded() bool {
	return !rc.IsRebalancing() && !rc.IsResharding() && !rc.IsFixing() && !rc.IsCheckingIntegrity() && !rc.IsScalingUp() && !rc.IsScalingDown() && !rc.IsResetting()
}

// CanBeChecked returns true if there is not a conflicting operation with a check cluster integrity. That is, if the cluster is not resharding, checking integrity or resetting
func (rc *RedKeyCluster) CanBeChecked() bool {
	return !rc.IsResharding() && !rc.IsCheckingIntegrity() && !rc.IsResetting()
}

// HasMissingSlots returns true if the cluster has missing slots
func (rc *RedKeyCluster) HasMissingSlots() bool {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	slotCount := 0
	for _, node := range rc.GetNodes() {
		slotCount += node.GetNumberOfSlots()
	}
	return slotCount != RedKeyClusterTotalSlots
}

// HasDesiredReplicas returns true if the RedKey cluster has the desired number of replicas
func (rc *RedKeyCluster) HasDesiredReplicas() bool {
	return rc.GetDesiredReplicas() == len(rc.GetNodes()) && rc.GetPrimaries() == len(rc.GetPrimaryNodes()) && rc.GetReplicasPerPrimary()*rc.GetPrimaries() == len(rc.GetReplicaNodes())
}

// HasBeenRebalanced returns true if the cluster has been rebalanced recently
func (rc *RedKeyCluster) HasBeenRebalanced() bool {
	return rc.hasOperation(Rebalancing, "Finished")
}

// HasBeenResharded returns true if there is a reshard operation between the specified nodes that has finished
func (rc *RedKeyCluster) HasBeenResharded(from, to redis.RedisNode) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Finished", from, to)
}

// HasNode returns true if the RedKey cluster has a node with the specified name
func (rc *RedKeyCluster) HasNode(name string) bool {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	_, ok := rc.nodes[name]
	return ok
}

// NodeHasReplicas returns true if the specified node has replicas
func (rc *RedKeyCluster) NodeHasReplicas(node *redis.RedisNode) bool {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	if node.IsReplica() {
		return false
	}

	for _, n := range rc.GetNodes() {
		if n.IsReplica() && n.PrimaryID == node.ID {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PRIVATE ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// addNode creates and adds a new Redis node to the cluster
// It just initializes the node, adds it internally and returns it. The node is not added to the cluster until the cluster is reconciled
func (rc *RedKeyCluster) addNode(name, addr string) *redis.RedisNode {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	node := redis.NewRedisNode(name, addr, rc.GetClusterMaxRetries(), rc.GetClusterBackOff())

	rc.logger.Info("Initializing node", "node", node.Name)
	if err := node.Init(rc.ctx); err != nil {
		rc.logger.Error("Error initializing node", "error", err, "node", node.Name)
	} else {
		rc.logger.Info("Node initialized successfully", "node", node.Name, "ID", node.ID, "IP", node.IP)
	}

	rc.nodes[name] = node
	return node
}

// removeNode removes a Redis node from the cluster
func (rc *RedKeyCluster) removeNode(ctx context.Context, nodeToForget redis.RedisNode) error {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	_, ok := rc.nodes[nodeToForget.Name]
	if !ok {
		return fmt.Errorf("node %s not found", nodeToForget.Name)
	}

	delete(rc.nodes, nodeToForget.Name)
	return nil
}

// forgetNode removes a node from the cluster
func (rc *RedKeyCluster) forgetNode(ctx context.Context, nodeToForget redis.RedisNode) error {
	// Check if the node exists
	if !rc.HasNode(nodeToForget.Name) {
		return fmt.Errorf("node %s not found", nodeToForget.Name)
	}

	// Forget the node from all the other nodes
	for _, node := range rc.nodes {
		if node.Name == nodeToForget.Name {
			continue
		}

		if err := node.ForgetNode(ctx, nodeToForget); err != nil {
			return fmt.Errorf("error forgetting node %s from node %s: %v", node.Name, nodeToForget.Name, err)
		}

		rc.logger.Info("Node forgotten successfully", "node", nodeToForget.Name, "from", node.Name)
	}

	return nil
}

// refreshNodesInfo refreshes the nodes info of the RedKey cluster
func (rc *RedKeyCluster) refreshNodesInfo() error {
	// Get Redis client and check connection
	redisClient, err := rc.getRedisClient(false)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	// Get nodes info
	nodesInfo, err := redisClient.GetNodesInfo()
	if err != nil {
		return err
	}

	// Update nodes info
	rc.updateNodesInfo(nodesInfo)

	return nil
}

// checkNodes checks the nodes of the RedKey cluster
// refreshNodesList indicates whether to refresh the internal nodes list or not
func (rc *RedKeyCluster) checkNodes(refreshNodesList bool) error {
	refresedNodes := make(map[string]*redis.RedisNode)
	for i := range rc.GetDesiredReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)

		// Check if the node exists
		node := rc.GetNode(nodeName)
		if node == nil {
			rc.logger.Info("Node not found (probably been forgotten), creating...", "node", nodeName)
			nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
			nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
			freshNode := redis.NewRedisNode(nodeName, nodeAddr, rc.GetClusterMaxRetries(), rc.GetClusterBackOff())
			if err := freshNode.Init(rc.ctx); err != nil {
				rc.logger.Info("Error initializing node", "error", err, "node", nodeName)
				continue
			}
			rc.nodes[nodeName] = freshNode
		} else {

			// Init a fresh node to check if IP or ID have changed. This can happen if the node has been restarted
			freshNode := redis.NewRedisNode(nodeName, node.Addr, rc.GetClusterMaxRetries(), rc.GetClusterBackOff())
			if err := freshNode.Init(rc.ctx); err != nil {
				rc.logger.Info("Error initializing node", "error", err, "node", nodeName)
				continue
			}

			// Update ID and IP
			if node.ID != freshNode.ID {
				rc.logger.Info("Node ID has changed", "node", nodeName, "oldID", node.ID, "newID", freshNode.ID)
				node.SetID(freshNode.ID)
			}
			if node.IP != freshNode.IP {
				rc.logger.Info("Node IP has changed", "node", nodeName, "oldIP", node.IP, "newIP", freshNode.IP)
				node.SetIP(freshNode.IP)
			}
		}
		refresedNodes[nodeName] = rc.nodes[nodeName]
	}
	if refreshNodesList {
		rc.nodes = refresedNodes
	}

	// Update nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return err
	}

	return nil
}

// ClearnNodes clears the stored nodes creating a new map.
func (rc *RedKeyCluster) clearNodes() error {
	rc.mux.Lock()
	defer rc.mux.Unlock()
	rc.nodes = make(map[string]*redis.RedisNode)
	return nil
}

// updateNodesInfo updates the info of the Redis nodes in the cluster
func (rc *RedKeyCluster) updateNodesInfo(nodesInfo []redis.RedisNode) {
	for _, nodeInfo := range nodesInfo {
		node := rc.GetNodeFromID(nodeInfo.ID)
		if node == nil {
			continue
		}
		node.UpdateInfo(nodeInfo)
	}
}

// needsMeet checks if the RedKey cluster needs to meet nodes
func (rc *RedKeyCluster) needsMeet(ctx context.Context) (bool, error) {
	// Compile a map of all the IPs which should be listed for each node.
	// We are using a map to make it faster, as searching a has table is better than a list
	ipList := map[string]struct{}{}
	for _, node := range rc.nodes {
		ipList[node.IP] = struct{}{}
	}

	// Now for every node, make sure that the nodes it knows about, is the same as the nodes we know about.
	for _, node := range rc.nodes {
		clusterNodes, err := node.GetClusterNodes(ctx)
		if err != nil {
			return false, err
		}

		clusterNodeCount := 0
		for _, clusterNode := range clusterNodes {
			clusterNodeCount++

			// If the IP does not exist in our list,
			// we are probably using an outdated one and should ClusterMeet.
			if _, ok := ipList[clusterNode.IP]; !ok {
				rc.logger.Info("Cluster needs meet: IP of cluster node not found", "clusterNode", clusterNode.String())
				return true, nil
			}
		}
		// Every cluster node should see all the nodes.
		// If a node has forgotten any other node we need to meet nodes.
		if len(rc.nodes) > clusterNodeCount {
			rc.logger.Info("Cluster needs meet: node has less peer nodes than expected", "nodes", len(rc.nodes), "clusterNodeCount", clusterNodeCount, "node", node.String())
			return true, nil
		}
	}
	return false, nil
}

// needsFix checks if the RedKey cluster needs to be fixed
func (rc *RedKeyCluster) needsFix(ctx context.Context) (bool, error) {
	// Get Redis client and check connection
	redisClient, err := rc.getRedisClient(false)
	if err != nil {
		return false, fmt.Errorf("error getting and checking Redis client: %v", err)
	}
	defer redisClient.Close()

	// Get a timedout context for the cluster check
	checkCtx, cancel := context.WithTimeout(ctx, rc.GetClusterCheckTimeout())
	defer cancel()

	// Check if the cluster needs to be fixed
	result, err := redisClient.ClusterCheck(checkCtx)
	if err != nil {
		return false, fmt.Errorf("error checking cluster: %v", err)
	}

	return result.CommandCodeOutput != 0, nil
}

// needsUpscale checks if the RedKey cluster needs to be scaled up
func (rc *RedKeyCluster) needsUpscale() bool {
	return len(rc.GetNodes()) < rc.GetDesiredReplicas()
}

// needsDownscale checks if the RedKey cluster needs to be scaled down
func (rc *RedKeyCluster) needsDownscale() bool {
	return len(rc.GetNodes()) > rc.GetDesiredReplicas()
}

// meetNodesIfNeeded meets the nodes of the RedKey cluster if needed
func (rc *RedKeyCluster) meetNodesIfNeeded(ctx context.Context) error {
	// Check if the cluster needs to meet nodes
	needsMeet, err := rc.needsMeet(ctx)
	if err != nil || !needsMeet {
		return err
	}

	// Meet the nodes
	if err := rc.meetNodes(ctx); err != nil {
		return err
	}

	return nil
}

// assignMissingSlotsIfNeeded assigns missing slots to the RedKey cluster if needed
func (rc *RedKeyCluster) assignMissingSlotsIfNeeded(ctx context.Context) error {
	// Check if the cluster has missing slots
	if !rc.HasMissingSlots() {
		return nil
	}

	rc.logger.Info("Cluster has missing slots")

	// Assign missing slots
	if err := rc.assignMissingSlots(ctx); err != nil {
		return err
	}

	return nil
}

// balanceClusterIfNeeded balances the RedKey cluster if needed
func (rc *RedKeyCluster) balanceClusterIfNeeded(weights map[string]int) error {
	// Check if the cluster is balanced
	if rc.IsBalanced() {
		return nil
	}

	// Balance the cluster
	if err := rc.Rebalance(false, weights, true); err != nil {
		return err
	}

	return nil
}

// fixClusterIfNeeded fixes the RedKey cluster if needed
func (rc *RedKeyCluster) fixClusterIfNeeded(ctx context.Context) error {
	// Check if the cluster needs a fix
	needsFix, err := rc.needsFix(ctx)
	if err != nil {
		return err
	}
	if !needsFix {
		return nil
	}

	rc.logger.Info("Cluster needs fix")

	// Fix the cluster
	if err := rc.Fix(false, true); err != nil {
		return err
	}

	return nil
}

// addNewNodesIfNeeded adds new nodes to the RedKey cluster if needed
func (rc *RedKeyCluster) addNewNodesIfNeeded() error {
	// Check if the cluster needs to be scaled up
	if !rc.needsUpscale() {
		return nil
	}

	// Add new nodes
	currentReplicas := len(rc.nodes)
	desiredReplicas := rc.GetDesiredReplicas()
	for i := currentReplicas; i < desiredReplicas; i++ {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
		nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
		rc.addNode(nodeName, nodeAddr)
	}

	return nil
}

func (rc *RedKeyCluster) removeNodesIfNeeded(ctx context.Context) error {
	// Check if the cluster needs to be scaled down
	if !rc.needsDownscale() {
		return nil
	}

	// Get nodes to remove
	nodesToRemove, err := rc.getNodesToRemove(ctx)
	if err != nil {
		return err
	}

	// Remove the slots from the nodes to remove
	if err := rc.removeSlotsFromNodes(nodesToRemove); err != nil {
		return err
	}

	// Forget and remove nodes
	if err := rc.forgetAndRemoveNodes(ctx, nodesToRemove); err != nil {
		return err
	}

	return nil
}

func (rc *RedKeyCluster) removeSlotsFromNodes(nodes []*redis.RedisNode) error {
	// Get the weights for the rebalance
	weights := map[string]int{}
	for _, node := range nodes {
		// Skip nodes with no slots
		if !node.HasSlots() {
			continue
		}

		weights[node.ID] = 0
	}

	// Skip rebalance if there are no nodes with slots to move
	nodesToReshard := len(weights)
	if nodesToReshard < 1 {
		return nil
	}

	// Launch the rebalance operation as many times as nodes to remove. This is because the rebalance operation usually fails when resharding multiple nodes after resharding a node.
	// We retry the rebalance operation until it succeeds or we reach the maximum number of retries, as long as the error is "ERR Please use SETSLOT only with masters".
	// The maximum number of retries is the number of nodes to remove plus one, because we also retry the rebalance operation after removing the last node just in case.
	for i := range len(weights) + 1 {
		if err := rc.Rebalance(false, weights, true); err != nil {
			// Return error if the maximum number of retries is reached
			if i == nodesToReshard {
				return err
			}

			// Retry rebalance if the error is "ERR Please use SETSLOT only with masters"
			if strings.Contains(err.Error(), "ERR Please use SETSLOT only with masters") {
				rc.logger.Info("Retrying rebalance operation", "attempt", i+1, "maxAttempts", nodesToReshard+1)
				continue
			}

			// Return the error otherwise
			return err
		}

		// Finish loop if rebalance is successful
		break
	}

	// Refresh nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %v", err)
	}
	return nil
}

// forgetAndRemoveNodes forgets and removes nodes from the RedKey cluster
func (rc *RedKeyCluster) forgetAndRemoveNodes(ctx context.Context, nodes []*redis.RedisNode) error {
	for _, node := range nodes {
		// Forget the node
		if err := rc.forgetNode(ctx, *node); err != nil {
			return err
		}

		// Remove the node
		if err := rc.removeNode(ctx, *node); err != nil {
			return err
		}
	}

	// Wait for cluster meet so nodes can agree on configuration
	time.Sleep(rc.GetClusterMeetWaitTime())

	return nil
}

// getNodesToRemove returns the nodes to remove in order of preference
func (rc *RedKeyCluster) getNodesToRemove(ctx context.Context) ([]*redis.RedisNode, error) {
	// Get nodes
	nodes := rc.GetNodes()
	desiredReplicas := int(rc.GetDesiredReplicas())
	if len(nodes) <= desiredReplicas {
		return nil, fmt.Errorf("not enough nodes to remove")
	}

	// Sort nodes by name to remove the ones with the highest ordinal
	sort.Slice(nodes, func(i, j int) bool {
		namePrefix := fmt.Sprintf("%s-", rc.GetName())
		ordinalI, _ := strconv.Atoi(strings.TrimPrefix(nodes[i].Name, namePrefix))
		ordinalJ, _ := strconv.Atoi(strings.TrimPrefix(nodes[j].Name, namePrefix))
		return ordinalI < ordinalJ
	})

	// Assure all nodes to keep that should be primaries are already primaries, converting them to primary if needed
	// The rest nodes might be replicas or primaries, we will handle that later
	if err := rc.convertNodesToPrimary(ctx, nodes[:int(rc.GetPrimaries())]); err != nil {
		return nil, err
	}

	return nodes[desiredReplicas:], nil
}

// meetNodes meets the nodes of the RedKey cluster
func (rc *RedKeyCluster) meetNodes(ctx context.Context) error {
	rc.logger.Info("Meeting nodes")

	if len(rc.nodes) == 0 {
		return fmt.Errorf("there are no nodes in the cluster")
	}

	// Meet all the nodes with each other
	for _, sourceNode := range rc.nodes {
		for _, targetNode := range rc.nodes {
			if sourceNode.Name == targetNode.Name {
				continue
			}

			if err := sourceNode.MeetNode(ctx, *targetNode); err != nil {
				return fmt.Errorf("error in ClusterMeet between '%s' and '%s': %v", sourceNode.Name, targetNode.Name, err)
			}
		}
	}

	// Wait for cluster meet so nodes can agree on configuration
	time.Sleep(rc.GetClusterMeetWaitTime())

	// Refresh nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %v", err)
	}

	rc.logger.Info("Nodes met successfully")
	return nil
}

// removeOutdatedNodes removes nodes that are not part of the cluster
func (rc *RedKeyCluster) removeOutdatedNodes(ctx context.Context) error {
	// Get all cluster nodes and forget the ones that are not part of the cluster
	for _, node := range rc.nodes {
		clusterNodes, err := node.GetClusterNodes(ctx)
		if err != nil {
			return fmt.Errorf("error getting cluster nodes from node %s: %v", node.Name, err)
		}

		for _, clusterNode := range clusterNodes {
			// Skip nodes that should not be removed
			if !clusterNode.ShouldBeRemoved() {
				continue
			}

			// Forget the node
			if err := node.ForgetNode(ctx, clusterNode); err != nil {
				return fmt.Errorf("error forgetting node %s from node %s: %v", clusterNode.ID, node.Name, err)
			}

			rc.logger.Info("Outdated node forgotten successfully", "node", clusterNode.ID, "from", node.Name)
		}
	}

	// Update nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %v", err)
	}

	return nil
}

// ensureClusterRatio ensures that the RedKey cluster has the right ratio of primaries and replicas
func (rc *RedKeyCluster) ensureClusterRatio(ctx context.Context) error {
	// When all the nodes are ready, we need to make sure there is the right ratio of primaries and replicas for the redkey cluster
	// If there are too many replicas, we need to reset and add as a primary
	//
	// If there are too few replicas, we need to reset and add as a
	// replica of a primary with the least amount of replicas attached
	activePrimaries := rc.GetPrimaryNodes()
	activeReplicas := rc.GetReplicaNodes()
	desiredPrimaries := rc.GetPrimaries()
	desiredReplicas := rc.GetReplicasPerPrimary()

	// We have the right amount of primaries and replicas: ensure replica spread
	if len(activePrimaries) == desiredPrimaries {
		return rc.ensureReplicaSpread(ctx)
	}

	// We have more primaries than needed: convert some primaries to replicas
	if len(activePrimaries) > desiredPrimaries {
		rc.logger.Info("Converting primaries to replicas", "primaries", len(activePrimaries), "desiredPrimaries", desiredPrimaries)

		// Sort primaries by name to convert the ones with the highest ordinal
		sort.Slice(activePrimaries, func(i, j int) bool {
			namePrefix := fmt.Sprintf("%s-", rc.GetName())
			ordinalI, _ := strconv.Atoi(strings.TrimPrefix(activePrimaries[i].Name, namePrefix))
			ordinalJ, _ := strconv.Atoi(strings.TrimPrefix(activePrimaries[j].Name, namePrefix))
			return ordinalI < ordinalJ
		})

		keepPrimaries := activePrimaries[:desiredPrimaries]
		deletePrimaries := activePrimaries[desiredPrimaries:]
		if err := rc.convertNodesToReplica(ctx, deletePrimaries, keepPrimaries); err != nil {
			return err
		}
		return rc.ensureReplicaSpread(ctx)
	}

	// We have less primaries than needed and we have replicas: promote some replicas to primaries
	if len(activePrimaries) < desiredPrimaries && desiredReplicas > 0 {
		rc.logger.Info("Promoting replicas to primaries", "primaries", len(activePrimaries), "desiredPrimaries", desiredPrimaries, "replicas", len(activeReplicas), "desiredReplicas", desiredReplicas)
		needsPrimaries := desiredPrimaries - len(activePrimaries)
		convertableReplicas := activeReplicas[:needsPrimaries]
		if err := rc.convertNodesToPrimary(ctx, convertableReplicas); err != nil {
			return err
		}
		return rc.ensureReplicaSpread(ctx)
	}

	// We have replicas but we don't want any: promote all to primaries
	if len(activeReplicas) > 0 && desiredReplicas == 0 {
		rc.logger.Info("Promoting all replicas to primaries", "replicas", len(activeReplicas))
		if err := rc.convertNodesToPrimary(ctx, activeReplicas); err != nil {
			return err
		}
		return rc.ensureReplicaSpread(ctx)
	}

	return nil
}

// ensureReplicaSpread ensures that the number of replicas is spread across the primaries
func (rc *RedKeyCluster) ensureReplicaSpread(ctx context.Context) error {
	var primaryNeedsReplicas []*redis.RedisNode
	var replicaNeedsMove []*redis.RedisNode

	primaries := rc.GetPrimaryNodes()
	replicas := rc.GetReplicaNodes()

	// Find primaries that need replicas
	for _, primary := range primaries {
		replicas := rc.GetReplicasOfNode(primary)
		replicasPerPrimary := rc.GetReplicasPerPrimary()

		if len(replicas) == int(replicasPerPrimary) { // Primary has the right number of replicas
			continue
		} else if len(replicas) < int(replicasPerPrimary) { // Too few replicas
			primaryNeedsReplicas = append(primaryNeedsReplicas, primary)
		} else if len(replicas) > int(replicasPerPrimary) { // Too much replicas
			replicaNeedsMove = append(replicaNeedsMove, replicas[replicasPerPrimary:]...)
		}
	}

	// There might be replicas which are replicating replicas. We want to change these to point at primaries
	for _, replica := range replicas {
		replicasPointedAtReplicas := rc.GetReplicasOfNode(replica)
		replicaNeedsMove = append(replicaNeedsMove, replicasPointedAtReplicas...)
	}

	// There might be replicas which are replicating primaries which we cannot see.
	for _, replica := range replicas {
		// Is the replica pointing at one of the primaries ?
		pointedAtPrimary := false
		for _, primary := range primaries {
			if primary.Name == replica.PrimaryID {
				pointedAtPrimary = true
				// Update the replica's PrimaryID to use the primary's ID instead of name
				replica.PrimaryID = primary.ID

				// Check if this primary now has the right number of replicas and can be removed from primaryNeedsReplicas
				currentReplicas := rc.GetReplicasOfNode(primary)
				replicasPerPrimary := rc.GetReplicasPerPrimary()
				if len(currentReplicas)+1 >= int(replicasPerPrimary) { // +1 because we just assigned this replica
					// Remove this primary from primaryNeedsReplicas
					for i, needsReplicasPrimary := range primaryNeedsReplicas {
						if needsReplicasPrimary.ID == primary.ID {
							primaryNeedsReplicas = append(primaryNeedsReplicas[:i], primaryNeedsReplicas[i+1:]...)
							break
						}
					}
				}
				break
			}
		}
		if !pointedAtPrimary {
			replicaNeedsMove = append(replicaNeedsMove, replica)
		}
	}

	// We have more primaries that need replicas than available replicas
	if len(replicaNeedsMove) < len(primaryNeedsReplicas) {
		return fmt.Errorf("there are not enough replicas to convert. primaries=%d replicas=%d", len(primaryNeedsReplicas), len(replicaNeedsMove))
	}

	// Replicas that need to be converted to primaries
	for i := 0; i < len(primaryNeedsReplicas); i++ {
		rc.logger.Info("Converting node to replica", "node", replicaNeedsMove[i].Name, "primary", primaryNeedsReplicas[i].Name)

		if err := replicaNeedsMove[i].ReplicateNode(ctx, *primaryNeedsReplicas[i]); err != nil {
			return fmt.Errorf("error promoting replica %s to primary %s: %v", replicaNeedsMove[i].Name, primaryNeedsReplicas[i].Name, err)
		}
	}

	// Refresh nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %v", err)
	}

	return nil
}

// convertNodesToReplicas converts the specified nodes to replicas of the specified nodes to keep
func (rc *RedKeyCluster) convertNodesToReplica(ctx context.Context, nodesToConvert []*redis.RedisNode, nodesToKeep []*redis.RedisNode) error {
	// Rebalance cluster removing slots from the primaries we want to delete
	if err := rc.removeSlotsFromNodes(nodesToConvert); err != nil {
		return err
	}

	// Set the nodes to convert as replicas of the nodes to keep
	currentKeepable := 0
	for _, convertable := range nodesToConvert {
		// Skip nodes that are already replicas
		if convertable.IsReplica() {
			continue
		}

		// Replicate the node
		rc.logger.Info("Converting node to replica", "node", convertable.Name, "primary", nodesToKeep[currentKeepable].Name)
		if err := convertable.ReplicateNode(ctx, *nodesToKeep[currentKeepable]); err != nil {
			return err
		}

		currentKeepable = currentKeepable + 1
		if currentKeepable > rc.GetPrimaries()-1 {
			currentKeepable = 0
		}
	}

	// Wait for cluster meet so nodes can agree on configuration
	time.Sleep(rc.GetClusterMeetWaitTime())

	// Refresh nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %v", err)
	}

	return nil
}

// convertNodesToPrimary promotes the specified nodes to primaries
func (rc *RedKeyCluster) convertNodesToPrimary(ctx context.Context, nodes []*redis.RedisNode) error {
	// Reset nodes that will be promoted to primaries
	for _, node := range nodes {
		// Skip nodes that are already primaries
		if node.IsPrimary() {
			continue
		}

		// Reset the node
		rc.logger.Info("Converting node to primary", "node", node.Name)
		if err := node.Reset(ctx); err != nil {
			return err
		}
		time.Sleep(rc.GetNodeResetWaitTime())
	}

	// Meet the cluster to promote the nodes to primaries
	if err := rc.meetNodes(ctx); err != nil {
		return fmt.Errorf("error meeting nodes: %v", err)
	}

	return nil
}

// promoteReplicaOfNode promotes the first replica of the specified node to primary
func (rc *RedKeyCluster) promoteReplicaOfNode(ctx context.Context, node *redis.RedisNode) error {
	// Check if the node is a primary
	if !node.IsPrimary() {
		return fmt.Errorf("node %s is not a primary", node.Name)
	}

	// Get the current replicas
	replicas := rc.GetReplicasOfNode(node)
	if len(replicas) == 0 {
		return fmt.Errorf("node %s has no replicas to promote", node.Name)
	}

	// Promote the first replica
	rc.logger.Info("Promoting replica to primary", "replica", replicas[0].Name, "primary", node.Name)
	if err := replicas[0].Failover(ctx); err != nil {
		return fmt.Errorf("error promoting replica %s to primary %s: %v", replicas[0].Name, node.Name, err)
	}

	// Refresh nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %v", err)
	}

	return nil
}

// ensureNodesAreUp checks if the nodes received are up and running
func (rc *RedKeyCluster) ensureNodesAreUp(ctx context.Context) error {
	for _, node := range rc.GetNodes() {
		if err := node.CheckConnection(ctx); err != nil {
			return err
		}
	}
	return nil
}

// assignMissingSlots assigns missing slots to the RedKey cluster
func (rc *RedKeyCluster) assignMissingSlots(ctx context.Context) error {
	rc.logger.Info("Assigning missing slots")

	// Get the primary nodes
	primaries := rc.GetPrimaryNodes()

	// We start with a map so we can easily delete slots if they are already assigned
	allSlots := util.MakeRangeMap(0, 16383)
	for _, node := range primaries {
		for _, slotRange := range node.Slots {
			// We want to remove any assigned slots from our list so we end up with unassigned slots
			for slot := slotRange.Start; slot <= slotRange.End; slot++ {
				delete(allSlots, slot)
			}
		}
	}
	totalPrimaryNodes := len(primaries)
	slotsPerNode := calculateMaxSlotsPerPrimary(RedKeyClusterTotalSlots, totalPrimaryNodes)

	// We convert the slots left over in the map to an array so we can easily sort them,
	// and cut off slices
	var slotsToAssign []int
	for slot := range allSlots {
		slotsToAssign = append(slotsToAssign, slot)
	}
	sort.Ints(slotsToAssign)

	for i, node := range primaries {
		slotAmountToAssign := slotsPerNode - node.GetNumberOfSlots()
		if slotAmountToAssign <= 0 {
			continue
		}
		// If we reach the last primary node with a number of not assigned slots greater than slotsPerNodes
		// (mainly when configuring a new redkey cluster) the exceeding slots will be assigned to this node.
		var slotList []int
		if i == totalPrimaryNodes-1 || len(slotsToAssign) <= slotAmountToAssign {
			slotList = slotsToAssign[0:]
			slotsToAssign = []int{}
		} else {
			slotList = slotsToAssign[0:slotAmountToAssign]
			slotsToAssign = slotsToAssign[slotAmountToAssign:]
		}

		// Only asign slots if the slotList is not empty
		if len(slotList) == 0 {
			continue
		}

		// Assign the slots to the node
		if err := node.AddSlots(ctx, slotList...); err != nil {
			return err
		}
	}

	// Wait for cluster meet so nodes can agree on configuration
	time.Sleep(rc.GetClusterMeetWaitTime())

	// Refresh nodes info
	if err := rc.refreshNodesInfo(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %v", err)
	}

	rc.logger.Info("Missing slots assigned successfully")
	return nil
}

// getClient returns a Redis client using the configured client factory
func (rc *RedKeyCluster) getClient() (redis.RedisClientInterface, error) {

	if len(rc.nodes) > 0 {

		// Prefer the canonical node with ordinal 0 ("<clusterName>-0") when available and
		// it has an Addr set.
		if n, ok := rc.nodes[rc.GetAddress()+"-0"]; ok && n != nil && n.Addr != "" {
			return rc.clientFactory(rc.ctx, n.Addr, rc.GetClusterMaxRetries(), rc.GetClusterBackOff())
		}

		// Otherwise try to use any node that has an Addr set.
		for _, n := range rc.nodes {
			if n != nil && n.Addr != "" {
				return rc.clientFactory(rc.ctx, n.Addr, rc.GetClusterMaxRetries(), rc.GetClusterBackOff())
			}
		}
	}

	// If we don't have any nodes stored, fall back to using the cluster address so
	// client factories that expect an address string still receive a value and can
	// return the expected errors.
	return rc.clientFactory(rc.ctx, rc.GetAddress(), rc.GetClusterMaxRetries(), rc.GetClusterBackOff())
}

// getRedisClient creates a Redis client
func (rc *RedKeyCluster) getRedisClient(close bool) (redis.RedisClientInterface, error) {
	// Create Redis client using the client factory
	redisClient, err := rc.getClient()
	if err != nil {
		return nil, err
	}

	if close {
		defer redisClient.Close()
	}

	return redisClient, nil
}

// launchOperation launches a Redis operation, adds it to the cluster operations map and waits for it to finish synchronously or asynchronously depending on the async flag.
func (rc *RedKeyCluster) launchOperation(operation RedisOperation, name string, async bool) error {
	// Launch the operation
	err := operation.Launch()
	if err != nil {
		return err
	}
	rc.addOperation(name, operation)

	// Wait for operation to finish synchronously or asynchronously depending on the async flag
	if async {
		go operation.Wait()
	} else {
		return operation.Wait()
	}
	return nil
}

// hasOperation returns true if the cluster has an operation with the specified name and status
func (rc *RedKeyCluster) hasOperation(name string, status string) bool {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.GetStatus() == status {
			return true
		}
	}
	return false
}

// hasOperationInNode returns true if the cluster has an operation with the specified name and status in the specified node
func (rc *RedKeyCluster) hasOperationInNode(name string, status string, node redis.RedisNode) bool {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.GetStatus() != status {
			continue
		}

		if operation.GetNodeFrom() != nil && operation.GetNodeFrom().Name == node.Name {
			return true
		}
	}
	return false
}

// hasOperationBetweenNodes returns true if the cluster has an operation with the specified name and status between the specified nodes
func (rc *RedKeyCluster) hasOperationBetweenNodes(name string, status string, from redis.RedisNode, to redis.RedisNode) bool {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.GetStatus() != status {
			continue
		}

		if operation.GetNodeFrom() == nil || operation.GetNodeTo() == nil {
			continue
		}

		if operation.GetNodeFrom().Name == from.Name && operation.GetNodeTo().Name == to.Name {
			return true
		}
	}
	return false
}

// addOperation adds a new operation to the cluster operations map
func (rc *RedKeyCluster) addOperation(operationName string, operation RedisOperation) {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	if rc.operations[operationName] == nil {
		rc.operations[operationName] = make([]RedisOperation, 0)
	}

	rc.operations[operationName] = append(rc.operations[operationName], operation)
}

// getOperation returns the operation if the cluster has an operation with the specified name and status or nil otherwise
func (rc *RedKeyCluster) getOperation(name string, status string) RedisOperation {
	rc.mux.RLock()
	defer rc.mux.RUnlock()

	operations, ok := rc.operations[name]
	if !ok {
		return nil
	}

	for _, operation := range operations {
		if operation.GetStatus() == status {
			return operation
		}
	}
	return nil
}

// removeOutdatedOperations removes outdated operations from the cluster
func (rc *RedKeyCluster) removeOutdatedOperations() {
	rc.outdatedOperationsRunning = true
	timeout := time.Duration(rc.GetReconcilerInterval()) * time.Second

	for {
		select {
		case <-rc.ctx.Done():
			rc.logger.Info("Context cancelled, stopping remove outdated operations")
			return
		case <-time.After(timeout):
			rc.doRemoveOutdatedNodes()
		}
	}
}

// doRemoveOutdatedNodes removes outdated nodes from the RedKey cluster
func (rc *RedKeyCluster) doRemoveOutdatedNodes() {
	cleanupThreshold := time.Duration(rc.GetReconcilerOperationCleanupInterval()) * time.Second

	for name, operations := range rc.operations {
		for i := len(operations) - 1; i >= 0; i-- {
			if operations[i].GetElapsedTimeFromEnd() > cleanupThreshold {
				rc.operations[name] = append(rc.operations[name][:i], rc.operations[name][i+1:]...)
			}
		}
	}
}

// getConflictingOperation returns an operation that conflicts with the specified operation name, or nil if there is no conflicting operation
// A conflicting operation is one that cannot run at the same time as the specified operation, thus the specified operation must wait for the conflicting operation to finish
// It does not include operations of the same type, which are handled separately.
func (rc *RedKeyCluster) getConflictingOperation(operationName string) RedisOperation {
	conflictMatrix := map[string][]string{
		// ATOMIC OPERATIONS: operations that does one single change to the cluster
		// Rebalancing conflicts with resharding and resetting.
		// It does not conflict with checking cluster, scaling up, scaling down, upgrade and recreate because those operations may include rebalancing as part of their process.
		Rebalancing: {Resharding, Resetting},
		// Fixing does not conflict with any other operation: there is not problem running it at the same time as other operations.
		// It does not conflict with checking cluster, scaling up, scaling down, upgrade and recreate because those operations may include fixing as part of their process.
		Fixing: {},
		// Resharding conflicts with rebalancing, checking integrity, scaling up, scaling down, resetting and recreating.
		// This operation can only be launched manually (using the API).
		Resharding: {Rebalancing, CheckingIntegrity, ScalingUp, ScalingDown, Resetting, Recreating},
		// CheckCluster conflicts with all other operations: launching at the same time can cause inconsistent results.
		// This operation can only be launched manually (using the API).
		CheckCluster: {Rebalancing, Resharding, CheckingIntegrity, ScalingUp, ScalingDown, Resetting, Upgrading, Recreating},

		// NON-ATOMIC OPERATIONS: operations that may do multiple changes to the cluster
		// CheckingIntegrity conflicts with the rest non-atomic operations.
		CheckingIntegrity: {ScalingUp, ScalingDown, Resetting, Upgrading, Recreating},
		// ScalingUp conflicts with the rest non-atomic operations.
		ScalingUp: {CheckingIntegrity, ScalingDown, Upgrading, Resetting, Recreating},
		// ScalingDown conflicts with the rest non-atomic operations.
		ScalingDown: {CheckingIntegrity, ScalingUp, Upgrading, Resetting, Recreating},
		// Upgrading conflicts with the rest non-atomic operations.
		Upgrading: {CheckingIntegrity, ScalingUp, ScalingDown, Resetting, Recreating},
		// Resetting conflicts with the rest non-atomic operations.
		Resetting: {CheckingIntegrity, ScalingUp, ScalingDown, Upgrading, Recreating},
		// Recreating conflicts with the rest non-atomic operations.
		Recreating: {CheckingIntegrity, ScalingUp, ScalingDown, Upgrading, Resetting},
	}

	if _, ok := conflictMatrix[operationName]; !ok {
		rc.logger.Error("Operation not found in GetConflictOperation", "operation", operationName)
		return nil
	}

	for _, conflictingOperationName := range conflictMatrix[operationName] {
		operation := rc.getOperation(conflictingOperationName, "Running")
		if operation != nil {
			return operation
		}
	}
	return nil
}

// stabilizeOpenSlots sets open slots as stable if the check counter threshold is reached
func (rc *RedKeyCluster) stabilizeOpenSlots(ctx context.Context, counter map[int]int, threshold int) (map[int]int, error) {
	var slotsToStabilize []MigratingSlot
	updatedCounter := make(map[int]int)
	for _, node := range rc.nodes {
		clusterNodes, err := node.GetClusterNodes(ctx)
		if err != nil {
			return nil, fmt.Errorf("error getting cluster nodes from node %s: %v", node.Name, err)
		}

		for _, clusterNode := range clusterNodes {
			if clusterNode.ID == node.ID && len(clusterNode.Migrating) > 0 {
				for slot := range clusterNode.Migrating {
					if count, ok := counter[slot]; ok {
						if count+1 > threshold {
							slotsToStabilize = append(slotsToStabilize, MigratingSlot{Slot: slot, From: clusterNode.ID, To: clusterNode.Migrating[slot]})
						} else {
							updatedCounter[slot] = count + 1
						}
					} else {
						updatedCounter[slot] = 1
					}
				}
			}
		}
	}

	if len(slotsToStabilize) > 0 {
		for _, slot := range slotsToStabilize {
			rc.logger.Info("Slot needs to be stabilized", "slot", slot.Slot, "from", slot.From, "to", slot.To, "checks", threshold)
			if fromNode := rc.GetNodeById(slot.From); fromNode != nil {
				if err := fromNode.StabilizeSlot(ctx, fromNode.IP, slot.Slot); err != nil {
					rc.logger.Error("Error stabilizing slot", "slot", slot.Slot, "from", slot.From, "error", err)
				}
				rc.logger.Info("Slot stabilized on from node", "slot", slot.Slot, "from", slot.From)
			}
			if toNode := rc.GetNodeById(slot.To); toNode != nil {
				if err := toNode.StabilizeSlot(ctx, toNode.IP, slot.Slot); err != nil {
					rc.logger.Error("Error stabilizing slot", "slot", slot.Slot, "to", slot.To, "error", err)
				}
				rc.logger.Info("Slot stabilized on to node", "slot", slot.Slot, "to", slot.To)
			}
		}
	}

	return updatedCounter, nil
}

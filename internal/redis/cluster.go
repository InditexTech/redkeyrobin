// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/util"
)

const (
	Initializing                  = "Initializing"
	Ready                         = "Ready"
	Error                         = "Error"
	Upgrading                     = "Upgrading"
	UpgradingError                = "UpgradingError"
	ScalingDown                   = "ScalingDown"
	ScalingDownError              = "ScalingDownError"
	ScalingUp                     = "ScalingUp"
	ScalingUpError                = "ScalingUpError"
	Maintenance                   = "Maintenance"
	Unknown                       = "Unknown"
	Resharding                    = "Resharding"
	ReshardingError               = "ReshardingError"
	Rebalancing                   = "Rebalancing"
	RebalancingError              = "RebalancingError"
	Fixing                        = "Fixing"
	FixingError                   = "FixingError"
	CheckingIntegrity             = "CheckingIntegrity"
	CheckingIntegrityError        = "CheckingIntegrityError"
	Resetting                     = "Resetting"
	ResettingError                = "ResettingError"
	RedisClusterTotalSlots        = 16384
	RedisNodesUnbalancedThreshold = 2
)

// RedisCluster represents a Redis cluster
type RedisCluster struct {
	ctx        context.Context
	logger     *slog.Logger
	conf       *config.Configuration
	status     string
	nodes      map[string]*RedisNode
	operations map[string][]RedisOperation
	channel    chan struct{}
}

// NewRedisCluster creates a new Redis cluster
func NewRedisCluster(ctx context.Context, conf *config.Configuration, channel chan struct{}) *RedisCluster {
	return &RedisCluster{
		ctx:        ctx,
		logger:     util.GetLogger("redis-cluster"),
		conf:       conf,
		status:     Unknown,
		nodes:      make(map[string]*RedisNode),
		operations: make(map[string][]RedisOperation),
		channel:    channel,
	}
}

func NewFakeRedisCluster(ctx context.Context, conf *config.Configuration, status string, nodes map[string]*RedisNode, operations map[string][]RedisOperation, channel chan struct{}) *RedisCluster {
	return &RedisCluster{
		ctx:        ctx,
		logger:     util.GetLogger("redis-cluster"),
		conf:       conf,
		status:     status,
		nodes:      nodes,
		operations: operations,
		channel:    channel,
	}
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- GETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// GetRedisClusterStatus returns the status of the Redis cluster from Operator perspective
func (rc *RedisCluster) GetRedisClusterStatus() string {
	return rc.conf.Redis.Cluster.Status
}

// GetStatus returns the status of the Redis cluster from Robin perspective
func (rc *RedisCluster) GetStatus() string {
	return rc.status
}

// GetReplicas returns the number of replicas in the Redis cluster
func (rc *RedisCluster) GetReplicas() int {
	return rc.conf.Redis.Cluster.Replicas
}

// GetReplicasPerMaster returns the number of replicas per master in the Redis cluster
func (rc *RedisCluster) GetReplicasPerMaster() int {
	return rc.conf.Redis.Cluster.ReplicasPerMaster
}

// GetDesiredReplicas returns the number of nodes needed to reach the desired number of replicas
func (rc *RedisCluster) GetDesiredReplicas() int {
	return rc.GetReplicas() + (rc.GetReplicas() * rc.GetReplicasPerMaster())
}

// GetName returns the name of the Redis cluster
func (rc *RedisCluster) GetName() string {
	return rc.conf.Redis.Cluster.Name
}

// GetNamespace returns the namespace of the Redis cluster
func (rc *RedisCluster) GetNamespace() string {
	return rc.conf.Redis.Cluster.Namespace
}

// GetAddress returns the address of the Redis cluster
func (rc *RedisCluster) GetAddress() string {
	return rc.conf.Redis.Cluster.Name
}

// IsEphemeral returns true if the Redis cluster is ephemeral
func (rc *RedisCluster) IsEphemeral() bool {
	return rc.conf.Redis.Cluster.Ephemeral
}

// GetReconcilerInterval returns the interval of the Redis cluster reconciler
func (rc *RedisCluster) GetReconcilerInterval() int {
	return rc.conf.Redis.Reconciler.IntervalSeconds
}

// GetReconcilerOperationCleanupInterval returns the interval for cleaning up old operations
func (rc *RedisCluster) GetReconcilerOperationCleanupInterval() int {
	return rc.conf.Redis.Reconciler.OperationCleanupIntervalSeconds
}

// GetClusterMaxRetries returns the maximum number of retries for a Redis cluster check connection operation
func (rc *RedisCluster) GetClusterMaxRetries() int {
	return rc.conf.Redis.Cluster.MaxRetries
}

// GetClusterBackOff returns the backoff duration for a Redis cluster check connection operation
func (rc *RedisCluster) GetClusterBackOff() time.Duration {
	return rc.conf.Redis.Cluster.BackOff
}

// GetClusterHealingTime returns the healing time for a Redis cluster
func (rc *RedisCluster) GetClusterHealingTime() int {
	return rc.conf.Redis.Cluster.HealingTimeSeconds
}

// GetClusterHealthProbePeriod returns the health probe period for a Redis cluster
func (rc *RedisCluster) GetClusterHealthProbePeriod() int {
	return rc.conf.Redis.Cluster.HealthProbePeriodSeconds
}

// GetMetricsRedisInfoKeys returns the Redis info keys to be collected
func (rc *RedisCluster) GetMetricsRedisInfoKeys() []string {
	return rc.conf.Redis.Metrics.RedisInfoKeys
}

// GetMetricsInterval returns the interval for collecting Redis metrics
func (rc *RedisCluster) GetMetricsInterval() int {
	return rc.conf.Redis.Metrics.IntervalSeconds
}

// GetNodes returns the Redis nodes in the cluster
func (rc *RedisCluster) GetNodes() []*RedisNode {
	nodes := []*RedisNode{}
	for _, node := range rc.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetMetadata returns the metadata of the Redis cluster
func (rc *RedisCluster) GetMetadata() map[string]string {
	return rc.conf.Metadata
}

// GetNode returns the Redis node with the specified name or nil if it doesn't exist
func (rc *RedisCluster) GetNode(name string) *RedisNode {
	// If the name is a number, add the cluster name as a prefix
	if _, err := strconv.Atoi(name); err == nil {
		name = fmt.Sprintf("%s-%s", rc.GetName(), name)
	}
	return rc.nodes[name]
}

// GetNodeFromID returns the Redis node with the specified ID or nil if it doesn't exist
func (rc *RedisCluster) GetNodeFromID(nodeID string) *RedisNode {
	for _, node := range rc.nodes {
		if node.ID == nodeID {
			return node
		}
	}
	return nil
}

// GetMasterNodes returns the masters in the Redis cluster
func (rc *RedisCluster) GetMasterNodes() []*RedisNode {
	masters := make([]*RedisNode, 0)
	for _, node := range rc.nodes {
		if node.IsMaster() {
			masters = append(masters, node)
		}
	}
	return masters
}

// GetReplicaNodes returns the replicas in the Redis cluster
func (rc *RedisCluster) GetReplicaNodes() []*RedisNode {
	replicas := make([]*RedisNode, 0)
	for _, node := range rc.nodes {
		if node.IsReplica() {
			replicas = append(replicas, node)
		}
	}
	return replicas
}

// GetReplicasOfNode returns the replicas of the specified node
func (rc *RedisCluster) GetReplicasOfNode(node *RedisNode) []*RedisNode {
	if node.IsReplica() {
		return []*RedisNode{}
	}

	replicas := make([]*RedisNode, 0)
	for _, other := range rc.nodes {
		if other.IsReplica() && other.MasterID == node.ID {
			replicas = append(replicas, other)
		}
	}
	return replicas
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- SETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// SetReplicas sets the number of replicas in the Redis cluster.
// It returns an OperationAlreadyDoneError if the current number of replicas is equal to the desired number of replicas.
func (rc *RedisCluster) SetReplicas(replicas int, replicasPerMaster *int) error {
	currentReplicas := rc.GetReplicas()
	currentReplicasPerMaster := rc.GetReplicasPerMaster()
	rc.logger.Info("Changing Redis Cluster replicas", "current", currentReplicas, "desired", replicas)

	if replicasPerMaster != nil && currentReplicasPerMaster != *replicasPerMaster {
		rc.logger.Info("Changing Redis Cluster replicas per master", "current", currentReplicasPerMaster, "desired", replicasPerMaster)
		rc.conf.Redis.Cluster.ReplicasPerMaster = *replicasPerMaster
	}

	// Check if the current number of replicas is equal to the desired number of replicas
	if replicas == currentReplicas {
		if replicasPerMaster == nil || *replicasPerMaster == currentReplicasPerMaster {
			return &OperationCompletedError{Operation: "SetReplicas"}
		}
	}

	// Set the desired replicas
	rc.conf.Redis.Cluster.Replicas = replicas

	// Write in the channel to trigger the reconciler
	rc.channel <- struct{}{}

	return nil
}

// SetRedisClusterStatus sets the status of the Redis cluster
func (rc *RedisCluster) SetRedisClusterStatus(status string) error {
	rc.logger.Info("Changing Redis Cluster status", "current", rc.GetRedisClusterStatus(), "desired", status)
	rc.conf.Redis.Cluster.Status = status
	return nil
}

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PUBLIC  ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// Init initializes the Redis cluster. It should be called after creating a new Redis cluster.
// It sets the Robin status from the Redis Cluster status and creates the nodes and initializes them.
func (rc *RedisCluster) Init() error {
	// Initialize nodes
	for i := range rc.GetDesiredReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
		nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
		rc.addNode(nodeName, nodeAddr)
	}

	// Refresh nodes info
	if err := rc.refreshNodes(); err != nil {
		return fmt.Errorf("Error refreshing nodes info: %v", err)
	}

	return nil
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------- PUBLIC OPERATIONS -----------------------------------------
// ----------------------------------------------------------------------------------------------------

// Rebalance rebalances the Redis cluster
// It launches the cluster rebalance command and, depending on the async flag, waits for it to finish synchronously or asynchronously
// It returns an OperationInProgressError if the cluster is already rebalancing
// It returns an OperationAlreadyDoneError if the cluster is already rebalanced
func (rc *RedisCluster) Rebalance(async bool, weights map[string]int, force bool) error {
	// Check if the cluster is already rebalancing or has been rebalanced
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
	return rc.launchOperation(NewRedisOperationRebalance(rc.ctx, rc, weights), Rebalancing, async)
}

// MoveSlots moves slots from one Redis node to another
func (rc *RedisCluster) MoveSlots(from, to *RedisNode, slots int) error {
	rc.logger.Info("Moving slots", "slots", slots, "from", from.Name, "to", to.Name)

	// Check if a move operation should be launched
	if rc.IsReshardingNodes(*from, *to) { // There is an ongoing move between the specified nodes
		rc.logger.Info("There is already a reshard operation between nodes", "from", from.Name, "to", to.Name)
		return &OperationInProgressError{Operation: "Resharding"}

	} else if from.IsReplica() { // The origin node is a replica
		rc.logger.Info("Origin node is a replica", "from", from.Name)
		return &OperationCompletedError{Operation: "Resharding", Reason: "Origin node is a replica"}

	} else if !from.HasSlots() { // The origin node has no slots
		rc.logger.Info("Origin node has no slots", "from", from.Name)
		return &OperationCompletedError{Operation: "Resharding", Reason: "Origin node has no slots"}

	} else if rc.NodeHasReplicas(from) { // The origin node has replicas
		rc.logger.Info("Origin node has replicas", "from", from.Name)

		// Promote a replica of the origin node
		if err := rc.promoteReplicaOfNode(rc.ctx, from); err != nil {
			return fmt.Errorf("error promoting replica of node '%s': %v", from.Name, err)
		}
		return &OperationCompletedError{Operation: "Resharding", Reason: "Promoted replica of origin node"}
	}

	// Launch move operation
	return rc.launchOperation(NewRedisOperationMove(rc.ctx, rc, from, to, slots), Resharding, true)
}

// Check checks the Redis cluster
func (rc *RedisCluster) Check() (*ClusterCheckResult, error) {
	rc.logger.Info("Checking cluster")

	// Get Redis client and check connection
	redisClient, err := rc.getAndCheckRedisClient(true)
	if err != nil {
		return nil, fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Launch check operation
	result, err := redisClient.ClusterCheck(rc.ctx)
	if err != nil {
		return nil, fmt.Errorf("error checking cluster: %v", err)
	}

	return result, nil
}

// Fix fixes the Redis cluster
func (rc *RedisCluster) Fix(async bool, force bool) error {
	rc.logger.Info("Fixing cluster")

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
			return &OperationInProgressError{Operation: "Fixing"}
		}
	}

	// Launch cluster fix operation
	return rc.launchOperation(NewRedisOperationFix(rc.ctx, rc), Fixing, async)
}

// CheckIntegrity checks the integrity of the Redis cluster
func (rc *RedisCluster) CheckIntegrity(async, force bool) error {
	rc.logger.Info("Checking cluster integrity")

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
			return &OperationInProgressError{Operation: "CheckingIntegrity"}
		}
	}

	// Launch cluster check integrity operation
	return rc.launchOperation(NewRedisOperationCheckIntegrity(rc.ctx, rc), CheckingIntegrity, async)
}

func (rc *RedisCluster) ScaleUp(force bool) error {
	rc.logger.Info("Scaling up cluster")

	// Check if the cluster is already scaling up
	if rc.IsScalingUp() && !force {
		rc.logger.Info("Cluster is already scaling up")
		return &OperationInProgressError{Operation: "ScaleUp"}
	}

	// Launch cluster scaling up operation
	return rc.launchOperation(NewRedisOperationScaleUp(rc.ctx, rc), ScalingUp, false)
}

func (rc *RedisCluster) ScaleDown(force bool) error {
	rc.logger.Info("Scaling down cluster")

	// Check if the cluster is already scaling down
	if rc.IsScalingDown() && !force {
		rc.logger.Info("Cluster is already scaling down")
		return &OperationInProgressError{Operation: "ScaleDown"}
	}

	// Launch cluster scaling down operation
	return rc.launchOperation(NewRedisOperationScaleDown(rc.ctx, rc), ScalingDown, false)
}

func (rc *RedisCluster) Upgrade(force bool) error {
	rc.logger.Info("Upgrading cluster")

	// Check if the cluster is already upgrading
	if rc.IsUpgrading() && !force {
		rc.logger.Info("Cluster is already upgrading")
		return &OperationInProgressError{Operation: "Upgrade"}
	}

	// Launch cluster upgrade operation
	return rc.launchOperation(NewRedisOperationUpgrade(rc.ctx, rc), Upgrading, false)
}

// Check reset a Redis node in the cluster
func (rc *RedisCluster) ResetNode(node *RedisNode) error {
	rc.logger.Info("Reseting node", "node", node.Name)

	// Check if the cluster node is already being resetted
	if rc.IsResettingNode(*node) {
		rc.logger.Info("Cluster node is already being resetted", "node", node.Name)
		return &OperationInProgressError{Operation: "Resetting"}
	}

	// Launch reset node operation
	return rc.launchOperation(NewRedisOperationResetNode(rc.ctx, rc, node), Upgrading, false)
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- ASKERS ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// IsBalanced returns true if the Redis cluster is balanced
func (rc *RedisCluster) IsBalanced() bool {
	masters := rc.GetMasterNodes()
	slotsPerMaster := int(math.Ceil(float64(RedisClusterTotalSlots) / float64(len(masters))))

	for _, master := range masters {
		masterSlots := master.GetNumberOfSlots()

		// Cluster is not balanced if any of master node has no slots
		if masterSlots == 0 {
			rc.logger.Info("Cluster needs rebalance: node has no slots", "node", master.Name)
			return false
		}

		// Cluster is not balanced if the number of slots is greater than the maximum
		maximumSlots := slotsPerMaster + (slotsPerMaster * RedisNodesUnbalancedThreshold / 100)
		if masterSlots > maximumSlots {
			rc.logger.Info("Cluster needs rebalance: node has more slots than the maximum", "node", master.Name, "slots", masterSlots, "maximumSlots", maximumSlots)
			return false
		}
	}

	// Cluster is balanced otherwise
	return true
}

// IsRebalancing returns true if the cluster is rebalancing right now
func (rc *RedisCluster) IsRebalancing() bool {
	return rc.hasOperation(Rebalancing, "Running")
}

// IsReshardingNodes returns true if there is a reshard operation between the specified nodes
func (rc *RedisCluster) IsReshardingNodes(from, to RedisNode) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Running", from, to)
}

// IsResharding returns true if the cluster is resharding right now
func (rc *RedisCluster) IsResharding() bool {
	return rc.hasOperation(Resharding, "Running")
}

// IsFixing returns true if the cluster is being fixing right now
func (rc *RedisCluster) IsFixing() bool {
	return rc.hasOperation(Fixing, "Running")
}

// IsCheckingIntegrity returns true if we are checkint the integrity of the cluster right now
func (rc *RedisCluster) IsCheckingIntegrity() bool {
	return rc.hasOperation(CheckingIntegrity, "Running")
}

// IsScalingUp returns true if the cluster is scaling up right now
func (rc *RedisCluster) IsScalingUp() bool {
	return rc.hasOperation(ScalingUp, "Running")
}

// IsScalingDown returns true if the cluster is scaling down right now
func (rc *RedisCluster) IsScalingDown() bool {
	return rc.hasOperation(ScalingDown, "Running")
}

// IsUpgrading returns true if the cluster is upgrading right now
func (rc *RedisCluster) IsUpgrading() bool {
	return rc.hasOperation(Upgrading, "Running")
}

// IsResettingNode returns true if the cluster nose is being resetted right now
func (rc *RedisCluster) IsResettingNode(node RedisNode) bool {
	return rc.hasOperationInNode(Resetting, "Running", node)
}

// IsResetting returns true if the cluster is resetting right now
func (rc *RedisCluster) IsResetting() bool {
	return rc.hasOperation(Resetting, "Running")
}

// IsScaled returns true if the cluster is scaled. That is, if it has the desired number of replicas, has no missing slots and is balanced
func (rc *RedisCluster) IsScaled() bool {
	return rc.HasDesiredReplicas() && !rc.HasMissingSlots() && rc.IsBalanced()
}

// IsUpgraded returns true if the cluster is correctly configured for an upgrade. That is, if it has the desired number of replicas and has no missing slots
func (rc *RedisCluster) IsUpgraded() bool {
	return rc.HasDesiredReplicas() && !rc.HasMissingSlots()
}

// CanBeUpgraded returns true if there is not a conflicting operation with a cluster upgrade. That is, if the cluster is not rebalancing, resharding, fixing,
// checking integrity, scaling up, scaling down or resetting
func (rc *RedisCluster) CanBeUpgraded() bool {
	return !rc.IsRebalancing() && !rc.IsResharding() && !rc.IsFixing() && !rc.IsCheckingIntegrity() && !rc.IsScalingUp() && !rc.IsScalingDown() && !rc.IsResetting()
}

// CanBeChecked returns true if there is not a conflicting operationr with a check cluster integrity. That is, if the cluster is not resharding, checking integrity or resetting
func (rc *RedisCluster) CanBeChecked() bool {
	return !rc.IsResharding() && !rc.IsCheckingIntegrity() && !rc.IsResetting()
}

// HasMissingSlots returns true if the cluster has missing slots
func (rc *RedisCluster) HasMissingSlots() bool {
	slotCount := 0
	for _, node := range rc.GetNodes() {
		slotCount += node.GetNumberOfSlots()
	}
	return slotCount != RedisClusterTotalSlots
}

// HasDesiredReplicas returns true if the Redis cluster has the desired number of replicas
func (rc *RedisCluster) HasDesiredReplicas() bool {
	return rc.GetDesiredReplicas() == len(rc.GetNodes()) && rc.GetReplicas() == len(rc.GetMasterNodes()) && rc.GetReplicasPerMaster()*rc.GetReplicas() == len(rc.GetReplicaNodes())
}

// HasBeenRebalanced returns true if the cluster has been rebalanced recently
func (rc *RedisCluster) HasBeenRebalanced() bool {
	return rc.hasOperation(Rebalancing, "Finished")
}

// HasBeenResharded returns true if there is a reshard operation between the specified nodes that has finished
func (rc *RedisCluster) HasBeenResharded(from, to RedisNode) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Finished", from, to)
}

// HasNode returns true if the Redis cluster has a node with the specified name
func (rc *RedisCluster) HasNode(name string) bool {
	_, ok := rc.nodes[name]
	return ok
}

// NodeHasReplicas returns true if the specified node has replicas
func (rc *RedisCluster) NodeHasReplicas(node *RedisNode) bool {
	if node.IsReplica() {
		return false
	}

	for _, n := range rc.GetNodes() {
		if n.IsReplica() && n.MasterID == node.ID {
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
func (rc *RedisCluster) addNode(name, addr string) *RedisNode {
	node := &RedisNode{
		Name:       name,
		Addr:       addr,
		MaxRetries: rc.GetClusterMaxRetries(),
		Backoff:    rc.GetClusterBackOff(),
	}

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
func (rc *RedisCluster) removeNode(name string) error {
	_, ok := rc.nodes[name]
	if !ok {
		return fmt.Errorf("node %s not found", name)
	}

	delete(rc.nodes, name)
	return nil
}

// forgetNode removes a node from the cluster
func (rc *RedisCluster) forgetNode(ctx context.Context, nodeToForget RedisNode) error {
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

// refreshNodes refreshes the nodes info of the Redis cluster
func (rc *RedisCluster) refreshNodes() error {
	// Get Redis client and check connection
	redisClient, err := rc.getAndCheckRedisClient(false)
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

// checkNodes checks the nodes of the Redis cluster
func (rc *RedisCluster) checkNodes() error {
	for i := range rc.GetDesiredReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)

		// Check if the node exists
		node := rc.GetNode(nodeName)
		if node == nil {
			rc.logger.Error("Node not found", "node", nodeName)
			continue
		}

		// Init a fresh node to check if IP or ID have changed. This can happen if the node has been restarted
		freshNode := RedisNode{
			Name:       nodeName,
			Addr:       node.Addr,
			MaxRetries: rc.GetClusterMaxRetries(),
			Backoff:    rc.GetClusterBackOff(),
		}
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

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	return nil
}

// updateNodesInfo updates the info of the Redis nodes in the cluster
func (rc *RedisCluster) updateNodesInfo(nodesInfo []RedisNode) {
	for _, nodeInfo := range nodesInfo {
		node := rc.GetNodeFromID(nodeInfo.ID)
		if node == nil {
			continue
		}
		node.UpdateInfo(nodeInfo)
	}
}

// needsMeet checks if the Redis cluster needs to meet nodes
func (rc *RedisCluster) needsMeet(ctx context.Context) (bool, error) {
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

// needsFix checks if the Redis cluster needs to be fixed
func (rc *RedisCluster) needsFix(ctx context.Context) (bool, error) {
	// Get Redis client and check connection
	redisClient, err := rc.getAndCheckRedisClient(false)
	if err != nil {
		return false, fmt.Errorf("error getting and checking Redis client: %v", err)
	}
	defer redisClient.Close()

	// Check if the cluster needs to be fixed
	result, err := redisClient.ClusterCheck(ctx)
	if err != nil {
		return false, fmt.Errorf("error checking cluster: %v", err)
	}

	return result.CommandCodeOutput != 0, nil
}

func (rc *RedisCluster) needsUpscale() bool {
	return len(rc.GetNodes()) < rc.GetDesiredReplicas()
}

func (rc *RedisCluster) needsDownscale() bool {
	return len(rc.GetNodes()) > rc.GetDesiredReplicas()
}

// meetNodesIfNeeded meets the nodes of the Redis cluster if needed
func (rc *RedisCluster) meetNodesIfNeeded(ctx context.Context) error {
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

// assignMissingSlotsIfNeeded assigns missing slots to the Redis cluster if needed
func (rc *RedisCluster) assignMissingSlotsIfNeeded(ctx context.Context) error {
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

// balanceClusterIfNeeded balances the Redis cluster if needed
func (rc *RedisCluster) balanceClusterIfNeeded(ctx context.Context, weights map[string]int) error {
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

// fixClusterIfNeeded fixes the Redis cluster if needed
func (rc *RedisCluster) fixClusterIfNeeded(ctx context.Context) error {
	// Check if the cluster needs a fix
	needsFix, err := rc.needsFix(ctx)
	if err != nil || !needsFix {
		return nil
	}

	rc.logger.Info("Cluster needs fix")

	// Fix the cluster
	if err := rc.Fix(false, true); err != nil {
		return err
	}

	return nil
}

// addNewNodesIfNeeded adds new nodes to the Redis cluster if needed
func (rc *RedisCluster) addNewNodesIfNeeded(ctx context.Context) error {
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

func (rc *RedisCluster) removeNodesIfNeeded(ctx context.Context) error {
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
	if err := rc.removeSlotsFromNodes(ctx, nodesToRemove); err != nil {
		return err
	}

	// Forget and remove nodes
	if err := rc.forgetAndRemoveNodes(ctx, nodesToRemove); err != nil {
		return err
	}

	return nil
}

func (rc *RedisCluster) removeSlotsFromNodes(ctx context.Context, nodes []*RedisNode) error {
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
	if err := rc.refreshNodes(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %w", err)
	}
	return nil
}

// forgetAndRemoveNodes forgets and removes nodes from the Redis cluster
func (rc *RedisCluster) forgetAndRemoveNodes(ctx context.Context, nodes []*RedisNode) error {
	for _, node := range nodes {
		// Forget the node
		if err := rc.forgetNode(ctx, *node); err != nil {
			return err
		}

		// Remove the node
		if err := rc.removeNode(node.Name); err != nil {
			return err
		}
	}

	// Wait for cluster meet so nodes can agree on configuration
	time.Sleep(5 * time.Second)

	return nil
}

// getNodesToRemove returns the nodes to remove in order of preference
func (rc *RedisCluster) getNodesToRemove(ctx context.Context) ([]*RedisNode, error) {
	namePrefix := fmt.Sprintf("%s-", rc.GetName())

	// Get nodes
	nodes := rc.GetNodes()
	desiredReplicas := int(rc.GetDesiredReplicas())
	if len(nodes) <= desiredReplicas {
		return nil, fmt.Errorf("not enough nodes to remove")
	}

	// Sort nodes by name to remove the ones with the highest ordinal
	sort.Slice(nodes, func(i, j int) bool {
		ordinalI, _ := strconv.Atoi(strings.TrimPrefix(nodes[i].Name, namePrefix))
		ordinalJ, _ := strconv.Atoi(strings.TrimPrefix(nodes[j].Name, namePrefix))
		return ordinalI < ordinalJ
	})

	// Assure all nodes to keep are masters, converting them to master if needed
	if err := rc.convertNodesToMaster(ctx, nodes[:desiredReplicas]); err != nil {
		return nil, err
	}

	return nodes[desiredReplicas:], nil
}

// meetNodes meets the nodes of the Redis cluster
func (rc *RedisCluster) meetNodes(ctx context.Context) error {
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
				return fmt.Errorf("error in ClusterMeet between '%s' and '%s': %w", sourceNode.Name, targetNode.Name, err)
			}
		}
	}

	// Wait for cluster meet so nodes can agree on configuration
	time.Sleep(5 * time.Second)

	// Refresh nodes info
	if err := rc.refreshNodes(); err != nil {
		return fmt.Errorf("error refreshing nodes info: %w", err)
	}

	rc.logger.Info("Nodes met successfully")
	return nil
}

// removeOutdatedNodes removes nodes that are not part of the cluster
func (rc *RedisCluster) removeOutdatedNodes(ctx context.Context) error {
	// Get all cluster nodes and forget the ones that are not part of the cluster
	for _, node := range rc.nodes {
		clusterNodes, err := node.GetClusterNodes(ctx)
		if err != nil {
			return fmt.Errorf("error getting cluster nodes from node %s: %w", node.Name, err)
		}

		for _, clusterNode := range clusterNodes {
			// Skip nodes that should not be removed
			if !clusterNode.ShouldBeRemoved() {
				continue
			}

			// Forget the node
			if err := node.ForgetNode(ctx, clusterNode); err != nil {
				return fmt.Errorf("error forgetting node %s from node %s: %w", clusterNode.ID, node.Name, err)
			}

			rc.logger.Info("Node forgotten successfully", "node", clusterNode.ID, "from", node.Name)
		}
	}

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	return nil
}

// ensureClusterRatio ensures that the Redis cluster has the right ratio of masters and replicas
func (rc *RedisCluster) ensureClusterRatio(ctx context.Context) error {
	// When all the nodes are ready, we need to make sure there is the right ratio of masters and replicas for the redis cluster
	// If there are too many replicas, we need to reset and add as a master
	//
	// If there are too few replicas, we need to reset and add as a
	// replica of a master with the least amount of replicas attached
	activeMasters := rc.GetMasterNodes()
	activeReplicas := rc.GetReplicaNodes()
	desiredMasters := rc.GetReplicas()
	desiredReplicas := rc.GetReplicasPerMaster()

	// We have the right amount of masters and replicas: ensure replica spread
	if len(activeMasters) == desiredMasters {
		return rc.ensureReplicaSpread(ctx)
	}

	// We have more masters than needed: convert some masters to replicas
	if len(activeMasters) > desiredMasters {
		rc.logger.Info("Converting masters to replicas", "masters", len(activeMasters), "desiredMasters", desiredMasters)

		// Sort masters by number of slots in descending order to keep the ones with the most slots
		sort.Slice(activeMasters, func(i, j int) bool {
			return activeMasters[i].GetNumberOfSlots() > activeMasters[j].GetNumberOfSlots()
		})

		keepMasters := activeMasters[:desiredMasters]
		deleteMasters := activeMasters[desiredMasters:]
		if err := rc.convertNodesToReplica(ctx, deleteMasters, keepMasters); err != nil {
			return err
		}
		return rc.ensureReplicaSpread(ctx)
	}

	// We have less masters than needed and we have replicas: promote some replicas to masters
	if len(activeMasters) < desiredMasters && desiredReplicas > 0 {
		rc.logger.Info("Promoting replicas to masters", "masters", len(activeMasters), "desiredMasters", desiredMasters, "replicas", len(activeReplicas), "desiredReplicas", desiredReplicas)
		needsReplicas := desiredMasters - len(activeMasters)
		convertableReplicas := activeReplicas[:needsReplicas]
		if err := rc.convertNodesToMaster(ctx, convertableReplicas); err != nil {
			return err
		}
		return rc.ensureReplicaSpread(ctx)
	}

	// We have replicas but we don't want any: promote all to masters
	if len(activeReplicas) > 0 && desiredReplicas == 0 {
		rc.logger.Info("Promoting all replicas to masters", "replicas", len(activeReplicas))
		if err := rc.convertNodesToMaster(ctx, activeReplicas); err != nil {
			return err
		}
		return rc.ensureReplicaSpread(ctx)
	}

	return nil
}

// ensureReplicaSpread ensures that the number of replicas is spread across the masters
func (rc *RedisCluster) ensureReplicaSpread(ctx context.Context) error {
	var masterNeedsReplicas []*RedisNode
	var replicaNeedsMove []*RedisNode

	masters := rc.GetMasterNodes()
	replicas := rc.GetReplicaNodes()

	// Find masters that need replicas
	for _, master := range masters {
		replicas := rc.GetReplicasOfNode(master)
		replicasPerMaster := rc.GetReplicasPerMaster()

		if len(replicas) == int(replicasPerMaster) { // Master has the right number of replicas
			continue
		} else if len(replicas) < int(replicasPerMaster) { // Too few replicas
			masterNeedsReplicas = append(masterNeedsReplicas, master)
		} else if len(replicas) > int(replicasPerMaster) { // Too much replicas
			replicaNeedsMove = append(replicaNeedsMove, replicas[:replicasPerMaster]...)
		}
	}

	// There might be replicas which are replicating replicas. We want to change these to point at masters
	for _, replica := range replicas {
		replicasPointedAtReplicas := rc.GetReplicasOfNode(replica)
		replicaNeedsMove = append(replicaNeedsMove, replicasPointedAtReplicas...)
	}

	// There might be replicas which are replicating masters which we cannot see.
	for _, replica := range replicas {
		// Is the replica pointing at one of the masters ?
		pointedAtMaster := false
		for _, master := range masters {
			if master.Name == replica.MasterID {
				pointedAtMaster = true
			}
		}
		if !pointedAtMaster {
			replicaNeedsMove = append(replicaNeedsMove, replica)
		}
	}

	// We have more masters that need replicas than available replicas
	if len(replicaNeedsMove) < len(masterNeedsReplicas) {
		return fmt.Errorf("there are not enough replicas to convert. masters=%d replicas=%d", len(masterNeedsReplicas), len(replicaNeedsMove))
	}

	// Replicas that need to be converted to masters
	for i := 0; i < len(masterNeedsReplicas); i++ {
		rc.logger.Info("Converting node to replica", "node", replicaNeedsMove[i].Name, "master", masterNeedsReplicas[i].Name)

		if err := replicaNeedsMove[i].ReplicateNode(ctx, *masterNeedsReplicas[i]); err != nil {
			return err
		}
	}

	// Refresh nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	return nil
}

// convertNodesToReplicas converts the specified nodes to replicas of the specified nodes to keep
func (rc *RedisCluster) convertNodesToReplica(ctx context.Context, nodesToConvert []*RedisNode, nodesToKeep []*RedisNode) error {
	// Rebalance cluster removing slots from the masters we want to delete
	if err := rc.removeSlotsFromNodes(ctx, nodesToConvert); err != nil {
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
		rc.logger.Info("Converting node to replica", "node", convertable.Name, "master", nodesToKeep[currentKeepable].Name)
		if err := convertable.ReplicateNode(ctx, *nodesToKeep[currentKeepable]); err != nil {
			return err
		}

		currentKeepable = currentKeepable + 1
		if currentKeepable > rc.GetReplicas()-1 {
			currentKeepable = 0
		}
	}

	// Wait for cluster meet so nodes can agree on configuration
	time.Sleep(5 * time.Second)

	// Refresh nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	return nil
}

// convertNodesToMaster promotes the specified nodes to masters
func (rc *RedisCluster) convertNodesToMaster(ctx context.Context, nodes []*RedisNode) error {
	// Reset nodes that will be promoted to masters
	for _, node := range nodes {
		// Skip nodes that are already masters
		if node.IsMaster() {
			continue
		}

		// Reset the node
		rc.logger.Info("Converting node to master", "node", node.Name)
		if err := node.Reset(ctx); err != nil {
			return err
		}
		time.Sleep(2 * time.Second)
	}

	// Meet the cluster to promote the nodes to masters
	if err := rc.meetNodes(ctx); err != nil {
		return err
	}

	return nil
}

// promoteReplicaOfNode promotes the first replica of the specified node to master
func (rc *RedisCluster) promoteReplicaOfNode(ctx context.Context, node *RedisNode) error {
	// Check if the node is a master
	if !node.IsMaster() {
		return fmt.Errorf("node %s is not a master", node.Name)
	}

	// Get the current replicas
	replicas := rc.GetReplicasOfNode(node)
	if len(replicas) == 0 {
		return fmt.Errorf("node %s has no replicas to promote", node.Name)
	}

	// Promote the first replica
	rc.logger.Info("Promoting replica to master", "replica", replicas[0].Name, "master", node.Name)
	if err := replicas[0].Failover(ctx); err != nil {
		return err
	}

	// Refresh nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	return nil
}

// ensureNodesAreUp checks if the nodes received are up and running
func (rc *RedisCluster) ensureNodesAreUp(ctx context.Context) error {
	for _, node := range rc.GetNodes() {
		if err := node.CheckConnection(ctx); err != nil {
			return err
		}
	}
	return nil
}

// assignMissingSlots assigns missing slots to the Redis cluster
func (rc *RedisCluster) assignMissingSlots(ctx context.Context) error {
	rc.logger.Info("Assigning missing slots")

	// Get the master nodes
	masters := rc.GetMasterNodes()

	// We start with a map so we can easily delete slots if they are already assigned
	allSlots := util.MakeRangeMap(0, 16383)
	for _, node := range masters {
		for _, slotRange := range node.Slots {
			// We want to remove any assigned slots from our list so we end up with unassigned slots
			for slot := slotRange.Start; slot <= slotRange.End; slot++ {
				delete(allSlots, slot)
			}
		}
	}
	totalMasterNodes := len(masters)
	slotsPerNode := calculateMaxSlotsPerMaster(RedisClusterTotalSlots, totalMasterNodes)

	// We convert the slots left over in the map to an array so we can easily sort them,
	// and cut off slices
	var slotsToAssign []int
	for slot := range allSlots {
		slotsToAssign = append(slotsToAssign, slot)
	}
	sort.Ints(slotsToAssign)

	for i, node := range masters {
		slotAmountToAssign := slotsPerNode - node.GetNumberOfSlots()
		if slotAmountToAssign <= 0 {
			continue
		}
		// If we reach the last master node with a number of not assigned slots greater than slotsPerNodes
		// (mainly when configuring a new redis cluster) the exceeding slots will be assigned to this node.
		var slotList []int
		if i == totalMasterNodes-1 || len(slotsToAssign) <= slotAmountToAssign {
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
	time.Sleep(5 * time.Second)

	// Refresh nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	rc.logger.Info("Missing slots assigned successfully")
	return nil
}

// getAndCheckRedisClient creates a Redis client and checks the connection
func (rc *RedisCluster) getAndCheckRedisClient(close bool) (*RedisClient, error) {
	// Create Redis client
	redisClient := NewRedisClient(rc.ctx, rc.GetAddress(), os.Getenv("REDISAUTH"), 0)
	if close {
		defer redisClient.Close()
	}

	// Check connection
	if err := redisClient.CheckConnection(rc.GetClusterMaxRetries(), rc.GetClusterBackOff()); err != nil {
		return nil, err
	}

	return redisClient, nil
}

func (rc *RedisCluster) launchOperation(operation RedisOperation, name string, async bool) error {
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
func (rc *RedisCluster) hasOperation(name string, status string) bool {
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
func (rc *RedisCluster) hasOperationInNode(name string, status string, node RedisNode) bool {
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
func (rc *RedisCluster) hasOperationBetweenNodes(name string, status string, from RedisNode, to RedisNode) bool {
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
func (rc *RedisCluster) addOperation(operationName string, operation RedisOperation) {
	if rc.operations[operationName] == nil {
		rc.operations[operationName] = make([]RedisOperation, 0)
	}

	rc.operations[operationName] = append(rc.operations[operationName], operation)
}

// getOperation returns the operation if the cluster has an operation with the specified name and status or nil otherwise
func (rc *RedisCluster) getOperation(name string, status string) RedisOperation {
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

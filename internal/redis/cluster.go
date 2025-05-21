// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/util"
)

const (
	Initializing                  = "Initializing"
	Ready                         = "Ready"
	Error                         = "Error"
	Upgrading                     = "Upgrading"
	ScalingDown                   = "ScalingDown"
	ScalingUp                     = "ScalingUp"
	Maintenance                   = "Maintenance"
	Unknown                       = "Unknown"
	Resharding                    = "Resharding"
	ReshardingError               = "ReshardingError"
	Rebalancing                   = "Rebalancing"
	RebalancingError              = "RebalancingError"
	Fixing                        = "Fixing"
	FixingError                   = "FixingError"
	ChekingIntegrity              = "CheckingIntegrity"
	CheckIntegrityError           = "CheckIntegrityError"
	RedisClusterTotalSlots        = 16384
	RedisNodesUnbalancedThreshold = 2
)

// RedisCluster represents a Redis cluster
type RedisCluster struct {
	ctx        context.Context
	logger     logr.Logger
	conf       *config.Configuration
	status     string
	nodes      map[string]*RedisNode
	operations map[string][]*RedisOperation
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
		operations: make(map[string][]*RedisOperation),
		channel:    channel,
	}
}

func NewFakeRedisCluster(ctx context.Context, conf *config.Configuration, status string, nodes map[string]*RedisNode, operations map[string][]*RedisOperation) *RedisCluster {
	return &RedisCluster{
		ctx:        ctx,
		logger:     util.GetLogger("redis-cluster"),
		conf:       conf,
		status:     status,
		nodes:      nodes,
		operations: operations,
	}
}

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

func (rc *RedisCluster) GetNode(name string) *RedisNode {
	return rc.nodes[name]
}

// Init initializes the Redis cluster. It should be called after creating a new Redis cluster.
// It sets the Robin status from the Redis Cluster status and creates the nodes and initializes them.
func (rc *RedisCluster) Init() error {
	// Initialize nodes
	for i := range rc.GetReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
		nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
		node := rc.addNode(nodeName, nodeAddr)

		rc.logger.Info("Initializing node", "node", nodeName)

		if err := node.Init(rc.ctx); err != nil {
			// TODO: retry init afterwards
			rc.logger.Info("Error initializing node", "error", err, "node", nodeName)
			continue
		}

		rc.logger.Info("Node initialized successfully", "node", nodeName, "ID", node.ID, "IP", node.IP)
	}

	// Refresh nodes info
	if err := rc.refreshNodes(); err != nil {
		return fmt.Errorf("Error refreshing nodes info: %v", err)
	}

	return nil
}

// SetReplicas sets the number of replicas in the Redis cluster.
// It adds or removes nodes to match the desired number of replicas.
// It returns an OperationAlreadyDoneError if the current number of replicas is equal to the desired number of replicas.
func (rc *RedisCluster) SetReplicas(replicas int) error {
	currentReplicas := rc.GetReplicas()
	rc.logger.Info("Changing Redis Cluster replicas", "current", currentReplicas, "desired", replicas)

	if replicas == currentReplicas {
		return &OperationCompletedError{Operation: "SetReplicas"}
	}

	rc.conf.Redis.Cluster.Replicas = replicas

	if replicas > currentReplicas {
		for i := currentReplicas; i < replicas; i++ {
			nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
			nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
			rc.addNode(nodeName, nodeAddr)
		}
	} else {
		for i := currentReplicas - 1; i > replicas-1; i-- {
			nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
			rc.removeNode(nodeName)
		}
	}

	return nil
}

func (rc *RedisCluster) SetRedisClusterStatus(status string) error {
	rc.logger.Info("Changing Redis Cluster status", "current", rc.GetRedisClusterStatus(), "desired", status)
	rc.conf.Redis.Cluster.Status = status
	return nil
}

// Rebalance rebalances the Redis cluster
// It launches the cluster rebalance command and, depending on the async flag, waits for it to finish synchronously or asynchronously
// It returns an OperationInProgressError if the cluster is already rebalancing
// It returns an OperationAlreadyDoneError if the cluster has already been rebalanced
func (rc *RedisCluster) Rebalance(async bool, weights map[string]int, force bool) error {
	rc.logger.Info("Rebalancing cluster")

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
	} else if rc.HasBeenRebalanced() && !force {
		rc.logger.Info("Cluster has already been rebalanced")
		return &OperationCompletedError{Operation: "Rebalance"}
	}

	// Launch cluster rebalance operation
	operation, err := rc.launchRebalanceOperation(weights)
	if err != nil {
		return err
	}

	// Launch wait for rebalance to finish synchronously or asynchronously depending on the async flag
	if async {
		go rc.waitForRebalanceToFinish(operation)
	} else {
		rc.waitForRebalanceToFinish(operation)
	}

	return nil
}

func (rc *RedisCluster) MoveSlots(from, to *RedisNode, slots int) error {
	rc.logger.Info("Moving slots", "slots", slots, "from", from.Name, "to", to.Name)

	// Check if there is an ongoing reshard between the specified nodes
	if rc.IsResharding(*from, *to) {
		rc.logger.Info("There is already a reshard operation between nodes", "from", from.Name, "to", to.Name)
		return &OperationInProgressError{Operation: "Resharding"}
	} else if rc.HasBeenResharded(*from, *to) {
		rc.logger.Info("Slots have already been moved between nodes", "from", from.Name, "to", to.Name)
		return &OperationCompletedError{Operation: "Resharding"}
	}

	// Launch reshard operation
	operation, err := rc.launchReshardOperation(from, to, slots)
	if err != nil {
		return err
	}

	// Wait for reshard to finish asynchronously
	go rc.waitForReshardToFinish(operation)

	return nil
}

func (rc *RedisCluster) Check() (*ClusterCheckResult, error) {
	rc.logger.Info("Checking cluster")

	// Launch cluster check operation
	result, err := rc.launchCheckOperation()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (rc *RedisCluster) CheckClusterIntegrity(async, force bool) error {
	rc.logger.Info("Checking cluster integrity")

	// Check if the cluster integrity is being checked
	if rc.IsCheckingIntegrity() {
		if force {
			rc.logger.Info("Cancelling ongoing checking integrity operation")
			operation := rc.getOperation(ChekingIntegrity, "Running")
			if operation != nil {
				operation.Cancel()
			}
		} else {
			rc.logger.Info("Cluster integrity is already being checked")
			return &OperationInProgressError{Operation: "CheckingIntegrity"}
		}
	}

	// Launch cluster fixing operation
	operation, err := rc.launchCheckIntegrityOperation()
	if err != nil {
		return err
	}

	// Launch wait for fixing to finish synchronously or asynchronously depending on the async flag
	if async {
		go rc.waitForCheckIntegrityToFinish(operation)
	} else {
		rc.waitForCheckIntegrityToFinish(operation)
	}

	return nil
}

// IsBalanced returns true if the Redis cluster is balanced
func (rc *RedisCluster) IsBalanced() bool {
	masters := rc.getMasters()
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

// IsResharding returns true if there is a reshard operation between the specified nodes
func (rc *RedisCluster) IsResharding(from, to RedisNode) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Running", from, to)
}

// IsFixing returns true if the cluster is being fixing right now
func (rc *RedisCluster) IsFixing() bool {
	return rc.hasOperation(Fixing, "Running")
}

// IsCheckingIntegrity returns true if we are checking the integrity of the cluster right now
func (rc *RedisCluster) IsCheckingIntegrity() bool {
	return rc.hasOperation(ChekingIntegrity, "Running")
}

// HasMissingSlots returns true if the cluster has missing slots
func (rc *RedisCluster) HasMissingSlots() bool {
	slotCount := 0

	for _, node := range rc.GetNodes() {
		slotCount += node.GetNumberOfSlots()
	}

	return slotCount != RedisClusterTotalSlots
}

// HasBeenRebalanced returns true if the cluster has been rebalanced recently
func (rc *RedisCluster) HasBeenRebalanced() bool {
	return rc.hasOperation(Rebalancing, "Finished")
}

// HasBeenResharded returns true if there is a reshard operation between the specified nodes that has finished
func (rc *RedisCluster) HasBeenResharded(from, to RedisNode) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Finished", from, to)
}

func (rc *RedisCluster) RemoveOutdatedOperations() {
	cleanupThreshold := time.Duration(rc.GetReconcilerOperationCleanupInterval()) * time.Second

	for name, operations := range rc.operations {
		for i := len(operations) - 1; i >= 0; i-- {
			if operations[i].GetElapsedTimeFromEnd() > cleanupThreshold {
				rc.operations[name] = append(rc.operations[name][:i], rc.operations[name][i+1:]...)
			}
		}
	}
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

func (rc *RedisCluster) checkNodes() error {
	for i := range rc.GetReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)

		// Check if the node exists
		node := rc.GetNode(nodeName)
		if node == nil {
			rc.logger.Info("Node not found", "node", nodeName)
			continue
		}

		// Init a fresh node to check if IP or ID have changed. This can happen if the node has been restarted
		freshNode := RedisNode{
			Name:     nodeName,
			Addr:     node.Addr,
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

	return nil
}

// launchReshardOperation launches a reshard operation between the specified nodes
func (rc *RedisCluster) launchReshardOperation(from, to *RedisNode, slots int) (*RedisOperation, error) {
	// Get Redis client and check connection
	redisClient, err := rc.getAndCheckRedisClient(true)
	if err != nil {
		return nil, fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Launch reshard operation
	cmd := redisClient.ReshardNode(rc.ctx, *from, *to, slots)
	if cmd.Err != nil {
		return nil, fmt.Errorf("error moving slots: %v", cmd.Err)
	}

	// Launch wait for reshard to finish
	return rc.addOperation(Resharding, cmd, from, to), nil
}

// waitForReshardToFinish waits for the reshard operation to finish and updates the status
func (rc *RedisCluster) waitForReshardToFinish(operation *RedisOperation) {
	rc.status = Resharding

	// Wait for reshard to finish
	err := operation.Wait()

	// Reshard failed
	if err != nil {
		rc.status = ReshardingError
		rc.logger.Info("Error resharding node", "error", err, "from", operation.NodeFrom.Name, "to", operation.NodeTo.Name)
		return
	}

	// Reshard finished successfully
	rc.status = Ready
	rc.logger.Info("Slots moved successfully between nodes", "from", operation.NodeFrom.Name, "to", operation.NodeTo.Name)

	// Forget the node if it is ephemeral
	if rc.IsEphemeral() {
		rc.forgetNode(rc.ctx, *operation.NodeFrom)
	}

	// Update nodes info
	rc.refreshNodes()
}

// launchRebalanceOperation launches a rebalance operation with the specified weights
func (rc *RedisCluster) launchRebalanceOperation(weights map[string]int) (*RedisOperation, error) {
	// Get Redis client and check connection
	redisClient, err := rc.getAndCheckRedisClient(true)
	if err != nil {
		return nil, fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Launch rebalance operation
	cmd := redisClient.ClusterRebalance(rc.ctx, weights)
	if cmd.Err != nil {
		return nil, fmt.Errorf("error rebalancing cluster: %v", cmd.Err)
	}

	// Launch wait for rebalance to finish
	return rc.addOperation(Rebalancing, cmd, nil, nil), nil
}

// waitForRebalanceToFinish waits for the cluster rebalance to finish and updates the status
func (rc *RedisCluster) waitForRebalanceToFinish(operation *RedisOperation) {
	rc.status = Rebalancing

	// Wait for cluster rebalance to finish
	err := operation.Wait()

	// Rebalance failed
	if err != nil {
		rc.status = RebalancingError
		rc.logger.Info("Error rebalancing cluster", "error", err)
		return
	}

	// Rebalance finished successfully
	rc.status = Ready
	rc.logger.Info("Cluster rebalanced successfully")

	// Update nodes info
	rc.refreshNodes()
}

// launchFixOperation launches a fix operation
func (rc *RedisCluster) launchFixOperation() (*RedisOperation, error) {
	// Get Redis client and check connection
	redisClient, err := rc.getAndCheckRedisClient(true)
	if err != nil {
		return nil, fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Launch fix operation
	cmd := redisClient.ClusterFix(rc.ctx)
	if cmd.Err != nil {
		return nil, fmt.Errorf("error fixing cluster: %v", cmd.Err)
	}

	// Launch wait for fix to finish
	return rc.addOperation(Fixing, cmd, nil, nil), nil
}

// waitForFixToFinish waits for the cluster fix to finish and updates the status
func (rc *RedisCluster) waitForFixToFinish(operation *RedisOperation) {
	rc.status = Fixing

	// Wait for cluster fix to finish
	err := operation.Wait()

	// Fix failed
	if err != nil {
		rc.status = FixingError
		rc.logger.Info("Error fixing cluster", "error", err, "stdout", operation.Cmd.GetStdout(), "stderr", operation.Cmd.GetStderr())
		return
	}

	// Fix finished successfully
	rc.status = Ready
	rc.logger.Info("Cluster fixed successfully")

	// Update nodes info
	rc.refreshNodes()
}

// launchCheckIntegrityOperation launches a integrity check operation
func (rc *RedisCluster) launchCheckIntegrityOperation() (*RedisOperation, error) {
	// Launch integrity check operation
	cmd := NewRedisLibraryCommand(rc.ctx, rc.doCheckIntegrity)
	cmd.Start()

	// Launch wait for check integrity to finish
	return rc.addOperation(ChekingIntegrity, cmd, nil, nil), nil
}

// waitForCheckIntegrityToFinish waits for the cluster check integrity to finish and updates the status
func (rc *RedisCluster) waitForCheckIntegrityToFinish(operation *RedisOperation) {
	rc.status = ChekingIntegrity

	// Wait for cluster integrity check to finish
	err := operation.Wait()

	// Check integrity failed
	if err != nil {
		rc.status = CheckIntegrityError
		rc.logger.Info("Error checking cluster integrity", "error", err)
		return
	}

	// Check integrity finished successfully
	rc.status = Ready
	rc.logger.Info("Cluster integrity checked successfully")
}

// launchCheckOperation launches a check operation and returns the result
func (rc *RedisCluster) launchCheckOperation() (*ClusterCheckResult, error) {
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

// doCheckIntegrity checks the integrity of the Redis cluster
func (rc *RedisCluster) doCheckIntegrity(ctx context.Context) error {
	// Check nodes info
	if err := rc.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := rc.removeOutdatedNodes(ctx); err != nil {
		rc.logger.Error(err, "Error removing outdated nodes")
	}

	// Meet nodes if needed
	if err := rc.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := rc.ensureClusterRatio(ctx); err != nil {
		rc.logger.Error(err, "Error ensuring cluster ratio")
	}

	// Assign missing slots if needed
	if err := rc.assignMissingSlotsIfNeeded(ctx); err != nil {
		return err
	}

	// Fix cluster if needed
	if err := rc.fixClusterIfNeeded(ctx); err != nil {
		return err
	}

	// Balance cluster if needed
	if err := rc.balanceNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

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

// addNode adds a new Redis node to the cluster
func (rc *RedisCluster) addNode(name, addr string) *RedisNode {
	node := &RedisNode{
		Name:       name,
		Addr:       addr,
		MaxRetries: rc.GetClusterMaxRetries(),
		Backoff:    rc.GetClusterBackOff(),
	}

	// TODO: cluster meet, cluster rebalance, etc.

	rc.nodes[name] = node
	return node
}

// removeNode removes a Redis node from the cluster
func (rc *RedisCluster) removeNode(name string) {
	// TODO: cluster forget, cluster rebalance, etc.

	delete(rc.nodes, name)
}

// hasOperation returns true if the cluster has an operation with the specified name and status
func (rc *RedisCluster) hasOperation(name string, status string) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.Status == status {
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
		if operation.Status != status {
			continue
		}

		if operation.NodeFrom == nil || operation.NodeTo == nil {
			continue
		}

		if operation.NodeFrom.Name == from.Name && operation.NodeTo.Name == to.Name {
			return true
		}
	}
	return false
}

// addOperation adds a new operation to the cluster operations map
func (rc *RedisCluster) addOperation(operationName string, cmd RedisCommand, nodeFrom, nodeTo *RedisNode) *RedisOperation {
	operation := &RedisOperation{
		Name:          operationName,
		Status:        "Running",
		InitTimestamp: time.Now(),
		Cmd:           cmd,
		NodeFrom:      nodeFrom,
		NodeTo:        nodeTo,
	}
	if rc.operations[operationName] == nil {
		rc.operations[operationName] = make([]*RedisOperation, 0)
	}

	rc.operations[operationName] = append(rc.operations[operationName], operation)
	return operation
}

// getOperation returns the operation if the cluster has an operation with the specified name and status or nil otherwise
func (rc *RedisCluster) getOperation(name string, status string) *RedisOperation {
	operations, ok := rc.operations[name]
	if !ok {
		return nil
	}

	for _, operation := range operations {
		if operation.Status == status {
			return operation
		}
	}
	return nil
}

// updateNodesInfo updates the info of the Redis nodes in the cluster
func (rc *RedisCluster) updateNodesInfo(nodesInfo []RedisNode) {
	for _, nodeInfo := range nodesInfo {
		node := rc.getNodeFromID(nodeInfo.ID)
		if node == nil {
			continue
		}
		node.UpdateInfo(nodeInfo)
	}
}

// getNodeFromID returns the Redis node with the specified ID or nil if it doesn't exist
func (rc *RedisCluster) getNodeFromID(nodeID string) *RedisNode {
	for _, node := range rc.nodes {
		if node.ID == nodeID {
			return node
		}
	}
	return nil
}

// getMasters returns the masters in the Redis cluster
func (rc *RedisCluster) getMasters() []*RedisNode {
	masters := make([]*RedisNode, 0)
	for _, node := range rc.nodes {
		if node.IsMaster() {
			masters = append(masters, node)
		}
	}
	return masters
}

// getReplicas returns the replicas in the Redis cluster
func (rc *RedisCluster) getReplicas() []*RedisNode {
	replicas := make([]*RedisNode, 0)
	for _, node := range rc.nodes {
		if !node.IsMaster() {
			replicas = append(replicas, node)
		}
	}
	return replicas
}

// getMasterReplicas returns the replicas of the specified master
func (rc *RedisCluster) getMasterReplicas(master *RedisNode) []*RedisNode {
	replicas := make([]*RedisNode, 0)
	for _, node := range rc.nodes {
		if node.IsReplica() && node.MasterID == master.ID {
			replicas = append(replicas, node)
		}
	}
	return replicas
}

func (rc *RedisCluster) fix(async bool, force bool) error {
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

	// Launch cluster fixing operation
	operation, err := rc.launchFixOperation()
	if err != nil {
		return err
	}

	// Launch wait for fixing to finish synchronously or asynchronously depending on the async flag
	if async {
		go rc.waitForFixToFinish(operation)
	} else {
		rc.waitForFixToFinish(operation)
	}

	return nil
}

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
	time.Sleep(6 * time.Second)

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

	return nil
}

func (rc *RedisCluster) forgetNode(ctx context.Context, nodeToForget RedisNode) error {
	// Check if the node exists
	_, ok := rc.nodes[nodeToForget.Name]
	if !ok {
		return fmt.Errorf("node %s not found", nodeToForget.Name)
	}

	// Forget the node from all the other nodes
	for _, node := range rc.nodes {
		if node.Name == nodeToForget.Name {
			continue
		}

		if err := node.ForgetNode(ctx, nodeToForget); err != nil {
			return fmt.Errorf("error forgetting node %s from node %s: %w", node.Name, nodeToForget.Name, err)
		}

		rc.logger.Info("Node forgotten successfully", "node", nodeToForget.Name, "from", node.Name)
	}

	return nil
}

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
				rc.logger.Info("Cluster needs meet: IP of cluster node not found", "clusterNode", clusterNode)
				return true, nil
			}
		}
		// Every cluster node should see all the nodes.
		// If a node has forgotten any other node we need to meet nodes.
		if len(rc.nodes) > clusterNodeCount {
			rc.logger.Info("Cluster needs meet: node has less nodes than expected", "nodes", len(rc.nodes), "clusterNodeCount", clusterNodeCount, "node", node)
			return true, nil
		}
	}
	return false, nil
}

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

func (rc *RedisCluster) balanceNodesIfNeeded(ctx context.Context) error {
	// Check if the cluster is balanced
	if rc.IsBalanced() {
		return nil
	}

	// Balance the cluster
	if err := rc.Rebalance(false, nil, true); err != nil {
		return err
	}

	return nil
}

func (rc *RedisCluster) fixClusterIfNeeded(ctx context.Context) error {
	// Check if the cluster needs a fix
	needsFix, err := rc.needsFix(ctx)
	if err != nil || !needsFix {
		return nil
	}

	rc.logger.Info("Cluster needs fix")

	// Fix the cluster
	if err := rc.fix(false, true); err != nil {
		return err
	}

	return nil
}

func (rc *RedisCluster) ensureClusterRatio(ctx context.Context) error {
	// When all the nodes are ready, we need to make sure there is the right ratio of masters and replicas for the redis cluster
	// If there are too many replicas, we need to reset and add as a master
	//
	// If there are too few replicas, we need to reset and add as a
	// replica of a master with the least amount of replicas attached
	activeMasters := rc.getMasters()
	activeReplicas := rc.getReplicas()
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
		if err := rc.promoteNodesToMaster(ctx, convertableReplicas); err != nil {
			return err
		}
		return rc.ensureReplicaSpread(ctx)
	}

	// We have replicas but we don't want any: promote all to masters
	if len(activeReplicas) > 0 && rc.GetReplicasPerMaster() == 0 {
		rc.logger.Info("Promoting all replicas to masters", "replicas", len(activeReplicas))

		if err := rc.promoteNodesToMaster(ctx, activeReplicas); err != nil {
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

	masters := rc.getMasters()
	replicas := rc.getReplicas()

	// Find masters that need replicas
	for _, master := range masters {
		replicas := rc.getMasterReplicas(master)
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
		replicasPointedAtReplicas := rc.getMasterReplicas(replica)
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

	// Replicas that need to be moved
	for i := 0; i < len(masterNeedsReplicas); i++ {
		rc.logger.Info("Converting node to replica", "replica", replicaNeedsMove[i].Name, "master", masterNeedsReplicas[i].Name)

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
	weights := map[string]int{}
	for _, deletable := range nodesToConvert {
		weights[deletable.Name] = 0
	}
	if err := rc.Rebalance(false, weights, true); err != nil {
		return err
	}

	// Set the nodes to convert as replicas of the nodes to keep
	currentKeepable := 0
	for _, deletable := range nodesToConvert {
		if err := deletable.ReplicateNode(ctx, *nodesToKeep[currentKeepable]); err != nil {
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

// promoteNodesToMaster promotes the specified nodes to masters
func (rc *RedisCluster) promoteNodesToMaster(ctx context.Context, nodes []*RedisNode) error {
	// Reset nodes that will be promoted to masters
	for _, node := range nodes {
		if err := node.Reset(ctx); err != nil {
			return err
		}
		time.Sleep(5 * time.Second)
	}

	// Meet the cluster to promote the nodes to masters
	if err := rc.meetNodes(ctx); err != nil {
		return err
	}

	return nil
}

func (rc *RedisCluster) assignMissingSlots(ctx context.Context) error {
	rc.logger.Info("Assigning missing slots")

	// Get the master nodes
	masters := rc.getMasters()

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

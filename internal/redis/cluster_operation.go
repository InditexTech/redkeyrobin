// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"time"
)

// RemoveOutdatedOperations removes outdated operations from the cluster
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
	if err := rc.balanceNodesIfNeeded(ctx, nil); err != nil {
		return err
	}

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	return nil
}

// doScaleUp scales up the Redis cluster
func (rc *RedisCluster) doScaleUp(ctx context.Context) error {
	// Add new nodes if needed
	if err := rc.addNewNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Do integrity check
	if err := rc.doCheckIntegrity(ctx); err != nil {
		return err
	}

	return nil
}

// doScaleDown scales down the Redis cluster
func (rc *RedisCluster) doScaleDown(ctx context.Context) error {
	// TODO: check scale down with replicas
	// Check nodes info
	if err := rc.checkNodes(); err != nil {
		return err
	}

	// Remove nodes if needed
	if err := rc.removeNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Do integrity check
	if err := rc.doCheckIntegrity(ctx); err != nil {
		return err
	}

	return nil
}

func (rc *RedisCluster) doUpgrade(ctx context.Context) error {
	// Add new nodes if needed
	if err := rc.addNewNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Check nodes info
	if err := rc.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := rc.removeOutdatedNodes(ctx); err != nil {
		return err
	}

	// Remove nodes if needed
	if err := rc.removeNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Meet nodes if needed
	if err := rc.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := rc.ensureClusterRatio(ctx); err != nil {
		return nil
	}

	// Assign missing slots if needed
	if err := rc.assignMissingSlotsIfNeeded(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}

	return nil
}

func (rc *RedisCluster) doResetNode(ctx context.Context) error {
	// Get the node
	nodeName, ok := ctx.Value("nodeName").(string)
	if !ok {
		return fmt.Errorf("node name not found in context")
	}

	node := rc.GetNode(nodeName)
	if node == nil {
		return fmt.Errorf("node '%s' not found", nodeName)
	}

	// Reset the node
	if err := node.Reset(ctx); err != nil {
		return err
	}

	// Forget the node if it is ephemeral
	if rc.IsEphemeral() {
		if err := rc.forgetNode(ctx, *node); err != nil {
			return err
		}
	}

	// Check nodes info
	if err := rc.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := rc.removeOutdatedNodes(ctx); err != nil {
		return err
	}

	// Meet nodes if needed
	if err := rc.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := rc.ensureClusterRatio(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
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

	// Return the operation
	return rc.addOperation(Resharding, cmd, from, to), nil
}

// waitForReshardToFinish waits for the reshard operation to finish and updates the status
func (rc *RedisCluster) waitForReshardToFinish(operation *RedisOperation) error {
	rc.status = Resharding

	// Wait for reshard to finish
	err := operation.Wait()

	// Reshard failed
	if err != nil {
		rc.status = ReshardingError
		rc.logger.Info("Error resharding node", "error", err, "from", operation.NodeFrom.Name, "to", operation.NodeTo.Name)
		return err
	}

	// Reshard finished successfully
	rc.status = Ready
	rc.logger.Info("Slots moved successfully between nodes", "from", operation.NodeFrom.Name, "to", operation.NodeTo.Name)

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}
	return nil
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

	// Return the operation
	return rc.addOperation(Rebalancing, cmd, nil, nil), nil
}

// waitForRebalanceToFinish waits for the cluster rebalance to finish and updates the status
func (rc *RedisCluster) waitForRebalanceToFinish(operation *RedisOperation) error {
	// Wait for cluster rebalance to finish
	err := operation.Wait()

	// Rebalance failed
	if err != nil {
		rc.logger.Info("Error rebalancing cluster", "error", err)
		return err
	}

	// Rebalance finished successfully
	rc.logger.Info("Cluster rebalanced successfully")

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}
	return nil
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

	// Return the operation
	return rc.addOperation(Fixing, cmd, nil, nil), nil
}

// waitForFixToFinish waits for the cluster fix to finish and updates the status
func (rc *RedisCluster) waitForFixToFinish(operation *RedisOperation) error {
	// Wait for cluster fix to finish
	err := operation.Wait()

	// Fix failed
	if err != nil {
		rc.logger.Info("Error fixing cluster", "error", err, "stdout", operation.Cmd.GetStdout(), "stderr", operation.Cmd.GetStderr())
		return err
	}

	// Fix finished successfully
	rc.logger.Info("Cluster fixed successfully")

	// Update nodes info
	if err := rc.refreshNodes(); err != nil {
		return err
	}
	return nil
}

// launchCheckIntegrityOperation launches a check integrity operation
func (rc *RedisCluster) launchCheckIntegrityOperation() (*RedisOperation, error) {
	// Launch check integrity operation
	cmd := NewRedisLibraryCommand(rc.ctx, rc.doCheckIntegrity)
	cmd.Start()

	// Return the operation
	return rc.addOperation(CheckingIntegrity, cmd, nil, nil), nil
}

// waitForCheckIntegrityToFinish waits for the cluster check integrity to finish and updates the status
func (rc *RedisCluster) waitForCheckIntegrityToFinish(operation *RedisOperation) error {
	rc.status = CheckingIntegrity

	// Wait for cluster reconcile to finish
	err := operation.Wait()

	// Reconcile failed
	if err != nil {
		rc.status = CheckingIntegrityError
		rc.logger.Info("Error checking cluster integrity", "error", err)
		return err
	}

	// Check integrity finished successfully
	rc.status = Ready
	rc.logger.Info("Cluster integrity successfully checked")
	return nil
}

func (rc *RedisCluster) launchScaleUpOperation() (*RedisOperation, error) {
	// Launch scale up operation
	cmd := NewRedisLibraryCommand(rc.ctx, rc.doScaleUp)
	cmd.Start()

	// Return the operation
	return rc.addOperation(ScalingUp, cmd, nil, nil), nil
}

func (rc *RedisCluster) waitForScaleUpToFinish(operation *RedisOperation) error {
	rc.status = ScalingUp

	// Wait for scale up to finish
	err := operation.Wait()

	// Scale up failed
	if err != nil {
		rc.status = ScalingUpError
		rc.logger.Info("Error scaling up cluster", "error", err)
		return err
	}

	// Scale up finished successfully
	rc.status = Ready
	rc.logger.Info("Cluster scaled up successfully")
	return nil
}

func (rc *RedisCluster) launchScaleDownOperation() (*RedisOperation, error) {
	// Launch scale down operation
	cmd := NewRedisLibraryCommand(rc.ctx, rc.doScaleDown)
	cmd.Start()

	// Return the operation
	return rc.addOperation(ScalingDown, cmd, nil, nil), nil
}

func (rc *RedisCluster) waitForScaleDownToFinish(operation *RedisOperation) error {
	rc.status = ScalingDown

	// Wait for scale down to finish
	err := operation.Wait()

	// Scale down failed
	if err != nil {
		rc.status = ScalingDownError
		rc.logger.Info("Error scaling down cluster", "error", err)
		return err
	}

	// Scale down finished successfully
	rc.status = Ready
	rc.logger.Info("Cluster scaled down successfully")
	return nil
}

func (rc *RedisCluster) launchUpgradeOperation() (*RedisOperation, error) {
	// Launch upgrade operation
	cmd := NewRedisLibraryCommand(rc.ctx, rc.doUpgrade)
	cmd.Start()

	// Return the operation
	return rc.addOperation(Upgrading, cmd, nil, nil), nil
}

func (rc *RedisCluster) waitForUpgradeToFinish(operation *RedisOperation) error {
	rc.status = Upgrading

	// Wait for upgrading to finish
	err := operation.Wait()

	// Upgrading failed
	if err != nil {
		rc.status = UpgradingError
		rc.logger.Info("Error upgrading cluster", "error", err)
		return err
	}

	// Upgrading finished successfully
	rc.status = Ready
	rc.logger.Info("Cluster upgraded successfully")
	return nil
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

// launchResetNodeOperation launches a reset node operation and returns the result
func (rc *RedisCluster) launchResetNodeOperation(node *RedisNode) (*RedisOperation, error) {
	// Create context with node name
	ctx := context.WithValue(rc.ctx, "nodeName", node.Name)

	// Launch upgrade operation
	cmd := NewRedisLibraryCommand(ctx, rc.doResetNode)
	cmd.Start()

	// Return the operation
	return rc.addOperation(Resetting, cmd, node, nil), nil
}

func (rc *RedisCluster) waitForResetNodeToFinish(operation *RedisOperation) error {
	// Wait for reset node to finish
	err := operation.Wait()

	// Reset node failed
	if err != nil {
		rc.logger.Info("Error resetting cluster node", "error", err, "node", operation.NodeFrom.Name)
		return err
	}

	// Reset node finished successfully
	rc.logger.Info("Cluster node resetted successfully", "node", operation.NodeFrom.Name)
	return nil
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

// hasOperationInNode returns true if the cluster has an operation with the specified name and status in the specified node
func (rc *RedisCluster) hasOperationInNode(name string, status string, node RedisNode) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.Status != status {
			continue
		}

		if operation.NodeFrom != nil && operation.NodeFrom.Name == node.Name {
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

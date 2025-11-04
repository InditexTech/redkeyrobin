// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/inditextech/redkeyrobin/internal/util"
)

// RedKeyStandalone represents a standalone Redis
type RedKeyStandalone struct {
	clusterBase
	node *redis.RedisNode
}

func NewRedKeyStandalone(ctx context.Context, conf *config.Configuration) Cluster {
	return &RedKeyStandalone{
		clusterBase: clusterBase{
			ctx:    ctx,
			logger: util.GetLogger("redis-standalone"),
			conf:   conf,
			status: Ready,
		},
	}
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- GETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// GetNodes returns the nodes of the cluster.
func (rc *RedKeyStandalone) GetNodes() []*redis.RedisNode {
	return []*redis.RedisNode{rc.node}
}

// GetNode returns the node with the provided name.
func (rc *RedKeyStandalone) GetNode(name string) *redis.RedisNode {
	if rc.node.Name == name {
		return rc.node
	}
	return nil
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- SETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// SetReplicas sets the number of replicas of the cluster.
func (rc *RedKeyStandalone) SetReplicas(replicas int, replicasPerMaster *int) error {
	return nil
}

// SetRedKeyClusterStatus sets the status of the RedKey cluster
func (rc *RedKeyStandalone) SetRedKeyClusterStatus(status string) error {
	rc.logger.Info("Changing Redis Standalone status", "current", rc.GetRedKeyClusterStatus(), "desired", status)
	rc.conf.Redis.Cluster.Status = status
	return nil
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- ASKERS ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// IsStandalone returns true if the cluster is standalone.
func (rc *RedKeyStandalone) IsStandalone() bool {
	return true
}

// IsBalanced returns true if the cluster is balanced.
func (rc *RedKeyStandalone) IsBalanced() bool {
	return true
}

// IsScaled returns true if the cluster is scaled.
func (rc *RedKeyStandalone) IsScaled() bool {
	return true
}

// IsUpgraded returns true if the cluster is upgraded.
func (rc *RedKeyStandalone) IsUpgraded() bool {
	return true
}

// IsEphemeral returns true if the cluster is ephemeral.
func (rc *RedKeyStandalone) IsEphemeral() bool {
	return true
}

// CanBeUpgraded returns true if the cluster can be upgraded.
func (rc *RedKeyStandalone) CanBeUpgraded() bool {
	return true
}

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PUBLIC  ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// Init initializes the cluster.
func (rc *RedKeyStandalone) Init() error {
	nodeName := fmt.Sprintf("%s-0", rc.GetName())
	nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
	rc.createNode(nodeName, nodeAddr)

	return nil
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------- PUBLIC OPERATIONS -----------------------------------------
// ----------------------------------------------------------------------------------------------------

// Check checks the cluster.
func (rc *RedKeyStandalone) Check() (*redis.ClusterCheckResult, error) {
	return nil, nil
}

// Fix fixes the cluster.
func (rc *RedKeyStandalone) Fix(async bool, force bool) error {
	return nil
}

// Rebalance rebalances the cluster.
func (rc *RedKeyStandalone) Rebalance(async bool, weights map[string]int, force bool) error {
	return nil
}

// MoveSlots returns the slots of the cluster.
func (rc *RedKeyStandalone) MoveSlots(from, to *redis.RedisNode, slots int) error {
	return nil
}

// CheckIntegrity checks the integrity of the cluster.
func (rc *RedKeyStandalone) CheckIntegrity(async, force bool) error {
	nodeName := fmt.Sprintf("%s-0", rc.GetName())
	nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())

	// Get the node
	node := rc.GetNode(nodeName)
	if node == nil {
		rc.logger.Warn("Redis standalone has not been initialized. Creating new node")
		node = rc.createNode(nodeName, nodeAddr)
	}

	// Init a fresh node to check if IP has changed. This can happen if the node has been restarted
	freshNode := redis.NewRedisNode(nodeName, node.Addr, rc.GetClusterMaxRetries(), rc.GetClusterBackOff())
	if err := freshNode.InitStandalone(rc.ctx); err != nil {
		return err
	}

	// Update IP
	if node.IP != freshNode.IP {
		rc.logger.Info("Node IP has changed", "node", nodeName, "oldIP", node.IP, "newIP", freshNode.IP)
		node.SetIP(freshNode.IP)
	}

	return nil
}

// ScaleUp scales up the cluster.
func (rc *RedKeyStandalone) ScaleUp(force bool) error {
	return nil
}

// ScaleDown scales down the cluster.
func (rc *RedKeyStandalone) ScaleDown(force bool) error {
	return nil
}

// Upgrade upgrades the cluster.
func (rc *RedKeyStandalone) Upgrade(force bool) error {
	return nil
}

// ResetNode resets a node of the cluster.
func (rc *RedKeyStandalone) ResetNode(node *redis.RedisNode) error {
	return nil
}

// Clears the stored nodes.
func (rc *RedKeyStandalone) ClearNodes() error {
	return nil
}

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PRIVATE ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// ensureNodesAreUp ensures that all nodes are up.
func (rc *RedKeyStandalone) ensureNodesAreUp(ctx context.Context) error {
	// For standalone, this is a no-op
	return nil
}

// convertNodesToMaster converts the provided nodes to master.
func (rc *RedKeyStandalone) convertNodesToMaster(ctx context.Context, nodes []*redis.RedisNode) error {
	// For standalone, this is a no-op
	return nil
}

// getRedisClient returns a Redis client and checks the connection.
func (rc *RedKeyStandalone) getRedisClient(close bool) (redis.RedisClientInterface, error) {
	// For standalone, return a simple implementation or nil
	return nil, nil
}

// refreshNodesInfo refreshes the nodes of the cluster.
func (rc *RedKeyStandalone) refreshNodesInfo() error {
	// For standalone, this is a no-op
	return nil
}

// checkNodes checks the nodes of the cluster.
func (rc *RedKeyStandalone) checkNodes() error {
	// For standalone, this is a no-op
	return nil
}

// removeOutdatedNodes removes outdated nodes from the cluster.
func (rc *RedKeyStandalone) removeOutdatedNodes(ctx context.Context) error {
	// For standalone, this is a no-op
	return nil
}

// meetNodesIfNeeded meets nodes if needed.
func (rc *RedKeyStandalone) meetNodesIfNeeded(ctx context.Context) error {
	// For standalone, this is a no-op
	return nil
}

// ensureClusterRatio ensures the cluster ratio.
func (rc *RedKeyStandalone) ensureClusterRatio(ctx context.Context) error {
	// For standalone, this is a no-op
	return nil
}

// assignMissingSlotsIfNeeded assigns missing slots if needed.
func (rc *RedKeyStandalone) assignMissingSlotsIfNeeded(ctx context.Context) error {
	// For standalone, this is a no-op
	return nil
}

// fixClusterIfNeeded fixes the cluster if needed.
func (rc *RedKeyStandalone) fixClusterIfNeeded(ctx context.Context) error {
	// For standalone, this is a no-op
	return nil
}

// balanceClusterIfNeeded balances the cluster if needed.
func (rc *RedKeyStandalone) balanceClusterIfNeeded(weights map[string]int) error {
	// For standalone, this is a no-op
	return nil
}

// forgetNode forgets a node from the cluster.
func (rc *RedKeyStandalone) forgetNode(ctx context.Context, node redis.RedisNode) error {
	// For standalone, this is a no-op
	return nil
}

// removeNode removes a node from the cluster.
func (rc *RedKeyStandalone) removeNode(ctx context.Context, node redis.RedisNode) error {
	// For standalone, this is a no-op
	return nil
}

// removeNodesIfNeeded removes nodes if needed.
func (rc *RedKeyStandalone) removeNodesIfNeeded(ctx context.Context) error {
	// For standalone, this is a no-op
	return nil
}

// addNewNodesIfNeeded adds new nodes if needed.
func (rc *RedKeyStandalone) addNewNodesIfNeeded() error {
	// For standalone, this is a no-op
	return nil
}

// createNode creates a node and set it as the node to handle by RedisStandalone
func (rc *RedKeyStandalone) createNode(name, addr string) *redis.RedisNode {
	node := redis.NewRedisNode(name, addr, rc.GetClusterMaxRetries(), rc.GetClusterBackOff())

	rc.logger.Info("Initializing standalone node", "node", node.Name)
	if err := node.InitStandalone(rc.ctx); err != nil {
		rc.logger.Error("Error initializing standalone node", "error", err, "node", node.Name)
	} else {
		rc.logger.Info("Standalone node initialized successfully", "node", node.Name, "IP", node.IP)
	}

	rc.node = node
	return node
}

// stabilizeOpenSlots stabilizes open slots in the cluster.
func (rc *RedKeyStandalone) stabilizeOpenSlots(ctx context.Context, counter map[int]int, threshold int) (map[int]int, error) {
	// For standalone, this is a no-op
	return nil, nil
}

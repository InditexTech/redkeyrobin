// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/inditextech/redisrobin/internal/util"
)

// RedKeyStandalone represents a standalone Redis
type RedKeyStandalone struct {
	clusterBase
	node *redis.RedisNode
}

func NewRedKeyStandalone(ctx context.Context, conf *config.Configuration) *RedKeyStandalone {
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
	freshNode := redis.RedisNode{
		Name:       nodeName,
		Addr:       node.Addr,
		MaxRetries: rc.GetClusterMaxRetries(),
		Backoff:    rc.GetClusterBackOff(),
	}
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

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PRIVATE ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// createNode creates a node and set it as the node to handle by RedisStandalone
func (rc *RedKeyStandalone) createNode(name, addr string) *redis.RedisNode {
	node := &redis.RedisNode{
		Name:       name,
		Addr:       addr,
		MaxRetries: rc.GetClusterMaxRetries(),
		Backoff:    rc.GetClusterBackOff(),
	}

	rc.logger.Info("Initializing standalone node", "node", node.Name)
	if err := node.InitStandalone(rc.ctx); err != nil {
		rc.logger.Error("Error initializing standalone node", "error", err, "node", node.Name)
	} else {
		rc.logger.Info("Standalone node initialized successfully", "node", node.Name, "IP", node.IP)
	}

	rc.node = node
	return node
}

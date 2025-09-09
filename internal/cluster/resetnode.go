// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/inditextech/redkeyrobin/internal/util"
)

// RedisOperationResetNode represents a reset node operation for a RedKey cluster.
type RedisOperationResetNode struct {
	RedisOperationBase
}

func NewRedisOperationResetNode(ctx context.Context, redkeyCluster Cluster, node *redis.RedisNode) *RedisOperationResetNode {
	return &RedisOperationResetNode{
		RedisOperationBase: RedisOperationBase{
			name:     "ResetNode",
			status:   "Pending",
			logger:   util.GetLogger("operation.resetnode"),
			ctx:      ctx,
			cluster:  redkeyCluster,
			nodeFrom: node,
		},
	}
}

func NewFakeRedisOperationResetNode(ctx context.Context, redkeyCluster Cluster, status string, node *redis.RedisNode) *RedisOperationResetNode {
	return &RedisOperationResetNode{
		RedisOperationBase: RedisOperationBase{
			name:     "ResetNode",
			status:   status,
			logger:   util.GetLogger("operation.resetnode"),
			ctx:      ctx,
			cluster:  redkeyCluster,
			nodeFrom: node,
		},
	}
}

// Launch launches the reset node operation.
func (ro *RedisOperationResetNode) Launch() error {
	ro.logger.Info("Resetting node", "node", ro.nodeFrom.Name)

	// Create context with node name
	ctx := context.WithValue(ro.ctx, nodeNameKey, ro.nodeFrom.Name)

	// Launch upgrade operation
	cmd := redis.NewRedisLibraryCommand(ctx, ro.doResetNode)
	cmd.Start()

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

// Wait waits for the reset node operation to finish.
func (ro *RedisOperationResetNode) Wait() error {
	// Wait for reset node to finish
	err := ro.Run()

	// Reset node failed
	if err != nil {
		return fmt.Errorf("error resetting cluster node '%s': %v", ro.nodeFrom.Name, err)
	}

	// Reset node successfully
	ro.logger.Info("Cluster node resetted successfully", "node", ro.nodeFrom.Name)

	return nil
}

// doResetNode reset a node of the RedKey cluster
func (ro *RedisOperationResetNode) doResetNode(ctx context.Context) error {
	// Get the node
	nodeName, ok := ctx.Value(nodeNameKey).(string)
	if !ok {
		return fmt.Errorf("node name not found in context")
	}

	node := ro.cluster.GetNode(nodeName)
	if node == nil {
		return fmt.Errorf("node '%s' not found", nodeName)
	}

	// Reset the node
	if err := node.Reset(ctx); err != nil {
		return err
	}

	// Forget the node if it is ephemeral
	if ro.cluster.IsEphemeral() {
		if err := ro.cluster.forgetNode(ctx, *node); err != nil {
			return err
		}
	}

	// Check nodes info
	if err := ro.cluster.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.cluster.removeOutdatedNodes(ctx); err != nil {
		return err
	}

	// Meet nodes if needed
	if err := ro.cluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.cluster.ensureClusterRatio(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.cluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

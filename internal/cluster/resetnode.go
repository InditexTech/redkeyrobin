// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/inditextech/redisrobin/internal/util"
)

// RedisOperationResetNode represents a reset node operation for a RedKey cluster.
type RedisOperationResetNode struct {
	RedisOperationBase
}

func NewRedisOperationResetNode(ctx context.Context, redkeyCluster *RedKeyCluster, node *redis.RedisNode) *RedisOperationResetNode {
	return &RedisOperationResetNode{
		RedisOperationBase: RedisOperationBase{
			name:          "ResetNode",
			status:        "Pending",
			logger:        util.GetLogger("operation.resetnode"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
			nodeFrom:      node,
		},
	}
}

func NewFakeRedisOperationResetNode(ctx context.Context, redkeyCluster *RedKeyCluster, status string, node *redis.RedisNode) *RedisOperationResetNode {
	return &RedisOperationResetNode{
		RedisOperationBase: RedisOperationBase{
			name:          "ResetNode",
			status:        status,
			logger:        util.GetLogger("operation.resetnode"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
			nodeFrom:      node,
		},
	}
}

// Launch launches the reset node operation.
func (ro *RedisOperationResetNode) Launch() error {
	ro.logger.Info("Resetting node", "node", ro.nodeFrom.Name)

	// Create context with node name
	ctx := context.WithValue(ro.ctx, "nodeName", ro.nodeFrom.Name)

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
	nodeName, ok := ctx.Value("nodeName").(string)
	if !ok {
		return fmt.Errorf("node name not found in context")
	}

	node := ro.redkeyCluster.GetNode(nodeName)
	if node == nil {
		return fmt.Errorf("node '%s' not found", nodeName)
	}

	// Reset the node
	if err := node.Reset(ctx); err != nil {
		return err
	}

	// Forget the node if it is ephemeral
	if ro.redkeyCluster.IsEphemeral() {
		if err := ro.redkeyCluster.forgetNode(ctx, *node); err != nil {
			return err
		}
	}

	// Check nodes info
	if err := ro.redkeyCluster.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.redkeyCluster.removeOutdatedNodes(ctx); err != nil {
		return err
	}

	// Meet nodes if needed
	if err := ro.redkeyCluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.redkeyCluster.ensureClusterRatio(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.redkeyCluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

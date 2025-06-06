// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package rediscluster

import (
	"context"
	"fmt"
	"time"

	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/inditextech/redisrobin/internal/util"
)

type RedisOperationResetNode struct {
	RedisOperationBase
}

func NewRedisOperationResetNode(ctx context.Context, redisCluster *RedisCluster, node *redis.RedisNode) *RedisOperationResetNode {
	return &RedisOperationResetNode{
		RedisOperationBase: RedisOperationBase{
			name:         "ResetNode",
			status:       "Pending",
			logger:       util.GetLogger("operation.resetnode"),
			ctx:          ctx,
			redisCluster: redisCluster,
			nodeFrom:     node,
		},
	}
}

func NewFakeRedisOperationResetNode(ctx context.Context, redisCluster *RedisCluster, status string, node *redis.RedisNode) *RedisOperationResetNode {
	return &RedisOperationResetNode{
		RedisOperationBase: RedisOperationBase{
			name:         "ResetNode",
			status:       status,
			logger:       util.GetLogger("operation.resetnode"),
			ctx:          ctx,
			redisCluster: redisCluster,
			nodeFrom:     node,
		},
	}
}

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

// doResetNode reset a node of the Redis cluster
func (ro *RedisOperationResetNode) doResetNode(ctx context.Context) error {
	// Get the node
	nodeName, ok := ctx.Value("nodeName").(string)
	if !ok {
		return fmt.Errorf("node name not found in context")
	}

	node := ro.redisCluster.GetNode(nodeName)
	if node == nil {
		return fmt.Errorf("node '%s' not found", nodeName)
	}

	// Reset the node
	if err := node.Reset(ctx); err != nil {
		return err
	}

	// Forget the node if it is ephemeral
	if ro.redisCluster.IsEphemeral() {
		if err := ro.redisCluster.forgetNode(ctx, *node); err != nil {
			return err
		}
	}

	// Check nodes info
	if err := ro.redisCluster.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.redisCluster.removeOutdatedNodes(ctx); err != nil {
		return err
	}

	// Meet nodes if needed
	if err := ro.redisCluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.redisCluster.ensureClusterRatio(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.redisCluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

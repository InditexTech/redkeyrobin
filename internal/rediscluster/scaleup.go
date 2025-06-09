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

// RedisOperationScaleUp represents a scale up operation for a Redis cluster.
type RedisOperationScaleUp struct {
	RedisOperationBase
}

func NewRedisOperationScaleUp(ctx context.Context, redisCluster *RedisCluster) *RedisOperationScaleUp {
	return &RedisOperationScaleUp{
		RedisOperationBase: RedisOperationBase{
			name:         "ScaleUp",
			status:       "Pending",
			logger:       util.GetLogger("operation.scaleup"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func NewFakeRedisOperationScaleUp(ctx context.Context, redisCluster *RedisCluster, status string) *RedisOperationScaleUp {
	return &RedisOperationScaleUp{
		RedisOperationBase: RedisOperationBase{
			name:         "ScaleUp",
			status:       status,
			logger:       util.GetLogger("operation.scaleup"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

// Launch launches the scale up operation.
func (ro *RedisOperationScaleUp) Launch() error {
	ro.logger.Info("Scaling up cluster")

	// Launch scale up operation
	cmd := redis.NewRedisLibraryCommand(ro.ctx, ro.doScaleUp)
	cmd.Start()

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

// Wait waits for the scale up operation to finish.
func (ro *RedisOperationScaleUp) Wait() error {
	ro.redisCluster.status = ScalingUp

	// Wait for scale up to finish
	err := ro.Run()

	// Scale up failed
	if err != nil {
		ro.redisCluster.status = ScalingUpError
		return fmt.Errorf("error scaling up cluster: %v", err)
	}

	// Scale up finished successfully
	ro.redisCluster.status = Ready
	ro.logger.Info("Cluster scaled up successfully")

	return nil
}

// doScaleUp scales up the Redis cluster
func (ro *RedisOperationScaleUp) doScaleUp(ctx context.Context) error {
	// Add new nodes if needed
	if err := ro.redisCluster.addNewNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Check nodes info
	if err := ro.redisCluster.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.redisCluster.removeOutdatedNodes(ctx); err != nil {
		ro.logger.Error("Error removing outdated nodes", "error", err)
	}

	// Meet nodes if needed
	if err := ro.redisCluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.redisCluster.ensureClusterRatio(ctx); err != nil {
		return err
	}

	// Assign missing slots if needed
	if err := ro.redisCluster.assignMissingSlotsIfNeeded(ctx); err != nil {
		return err
	}

	// Fix cluster if needed
	if err := ro.redisCluster.fixClusterIfNeeded(ctx); err != nil {
		return err
	}

	// Balance cluster if needed
	if err := ro.redisCluster.balanceClusterIfNeeded(ctx, nil); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.redisCluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

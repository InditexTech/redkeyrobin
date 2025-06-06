// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/inditextech/redisrobin/internal/util"
)

type RedisOperationScaleDown struct {
	RedisOperationBase
}

func NewRedisOperationScaleDown(ctx context.Context, redisCluster *RedisCluster) *RedisOperationScaleDown {
	return &RedisOperationScaleDown{
		RedisOperationBase: RedisOperationBase{
			name:         "ScaleDown",
			status:       "Pending",
			logger:       util.GetLogger("operation.scaledown"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func NewFakeRedisOperationScaleDown(ctx context.Context, redisCluster *RedisCluster, status string) *RedisOperationScaleDown {
	return &RedisOperationScaleDown{
		RedisOperationBase: RedisOperationBase{
			name:         "ScaleDown",
			status:       status,
			logger:       util.GetLogger("operation.scaledown"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func (ro *RedisOperationScaleDown) Launch() error {
	ro.logger.Info("Scaling down cluster")

	// Launch scale down operation
	cmd := NewRedisLibraryCommand(ro.ctx, ro.doScaleDown)
	cmd.Start()

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

func (ro *RedisOperationScaleDown) Wait() error {
	ro.redisCluster.status = ScalingDown

	// Wait for scale down to finish
	err := ro.Run()

	// Scale down failed
	if err != nil {
		ro.redisCluster.status = ScalingDownError
		return fmt.Errorf("error scaling down cluster: %v", err)
	}

	// Scale down finished successfully
	ro.redisCluster.status = Ready
	ro.logger.Info("Cluster scaled down successfully")

	return nil
}

// doScaleDown scales down the Redis cluster
func (ro *RedisOperationScaleDown) doScaleDown(ctx context.Context) error {
	// Check nodes info
	if err := ro.redisCluster.checkNodes(); err != nil {
		return err
	}

	// Remove nodes if needed
	if err := ro.redisCluster.removeNodesIfNeeded(ctx); err != nil {
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

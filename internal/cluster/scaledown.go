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

// RedisOperationScaleDown represents a scale down operation for a RedKey cluster .
type RedisOperationScaleDown struct {
	RedisOperationBase
}

func NewRedisOperationScaleDown(ctx context.Context, redkeyCluster *RedKeyCluster) *RedisOperationScaleDown {
	return &RedisOperationScaleDown{
		RedisOperationBase: RedisOperationBase{
			name:          "ScaleDown",
			status:        "Pending",
			logger:        util.GetLogger("operation.scaledown"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
		},
	}
}

func NewFakeRedisOperationScaleDown(ctx context.Context, redkeyCluster *RedKeyCluster, status string) *RedisOperationScaleDown {
	return &RedisOperationScaleDown{
		RedisOperationBase: RedisOperationBase{
			name:          "ScaleDown",
			status:        status,
			logger:        util.GetLogger("operation.scaledown"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
		},
	}
}

// Launch launches the scale down operation.
func (ro *RedisOperationScaleDown) Launch() error {
	ro.logger.Info("Scaling down cluster")

	// Launch scale down operation
	cmd := redis.NewRedisLibraryCommand(ro.ctx, ro.doScaleDown)
	cmd.Start()

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

// Wait waits for the scale down operation to finish.
func (ro *RedisOperationScaleDown) Wait() error {
	ro.redkeyCluster.status = ScalingDown

	// Wait for scale down to finish
	err := ro.Run()

	// Scale down failed
	if err != nil {
		ro.redkeyCluster.status = ScalingDownError
		return fmt.Errorf("error scaling down cluster: %v", err)
	}

	// Scale down finished successfully
	ro.redkeyCluster.status = Ready
	ro.logger.Info("Cluster scaled down successfully")

	return nil
}

// doScaleDown scales down the RedKey cluster
func (ro *RedisOperationScaleDown) doScaleDown(ctx context.Context) error {
	// Check nodes info
	if err := ro.redkeyCluster.checkNodes(); err != nil {
		return err
	}

	// Remove nodes if needed
	if err := ro.redkeyCluster.removeNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Check nodes info
	if err := ro.redkeyCluster.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.redkeyCluster.removeOutdatedNodes(ctx); err != nil {
		ro.logger.Error("Error removing outdated nodes", "error", err)
	}

	// Meet nodes if needed
	if err := ro.redkeyCluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.redkeyCluster.ensureClusterRatio(ctx); err != nil {
		return err
	}

	// Assign missing slots if needed
	if err := ro.redkeyCluster.assignMissingSlotsIfNeeded(ctx); err != nil {
		return err
	}

	// Fix cluster if needed
	if err := ro.redkeyCluster.fixClusterIfNeeded(ctx); err != nil {
		return err
	}

	// Balance cluster if needed
	if err := ro.redkeyCluster.balanceClusterIfNeeded(nil); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.redkeyCluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

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

// RedisOperationScaleUp represents a scale up operation for a RedKey cluster .
type RedisOperationScaleUp struct {
	RedisOperationBase
}

func NewRedisOperationScaleUp(ctx context.Context, redkeyCluster *RedKeyCluster) *RedisOperationScaleUp {
	return &RedisOperationScaleUp{
		RedisOperationBase: RedisOperationBase{
			name:          "ScaleUp",
			status:        "Pending",
			logger:        util.GetLogger("operation.scaleup"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
		},
	}
}

func NewFakeRedisOperationScaleUp(ctx context.Context, redkeyCluster *RedKeyCluster, status string) *RedisOperationScaleUp {
	return &RedisOperationScaleUp{
		RedisOperationBase: RedisOperationBase{
			name:          "ScaleUp",
			status:        status,
			logger:        util.GetLogger("operation.scaleup"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
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
	ro.redkeyCluster.status = ScalingUp

	// Wait for scale up to finish
	err := ro.Run()

	// Scale up failed
	if err != nil {
		ro.redkeyCluster.status = ScalingUpError
		return fmt.Errorf("error scaling up cluster: %v", err)
	}

	// Scale up finished successfully
	ro.redkeyCluster.status = Ready
	ro.logger.Info("Cluster scaled up successfully")

	return nil
}

// doScaleUp scales up the RedKey cluster
func (ro *RedisOperationScaleUp) doScaleUp(ctx context.Context) error {
	// Add new nodes if needed
	if err := ro.redkeyCluster.addNewNodesIfNeeded(); err != nil {
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

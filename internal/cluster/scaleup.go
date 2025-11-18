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

// RedisOperationScaleUp represents a scale up operation for a RedKey cluster.
type RedisOperationScaleUp struct {
	RedisOperationBase
}

func NewRedisOperationScaleUp(ctx context.Context, cluster Cluster) *RedisOperationScaleUp {
	return &RedisOperationScaleUp{
		RedisOperationBase: RedisOperationBase{
			name:    "ScaleUp",
			status:  "Pending",
			logger:  util.GetLogger("operation.scaleup"),
			ctx:     ctx,
			cluster: cluster,
		},
	}
}

func NewFakeRedisOperationScaleUp(ctx context.Context, cluster Cluster, status string) *RedisOperationScaleUp {
	return &RedisOperationScaleUp{
		RedisOperationBase: RedisOperationBase{
			name:    "ScaleUp",
			status:  status,
			logger:  util.GetLogger("operation.scaleup"),
			ctx:     ctx,
			cluster: cluster,
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
	ro.cluster.SetStatus(ScalingUp)

	// Wait for scale up to finish
	err := ro.Run()

	// Scale up failed
	if err != nil {
		ro.cluster.SetStatus(ScalingUpError)
		return fmt.Errorf("error scaling up cluster: %v", err)
	}

	// Scale up finished successfully
	ro.cluster.SetStatus(Ready)
	ro.logger.Info("Cluster scaled up successfully")

	return nil
}

// doScaleUp scales up the RedKey cluster
func (ro *RedisOperationScaleUp) doScaleUp(ctx context.Context) error {
	// Add new nodes if needed
	if err := ro.cluster.addNewNodesIfNeeded(); err != nil {
		return err
	}

	// Check nodes info
	if err := ro.cluster.checkNodes(false); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.cluster.removeOutdatedNodes(ctx); err != nil {
		ro.logger.Error("Error removing outdated nodes", "error", err)
	}

	// Meet nodes if needed
	if err := ro.cluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.cluster.ensureClusterRatio(ctx); err != nil {
		return err
	}

	// Assign missing slots if needed
	if err := ro.cluster.assignMissingSlotsIfNeeded(ctx); err != nil {
		return err
	}

	// Fix cluster if needed
	if err := ro.cluster.fixClusterIfNeeded(ctx); err != nil {
		return err
	}

	// Balance cluster if needed
	if err := ro.cluster.balanceClusterIfNeeded(nil); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.cluster.refreshNodesInfo(); err != nil {
		return err
	}

	return nil
}

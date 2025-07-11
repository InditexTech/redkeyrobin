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

// RedisOperationCheckIntegrity represents a check integrity operation for a Redis cluster.
type RedisOperationCheckIntegrity struct {
	RedisOperationBase
}

func NewRedisOperationCheckIntegrity(ctx context.Context, redisCluster *RedisCluster) *RedisOperationCheckIntegrity {
	return &RedisOperationCheckIntegrity{
		RedisOperationBase: RedisOperationBase{
			name:         "CheckIntegrity",
			status:       "Pending",
			logger:       util.GetLogger("operation.integrity"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func NewFakeRedisOperationCheckIntegrity(ctx context.Context, redisCluster *RedisCluster, status string) *RedisOperationCheckIntegrity {
	return &RedisOperationCheckIntegrity{
		RedisOperationBase: RedisOperationBase{
			name:         "CheckIntegrity",
			status:       status,
			logger:       util.GetLogger("operation.integrity"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

// Launch launches the check integrity operation.
func (ro *RedisOperationCheckIntegrity) Launch() error {
	ro.logger.Info("Checking cluster integrity")

	// Launch check integrity operation
	cmd := redis.NewRedisLibraryCommand(ro.ctx, ro.doCheckIntegrity)
	cmd.Start()

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

// Wait waits for the check integrity operation to finish.
func (ro *RedisOperationCheckIntegrity) Wait() error {
	ro.redisCluster.status = CheckingIntegrity

	// Wait for check cluster integrity to finish
	err := ro.Run()

	// Fix failed
	if err != nil {
		ro.redisCluster.status = CheckingIntegrityError
		return fmt.Errorf("error checking cluster integrity: %v", err)
	}

	// Check integrity finished successfully
	ro.redisCluster.status = Ready
	ro.logger.Info("Cluster integrity successfully checked")

	return nil
}

// doCheckIntegrity checks the integrity of the Redis cluster
func (ro *RedisOperationCheckIntegrity) doCheckIntegrity(ctx context.Context) error {
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

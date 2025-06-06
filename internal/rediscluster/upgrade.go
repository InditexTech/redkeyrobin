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

type RedisOperationUpgrade struct {
	RedisOperationBase
}

func NewRedisOperationUpgrade(ctx context.Context, redisCluster *RedisCluster) *RedisOperationUpgrade {
	return &RedisOperationUpgrade{
		RedisOperationBase: RedisOperationBase{
			name:         "Upgrade",
			status:       "Pending",
			logger:       util.GetLogger("operation.ugrade"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func NewFakeRedisOperationUpgrade(ctx context.Context, redisCluster *RedisCluster, status string) *RedisOperationUpgrade {
	return &RedisOperationUpgrade{
		RedisOperationBase: RedisOperationBase{
			name:         "Upgrade",
			status:       status,
			logger:       util.GetLogger("operation.upgrade"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func (ro *RedisOperationUpgrade) Launch() error {
	ro.logger.Info("Scaling down cluster")

	// Launch upgrade operation
	cmd := redis.NewRedisLibraryCommand(ro.ctx, ro.doUpgrade)
	cmd.Start()

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

func (ro *RedisOperationUpgrade) Wait() error {
	ro.redisCluster.status = Upgrading

	// Wait for upgrade to finish
	err := ro.Run()

	// Upgrade failed
	if err != nil {
		ro.redisCluster.status = UpgradingError
		return fmt.Errorf("error upgrading cluster: %v", err)
	}

	// Upgrade finished successfully
	ro.redisCluster.status = Ready
	ro.logger.Info("Cluster upgraded successfully")

	return nil
}

// doUpgrade upgrades the Redis cluster
func (ro *RedisOperationUpgrade) doUpgrade(ctx context.Context) error {
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
		return err
	}

	// Remove nodes if needed
	if err := ro.redisCluster.removeNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Meet nodes if needed
	if err := ro.redisCluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.redisCluster.ensureClusterRatio(ctx); err != nil {
		return nil
	}

	// Assign missing slots if needed
	if err := ro.redisCluster.assignMissingSlotsIfNeeded(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.redisCluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

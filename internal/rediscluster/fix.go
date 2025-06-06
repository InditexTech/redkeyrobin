// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package rediscluster

import (
	"context"
	"fmt"
	"time"

	"github.com/inditextech/redisrobin/internal/util"
)

type RedisOperationFix struct {
	RedisOperationBase
}

func NewRedisOperationFix(ctx context.Context, redisCluster *RedisCluster) *RedisOperationFix {
	return &RedisOperationFix{
		RedisOperationBase: RedisOperationBase{
			name:         "Fix",
			status:       "Pending",
			logger:       util.GetLogger("operation.fix"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func NewFakeRedisOperationFix(ctx context.Context, redisCluster *RedisCluster, status string) *RedisOperationFix {
	return &RedisOperationFix{
		RedisOperationBase: RedisOperationBase{
			name:         "Fix",
			status:       status,
			logger:       util.GetLogger("operation.fix"),
			ctx:          ctx,
			redisCluster: redisCluster,
		},
	}
}

func (ro *RedisOperationFix) Launch() error {
	ro.logger.Info("Fixing cluster")

	// Assure all nodes are up (redis-cli needs all nodes to be up)
	if err := ro.redisCluster.ensureNodesAreUp(ro.ctx); err != nil {
		return fmt.Errorf("error ensuring nodes are up: %v", err)
	}

	// Get Redis client and check connection
	redisClient, err := ro.redisCluster.getAndCheckRedisClient(true)
	if err != nil {
		return fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Launch fix operation
	cmd := redisClient.ClusterFix(ro.ctx)
	if cmd.Err != nil {
		return fmt.Errorf("error fixing cluster: %v", cmd.Err)
	}

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

func (ro *RedisOperationFix) Wait() error {
	// Wait for cluster fix to finish
	err := ro.Run()

	// Fix failed
	if err != nil {
		return fmt.Errorf("error fixing cluster: %v", err)
	}

	// Fix finished successfully
	ro.logger.Info("Cluster fixed successfully")

	// Update nodes info
	if err := ro.redisCluster.refreshNodes(); err != nil {
		return err
	}
	return nil
}

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

type RedisOperationMove struct {
	RedisOperationBase
	slots int
}

func NewRedisOperationMove(ctx context.Context, redisCluster *RedisCluster, from, to *RedisNode, slots int) *RedisOperationMove {
	return &RedisOperationMove{
		RedisOperationBase: RedisOperationBase{
			name:         "Move",
			status:       "Pending",
			logger:       util.GetLogger("operation.move"),
			ctx:          ctx,
			redisCluster: redisCluster,
			nodeFrom:     from,
			nodeTo:       to,
		},

		slots: slots,
	}
}

func NewFakeRedisOperationMove(ctx context.Context, redisCluster *RedisCluster, status string, from, to *RedisNode, slots int, endTimestamp time.Time) *RedisOperationMove {
	return &RedisOperationMove{
		RedisOperationBase: RedisOperationBase{
			name:         "Move",
			status:       status,
			logger:       util.GetLogger("operation.move"),
			ctx:          ctx,
			redisCluster: redisCluster,
			endTimestamp: endTimestamp,
			nodeFrom:     from,
			nodeTo:       to,
		},
		slots: slots,
	}
}

func (ro *RedisOperationMove) Launch() error {
	ro.logger.Info("Moving slots", "slots", ro.slots, "from", ro.nodeFrom.Name, "to", ro.nodeTo.Name)

	// Assure all nodes are up (redis-cli needs all nodes to be up)
	if err := ro.redisCluster.ensureNodesAreUp(ro.ctx); err != nil {
		return fmt.Errorf("error ensuring nodes are up: %v", err)
	}

	// Asure destination node is master
	if !ro.nodeTo.IsMaster() {
		if err := ro.redisCluster.convertNodesToMaster(ro.ctx, []*RedisNode{ro.nodeTo}); err != nil {
			return fmt.Errorf("error converting node '%s' to master: %v", ro.nodeTo.Name, err)
		}
	}

	// Get Redis client and check connection
	redisClient, err := ro.redisCluster.getAndCheckRedisClient(true)
	if err != nil {
		return fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Launch reshard operation
	cmd := redisClient.ReshardNode(ro.ctx, *ro.nodeFrom, *ro.nodeTo, ro.slots)
	if cmd.Err != nil {
		return fmt.Errorf("error moving slots: %v", cmd.Err)
	}
	return nil
}

func (ro *RedisOperationMove) Wait() error {
	ro.redisCluster.status = Resharding

	// Wait for reshard to finish
	err := ro.Run()

	// Reshard failed
	if err != nil {
		ro.redisCluster.status = ReshardingError
		return fmt.Errorf("error moving slots from node '%s' to node '%s': %v", ro.nodeFrom.Name, ro.nodeTo.Name, err)
	}

	// Reshard finished successfully
	ro.redisCluster.status = Ready
	ro.logger.Info("Slots moved successfully between nodes", "from", ro.nodeFrom.Name, "to", ro.nodeTo.Name)

	// Update nodes info
	if err := ro.redisCluster.refreshNodes(); err != nil {
		return err
	}
	return nil
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/inditextech/redkeyrobin/internal/util"
)

// RedisOperationRebalance represents a rebalance operation for a RedKey cluster.
type RedisOperationRebalance struct {
	RedisOperationBase
	weights map[string]int
}

func NewRedisOperationRebalance(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
	return &RedisOperationRebalance{
		RedisOperationBase: RedisOperationBase{
			name:    "Rebalance",
			status:  "Pending",
			logger:  util.GetLogger("operation.rebalance"),
			ctx:     ctx,
			cluster: cluster,
		},
		weights: weights,
	}
}

func NewFakeRedisOperationRebalance(ctx context.Context, cluster Cluster, status string, endTimestamp time.Time) *RedisOperationRebalance {
	return &RedisOperationRebalance{
		RedisOperationBase: RedisOperationBase{
			name:         "Rebalance",
			status:       status,
			logger:       util.GetLogger("operation.rebalance"),
			ctx:          ctx,
			cluster:      cluster,
			endTimestamp: endTimestamp,
		},
	}
}

// Launch launches the rebalance operation.
func (ro *RedisOperationRebalance) Launch() error {
	ro.logger.Info("Rebalancing cluster")

	// Assure all nodes are up (redis-cli needs all nodes to be up)
	if err := ro.cluster.ensureNodesAreUp(ro.ctx); err != nil {
		return fmt.Errorf("error ensuring nodes are up: %v", err)
	}

	// Get Redis client and check connection
	redisClient, err := ro.cluster.getAndCheckRedisClient(true)
	if err != nil {
		return fmt.Errorf("error getting and checking Redis client: %v", err)
	}

	// Launch rebalance operation
	cmd := redisClient.ClusterRebalance(ro.ctx, ro.weights)
	if cmd.Err != nil {
		return fmt.Errorf("error rebalancing cluster: %v", cmd.Err)
	}

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

// Wait waits for the rebalance operation to finish.
func (ro *RedisOperationRebalance) Wait() error {
	// Wait for rebalance to finish
	err := ro.Run()

	// Rebalance failed
	if err != nil {
		return fmt.Errorf("error rebalancing cluster: %v", err)
	}

	// Rebalance finished successfully
	ro.logger.Info("Cluster rebalanced successfully")

	// Update nodes info
	if err := ro.cluster.refreshNodes(); err != nil {
		return err
	}
	return nil
}

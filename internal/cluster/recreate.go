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

// RedisOperationRecreate represents a recreate operation for a RedKey cluster.
type RedisOperationRecreate struct {
	RedisOperationBase
	weights map[string]int
}

func NewRedisOperationRecreate(ctx context.Context, cluster Cluster) *RedisOperationRecreate {
	return &RedisOperationRecreate{
		RedisOperationBase: RedisOperationBase{
			name:    "Recreate",
			status:  "Pending",
			logger:  util.GetLogger("operation.recreate"),
			ctx:     ctx,
			cluster: cluster,
		},
	}
}

func NewFakeRedisOperationRecreate(ctx context.Context, cluster Cluster, status string, endTimestamp time.Time) *RedisOperationRecreate {
	return &RedisOperationRecreate{
		RedisOperationBase: RedisOperationBase{
			name:         "Recreate",
			status:       status,
			logger:       util.GetLogger("operation.recreate"),
			ctx:          ctx,
			cluster:      cluster,
			endTimestamp: endTimestamp,
		},
	}
}

// Launch launches the recreate operation.
func (ro *RedisOperationRecreate) Launch() error {
	ro.logger.Info("Recreating cluster")

	// Launch check integrity operation
	cmd := redis.NewRedisLibraryCommand(ro.ctx, ro.doRecreateCluster)
	cmd.Start()

	// Update operation
	ro.cmd = cmd
	ro.status = "Running"
	ro.initTimestamp = time.Now()

	return nil
}

// Wait waits for the recreate operation to finish.
func (ro *RedisOperationRecreate) Wait() error {
	ro.cluster.SetStatus(Recreating)

	// Wait for recreate to finish
	err := ro.Run()

	// Recreate failed
	if err != nil {
		ro.cluster.SetStatus(RecreatingError)
		return fmt.Errorf("error recreating cluster: %v", err)
	}

	// Recreate finished successfully
	ro.cluster.SetStatus(Ready)
	ro.logger.Info("Cluster recreated successfully")

	// Update nodes info
	if err := ro.cluster.refreshNodesInfo(); err != nil {
		return err
	}
	return nil
}

// doRecreateCluster recreates the RedKey cluster
func (ro *RedisOperationRecreate) doRecreateCluster(ctx context.Context) error {
	// Clear existing nodes
	if err := ro.cluster.clearNodes(); err != nil {
		return err
	}

	// Initialize nodes
	if err := ro.cluster.Init(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.cluster.removeOutdatedNodes(ctx); err != nil {
		return err
	}

	// Check nodes info
	if err := ro.cluster.checkNodes(true); err != nil {
		return err
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

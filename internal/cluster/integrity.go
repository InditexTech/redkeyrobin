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

// RedisOperationCheckIntegrity represents a check integrity operation for a RedKey cluster.
type RedisOperationCheckIntegrity struct {
	RedisOperationBase
}

func NewRedisOperationCheckIntegrity(ctx context.Context, cluster Cluster) *RedisOperationCheckIntegrity {
	return &RedisOperationCheckIntegrity{
		RedisOperationBase: RedisOperationBase{
			name:    "CheckIntegrity",
			status:  "Pending",
			logger:  util.GetLogger("operation.integrity"),
			ctx:     ctx,
			cluster: cluster,
		},
	}
}

func NewFakeRedisOperationCheckIntegrity(ctx context.Context, cluster Cluster, status string) *RedisOperationCheckIntegrity {
	return &RedisOperationCheckIntegrity{
		RedisOperationBase: RedisOperationBase{
			name:    "CheckIntegrity",
			status:  status,
			logger:  util.GetLogger("operation.integrity"),
			ctx:     ctx,
			cluster: cluster,
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
	ro.cluster.SetStatus(CheckingIntegrity)

	// Wait for check cluster integrity to finish
	err := ro.Run()

	// Fix failed
	if err != nil {
		ro.cluster.SetStatus(CheckingIntegrityError)
		return fmt.Errorf("error checking cluster integrity: %v", err)
	}

	// Check integrity finished successfully
	ro.cluster.SetStatus(Ready)
	ro.logger.Info("Cluster integrity successfully checked")

	return nil
}

// doCheckIntegrity checks the integrity of the RedKey cluster
func (ro *RedisOperationCheckIntegrity) doCheckIntegrity(ctx context.Context) error {
	// Check nodes info
	if err := ro.cluster.checkNodes(); err != nil {
		return err
	}

	// Forget outdated nodes
	if err := ro.cluster.removeOutdatedNodes(ctx); err != nil {
		ro.logger.Error("Error removing outdated nodes", "error", err)
	}

	// Remove nodes if needed
	if err := ro.cluster.removeNodesIfNeeded(ctx); err != nil {
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

	// Fix cluster if needed
	if err := ro.cluster.fixClusterIfNeeded(ctx); err != nil {
		return err
	}

	// Balance cluster if needed
	if err := ro.cluster.balanceClusterIfNeeded(nil); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.cluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

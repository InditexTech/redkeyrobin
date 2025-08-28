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

// RedisOperationUpgrade represents an upgrade operation for a RedKey cluster.
type RedisOperationUpgrade struct {
	RedisOperationBase
}

func NewRedisOperationUpgrade(ctx context.Context, redkeyCluster *RedKeyCluster) *RedisOperationUpgrade {
	return &RedisOperationUpgrade{
		RedisOperationBase: RedisOperationBase{
			name:          "Upgrade",
			status:        "Pending",
			logger:        util.GetLogger("operation.ugrade"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
		},
	}
}

func NewFakeRedisOperationUpgrade(ctx context.Context, redkeyCluster *RedKeyCluster, status string) *RedisOperationUpgrade {
	return &RedisOperationUpgrade{
		RedisOperationBase: RedisOperationBase{
			name:          "Upgrade",
			status:        status,
			logger:        util.GetLogger("operation.upgrade"),
			ctx:           ctx,
			redkeyCluster: redkeyCluster,
		},
	}
}

// Launch launches the upgrade operation.
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

// Wait waits for the upgrade operation to finish.
func (ro *RedisOperationUpgrade) Wait() error {
	ro.redkeyCluster.status = Upgrading

	// Wait for upgrade to finish
	err := ro.Run()

	// Upgrade failed
	if err != nil {
		ro.redkeyCluster.status = UpgradingError
		return fmt.Errorf("error upgrading cluster: %v", err)
	}

	// Upgrade finished successfully
	ro.redkeyCluster.status = Ready
	ro.logger.Info("Cluster upgraded successfully")

	return nil
}

// doUpgrade upgrades the RedKey cluster
func (ro *RedisOperationUpgrade) doUpgrade(ctx context.Context) error {
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
		return err
	}

	// Remove nodes if needed
	if err := ro.redkeyCluster.removeNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Meet nodes if needed
	if err := ro.redkeyCluster.meetNodesIfNeeded(ctx); err != nil {
		return err
	}

	// Ensure cluster ratio
	if err := ro.redkeyCluster.ensureClusterRatio(ctx); err != nil {
		return nil
	}

	// Assign missing slots if needed
	if err := ro.redkeyCluster.assignMissingSlotsIfNeeded(ctx); err != nil {
		return err
	}

	// Update nodes info
	if err := ro.redkeyCluster.refreshNodes(); err != nil {
		return err
	}

	return nil
}

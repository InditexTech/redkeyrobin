// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/inditextech/redisrobin/internal/rediscluster"
	"github.com/inditextech/redisrobin/internal/util"
)

type RedisClusterReconciler struct {
	logger       *slog.Logger
	redisCluster *rediscluster.RedisCluster
	channel      chan struct{}
}

func NewRedisClusterReconciler(redisCluster *rediscluster.RedisCluster, channel chan struct{}) (*RedisClusterReconciler, error) {
	return &RedisClusterReconciler{
		logger:       util.GetLogger("reconciler"),
		redisCluster: redisCluster,
		channel:      channel,
	}, nil
}

func (r *RedisClusterReconciler) Start(ctx context.Context) {
	timeout := time.Duration(r.redisCluster.GetReconcilerInterval()) * time.Second

	r.Reconcile()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Context cancelled, stopping reconciler")
			return
		case <-r.channel:
			r.Reconcile()
		case <-time.After(timeout):
			r.Reconcile()
		}
	}
}

func (r *RedisClusterReconciler) Reconcile() {
	if err := r.doReconcile(); err != nil {
		r.logger.Error("Error reconciling cluster", "error", err)
	}
}

func (r *RedisClusterReconciler) doReconcile() error {
	// Remove outdated operations
	r.redisCluster.RemoveOutdatedOperations()

	// Reconcile based on the current status
	switch r.redisCluster.GetRedisClusterStatus() {
	case rediscluster.Ready:
		return r.reconcileReadyStatus()
	case rediscluster.ScalingUp:
		return r.reconcileScalingUpStatus()
	case rediscluster.ScalingDown:
		return r.reconcileScalingDownStatus()
	case rediscluster.Upgrading:
		return r.reconcileUpgradingStatus()
	default:
		return nil
	}
}

func (r *RedisClusterReconciler) reconcileReadyStatus() error {
	// Check cluster integrity
	if err := r.redisCluster.CheckIntegrity(false, true); err != nil {
		return err
	}

	return nil
}

func (r *RedisClusterReconciler) reconcileScalingUpStatus() error {
	// Check if the cluster needs to be scaled up
	if r.redisCluster.IsScaled() {
		return r.reconcileReadyStatus()
	}

	// Scale up the cluster
	if err := r.redisCluster.ScaleUp(true); err != nil {
		return err
	}

	return nil
}

func (r *RedisClusterReconciler) reconcileScalingDownStatus() error {
	// Check if the cluster needs to be scaled down
	// If the status is Unknown, ScaleDown should be called to check if the cluster is scaled and update the status. This can happen if Robin is restarted while the cluster is being scaled down.
	if r.redisCluster.IsScaled() && r.redisCluster.GetStatus() != rediscluster.Unknown {
		return nil
	}

	// Scale down the cluster
	if err := r.redisCluster.ScaleDown(true); err != nil {
		return err
	}

	return nil
}

func (r *RedisClusterReconciler) reconcileUpgradingStatus() error {
	// Check if the cluster needs to be upgraded
	if !r.redisCluster.IsUpgraded() && !r.redisCluster.CanBeUpgraded() {
		return nil
	}

	// Upgrade the cluster
	if err := r.redisCluster.Upgrade(true); err != nil {
		return err
	}

	return nil
}

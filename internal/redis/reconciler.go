// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/inditextech/redisrobin/internal/util"
)

type RedisClusterReconciler struct {
	logger       logr.Logger
	redisCluster *RedisCluster
	channel      chan struct{}
}

func NewRedisClusterReconciler(redisCluster *RedisCluster, channel chan struct{}) (*RedisClusterReconciler, error) {
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
	r.logger.Info("Reconciling cluster")
	if err := r.doReconcile(); err != nil {
		r.logger.Error(err, "Error reconciling cluster")
	} else {
		r.logger.Info("Cluster reconciled successfully")
	}
}

func (r *RedisClusterReconciler) doReconcile() error {
	// Remove outdated operations
	r.redisCluster.RemoveOutdatedOperations()

	// Reconcile based on the current status
	switch r.redisCluster.GetRedisClusterStatus() {
	case Ready:
		return r.reconcileReadyStatus()
	case ScalingUp:
		return r.reconcileScalingUpStatus()
	case ScalingDown:
		return r.reconcileScalingDownStatus()
	case Upgrading:
		return r.reconcileUpgradingStatus()
	default:
		return nil
	}
}

func (r *RedisClusterReconciler) reconcileReadyStatus() error {
	// Check cluster integrity
	err := r.redisCluster.CheckIntegrity(false, true)
	if err != nil {
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
	if r.redisCluster.IsScaled() {
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
	if !r.redisCluster.CanBeUpgraded() {  // r.redisCluster.IsUpgraded() ||
		return nil
	}

	// Upgrade the cluster
	if err := r.redisCluster.Upgrade(true); err != nil {
		return err
	}

	return nil
}

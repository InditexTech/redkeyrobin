// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/util"
)

// RedisClusterReconciler is responsible for reconciling a Redis cluster.
type RedisClusterReconciler struct {
	baseClusterReconciler
}

func NewRedisClusterReconciler(cluster cluster.Cluster, channel chan struct{}) (*RedisClusterReconciler, error) {
	reconciler := &RedisClusterReconciler{
		baseClusterReconciler: baseClusterReconciler{
			logger:  util.GetLogger("cluster-reconciler"),
			cluster: cluster,
			channel: channel,
		},
	}
	reconciler.delegate = reconciler
	return reconciler, nil
}

// doReconcile reconciles the Redis cluster based on its current status.
func (r *RedisClusterReconciler) doReconcile() error {
	// Reconcile based on the current status
	switch r.cluster.GetRedisClusterStatus() {
	case cluster.Configuring:
		return r.reconcileConfiguringStatus()
	case cluster.Ready:
		return r.reconcileReadyStatus()
	case cluster.ScalingUp:
		return r.reconcileScalingUpStatus()
	case cluster.ScalingDown:
		return r.reconcileScalingDownStatus()
	case cluster.Upgrading:
		return r.reconcileUpgradingStatus()
	default:
		return nil
	}
}

// reconciles the Redis cluster when it is in the Configuring status, building the cluster.
func (r *RedisClusterReconciler) reconcileConfiguringStatus() error {
	if err := r.cluster.CheckIntegrity(false, true); err != nil {
		return err
	}
	return nil
}

// reconcileReadyStatus reconciles the Redis cluster when it is in the Ready status.
func (r *RedisClusterReconciler) reconcileReadyStatus() error {
	// Check cluster integrity
	if err := r.cluster.CheckIntegrity(false, true); err != nil {
		return err
	}

	return nil
}

// reconcileScalingUpStatus reconciles the Redis cluster when it is in the ScalingUp status.
func (r *RedisClusterReconciler) reconcileScalingUpStatus() error {
	// Check if the cluster needs to be scaled up
	if r.cluster.IsScaled() {
		return r.reconcileReadyStatus()
	}

	// Scale up the cluster
	if err := r.cluster.ScaleUp(true); err != nil {
		return err
	}

	return nil
}

// reconcileScalingDownStatus reconciles the Redis cluster when it is in the ScalingDown status.
func (r *RedisClusterReconciler) reconcileScalingDownStatus() error {
	// Check if the cluster needs to be scaled down
	// If the status is Unknown, ScaleDown should be called to check if the cluster is scaled and update the status. This can happen if Robin is restarted while the cluster is being scaled down.
	if r.cluster.IsScaled() && r.cluster.GetStatus() != cluster.Unknown {
		return nil
	}

	// Scale down the cluster
	if err := r.cluster.ScaleDown(true); err != nil {
		return err
	}

	return nil
}

// reconcileUpgradingStatus reconciles the Redis cluster when it is in the Upgrading status.
func (r *RedisClusterReconciler) reconcileUpgradingStatus() error {
	// Check if the cluster needs to be upgraded
	if !r.cluster.IsUpgraded() && !r.cluster.CanBeUpgraded() {
		return nil
	}

	// Upgrade the cluster
	if err := r.cluster.Upgrade(true); err != nil {
		return err
	}

	return nil
}

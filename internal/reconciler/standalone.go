// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"

	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/util"
)

type RedisStandaloneReconciler struct {
	baseClusterReconciler
}

func NewStandaloneReconciler(cluster cluster.Cluster, channel chan struct{}) (*RedisStandaloneReconciler, error) {
	reconciler := &RedisStandaloneReconciler{
		baseClusterReconciler: baseClusterReconciler{
			logger:  util.GetLogger("standalone-reconciler"),
			cluster: cluster,
			channel: channel,
		},
	}
	reconciler.delegate = reconciler
	return reconciler, nil
}

// Start starts the reconciler loop.
func (r *RedisStandaloneReconciler) Start(ctx context.Context) {
	r.logger.Info("Redis standalone reconciler not needed. Finishing.")
}

// doReconcile reconciles the Redis standalone based on its current status.
func (r *RedisStandaloneReconciler) doReconcile() error {
	// Check cluster integrity
	if err := r.cluster.CheckIntegrity(false, true); err != nil {
		return err
	}

	return nil
}

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
	channel	  chan struct{}
}

func NewRedisClusterReconciler(redisCluster *RedisCluster, channel chan struct{}) (*RedisClusterReconciler, error) {
	return &RedisClusterReconciler{
		logger:       util.GetLogger("reconciler"),
		redisCluster: redisCluster,
		channel:	  channel,
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
	r.logger.Info("Reconcilling cluster")
	err := r.doReconcile()
	if err != nil {
		r.logger.Error(err, "Error reconcilling cluster")
	}
}

func (r *RedisClusterReconciler) doReconcile() error {
	// Remove outdated operations
	r.redisCluster.RemoveOutdatedOperations()

	// Check if the cluster is ready, finishing if not
	if r.redisCluster.GetRedisClusterStatus() != Ready {
		return nil
	}

	// Check cluster integrity
	err := r.redisCluster.CheckClusterIntegrity(false, true)
	if err != nil {
		return err
	}

	return nil
}

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
}

func NewRedisClusterReconciler(redisCluster *RedisCluster) (*RedisClusterReconciler, error) {
	return &RedisClusterReconciler{
		logger:       util.GetLogger("reconciler"),
		redisCluster: redisCluster,
	}, nil
}

func (r *RedisClusterReconciler) Start(ctx context.Context) {
	for ctx.Err() == nil {
		r.logger.Info("Reconcilling cluster")

		err := r.Reconcile(ctx)
		if err != nil {
			r.logger.Error(err, "Error reconcilling cluster")
		}

		time.Sleep(time.Second * time.Duration(r.redisCluster.GetReconcilerInterval()))
	}
}

func (r *RedisClusterReconciler) Reconcile(ctx context.Context) error {
	if r.redisCluster.GetRedisClusterStatus() != Ready {
		return nil
	}

	return nil
}

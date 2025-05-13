// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"log"
	"time"
)

type RedisClusterReconciler struct {
	redisCluster   *RedisCluster
}

func NewRedisClusterReconciler(redisCluster *RedisCluster) *RedisClusterReconciler {
	return &RedisClusterReconciler{
		redisCluster: redisCluster,
	}
}

func (r *RedisClusterReconciler) Start(ctx context.Context) error {
	for {
		log.Printf("Reconcilling cluster %s.", r.redisCluster.GetName())

		err := r.Reconcile(ctx)
		if err != nil {
			log.Printf("Error reconcilling cluster: %v", err)
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

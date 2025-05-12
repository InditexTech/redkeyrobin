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
		log.Printf("Reconcilling cluster %s.", r.redisCluster.Conf.Redis.Cluster.Name)

		err := r.Reconcile(ctx)
		if err != nil {
			log.Printf("Error reconcilling cluster: %v", err)
		}

		time.Sleep(time.Second * time.Duration(r.redisCluster.Conf.Redis.Reconciler.IntervalSeconds))
	}
}

func (r *RedisClusterReconciler) Reconcile(ctx context.Context) error {
	if r.redisCluster.Conf.Redis.Cluster.Status != Ready {
		return nil
	}

	return nil
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"log/slog"
	"time"

	"github.com/inditextech/redisrobin/internal/cluster"
)

// ClusterReconciler is an interface for reconciling a cluster.
type ClusterReconciler interface {
	// Start starts the reconciler loop.
	Start(ctx context.Context)
}

// ClusterReconcilerDelegate is an interface for reconciling a cluster.
type ClusterReconcilerDelegate interface {
	// doReconcile reconciles the cluster.
	doReconcile() error
}

// NewClusterReconciler creates a new cluster reconciler. It returns a standalone or cluster reconciler based on the cluster type.
func NewClusterReconciler(cluster cluster.Cluster, channel chan struct{}) (ClusterReconciler, error) {
	if cluster.IsStandalone() {
		return NewStandaloneReconciler(cluster, channel)
	} else {
		return NewRedisClusterReconciler(cluster, channel)
	}
}

// baseClusterReconciler is a base struct for all reconcilers.
type baseClusterReconciler struct {
	logger   *slog.Logger
	cluster  cluster.Cluster
	channel  chan struct{}
	delegate ClusterReconcilerDelegate
}

// Start starts the reconciler loop.
func (br *baseClusterReconciler) Start(ctx context.Context) {
	if br.delegate == nil {
		br.logger.Error("Metrics poller delegate must be set")
		return
	}

	timeout := time.Duration(br.cluster.GetReconcilerInterval()) * time.Second

	for {
		select {
		case <-ctx.Done():
			br.logger.Info("Context cancelled, stopping reconciler")
			return
		case <-br.channel:
			br.reconcile()
		case <-time.After(timeout):
			br.reconcile()
		}
	}
}

// reconcile reconciles the Redis cluster based on its current status.
func (br *baseClusterReconciler) reconcile() {
	if err := br.delegate.doReconcile(); err != nil {
		br.logger.Error("Error reconciling cluster", "error", err)
	}
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/inditextech/redkeyrobin/internal/cluster"
)

// Reconciler is an interface for reconciling a cluster.
type Reconciler interface {
	// Start starts the reconciler loop.
	Start(ctx context.Context) error
}

// ReconcilerDelegate is an interface for reconciling a cluster.
type ReconcilerDelegate interface {
	// doReconcile reconciles the cluster.
	doReconcile() error
}

// NewReconciler creates a new reconciler. It returns a standalone or cluster reconciler based on the cluster type.
func NewReconciler(cluster cluster.Cluster, channel chan struct{}) (Reconciler, error) {
	if cluster.IsStandalone() {
		return NewStandaloneReconciler(cluster, channel)
	} else {
		return NewRedKeyClusterReconciler(cluster, channel)
	}
}

// baseClusterReconciler is a base struct for all reconcilers.
type baseClusterReconciler struct {
	logger   *slog.Logger
	cluster  cluster.Cluster
	channel  chan struct{}
	delegate ReconcilerDelegate
}

// Start starts the reconciler loop.
func (br *baseClusterReconciler) Start(ctx context.Context) error {
	if br.delegate == nil {
		return fmt.Errorf("reconciler delegate must be set")
	}

	br.reconcile()

	timeout := time.Duration(br.cluster.GetReconcilerInterval()) * time.Second

	for {
		select {
		case <-ctx.Done():
			br.logger.Info("Context cancelled, stopping reconciler")
			return nil
		case <-br.channel:
			br.reconcile()
		case <-time.After(timeout):
			br.reconcile()
		}
	}
}

// reconcile reconciles the RedKey cluster based on its current status.
func (br *baseClusterReconciler) reconcile() {
	if err := br.delegate.doReconcile(); err != nil {
		br.logger.Error("Error reconciling cluster", "error", err)
	}
}

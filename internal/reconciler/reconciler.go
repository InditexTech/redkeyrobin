// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"log/slog"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/config"
)

const (
	// ClusterLabel is the label key used to filter RedkeyClusterConfig CRs by cluster name.
	ClusterLabel = "redkey.inditex.dev/cluster"
)

// Reconciler implements the polling reconciliation loop for a single RedkeyCluster.
// It periodically lists RedkeyClusterConfig CRs and processes them sequentially.
type Reconciler struct {
	client          client.Client
	clusterName     string
	namespace       string
	interval        time.Duration
	intervalOnError time.Duration
	runtimeConfig   *config.RuntimeConfig
	logger          *slog.Logger
}

// NewReconciler creates a new Reconciler.
func NewReconciler(c client.Client, clusterName, namespace string, interval time.Duration, intervalOnError time.Duration, runtimeConfig *config.RuntimeConfig) *Reconciler {
	return &Reconciler{
		client:          c,
		clusterName:     clusterName,
		namespace:       namespace,
		interval:        interval,
		intervalOnError: intervalOnError,
		runtimeConfig:   runtimeConfig,
		logger:          slog.Default().With("component", "reconciler", "cluster", clusterName),
	}
}

// Start begins the polling reconciliation loop. It blocks until the context is cancelled.
func (r *Reconciler) Start(ctx context.Context) error {
	r.logger.Info("Starting reconciliation loop", "interval", r.interval)

	// Run an initial reconciliation immediately
	hasPending, onError := r.reconcile(ctx)

	for {
		// Adaptive timing: loop immediately if there are pending configs, otherwise wait
		var waitDuration time.Duration
		if hasPending {
			waitDuration = 0
		} else if onError {
			waitDuration = r.intervalOnError
		} else {
			waitDuration = r.interval
		}

		select {
		case <-ctx.Done():
			r.logger.Info("Context cancelled, stopping reconciler")
			return nil
		case <-time.After(waitDuration):
			hasPending, onError = r.reconcile(ctx)
		}
	}
}

// reconcile performs a single reconciliation cycle. It returns true for pendingConfigs if there are
// pending configs that need processing (signaling the loop to re-poll immediately). It returns true
// for onError if an error occurred.
func (r *Reconciler) reconcile(ctx context.Context) (pendingConfigs bool, onError bool) {
	previousConfig, targetConfig, err := r.selectConfig(ctx)
	if err != nil {
		r.logger.Error("Failed to select RedkeyClusterConfig", "error", err)
		return false, true
	}
	if targetConfig == nil {
		r.logger.Info("No actionable RedkeyClusterConfig found")
		return false, false
	}

	// Log information about the target and previous configurations.
	r.logger.Info("Selected configuration to apply",
		"name", targetConfig.Name,
		"sequence", targetConfig.Spec.Sequence,
		"configPhase", targetConfig.Status.ConfigPhase,
	)
	if previousConfig != nil {
		r.logger.Info("Previous configuration detected",
			"name", previousConfig.Name,
			"sequence", previousConfig.Spec.Sequence,
			"configPhase", previousConfig.Status.ConfigPhase,
		)
	} else {
		r.logger.Info("No previous configuration exists")
	}

	// Validate the phase of the target configuration before processing.
	// Required before handling Robin and monitoring configurations.
	if !validConfigPhase(targetConfig.Status.ConfigPhase, true) {
		r.logger.Error("Configuration with unknown or not valid phase detected. Skipping processing of this configuration and waiting for next cycle.",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
			"configPhase", targetConfig.Status.ConfigPhase,
		)
		return false, true
	}

	// TODO: Handle Robin auto-configuration.

	// TODO: Handle monitoring configuration.

	// Apply Robin configuration from the target or applied config.
	r.applyRobinConfig(targetConfig, previousConfig)

	// From here, we start processing Redkey Cluster configuration and health checks.
	// We decide what to do based on the phase of the target configuration.
	switch targetConfig.Status.ConfigPhase {
	case redisv1.ConfigPhasePending:
		// Start applying a new config.
		if err := r.setConfigPhaseInProgress(ctx, targetConfig); err != nil {
			r.logger.Error("Failed to set ConfigPhase to InProgress",
				"name", targetConfig.Name, "error", err)
			// We'll retry on the next cycle.
			return true, true
		}
		r.logger.Info("Starting configuration",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
		)
		// TODO: apply the targetConfig (future phases will implement this)
	case redisv1.ConfigPhaseInProgress:
		// Resume an already in progress config.
		r.logger.Info("Resuming in-progress configuration",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
		)
		// TODO: resume applying the targetConfig (future phases will implement this)
	case redisv1.ConfigPhaseApplied:
		// No new config to apply, do a full check of the Redkey Cluster.
		r.logger.Info("Configuration already applied, performing full cluster check")
		// TODO: full Redkey Cluster check
	default:
		// This should never happen due to the earlier validation, but we check again just in case.
		r.logger.Error("Configuration with unknown phase detected",
			"name", targetConfig.Name,
			"sequence", targetConfig.Spec.Sequence,
			"configPhase", targetConfig.Status.ConfigPhase,
		)
		return false, true
	}

	// Config applied, iterate with the next one immediately in case there are more pending configs.
	return true, false
}

// applyRobinConfig reads the RobinConfig from the effective configuration and
// updates the shared RuntimeConfig so that other components (metrics collector,
// reconciler interval) pick up the changes.
// It also stores the topology for node discovery by the metrics collector.
func (r *Reconciler) applyRobinConfig(target *redisv1.RedkeyClusterConfig, previous *redisv1.RedkeyClusterConfig) {
	if r.runtimeConfig == nil {
		return
	}

	// Determine which config holds the effective Robin settings:
	// - If the target is Applied, use it (it's the current active config).
	// - If the target is InProgress or Pending, use the previous (last Applied) if available,
	//   since the new config hasn't been fully applied yet. But we also update topology
	//   from the target so node discovery reflects the desired state.
	var effectiveConfig *redisv1.RedkeyClusterConfig
	switch target.Status.ConfigPhase {
	case redisv1.ConfigPhaseApplied:
		effectiveConfig = target
	default:
		if previous != nil {
			effectiveConfig = previous
		} else {
			// No previous applied config — use target's RobinConfig even though it's not yet applied.
			effectiveConfig = target
		}
	}

	if effectiveConfig.Spec.RobinConfig != nil {
		r.runtimeConfig.SetFromRobinConfig(effectiveConfig.Spec.RobinConfig)
		// Update reconciler interval from the runtime config.
		newInterval := r.runtimeConfig.ReconcilerInterval()
		if newInterval != r.interval {
			r.logger.Info("Updating reconciler interval", "old", r.interval, "new", newInterval)
			r.interval = newInterval
		}
	}

	// Update topology for node discovery (always from the effective config).
	r.runtimeConfig.SetTopology(effectiveConfig.Spec.Primaries, effectiveConfig.Spec.ReplicasPerPrimary)

	// Update auth secret name so the metrics collector can read the password.
	r.runtimeConfig.SetAuthSecret(effectiveConfig.Spec.Auth.SecretName)
}

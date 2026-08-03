// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"

// DetermineStatusTransition decides the next cluster status based on the detected changes.
// It returns the new status string, or an empty string if no status transition is needed
// (e.g. only Robin config changed, which is handled by hot-reload).
//
// Transition rules:
//   - Only Robin changes → "" (no transition; mark as Applied)
//   - Topology scale to zero → ScalingToZero
//   - Topology scale-up → ScalingUp
//   - Topology scale-down → ScalingDown
//   - Kubernetes/Redis config changes only (no topology) → Upgrading
//   - Topology + other changes → scaling status takes priority (ScalingUp/ScalingDown)
//     After scaling completes, remaining changes are detected in the next reconciliation pass.
func DetermineStatusTransition(report ChangeReport) string {
	// No changes or only Robin changes: no status transition.
	if !report.HasAnyChange() || report.OnlyRobinChanges() {
		return ""
	}

	// PurgeKeysOnRebalance alone (without other cluster changes) doesn't require a transition.
	if report.HasPurgeKeysOnRebalanceChange && !report.RequiresClusterOperation() && !report.HasRobinChanges {
		return ""
	}

	// Topology changes take priority.
	if report.HasTopologyChanges {
		switch report.TopologyScaleDirection {
		case ScaleToZero:
			return redisv1.ClusterStatusScalingToZero
		case ScaleUp:
			return redisv1.ClusterStatusScalingUp
		case ScaleDown:
			return redisv1.ClusterStatusScalingDown
		}
	}

	// Non-topology cluster changes (Kubernetes objects or Redis config).
	if report.HasKubernetesChanges || report.HasRedisConfigChanges {
		return redisv1.ClusterStatusUpgrading
	}

	return ""
}

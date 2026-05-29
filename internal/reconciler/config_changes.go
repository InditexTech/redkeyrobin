// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"reflect"

	v1 "k8s.io/api/core/v1"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

// ScaleDirection represents the direction of a topology change.
type ScaleDirection int

const (
	// NoScale indicates no topology change.
	NoScale ScaleDirection = iota
	// ScaleUp indicates the cluster needs to grow.
	ScaleUp
	// ScaleDown indicates the cluster needs to shrink.
	ScaleDown
)

// ChangeReport holds the categorized results of comparing two RedkeyClusterConfigSpec values.
type ChangeReport struct {
	// HasRobinChanges is true when the RobinConfig operational settings differ.
	HasRobinChanges bool

	// HasTopologyChanges is true when Primaries and/or ReplicasPerPrimary differ.
	HasTopologyChanges bool

	// HasKubernetesChanges is true when fields affecting K8s objects differ
	// (Labels, Resources, Image, Override, Pdb, Storage, StorageClassName, Ephemeral, DeletePVC).
	HasKubernetesChanges bool

	// HasRedisConfigChanges is true when Redis configuration fields differ
	// (RedisConfig, Version, Auth).
	HasRedisConfigChanges bool

	// HasPurgeKeysOnRebalanceChange is true when PurgeKeysOnRebalance differs.
	HasPurgeKeysOnRebalanceChange bool

	// TopologyScaleDirection indicates the direction of topology change.
	TopologyScaleDirection ScaleDirection

	// PrimariesDelta is target.Primaries - previous.Primaries.
	PrimariesDelta int32

	// ReplicasDelta is target.ReplicasPerPrimary - previous.ReplicasPerPrimary.
	ReplicasDelta int32
}

// OnlyRobinChanges returns true if only Robin configuration changed (no cluster operations required).
func (r ChangeReport) OnlyRobinChanges() bool {
	return r.HasRobinChanges &&
		!r.HasTopologyChanges &&
		!r.HasKubernetesChanges &&
		!r.HasRedisConfigChanges &&
		!r.HasPurgeKeysOnRebalanceChange
}

// RequiresClusterOperation returns true if any change requires an operation on the cluster.
func (r ChangeReport) RequiresClusterOperation() bool {
	return r.HasTopologyChanges || r.HasKubernetesChanges || r.HasRedisConfigChanges
}

// HasAnyChange returns true if any difference was detected.
func (r ChangeReport) HasAnyChange() bool {
	return r.HasRobinChanges ||
		r.HasTopologyChanges ||
		r.HasKubernetesChanges ||
		r.HasRedisConfigChanges ||
		r.HasPurgeKeysOnRebalanceChange
}

// DetectChanges compares two RedkeyClusterConfigSpec values and returns a categorized
// report of what changed. Control fields (Sequence, SkipIfSuperseded) are ignored.
func DetectChanges(previous, target redisv1.RedkeyClusterConfigSpec) ChangeReport {
	var report ChangeReport

	// Topology changes: Primaries and ReplicasPerPrimary.
	report.PrimariesDelta = target.Primaries - previous.Primaries
	report.ReplicasDelta = target.ReplicasPerPrimary - previous.ReplicasPerPrimary

	if report.PrimariesDelta != 0 || report.ReplicasDelta != 0 {
		report.HasTopologyChanges = true
		report.TopologyScaleDirection = determineScaleDirection(report.PrimariesDelta, report.ReplicasDelta)
	}

	// Robin config changes.
	report.HasRobinChanges = !robinConfigEqual(previous.RobinConfig, target.RobinConfig)

	// Kubernetes object changes.
	report.HasKubernetesChanges = detectKubernetesChanges(previous, target)

	// Redis config changes.
	report.HasRedisConfigChanges = detectRedisConfigChanges(previous, target)

	// PurgeKeysOnRebalance change.
	report.HasPurgeKeysOnRebalanceChange = !boolPtrEqual(previous.PurgeKeysOnRebalance, target.PurgeKeysOnRebalance)

	return report
}

// determineScaleDirection decides the scale direction.
// Primaries direction takes precedence; if primaries are unchanged, replicas direction is used.
func determineScaleDirection(primariesDelta, replicasDelta int32) ScaleDirection {
	if primariesDelta > 0 {
		return ScaleUp
	}
	if primariesDelta < 0 {
		return ScaleDown
	}
	// Primaries unchanged — use replicas direction.
	if replicasDelta > 0 {
		return ScaleUp
	}
	if replicasDelta < 0 {
		return ScaleDown
	}
	return NoScale
}

// detectKubernetesChanges returns true if any field affecting Kubernetes objects differs.
func detectKubernetesChanges(previous, target redisv1.RedkeyClusterConfigSpec) bool {
	if previous.Image != target.Image {
		return true
	}
	if previous.Ephemeral != target.Ephemeral {
		return true
	}
	if previous.Storage != target.Storage {
		return true
	}
	if previous.StorageClassName != target.StorageClassName {
		return true
	}
	if !boolPtrEqual(previous.DeletePVC, target.DeletePVC) {
		return true
	}
	if !labelsPtrEqual(previous.Labels, target.Labels) {
		return true
	}
	if !annotationsPtrEqual(previous.Annotations, target.Annotations) {
		return true
	}
	if !resourcesEqual(previous.Resources, target.Resources) {
		return true
	}
	if !overrideEqual(previous.Override, target.Override) {
		return true
	}
	if !pdbEqual(previous.Pdb, target.Pdb) {
		return true
	}
	return false
}

// detectRedisConfigChanges returns true if any Redis configuration field differs.
func detectRedisConfigChanges(previous, target redisv1.RedkeyClusterConfigSpec) bool {
	if previous.RedisConfig != target.RedisConfig {
		return true
	}
	if previous.Version != target.Version {
		return true
	}
	if previous.Auth != target.Auth {
		return true
	}
	return false
}

// --- Nil-safe comparison helpers ---

func boolPtrEqual(a, b *bool) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func labelsPtrEqual(a, b *map[string]string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(*a, *b)
}

func annotationsPtrEqual(a, b *map[string]string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(*a, *b)
}

func resourcesEqual(a, b *v1.ResourceRequirements) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(*a, *b)
}

func overrideEqual(a, b *redisv1.RedkeyClusterOverrideSpec) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(*a, *b)
}

func pdbEqual(a, b redisv1.Pdb) bool {
	return reflect.DeepEqual(a, b)
}

// robinConfigEqual compares two RobinConfig pointers for equality.
func robinConfigEqual(a, b *redisv1.RobinConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if !robinReconcilerEqual(a.Reconciler, b.Reconciler) {
		return false
	}
	if !robinClusterEqual(a.Cluster, b.Cluster) {
		return false
	}
	if !robinMetricsEqual(a.Metrics, b.Metrics) {
		return false
	}
	if !robinProfilingEqual(a.Profiling, b.Profiling) {
		return false
	}
	return true
}

func robinReconcilerEqual(a, b *redisv1.RobinConfigReconciler) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if !intPtrEqual(a.IntervalSeconds, b.IntervalSeconds) {
		return false
	}
	if !intPtrEqual(a.IntervalOnErrorSeconds, b.IntervalOnErrorSeconds) {
		return false
	}
	if !intPtrEqual(a.IntervalOnWaitSeconds, b.IntervalOnWaitSeconds) {
		return false
	}
	return true
}

func robinClusterEqual(a, b *redisv1.RobinConfigCluster) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if !intPtrEqual(a.ConnectionMaxRetries, b.ConnectionMaxRetries) {
		return false
	}
	if !intPtrEqual(a.ConnectionBackOffSeconds, b.ConnectionBackOffSeconds) {
		return false
	}
	if !intPtrEqual(a.ClusterCommandTimeoutSeconds, b.ClusterCommandTimeoutSeconds) {
		return false
	}
	if !intPtrEqual(a.ClusterMeetWaitSeconds, b.ClusterMeetWaitSeconds) {
		return false
	}
	if !intPtrEqual(a.RebalanceTimeoutSeconds, b.RebalanceTimeoutSeconds) {
		return false
	}
	return true
}

func robinMetricsEqual(a, b *redisv1.RobinConfigMetrics) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if !intPtrEqual(a.CollectionIntervalSeconds, b.CollectionIntervalSeconds) {
		return false
	}
	if !reflect.DeepEqual(a.RedisInfoKeys, b.RedisInfoKeys) {
		return false
	}
	if !reflect.DeepEqual(a.MetricsLabels, b.MetricsLabels) {
		return false
	}
	return true
}

func robinProfilingEqual(a, b *redisv1.RobinConfigProfiling) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return boolPtrEqual(a.Enabled, b.Enabled)
}

func intPtrEqual(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

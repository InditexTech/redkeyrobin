// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

// intPtr is defined in reconciler_test.go (same package).

func boolPtr(v bool) *bool    { return &v }
func int32Ptr(v int32) *int32 { return &v }

func baseSpec() redisv1.RedkeyConfigSpec {
	return redisv1.RedkeyConfigSpec{
		Sequence:             1,
		SkipIfSuperseded:     true,
		Primaries:            3,
		ReplicasPerPrimary:   1,
		Ephemeral:            true,
		Image:                "redis:8-bookworm",
		Version:              "8.0",
		RedisConfig:          "maxmemory 100mb",
		Auth:                 redisv1.RedisAuth{SecretName: "my-secret"},
		PurgeKeysOnRebalance: boolPtr(true),
		RobinConfig: &redisv1.RobinConfig{
			Reconciler: &redisv1.RobinConfigReconciler{
				IntervalSeconds:        intPtr(30),
				IntervalOnErrorSeconds: intPtr(10),
				IntervalOnWaitSeconds:  intPtr(10),
			},
			Cluster: &redisv1.RobinConfigCluster{
				ConnectionMaxRetries:         intPtr(10),
				ConnectionBackOffSeconds:     intPtr(10),
				ClusterCommandTimeoutSeconds: intPtr(24),
				ClusterMeetWaitSeconds:       intPtr(5),
				RebalanceTimeoutSeconds:      intPtr(120),
			},
			Metrics: &redisv1.RobinConfigMetrics{
				CollectionIntervalSeconds: intPtr(60),
				RedisInfoKeys:             []string{"keyspace_hits", "evicted_keys"},
				MetricsLabels:             map[string]string{"env": "prod"},
			},
			Profiling: &redisv1.RobinConfigProfiling{
				Enabled: boolPtr(false),
			},
		},
	}
}

func TestDetectChanges_NoChanges(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	// Change control fields only — should be ignored
	target.Sequence = 2
	target.SkipIfSuperseded = false

	report := DetectChanges(previous, target)

	if report.HasAnyChange() {
		t.Errorf("expected no changes, got report: %+v", report)
	}
	if report.OnlyRobinChanges() {
		t.Error("OnlyRobinChanges should be false when no changes")
	}
	if report.RequiresClusterOperation() {
		t.Error("RequiresClusterOperation should be false when no changes")
	}
}

func TestDetectChanges_RobinOnly_ReconcilerInterval(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.RobinConfig.Reconciler.IntervalSeconds = intPtr(60)

	report := DetectChanges(previous, target)

	if !report.HasRobinChanges {
		t.Error("expected HasRobinChanges")
	}
	if !report.OnlyRobinChanges() {
		t.Error("expected OnlyRobinChanges")
	}
	if report.RequiresClusterOperation() {
		t.Error("should not require cluster operation")
	}
}

func TestDetectChanges_RobinOnly_MetricsKeys(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.RobinConfig.Metrics.RedisInfoKeys = []string{"keyspace_hits", "evicted_keys", "connected_clients"}

	report := DetectChanges(previous, target)

	if !report.HasRobinChanges {
		t.Error("expected HasRobinChanges")
	}
	if !report.OnlyRobinChanges() {
		t.Error("expected OnlyRobinChanges")
	}
}

func TestDetectChanges_RobinOnly_ClusterConfig(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.RobinConfig.Cluster.ConnectionMaxRetries = intPtr(20)

	report := DetectChanges(previous, target)

	if !report.HasRobinChanges {
		t.Error("expected HasRobinChanges")
	}
	if !report.OnlyRobinChanges() {
		t.Error("expected OnlyRobinChanges")
	}
}

func TestDetectChanges_RobinOnly_Profiling(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.RobinConfig.Profiling.Enabled = boolPtr(true)

	report := DetectChanges(previous, target)

	if !report.HasRobinChanges {
		t.Error("expected HasRobinChanges")
	}
	if !report.OnlyRobinChanges() {
		t.Error("expected OnlyRobinChanges")
	}
}

func TestDetectChanges_RobinOnly_NilToSet(t *testing.T) {
	previous := baseSpec()
	previous.RobinConfig = nil
	target := baseSpec()
	target.Sequence = 2

	report := DetectChanges(previous, target)

	if !report.HasRobinChanges {
		t.Error("expected HasRobinChanges")
	}
	if !report.OnlyRobinChanges() {
		t.Error("expected OnlyRobinChanges")
	}
}

func TestDetectChanges_TopologyOnly_ScaleUpPrimaries(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Primaries = 6

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	if report.TopologyScaleDirection != ScaleUp {
		t.Errorf("expected ScaleUp, got %v", report.TopologyScaleDirection)
	}
	if report.PrimariesDelta != 3 {
		t.Errorf("expected PrimariesDelta=3, got %d", report.PrimariesDelta)
	}
	if report.ReplicasDelta != 0 {
		t.Errorf("expected ReplicasDelta=0, got %d", report.ReplicasDelta)
	}
	if report.OnlyRobinChanges() {
		t.Error("should not be OnlyRobinChanges")
	}
	if !report.RequiresClusterOperation() {
		t.Error("should require cluster operation")
	}
}

func TestDetectChanges_TopologyOnly_ScaleDownPrimaries(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Primaries = 2

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	if report.TopologyScaleDirection != ScaleDown {
		t.Errorf("expected ScaleDown, got %v", report.TopologyScaleDirection)
	}
	if report.PrimariesDelta != -1 {
		t.Errorf("expected PrimariesDelta=-1, got %d", report.PrimariesDelta)
	}
}

func TestDetectChanges_TopologyOnly_ReplicasIncrease(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.ReplicasPerPrimary = 2

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	if report.TopologyScaleDirection != ScaleUp {
		t.Errorf("expected ScaleUp, got %v", report.TopologyScaleDirection)
	}
	if report.PrimariesDelta != 0 {
		t.Errorf("expected PrimariesDelta=0, got %d", report.PrimariesDelta)
	}
	if report.ReplicasDelta != 1 {
		t.Errorf("expected ReplicasDelta=1, got %d", report.ReplicasDelta)
	}
}

func TestDetectChanges_TopologyOnly_ReplicasDecrease(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.ReplicasPerPrimary = 0

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	if report.TopologyScaleDirection != ScaleDown {
		t.Errorf("expected ScaleDown, got %v", report.TopologyScaleDirection)
	}
}

func TestDetectChanges_Topology_PrimariesUpReplicasDown(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Primaries = 6
	target.ReplicasPerPrimary = 0

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	// Primaries direction takes precedence
	if report.TopologyScaleDirection != ScaleUp {
		t.Errorf("expected ScaleUp (primaries take precedence), got %v", report.TopologyScaleDirection)
	}
}

func TestDetectChanges_KubernetesOnly_Image(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Image = "redis:9-bookworm"

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
	if report.HasTopologyChanges || report.HasRobinChanges || report.HasRedisConfigChanges {
		t.Error("unexpected additional change flags")
	}
	if !report.RequiresClusterOperation() {
		t.Error("should require cluster operation")
	}
}

func TestDetectChanges_KubernetesOnly_Labels(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	labels := map[string]string{"app": "redis", "tier": "cache"}
	target.Labels = &labels

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
}

func TestDetectChanges_KubernetesOnly_Annotations(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	annotations := map[string]string{"prometheus.io/scrape": "true", "prometheus.io/port": "9121"}
	target.Annotations = &annotations

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
	if report.HasTopologyChanges || report.HasRobinChanges || report.HasRedisConfigChanges {
		t.Error("unexpected additional change flags")
	}
}

func TestDetectChanges_Annotations_NilToEmpty(t *testing.T) {
	previous := baseSpec()
	previous.Annotations = nil
	target := baseSpec()
	target.Sequence = 2
	empty := map[string]string{}
	target.Annotations = &empty

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges for Annotations nil→empty")
	}
}

func TestDetectChanges_Annotations_SameValues(t *testing.T) {
	previous := baseSpec()
	a1 := map[string]string{"key": "value"}
	previous.Annotations = &a1
	target := baseSpec()
	target.Sequence = 2
	a2 := map[string]string{"key": "value"}
	target.Annotations = &a2

	report := DetectChanges(previous, target)

	if report.HasKubernetesChanges {
		t.Error("expected no HasKubernetesChanges for identical annotations")
	}
}

func TestDetectChanges_KubernetesOnly_Resources(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Resources = &v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
}

func TestDetectChanges_KubernetesOnly_Override(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Override = &redisv1.RedkeyOverrideSpec{
		StatefulSet: &redisv1.PartialStatefulSet{
			Spec: &redisv1.PartialStatefulSetSpec{
				MinReadySeconds: int32Ptr(10),
			},
		},
	}

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
}

func TestDetectChanges_KubernetesOnly_Pdb(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Pdb = redisv1.Pdb{
		Enabled:            true,
		PdbSizeUnavailable: intstr.FromInt(1),
	}

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
}

func TestDetectChanges_KubernetesOnly_Storage(t *testing.T) {
	previous := baseSpec()
	previous.Ephemeral = false
	previous.Storage = "10Gi"
	target := baseSpec()
	target.Sequence = 2
	target.Ephemeral = false
	target.Storage = "20Gi"

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
}

func TestDetectChanges_RedisConfigOnly_Config(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.RedisConfig = "maxmemory 200mb"

	report := DetectChanges(previous, target)

	if !report.HasRedisConfigChanges {
		t.Error("expected HasRedisConfigChanges")
	}
	if report.HasKubernetesChanges || report.HasTopologyChanges || report.HasRobinChanges {
		t.Error("unexpected additional change flags")
	}
	if !report.RequiresClusterOperation() {
		t.Error("should require cluster operation")
	}
}

func TestDetectChanges_RedisConfigOnly_Version(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Version = "9.0"

	report := DetectChanges(previous, target)

	if !report.HasRedisConfigChanges {
		t.Error("expected HasRedisConfigChanges")
	}
}

func TestDetectChanges_AuthOnly_SecretName(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Auth = redisv1.RedisAuth{SecretName: "new-secret"}

	report := DetectChanges(previous, target)

	if !report.HasAuthChanges {
		t.Error("expected HasAuthChanges")
	}
	if report.HasRedisConfigChanges {
		t.Error("auth should NOT set HasRedisConfigChanges")
	}
	if report.RequiresClusterOperation() {
		t.Error("auth-only should NOT require cluster operation")
	}
}

func TestDetectChanges_AuthOnly_Disable(t *testing.T) {
	previous := baseSpec()
	previous.Auth = redisv1.RedisAuth{SecretName: "my-secret"}
	target := baseSpec()
	target.Auth = redisv1.RedisAuth{}

	report := DetectChanges(previous, target)

	if !report.HasAuthChanges {
		t.Error("expected HasAuthChanges for auth removal")
	}
	if report.RequiresClusterOperation() {
		t.Error("disabling auth should NOT require cluster operation")
	}
}

func TestDetectChanges_AuthOnly_NoSecretToSecret(t *testing.T) {
	previous := baseSpec()
	previous.Auth = redisv1.RedisAuth{}
	target := baseSpec()
	target.Sequence = 2
	target.Auth = redisv1.RedisAuth{SecretName: "new-secret"}

	report := DetectChanges(previous, target)

	if !report.HasAuthChanges {
		t.Error("expected HasAuthChanges for enabling auth")
	}
	if report.RequiresClusterOperation() {
		t.Error("enabling auth should NOT require cluster operation")
	}
}

func TestDetectChanges_AuthOnly_SecretToNoSecret(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Auth = redisv1.RedisAuth{}

	report := DetectChanges(previous, target)

	if !report.HasAuthChanges {
		t.Error("expected HasAuthChanges for disabling auth")
	}
	if report.RequiresClusterOperation() {
		t.Error("disabling auth should NOT require cluster operation")
	}
}

func TestDetectChanges_Combined_AuthAndRedisConfig(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Auth = redisv1.RedisAuth{SecretName: "new-secret"}
	target.RedisConfig = "maxmemory 200mb"

	report := DetectChanges(previous, target)

	if !report.HasAuthChanges {
		t.Error("expected HasAuthChanges")
	}
	if !report.HasRedisConfigChanges {
		t.Error("expected HasRedisConfigChanges")
	}
	if !report.RequiresClusterOperation() {
		t.Error("should require cluster operation (has redis config change)")
	}
}

func TestDetectChanges_Combined_AuthAndRobin(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Auth = redisv1.RedisAuth{SecretName: "new-secret"}
	target.RobinConfig.Reconciler.IntervalSeconds = intPtr(60)

	report := DetectChanges(previous, target)

	if !report.HasAuthChanges {
		t.Error("expected HasAuthChanges")
	}
	if !report.HasRobinChanges {
		t.Error("expected HasRobinChanges")
	}
	if !report.OnlyRobinChanges() {
		t.Error("auth + Robin should still be OnlyRobinChanges (no cluster op)")
	}
	if report.RequiresClusterOperation() {
		t.Error("auth + Robin should NOT require cluster operation")
	}
}

func TestDetectChanges_PurgeKeysOnRebalanceChange(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.PurgeKeysOnRebalance = boolPtr(false)

	report := DetectChanges(previous, target)

	if !report.HasPurgeKeysOnRebalanceChange {
		t.Error("expected HasPurgeKeysOnRebalanceChange")
	}
	// PurgeKeysOnRebalance alone doesn't require cluster operation
	if report.RequiresClusterOperation() {
		t.Error("PurgeKeysOnRebalance alone should not require cluster operation")
	}
}

func TestDetectChanges_Combined_TopologyAndKubernetes(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Primaries = 6
	target.Image = "redis:9-bookworm"

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
	if report.TopologyScaleDirection != ScaleUp {
		t.Errorf("expected ScaleUp, got %v", report.TopologyScaleDirection)
	}
}

func TestDetectChanges_Combined_TopologyAndRedisConfig(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Primaries = 2
	target.RedisConfig = "maxmemory 200mb"

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	if !report.HasRedisConfigChanges {
		t.Error("expected HasRedisConfigChanges")
	}
	if report.TopologyScaleDirection != ScaleDown {
		t.Errorf("expected ScaleDown, got %v", report.TopologyScaleDirection)
	}
}

func TestDetectChanges_Combined_AllCategories(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 2
	target.Primaries = 6
	target.Image = "redis:9-bookworm"
	target.RedisConfig = "maxmemory 200mb"
	target.RobinConfig.Reconciler.IntervalSeconds = intPtr(60)
	target.PurgeKeysOnRebalance = boolPtr(false)

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges")
	}
	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges")
	}
	if !report.HasRedisConfigChanges {
		t.Error("expected HasRedisConfigChanges")
	}
	if !report.HasRobinChanges {
		t.Error("expected HasRobinChanges")
	}
	if !report.HasPurgeKeysOnRebalanceChange {
		t.Error("expected HasPurgeKeysOnRebalanceChange")
	}
	if report.OnlyRobinChanges() {
		t.Error("should not be OnlyRobinChanges")
	}
	if !report.RequiresClusterOperation() {
		t.Error("should require cluster operation")
	}
}

func TestDetectChanges_IgnoresControlFields(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Sequence = 99
	target.SkipIfSuperseded = !previous.SkipIfSuperseded

	report := DetectChanges(previous, target)

	if report.HasAnyChange() {
		t.Errorf("control fields should be ignored, got: %+v", report)
	}
}

func TestDetectChanges_DeletePVC_NilToSet(t *testing.T) {
	previous := baseSpec()
	previous.DeletePVC = nil
	target := baseSpec()
	target.Sequence = 2
	target.DeletePVC = boolPtr(true)

	report := DetectChanges(previous, target)

	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges for DeletePVC nil→true")
	}
}

func TestDetectChanges_Labels_NilToEmpty(t *testing.T) {
	previous := baseSpec()
	previous.Labels = nil
	target := baseSpec()
	target.Sequence = 2
	empty := map[string]string{}
	target.Labels = &empty

	report := DetectChanges(previous, target)

	// nil vs &{} is a change
	if !report.HasKubernetesChanges {
		t.Error("expected HasKubernetesChanges for Labels nil→empty")
	}
}

func TestDetectChanges_RobinConfig_SubfieldNilToNil(t *testing.T) {
	previous := baseSpec()
	previous.RobinConfig.Profiling = nil
	target := baseSpec()
	target.Sequence = 2
	target.RobinConfig.Profiling = nil

	report := DetectChanges(previous, target)

	if report.HasRobinChanges {
		t.Error("nil-nil profiling should not be a change")
	}
}

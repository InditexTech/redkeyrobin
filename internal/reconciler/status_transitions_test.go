// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"testing"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

func TestDetermineStatusTransition_NoChanges(t *testing.T) {
	report := ChangeReport{}
	result := DetermineStatusTransition(report)
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestDetermineStatusTransition_OnlyRobinChanges(t *testing.T) {
	report := ChangeReport{HasRobinChanges: true}
	result := DetermineStatusTransition(report)
	if result != "" {
		t.Errorf("expected empty string for robin-only, got %q", result)
	}
}

func TestDetermineStatusTransition_OnlyPurgeKeysChange(t *testing.T) {
	report := ChangeReport{HasPurgeKeysOnRebalanceChange: true}
	result := DetermineStatusTransition(report)
	if result != "" {
		t.Errorf("expected empty string for purgeKeys-only, got %q", result)
	}
}

func TestDetermineStatusTransition_TopologyScaleUp(t *testing.T) {
	report := ChangeReport{
		HasTopologyChanges:     true,
		TopologyScaleDirection: ScaleUp,
		PrimariesDelta:         3,
	}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusScalingUp {
		t.Errorf("expected ScalingUp, got %q", result)
	}
}

func TestDetermineStatusTransition_TopologyScaleDown(t *testing.T) {
	report := ChangeReport{
		HasTopologyChanges:     true,
		TopologyScaleDirection: ScaleDown,
		PrimariesDelta:         -2,
	}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusScalingDown {
		t.Errorf("expected ScalingDown, got %q", result)
	}
}

func TestDetermineStatusTransition_TopologyScaleUpWithKubernetesChanges(t *testing.T) {
	report := ChangeReport{
		HasTopologyChanges:     true,
		TopologyScaleDirection: ScaleUp,
		PrimariesDelta:         2,
		HasKubernetesChanges:   true,
	}
	result := DetermineStatusTransition(report)
	// Topology takes priority.
	if result != redisv1.ClusterStatusScalingUp {
		t.Errorf("expected ScalingUp (topology priority), got %q", result)
	}
}

func TestDetermineStatusTransition_TopologyScaleDownWithRedisConfig(t *testing.T) {
	report := ChangeReport{
		HasTopologyChanges:     true,
		TopologyScaleDirection: ScaleDown,
		ReplicasDelta:          -1,
		HasRedisConfigChanges:  true,
	}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusScalingDown {
		t.Errorf("expected ScalingDown (topology priority), got %q", result)
	}
}

func TestDetermineStatusTransition_TopologyScaleUpWithRobinChanges(t *testing.T) {
	report := ChangeReport{
		HasTopologyChanges:     true,
		TopologyScaleDirection: ScaleUp,
		PrimariesDelta:         1,
		HasRobinChanges:        true,
	}
	result := DetermineStatusTransition(report)
	// Topology takes priority; Robin changes applied via hot-reload.
	if result != redisv1.ClusterStatusScalingUp {
		t.Errorf("expected ScalingUp, got %q", result)
	}
}

func TestDetermineStatusTransition_KubernetesChangesOnly(t *testing.T) {
	report := ChangeReport{HasKubernetesChanges: true}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusUpgrading {
		t.Errorf("expected Upgrading, got %q", result)
	}
}

func TestDetermineStatusTransition_RedisConfigChangesOnly(t *testing.T) {
	report := ChangeReport{HasRedisConfigChanges: true}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusUpgrading {
		t.Errorf("expected Upgrading, got %q", result)
	}
}

func TestDetermineStatusTransition_KubernetesAndRedisConfigChanges(t *testing.T) {
	report := ChangeReport{
		HasKubernetesChanges:  true,
		HasRedisConfigChanges: true,
	}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusUpgrading {
		t.Errorf("expected Upgrading, got %q", result)
	}
}

func TestDetermineStatusTransition_RobinAndKubernetesChanges(t *testing.T) {
	report := ChangeReport{
		HasRobinChanges:      true,
		HasKubernetesChanges: true,
	}
	result := DetermineStatusTransition(report)
	// Robin alone is no-op, but K8s changes trigger Upgrading.
	if result != redisv1.ClusterStatusUpgrading {
		t.Errorf("expected Upgrading, got %q", result)
	}
}

func TestDetermineStatusTransition_PurgeKeysAndTopology(t *testing.T) {
	report := ChangeReport{
		HasPurgeKeysOnRebalanceChange: true,
		HasTopologyChanges:            true,
		TopologyScaleDirection:        ScaleUp,
		PrimariesDelta:                2,
	}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusScalingUp {
		t.Errorf("expected ScalingUp, got %q", result)
	}
}

func TestDetermineStatusTransition_AllChanges(t *testing.T) {
	report := ChangeReport{
		HasRobinChanges:               true,
		HasTopologyChanges:            true,
		TopologyScaleDirection:        ScaleDown,
		PrimariesDelta:                -1,
		HasKubernetesChanges:          true,
		HasRedisConfigChanges:         true,
		HasPurgeKeysOnRebalanceChange: true,
	}
	result := DetermineStatusTransition(report)
	// Topology takes priority.
	if result != redisv1.ClusterStatusScalingDown {
		t.Errorf("expected ScalingDown, got %q", result)
	}
}

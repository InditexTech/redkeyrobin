// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"testing"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// makeFullConfig builds a RedkeyConfig with all spec fields set to
// a consistent baseline so that onlyTopologyChange comparisons are meaningful.
func makeFullConfig(name string, seq int, phase string, primaries, replicas int32, skip bool) redisv1.RedkeyConfig {
	return redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           seq,
			SkipIfSuperseded:   skip,
			Primaries:          primaries,
			ReplicasPerPrimary: replicas,
			Ephemeral:          true,
			Image:              "redis:7",
			Version:            "7.0",
		},
		Status: redisv1.RedkeyConfigStatus{ConfigPhase: phase},
	}
}

func TestApplySuperseding_NotSkippable(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, false), // skipIfSuperseded=false
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, false),
	}
	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	if result.Selected.Name != "cfg-2" {
		t.Fatalf("expected cfg-2, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %d", len(result.Superseded))
	}
}

func TestApplySuperseding_InProgress(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhaseInProgress, 5, 1, true),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, true),
	}
	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	if result.Selected.Name != "cfg-2" {
		t.Fatalf("expected cfg-2 (InProgress, not supersedable), got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %d", len(result.Superseded))
	}
}

func TestApplySuperseding_Applied(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, true),
	}
	selected := &configs[0]

	result := ApplySuperseding(configs, selected)

	if result.Selected.Name != "cfg-1" {
		t.Fatalf("expected cfg-1 (Applied), got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %d", len(result.Superseded))
	}
}

func TestApplySuperseding_SinglePending_NoNext(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, true),
	}
	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	if result.Selected.Name != "cfg-2" {
		t.Fatalf("expected cfg-2 (no next config), got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %d", len(result.Superseded))
	}
}

func TestApplySuperseding_TwoConfigs_TopologyOnly(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, true),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, false),
	}
	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	if result.Selected.Name != "cfg-3" {
		t.Fatalf("expected cfg-3, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 1 || result.Superseded[0].Name != "cfg-2" {
		t.Fatalf("expected [cfg-2] superseded, got %v", namesOf(result.Superseded))
	}
}

func TestApplySuperseding_ChainOfThree(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, true),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, true),
		makeFullConfig("cfg-4", 4, redisv1.ConfigPhasePending, 9, 2, false),
	}
	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	if result.Selected.Name != "cfg-4" {
		t.Fatalf("expected cfg-4, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 2 {
		t.Fatalf("expected 2 superseded, got %d: %v", len(result.Superseded), namesOf(result.Superseded))
	}
	if result.Superseded[0].Name != "cfg-2" || result.Superseded[1].Name != "cfg-3" {
		t.Fatalf("expected [cfg-2, cfg-3] superseded, got %v", namesOf(result.Superseded))
	}
}

func TestApplySuperseding_ChainBreaksOnNonTopologyChange(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, true),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, true),
	}
	// cfg-3 has a different Image — not a topology-only change.
	configs[2].Spec.Image = "redis:8"

	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	// Chain should stop at cfg-2 because cfg-3 has non-topology changes.
	if result.Selected.Name != "cfg-2" {
		t.Fatalf("expected cfg-2 (chain breaks at cfg-3), got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %v", namesOf(result.Superseded))
	}
}

func TestApplySuperseding_MiddleWithoutSkipStopsChain(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, true),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, false), // skipIfSuperseded=false
		makeFullConfig("cfg-4", 4, redisv1.ConfigPhasePending, 9, 2, true),
	}
	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	// cfg-3 is selected (topology-only vs baseline), but chain stops because
	// cfg-3 has skipIfSuperseded=false.
	if result.Selected.Name != "cfg-3" {
		t.Fatalf("expected cfg-3, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 1 || result.Superseded[0].Name != "cfg-2" {
		t.Fatalf("expected [cfg-2] superseded, got %v", namesOf(result.Superseded))
	}
}

func TestApplySuperseding_NoBaseline(t *testing.T) {
	// No Applied config exists — first config in the cluster.
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1, true),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, false),
	}
	selected := &configs[0]

	result := ApplySuperseding(configs, selected)

	// Without a baseline, compare cfg-2 vs cfg-1: only topology differs.
	if result.Selected.Name != "cfg-2" {
		t.Fatalf("expected cfg-2, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 1 || result.Superseded[0].Name != "cfg-1" {
		t.Fatalf("expected [cfg-1] superseded, got %v", namesOf(result.Superseded))
	}
}

func TestApplySuperseding_SelectedHasNonTopologyChangeVsBaseline(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, true),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, true),
	}
	// cfg-2 differs in a non-topology field from baseline cfg-1.
	configs[1].Spec.Version = "8.0"

	selected := &configs[1]

	result := ApplySuperseding(configs, selected)

	// Superseding should not start because cfg-2 has non-topology changes vs baseline.
	if result.Selected.Name != "cfg-2" {
		t.Fatalf("expected cfg-2, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %v", namesOf(result.Superseded))
	}
}

func TestOnlyTopologyChange(t *testing.T) {
	base := redisv1.RedkeyConfigSpec{
		Sequence:           1,
		SkipIfSuperseded:   false,
		Primaries:          3,
		ReplicasPerPrimary: 1,
		Ephemeral:          true,
		Image:              "redis:7",
		Version:            "7.0",
	}

	t.Run("identical except topology and control fields", func(t *testing.T) {
		other := base
		other.Sequence = 2
		other.SkipIfSuperseded = true
		other.Primaries = 5
		other.ReplicasPerPrimary = 2
		if !onlyTopologyChange(base, other) {
			t.Fatal("expected true")
		}
	})

	t.Run("different image", func(t *testing.T) {
		other := base
		other.Sequence = 2
		other.Primaries = 5
		other.Image = "redis:8"
		if onlyTopologyChange(base, other) {
			t.Fatal("expected false (image differs)")
		}
	})

	t.Run("different version", func(t *testing.T) {
		other := base
		other.Sequence = 2
		other.Version = "8.0"
		if onlyTopologyChange(base, other) {
			t.Fatal("expected false (version differs)")
		}
	})

	t.Run("different ephemeral", func(t *testing.T) {
		other := base
		other.Sequence = 2
		other.Ephemeral = false
		if onlyTopologyChange(base, other) {
			t.Fatal("expected false (ephemeral differs)")
		}
	})
}

// --- findBaseline tests ---

func TestFindBaseline_NoApplied(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, false),
	}
	selected := &configs[1]

	baseline := findBaseline(configs, selected)
	if baseline != nil {
		t.Fatalf("expected nil baseline, got %s", baseline.Name)
	}
}

func TestFindBaseline_SingleApplied(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1, false),
	}
	selected := &configs[1]

	baseline := findBaseline(configs, selected)
	if baseline == nil || baseline.Name != "cfg-1" {
		t.Fatalf("expected cfg-1 as baseline, got %v", baseline)
	}
}

func TestFindBaseline_MultipleApplied(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhaseApplied, 5, 1, false),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, false),
	}
	selected := &configs[2]

	baseline := findBaseline(configs, selected)
	if baseline == nil || baseline.Name != "cfg-2" {
		t.Fatalf("expected cfg-2 as baseline (last Applied), got %v", baseline)
	}
}

func TestFindBaseline_AppliedAfterSelected(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhaseApplied, 5, 1, false),
	}
	selected := &configs[0]

	baseline := findBaseline(configs, selected)
	if baseline != nil {
		t.Fatalf("expected nil baseline (Applied is after selected), got %s", baseline.Name)
	}
}

func TestFindBaseline_MixedPhases(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhaseSuperseded, 5, 1, false),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhaseApplied, 7, 1, false),
		makeFullConfig("cfg-4", 4, redisv1.ConfigPhasePending, 9, 2, false),
	}
	selected := &configs[3]

	baseline := findBaseline(configs, selected)
	if baseline == nil || baseline.Name != "cfg-3" {
		t.Fatalf("expected cfg-3 as baseline, got %v", baseline)
	}
}

// --- indexOfConfig tests ---

func TestIndexOfConfig_Found(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, "", 3, 1, false),
		makeFullConfig("cfg-2", 2, "", 5, 1, false),
		makeFullConfig("cfg-3", 3, "", 7, 1, false),
	}

	idx := indexOfConfig(configs, &configs[1])
	if idx != 1 {
		t.Fatalf("expected index 1, got %d", idx)
	}
}

func TestIndexOfConfig_NotFound(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, "", 3, 1, false),
	}
	other := makeFullConfig("cfg-99", 99, "", 3, 1, false)

	idx := indexOfConfig(configs, &other)
	if idx != -1 {
		t.Fatalf("expected -1, got %d", idx)
	}
}

func TestIndexOfConfig_EmptySlice(t *testing.T) {
	other := makeFullConfig("cfg-1", 1, "", 3, 1, false)

	idx := indexOfConfig(nil, &other)
	if idx != -1 {
		t.Fatalf("expected -1 for nil slice, got %d", idx)
	}
}

// --- Additional onlyTopologyChange tests ---

func TestOnlyTopologyChange_IdenticalSpecs(t *testing.T) {
	spec := redisv1.RedkeyConfigSpec{
		Sequence:           1,
		Primaries:          3,
		ReplicasPerPrimary: 1,
		Ephemeral:          true,
		Image:              "redis:7",
		Version:            "7.0",
	}
	if !onlyTopologyChange(spec, spec) {
		t.Fatal("expected true for identical specs")
	}
}

func TestOnlyTopologyChange_DifferentRedisConfig(t *testing.T) {
	a := redisv1.RedkeyConfigSpec{
		Sequence:    1,
		Primaries:   3,
		RedisConfig: "maxmemory 100mb",
	}
	b := a
	b.Sequence = 2
	b.Primaries = 5
	b.RedisConfig = "maxmemory 200mb"

	if onlyTopologyChange(a, b) {
		t.Fatal("expected false (RedisConfig differs)")
	}
}

func TestOnlyTopologyChange_DifferentStorage(t *testing.T) {
	a := redisv1.RedkeyConfigSpec{
		Sequence:  1,
		Primaries: 3,
		Storage:   "10Gi",
	}
	b := a
	b.Sequence = 2
	b.Primaries = 5
	b.Storage = "20Gi"

	if onlyTopologyChange(a, b) {
		t.Fatal("expected false (Storage differs)")
	}
}

func TestOnlyTopologyChange_DifferentStorageClassName(t *testing.T) {
	a := redisv1.RedkeyConfigSpec{
		Sequence:         1,
		Primaries:        3,
		StorageClassName: "standard",
	}
	b := a
	b.Sequence = 2
	b.Primaries = 5
	b.StorageClassName = "premium"

	if onlyTopologyChange(a, b) {
		t.Fatal("expected false (StorageClassName differs)")
	}
}

func TestOnlyTopologyChange_OnlyReplicasChange(t *testing.T) {
	a := redisv1.RedkeyConfigSpec{
		Sequence:           1,
		Primaries:          3,
		ReplicasPerPrimary: 1,
		Ephemeral:          true,
		Image:              "redis:7",
		Version:            "7.0",
	}
	b := a
	b.Sequence = 2
	b.ReplicasPerPrimary = 3

	if !onlyTopologyChange(a, b) {
		t.Fatal("expected true (only replicas differ)")
	}
}

// --- Additional ApplySuperseding edge cases ---

func TestApplySuperseding_SelectedNotInList(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
	}
	selected := makeFullConfig("cfg-99", 99, redisv1.ConfigPhasePending, 5, 1, true)

	result := ApplySuperseding(configs, &selected)
	// selected not found in list → no superseding
	if result.Selected.Name != "cfg-99" {
		t.Fatalf("expected cfg-99, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %d", len(result.Superseded))
	}
}

func TestApplySuperseding_EmptyConfigList(t *testing.T) {
	selected := makeFullConfig("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1, true)

	result := ApplySuperseding(nil, &selected)
	if result.Selected.Name != "cfg-1" {
		t.Fatalf("expected cfg-1, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 0 {
		t.Fatalf("expected no superseded, got %d", len(result.Superseded))
	}
}

func TestApplySuperseding_SupersededPhaseSelected(t *testing.T) {
	// A Superseded config with SkipIfSuperseded should still be eligible to start a chain
	configs := []redisv1.RedkeyConfig{
		makeFullConfig("cfg-1", 1, redisv1.ConfigPhaseApplied, 3, 1, false),
		makeFullConfig("cfg-2", 2, redisv1.ConfigPhaseSuperseded, 5, 1, true),
		makeFullConfig("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1, false),
	}
	selected := &configs[1]

	result := ApplySuperseding(configs, selected)
	// Superseded is neither InProgress nor Applied, so superseding can proceed
	if result.Selected.Name != "cfg-3" {
		t.Fatalf("expected cfg-3, got %s", result.Selected.Name)
	}
	if len(result.Superseded) != 1 || result.Superseded[0].Name != "cfg-2" {
		t.Fatalf("expected [cfg-2] superseded, got %v", namesOf(result.Superseded))
	}
}

// namesOf extracts names from a slice of config pointers for test output.
func namesOf(cfgs []*redisv1.RedkeyConfig) []string {
	names := make([]string, len(cfgs))
	for i, c := range cfgs {
		names[i] = c.Name
	}
	return names
}

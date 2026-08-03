// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"testing"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// helper to build a minimal RedkeyConfig for testing.
func makeConfig(name string, sequence int, phase string) redisv1.RedkeyConfig {
	return redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       redisv1.RedkeyConfigSpec{Sequence: sequence},
		Status:     redisv1.RedkeyConfigStatus{ConfigPhase: phase},
	}
}

func TestSelectConfig_EmptyList(t *testing.T) {
	prev, result := SelectConfig(nil)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
	if prev != nil {
		t.Fatalf("expected nil prev, got %v", prev)
	}

	prev, result = SelectConfig([]redisv1.RedkeyConfig{})
	if result != nil {
		t.Fatalf("expected nil for empty slice, got %v", result)
	}
	if prev != nil {
		t.Fatalf("expected nil prev for empty slice, got %v", prev)
	}
}

func TestSelectConfig_SingleConfig(t *testing.T) {
	tests := []struct {
		name  string
		phase string
	}{
		{"Pending", redisv1.ConfigPhasePending},
		{"InProgress", redisv1.ConfigPhaseInProgress},
		{"Applied", redisv1.ConfigPhaseApplied},
		{"Superseded", redisv1.ConfigPhaseSuperseded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := makeConfig("cfg-1", 1, tt.phase)
			prev, result := SelectConfig([]redisv1.RedkeyConfig{cfg})
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if result.Name != "cfg-1" {
				t.Fatalf("expected cfg-1, got %s", result.Name)
			}
			if prev != nil {
				t.Fatalf("expected nil prev for single config, got %s", prev.Name)
			}
		})
	}
}

func TestSelectConfig_SingleSuperseded(t *testing.T) {
	// A single Superseded config with no Applied configs: prev is nil.
	cfg := makeConfig("cfg-1", 1, redisv1.ConfigPhaseSuperseded)
	prev, result := SelectConfig([]redisv1.RedkeyConfig{cfg})
	if result == nil || result.Name != "cfg-1" {
		t.Fatalf("expected cfg-1, got %v", result)
	}
	if prev != nil {
		t.Fatalf("expected nil prev, got %s", prev.Name)
	}
}

func TestSelectConfig_FirstNonApplied(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeConfig("cfg-1", 1, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-2", 2, redisv1.ConfigPhasePending),
		makeConfig("cfg-3", 3, redisv1.ConfigPhasePending),
	}

	prev, result := SelectConfig(configs)
	if result == nil || result.Name != "cfg-2" {
		t.Fatalf("expected cfg-2, got %v", result)
	}
	if prev == nil || prev.Name != "cfg-1" {
		t.Fatalf("expected prev cfg-1, got %v", prev)
	}
}

func TestSelectConfig_FirstIsPending(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeConfig("cfg-1", 1, redisv1.ConfigPhasePending),
		makeConfig("cfg-2", 2, redisv1.ConfigPhasePending),
	}

	prev, result := SelectConfig(configs)
	if result == nil || result.Name != "cfg-1" {
		t.Fatalf("expected cfg-1, got %v", result)
	}
	if prev != nil {
		t.Fatalf("expected nil prev when first is selected, got %s", prev.Name)
	}
}

func TestSelectConfig_InProgressSelected(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeConfig("cfg-1", 1, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-2", 2, redisv1.ConfigPhaseInProgress),
		makeConfig("cfg-3", 3, redisv1.ConfigPhasePending),
	}

	prev, result := SelectConfig(configs)
	if result == nil || result.Name != "cfg-2" {
		t.Fatalf("expected cfg-2, got %v", result)
	}
	if prev == nil || prev.Name != "cfg-1" {
		t.Fatalf("expected prev cfg-1, got %v", prev)
	}
}

func TestSelectConfig_SupersededSkipped(t *testing.T) {
	// Superseded configs are skipped; the first Pending after them is selected.
	// previousConfig should be the last Applied, not the Superseded one.
	configs := []redisv1.RedkeyConfig{
		makeConfig("cfg-1", 1, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-2", 2, redisv1.ConfigPhaseSuperseded),
		makeConfig("cfg-3", 3, redisv1.ConfigPhasePending),
	}

	prev, result := SelectConfig(configs)
	if result == nil || result.Name != "cfg-3" {
		t.Fatalf("expected cfg-3, got %v", result)
	}
	if prev == nil || prev.Name != "cfg-1" {
		t.Fatalf("expected prev cfg-1 (last Applied), got %v", prev)
	}
}

func TestSelectConfig_MultipleSupersededSkipped(t *testing.T) {
	// Multiple Applied followed by multiple Superseded: target is the first Pending,
	// previousConfig is the last Applied before it.
	configs := []redisv1.RedkeyConfig{
		makeConfig("cfg-1", 1, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-2", 2, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-3", 3, redisv1.ConfigPhaseSuperseded),
		makeConfig("cfg-4", 4, redisv1.ConfigPhaseSuperseded),
		makeConfig("cfg-5", 5, redisv1.ConfigPhasePending),
	}

	prev, result := SelectConfig(configs)
	if result == nil || result.Name != "cfg-5" {
		t.Fatalf("expected cfg-5, got %v", result)
	}
	if prev == nil || prev.Name != "cfg-2" {
		t.Fatalf("expected prev cfg-2 (last Applied), got %v", prev)
	}
}

func TestSelectConfig_AllApplied(t *testing.T) {
	configs := []redisv1.RedkeyConfig{
		makeConfig("cfg-1", 1, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-2", 2, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-3", 3, redisv1.ConfigPhaseApplied),
	}

	prev, result := SelectConfig(configs)
	if result == nil || result.Name != "cfg-3" {
		t.Fatalf("expected cfg-3 (highest sequence), got %v", result)
	}
	if prev == nil || prev.Name != "cfg-2" {
		t.Fatalf("expected prev cfg-2, got %v", prev)
	}
}

func TestSelectConfig_EmptyPhaseIsNotApplied(t *testing.T) {
	// An empty ConfigPhase (not yet initialised) should be treated as non-Applied.
	configs := []redisv1.RedkeyConfig{
		makeConfig("cfg-1", 1, redisv1.ConfigPhaseApplied),
		makeConfig("cfg-2", 2, ""),
		makeConfig("cfg-3", 3, redisv1.ConfigPhasePending),
	}

	prev, result := SelectConfig(configs)
	if result == nil || result.Name != "cfg-2" {
		t.Fatalf("expected cfg-2 (empty phase), got %v", result)
	}
	if prev == nil || prev.Name != "cfg-1" {
		t.Fatalf("expected prev cfg-1, got %v", prev)
	}
}

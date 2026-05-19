// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsManager_UpdateDynamicMetric_RegistersOnce(t *testing.T) {
	reg := prometheus.NewRegistry()
	mm := NewMetricsManager(reg)

	labels := map[string]string{"cluster": "c", "namespace": "n", "instanceId": "c-0"}
	mm.UpdateDynamicMetric("used_memory", labels, nil, 100)
	mm.UpdateDynamicMetric("used_memory", labels, nil, 200)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	for _, f := range families {
		if f.GetName() == "redkey_used_memory" {
			m := f.GetMetric()[0]
			if m.GetGauge().GetValue() != 200 {
				t.Fatalf("expected 200, got %f", m.GetGauge().GetValue())
			}
			return
		}
	}
	t.Fatal("metric redkey_used_memory not found")
}

func TestMetricsManager_UpdateDynamicMetric_WithAdditionalLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	mm := NewMetricsManager(reg)

	labels := map[string]string{"cluster": "c", "namespace": "n", "instanceId": "c-0"}
	additional := map[string]string{"redis_version": "7.0.0"}
	mm.UpdateDynamicMetric("redis_version", labels, additional, -1)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	for _, f := range families {
		if f.GetName() == "redkey_version" {
			m := f.GetMetric()[0]
			if m.GetGauge().GetValue() != -1 {
				t.Fatalf("expected -1, got %f", m.GetGauge().GetValue())
			}
			labelMap := make(map[string]string)
			for _, l := range m.GetLabel() {
				labelMap[l.GetName()] = l.GetValue()
			}
			if labelMap["redis_version"] != "7.0.0" {
				t.Fatalf("expected redis_version=7.0.0, got %s", labelMap["redis_version"])
			}
			return
		}
	}
	t.Fatal("metric redkey_version not found")
}

func TestMetricsManager_MetricName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"used_memory", "redkey_used_memory"},
		{"used-memory-rss", "redkey_used_memory_rss"},
		{"total_commands_processed", "redkey_total_commands_processed"},
		{"redis_version", "redkey_version"},
		{"redkey_cluster_metrics", "redkey_cluster_metrics"},
		{"redis_nodes_metrics", "redkey_nodes_metrics"},
	}
	for _, tt := range tests {
		got := metricName(tt.input)
		if got != tt.expected {
			t.Errorf("metricName(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestMetricsManager_MultipleMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	mm := NewMetricsManager(reg)

	labels := map[string]string{"cluster": "c", "namespace": "n", "instanceId": "c-0"}
	mm.UpdateDynamicMetric("used_memory", labels, nil, 1024)
	mm.UpdateDynamicMetric("connected_clients", labels, nil, 5)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	names := make(map[string]float64)
	for _, f := range families {
		for _, m := range f.GetMetric() {
			names[f.GetName()] = m.GetGauge().GetValue()
		}
	}

	if names["redkey_used_memory"] != 1024 {
		t.Fatalf("expected used_memory=1024, got %f", names["redkey_used_memory"])
	}
	if names["redkey_connected_clients"] != 5 {
		t.Fatalf("expected connected_clients=5, got %f", names["redkey_connected_clients"])
	}
}

func TestMetricsManager_ResetMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	mm := NewMetricsManager(reg)

	labels := map[string]string{"cluster": "c", "namespace": "n"}
	mm.UpdateDynamicMetric("test_metric", labels, nil, 42)
	mm.ResetMetrics()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}
	// After reset, no metrics should be gathered.
	if len(families) != 0 {
		t.Fatalf("expected 0 families after reset, got %d", len(families))
	}
}

// Helper to suppress unused import warning for dto.
var _ = (*dto.MetricFamily)(nil)

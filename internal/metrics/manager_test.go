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

func TestMetricsManager_UpdateDynamicMetricWithTime_HandlesMissingLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	mm := NewMetricsManager(reg)

	mm.UpdateDynamicMetricWithTime("cluster_metrics", map[string]string{
		"cluster":   "c",
		"namespace": "n",
		"field_a":   "1",
		"field_b":   "2",
	}, []string{"cluster", "namespace", "field_a", "field_b"})

	mm.UpdateDynamicMetricWithTime("cluster_metrics", map[string]string{
		"cluster":   "c",
		"namespace": "n",
		"field_a":   "3",
	}, []string{"cluster", "namespace", "field_a"})

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	family := metricFamilyByName(families, "redkey_cluster_metrics")
	if family == nil {
		t.Fatal("metric redkey_cluster_metrics not found")
	}

	foundReducedLabelSet := false
	for _, metric := range family.GetMetric() {
		labels := metricLabels(metric)
		if _, ok := labels["field_b"]; !ok {
			t.Fatal("expected registered label field_b to be present on every time series")
		}
		if labels["field_a"] == "3" {
			foundReducedLabelSet = true
			if labels["field_b"] != "" {
				t.Fatalf("expected missing label field_b to be empty, got %q", labels["field_b"])
			}
		}
	}
	if !foundReducedLabelSet {
		t.Fatal("expected a time series for the reduced label set")
	}
}

func TestMetricsManager_ResetRegistry_AllowsLabelSchemaChange(t *testing.T) {
	reg := NewResettableRegistry()
	mm := NewMetricsManager(reg)

	mm.UpdateDynamicMetric("schema_change_metric", map[string]string{
		"cluster":   "c",
		"namespace": "n",
		"old_label": "old",
	}, nil, 1)

	if !mm.ResetRegistry() {
		t.Fatal("expected resettable registry to be reset")
	}

	mm.UpdateDynamicMetric("schema_change_metric", map[string]string{
		"cluster":   "c",
		"namespace": "n",
		"new_label": "new",
	}, nil, 2)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	family := metricFamilyByName(families, "redkey_schema_change_metric")
	if family == nil {
		t.Fatal("metric redkey_schema_change_metric not found")
	}
	labels := metricLabels(family.GetMetric()[0])
	if _, exists := labels["old_label"]; exists {
		t.Fatalf("expected old_label to be absent after reset, got labels %v", labels)
	}
	if labels["new_label"] != "new" {
		t.Fatalf("expected new_label=new, got labels %v", labels)
	}
	if value := family.GetMetric()[0].GetGauge().GetValue(); value != 2 {
		t.Fatalf("expected gauge value 2, got %f", value)
	}
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

func metricLabels(metric *dto.Metric) map[string]string {
	labels := make(map[string]string)
	for _, label := range metric.GetLabel() {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}

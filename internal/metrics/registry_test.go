// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestResettableRegistry_ResetAllowsMetricSchemaChange(t *testing.T) {
	reg := NewResettableRegistry()

	first := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "redkey_resettable_test_metric",
		Help: "Resettable registry test metric.",
	}, []string{"old_label"})
	reg.MustRegister(first)
	first.WithLabelValues("old").Set(1)

	reg.Reset()

	second := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "redkey_resettable_test_metric",
		Help: "Resettable registry test metric.",
	}, []string{"new_label"})
	reg.MustRegister(second)
	second.WithLabelValues("new").Set(2)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	family := metricFamilyByName(families, "redkey_resettable_test_metric")
	if family == nil {
		t.Fatal("expected redkey_resettable_test_metric")
	}
	labels := metricLabels(family.GetMetric()[0])
	if _, exists := labels["old_label"]; exists {
		t.Fatalf("expected old_label to be absent after reset, got labels %v", labels)
	}
	if labels["new_label"] != "new" {
		t.Fatalf("expected new_label=new, got labels %v", labels)
	}
}

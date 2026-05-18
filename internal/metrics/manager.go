// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsManager handles dynamic Prometheus gauge registration and updates.
// Metrics are registered lazily on first use with variable label sets.
type MetricsManager struct {
	mu       sync.RWMutex
	metrics  map[string]*prometheus.GaugeVec
	registry prometheus.Registerer
}

// NewMetricsManager creates a MetricsManager using the given registerer.
func NewMetricsManager(registry prometheus.Registerer) *MetricsManager {
	return &MetricsManager{
		metrics:  make(map[string]*prometheus.GaugeVec),
		registry: registry,
	}
}

// metricName normalises a Redis INFO key into a valid Prometheus metric name.
func metricName(key string) string {
	return "redis_" + strings.ReplaceAll(key, "-", "_")
}

// UpdateDynamicMetric registers or updates a dynamic metric.
// It merges primary labels with additionalLabels and ensures a consistent
// label order by sorting the unique keys.
func (m *MetricsManager) UpdateDynamicMetric(
	name string,
	labels, additionalLabels map[string]string,
	value float64,
) {
	mergedLabels, sortedKeys := mergeLabels(labels, additionalLabels)
	fullName := metricName(name)

	m.mu.RLock()
	g, exists := m.metrics[fullName]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		// Double-check after acquiring write lock.
		if g, exists = m.metrics[fullName]; !exists {
			g = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: fullName,
				Help: fmt.Sprintf("Redis dynamic metric: %s", name),
			}, sortedKeys)
			m.registry.MustRegister(g)
			m.metrics[fullName] = g
		}
		m.mu.Unlock()
	}

	g.With(mergedLabels).Set(value)
}

// UpdateDynamicMetricWithTime registers or updates a dynamic metric with SetToCurrentTime.
func (m *MetricsManager) UpdateDynamicMetricWithTime(
	name string,
	labels map[string]string,
	labelKeys []string,
) {
	fullName := metricName(name)

	m.mu.RLock()
	g, exists := m.metrics[fullName]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		if g, exists = m.metrics[fullName]; !exists {
			g = prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: fullName,
				Help: fmt.Sprintf("Redis dynamic metric: %s", name),
			}, labelKeys)
			m.registry.MustRegister(g)
			m.metrics[fullName] = g
		}
		m.mu.Unlock()
	}

	labelMap := make(prometheus.Labels, len(labelKeys))
	for _, k := range labelKeys {
		labelMap[k] = labels[k]
	}
	g.With(labelMap).SetToCurrentTime()
}

// ResetMetrics resets all registered metric vectors.
func (m *MetricsManager) ResetMetrics() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, g := range m.metrics {
		g.Reset()
	}
}

// mergeLabels merges two label maps and returns the combined map with sorted keys.
func mergeLabels(a, b map[string]string) (prometheus.Labels, []string) {
	merged := make(prometheus.Labels, len(a)+len(b))
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		merged[k] = v
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return merged, keys
}

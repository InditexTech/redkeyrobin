// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricsManager handles dynamic Prometheus gauge registration and updates.
// Metrics are registered lazily on first use; later updates reuse the same
// label keys because Prometheus requires a fixed label set per metric.
type MetricsManager struct {
	mu       sync.RWMutex
	metrics  map[string]*managedMetric
	registry prometheus.Registerer
}

type managedMetric struct {
	gauge     *prometheus.GaugeVec
	labelKeys []string
}

// NewMetricsManager creates a MetricsManager using the given registerer.
func NewMetricsManager(registry prometheus.Registerer) *MetricsManager {
	return &MetricsManager{
		metrics:  make(map[string]*managedMetric),
		registry: registry,
	}
}

// metricName normalises a metric key into a valid Prometheus metric name
// with a single redkey_ prefix.
func metricName(key string) string {
	normalized := strings.ReplaceAll(key, "-", "_")
	for {
		switch {
		case strings.HasPrefix(normalized, "redis_"):
			normalized = strings.TrimPrefix(normalized, "redis_")
		case strings.HasPrefix(normalized, "redkey_"):
			normalized = strings.TrimPrefix(normalized, "redkey_")
		default:
			return "redkey_" + normalized
		}
	}
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
	metric, exists := m.metrics[fullName]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		// Double-check after acquiring write lock.
		if metric, exists = m.metrics[fullName]; !exists {
			g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: fullName,
				Help: fmt.Sprintf("Redis dynamic metric: %s", name),
			}, sortedKeys)
			m.registry.MustRegister(g)
			metric = &managedMetric{gauge: g, labelKeys: sortedKeys}
			m.metrics[fullName] = metric
		}
		m.mu.Unlock()
	}

	metric.gauge.With(labelsForKeys(mergedLabels, metric.labelKeys)).Set(value)
}

// UpdateDynamicMetricWithTime registers or updates a dynamic metric with SetToCurrentTime.
func (m *MetricsManager) UpdateDynamicMetricWithTime(
	name string,
	labels map[string]string,
	labelKeys []string,
) {
	fullName := metricName(name)
	sortedKeys := normalizeLabelKeys(labelKeys)

	m.mu.RLock()
	metric, exists := m.metrics[fullName]
	m.mu.RUnlock()

	if !exists {
		m.mu.Lock()
		if metric, exists = m.metrics[fullName]; !exists {
			g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Name: fullName,
				Help: fmt.Sprintf("Redis dynamic metric: %s", name),
			}, sortedKeys)
			m.registry.MustRegister(g)
			metric = &managedMetric{gauge: g, labelKeys: sortedKeys}
			m.metrics[fullName] = metric
		}
		m.mu.Unlock()
	}

	metric.gauge.With(labelsForKeys(labels, metric.labelKeys)).SetToCurrentTime()
}

// ResetMetrics resets all registered metric vectors.
func (m *MetricsManager) ResetMetrics() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, metric := range m.metrics {
		metric.gauge.Reset()
	}
}

// ResetMetricByName resets a single metric vector by its raw name,
// removing all label combinations. This prevents stale time series from
// accumulating when label values (e.g. node state) change between cycles.
func (m *MetricsManager) ResetMetricByName(name string) {
	fullName := metricName(name)
	m.mu.RLock()
	metric, exists := m.metrics[fullName]
	m.mu.RUnlock()
	if exists {
		metric.gauge.Reset()
	}
}

// ResetRegistry resets the underlying registry and clears registered metric
// descriptors. It returns false when the registerer cannot be reset.
func (m *MetricsManager) ResetRegistry() bool {
	resetter, ok := m.registry.(interface{ Reset() })
	if !ok {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	resetter.Reset()
	m.metrics = make(map[string]*managedMetric)
	return true
}

// mergeLabels merges two label maps and returns the combined map with sorted keys.
func mergeLabels(a, b map[string]string) (prometheus.Labels, []string) {
	merged := make(prometheus.Labels, len(a)+len(b))
	maps.Copy(merged, a)
	maps.Copy(merged, b)

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return merged, keys
}

func normalizeLabelKeys(keys []string) []string {
	normalized := append([]string{}, keys...)
	sort.Strings(normalized)

	write := 0
	for _, key := range normalized {
		if write > 0 && normalized[write-1] == key {
			continue
		}
		normalized[write] = key
		write++
	}

	return normalized[:write]
}

func labelsForKeys(labels map[string]string, labelKeys []string) prometheus.Labels {
	labelMap := make(prometheus.Labels, len(labelKeys))
	for _, k := range labelKeys {
		labelMap[k] = labels[k]
	}
	return labelMap
}

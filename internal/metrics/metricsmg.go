// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"log/slog"

	"github.com/inditextech/redisrobin/internal/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Label keys for metrics.
const (
	Cluster             = "cluster"
	Slot                = "slot"
	Tenant              = "tenant"
	Domain              = "domain"
	Environment         = "environment"
	Namespace           = "namespace"
	PlatformID          = "platformid"
	Service             = "service"
	JiraKey             = "jirakey"
	InstanceId          = "instanceId"
	PendingClusterNodes = "pending_cluster_nodes"
	PendingMigrates     = "pending_migrates"
	PendingImports      = "pending_imports"
	NodeID              = "nodeId"
	NodeIP              = "nodeIp"
	Role                = "role"
	Slots               = "slots"
	MasterID            = "masterId"
	NodeFailures        = "nodeFailures"
)

// MetricsManager encapsulates Prometheus metrics for Redis.
type MetricsManager struct {
	logger  *slog.Logger
	metrics map[string]*prometheus.GaugeVec
}

// NewMetricsManager creates a new MetricsManager, registers the base metrics,
// and returns the instance.
func NewMetricsManager() *MetricsManager {
	m := &MetricsManager{
		logger:  util.GetLogger("manager"),
		metrics: make(map[string]*prometheus.GaugeVec),
	}

	return m
}

// ResetMetrics resets all registered metrics.
func (m *MetricsManager) ResetMetrics() {
	for _, gaugeVec := range m.metrics {
		gaugeVec.Reset()
	}
}

// RegisterDynamicMetric registers a dynamic metric with the provided name and help.
func (m *MetricsManager) RegisterDynamicMetric(name, help string, labels []string) error {
	_, exists := m.metrics[name]
	if exists {
		return fmt.Errorf("dynamic metric %s already registered", name)
	}
	m.metrics[name] = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: help,
		},
		labels,
	)
	return nil
}

func (m *MetricsManager) UpdateDynamicMetricWithTime(name string, tags map[string]string, labelKeys []string, reset bool) error {
	// Check if the dynamic metric is already registered.
	gaugeVec, exists := m.metrics[name]
	if !exists {
		return fmt.Errorf("dynamic metric %s not registered", name)
	}
	if reset {
		gaugeVec.Reset()
	}
	updateGaugeWithCurrentTime(gaugeVec, tags, labelKeys)
	return nil
}

// UpdateDynamicMetric registers or updates a dynamic metric.
// The full metric name will be "redis_" + name.
// It merges primary labels with additionalLabels, and ensures a consistent
// label order by sorting the unique keys.
func (m *MetricsManager) UpdateDynamicMetric(
	name string,
	labels, additionalLabels map[string]string,
	value float64,
) {
	mergedLabels, sortedLabelKeys := mergeLabels(labels, additionalLabels)

	// Check if the dynamic metric is already registered.
	gaugeVec, exists := m.metrics[name]
	if !exists {
		gaugeVec = promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "redis_" + name,
				Help: fmt.Sprintf("Redis dynamic metric: %s", name),
			},
			sortedLabelKeys,
		)
		m.metrics[name] = gaugeVec
	}

	gaugeVec.With(mergedLabels).Set(value)
}

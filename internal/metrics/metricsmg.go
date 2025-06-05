// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/inditextech/redisrobin/internal/redis"
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

// Predefined label sets for reuse.
var (
	clusterInfoLabelKeys = []string{
		redis.ClusterState, redis.ClusterSlotsAssigned, redis.ClusterSlotsOk,
		redis.ClusterSlotsFail, redis.ClusterKnownNodes, redis.ClusterSize,
		redis.ClusterStatsMMS, redis.ClusterStatsMMR, redis.ClusterStatsMS, redis.ClusterStatsMR,
	}

	nodeInfoLabelKeys = []string{
		NodeID, NodeIP, Role, Slots, MasterID, NodeFailures,
	}
)

// MetricsManager encapsulates Prometheus metrics for Redis.
type MetricsManager struct {
	logger         *slog.Logger
	clusterInfo    *prometheus.GaugeVec
	nodeInfo       *prometheus.GaugeVec
	dynamicMetrics map[string]*prometheus.GaugeVec
}

// NewMetricsManager creates a new MetricsManager, registers the base metrics,
// and returns the instance.
func NewMetricsManager(extraLabels map[string]string) *MetricsManager {
	for k := range extraLabels {
		clusterInfoLabelKeys = append(clusterInfoLabelKeys, k)
		nodeInfoLabelKeys = append(nodeInfoLabelKeys, k)
	}

	m := &MetricsManager{
		logger: util.GetLogger("manager"),
		clusterInfo: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "redis_cluster_metrics",
				Help: "Redis cluster info metrics",
			},
			clusterInfoLabelKeys,
		),
		nodeInfo: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "redis_nodes_metrics",
				Help: "Redis nodes metrics",
			},
			nodeInfoLabelKeys,
		),
		dynamicMetrics: make(map[string]*prometheus.GaugeVec),
	}

	return m
}

// ResetMetrics resets all registered metrics.
func (m *MetricsManager) ResetMetrics() {
	m.clusterInfo.Reset()
	m.nodeInfo.Reset()
	for _, gaugeVec := range m.dynamicMetrics {
		gaugeVec.Reset()
	}
}

// UpdateClusterInfo updates the cluster info metric using provided tags.
// Resets the clusterInfo gauge each time before setting new values.
func (m *MetricsManager) UpdateClusterInfo(tags map[string]string) {
	m.clusterInfo.Reset()
	updateGaugeWithCurrentTime(m.clusterInfo, tags, clusterInfoLabelKeys)
}

// UpdateNodeInfo updates the node info metric using provided tags.
func (m *MetricsManager) UpdateNodeInfo(tags map[string]string) {
	updateGaugeWithCurrentTime(m.nodeInfo, tags, nodeInfoLabelKeys)
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
	gaugeVec, exists := m.dynamicMetrics[name]
	if !exists {
		gaugeVec = promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "redis_" + name,
				Help: fmt.Sprintf("Redis dynamic metric: %s", name),
			},
			sortedLabelKeys,
		)
		m.dynamicMetrics[name] = gaugeVec
	}

	gaugeVec.With(mergedLabels).Set(value)
}

// mergeLabels merges two label maps and returns a combined map along with a
// sorted slice of unique label keys. This ensures consistent ordering for metric registration.
func mergeLabels(a, b map[string]string) (prometheus.Labels, []string) {
	merged := make(prometheus.Labels)
	uniqueKeys := make(map[string]struct{})

	for k, v := range a {
		merged[k] = v
		uniqueKeys[k] = struct{}{}
	}
	for k, v := range b {
		merged[k] = v
		uniqueKeys[k] = struct{}{}
	}

	keys := make([]string, 0, len(uniqueKeys))
	for k := range uniqueKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return merged, keys
}

// updateGaugeWithCurrentTime is a helper that pulls labels by the provided keys
// from 'tags' and calls SetToCurrentTime() on the matching gauge.
func updateGaugeWithCurrentTime(
	gaugeVec *prometheus.GaugeVec,
	tags map[string]string,
	labelKeys []string,
) {
	labelMap := buildLabels(tags, labelKeys)
	gaugeVec.With(labelMap).SetToCurrentTime()
}

// buildLabels constructs a prometheus.Labels map using the given tag keys.
func buildLabels(tags map[string]string, labelKeys []string) prometheus.Labels {
	out := make(prometheus.Labels, len(labelKeys))
	for _, key := range labelKeys {
		out[key] = tags[key]
	}
	return out
}

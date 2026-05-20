// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/health"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

const (
	// labelCluster is the Prometheus label for the cluster name.
	labelCluster = "cluster"
	// labelNamespace is the Prometheus label for the namespace.
	labelNamespace = "namespace"
	// labelInstanceId is the Prometheus label for the Redis node instance.
	labelInstanceId = "instanceId"

	// secretPasswordKey is the key in the auth Secret that holds the Redis password.
	secretPasswordKey = "password"
)

// Collector periodically collects Redis INFO metrics from all cluster nodes
// and exposes them as Prometheus gauges.
type Collector struct {
	runtimeConfig *config.RuntimeConfig
	clusterName   string
	namespace     string
	k8sClient     client.Client
	manager       *MetricsManager
	healthChecker clusterHealthChecker
	logger        *slog.Logger

	// cachedPassword holds the Redis password read from the Secret.
	cachedPassword   string
	cachedAuthSecret string
	passwordLoaded   bool

	metricsLabelsLoaded bool
	metricsLabels       map[string]string
	metricsLabelKeys    []string
}

type clusterHealthChecker interface {
	Check(ctx context.Context, nodes []health.Node, password string) (*health.Report, error)
}

// NewCollector creates a new metrics Collector.
func NewCollector(
	runtimeConfig *config.RuntimeConfig,
	clusterName, namespace string,
	k8sClient client.Client,
	registry prometheus.Registerer,
) *Collector {
	logger := slog.Default().With("component", "metrics-collector", "cluster", clusterName)
	return &Collector{
		runtimeConfig: runtimeConfig,
		clusterName:   clusterName,
		namespace:     namespace,
		k8sClient:     k8sClient,
		manager:       NewMetricsManager(registry),
		healthChecker: health.NewChecker(logger.With("subcomponent", "cluster-health"), func(addr, password string) health.ClusterClient {
			return redis.NewClient(addr, password)
		}, 5*time.Second),
		logger: logger,
	}
}

// Start begins the metrics collection polling loop. It blocks until the context is cancelled.
func (c *Collector) Start(ctx context.Context) error {
	c.logger.Info("Starting metrics collector")

	for {
		interval := c.runtimeConfig.MetricsInterval()
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping metrics collector")
			return nil
		case <-time.After(interval):
			c.collect(ctx)
		}
	}
}

// collect performs a single metrics collection cycle across all cluster nodes.
func (c *Collector) collect(ctx context.Context) {
	c.reconcileMetricsLabelSchema()
	redisInfoKeys := c.runtimeConfig.RedisInfoKeys()

	password, err := c.getPassword(ctx)
	if err != nil {
		c.logger.Error("Failed to get Redis password", "error", err)
		return
	}

	nodes := c.discoverNodes()
	if len(nodes) == 0 {
		c.logger.Debug("No nodes discovered, skipping collection")
		return
	}

	if len(redisInfoKeys) == 0 {
		c.logger.Debug("No Redis INFO keys configured, skipping node INFO collection")
	} else {
		for _, node := range nodes {
			if err := c.collectNode(ctx, node, password, redisInfoKeys); err != nil {
				c.logger.Warn("Failed to collect metrics from node", "node", node.name, "error", err)
			}
		}
	}

	// Collect cluster-level metrics regardless of INFO key selection.
	c.collectClusterMetrics(ctx, nodes, password)
}

// nodeInfo represents a discovered Redis node.
type nodeInfo struct {
	name string
	addr string
}

// discoverNodes returns the list of Redis nodes based on pod IPs from the Kubernetes API.
// Using pod IPs directly works both in-cluster and out-of-cluster (e.g. kind dev).
func (c *Collector) discoverNodes() []nodeInfo {
	cfg := c.runtimeConfig.AppliedTopology()
	if cfg.Primaries <= 0 {
		return nil
	}

	podList := &corev1.PodList{}
	if err := c.k8sClient.List(context.Background(), podList,
		client.InNamespace(c.namespace),
		client.MatchingLabels{
			"redkey.inditex.dev/cluster":   c.clusterName,
			"redkey.inditex.dev/component": "redis",
		},
	); err != nil {
		c.logger.Error("Failed to list pods for node discovery", "error", err)
		return nil
	}

	nodes := make([]nodeInfo, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.PodIP == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", pod.Status.PodIP, redis.DefaultPort)
		nodes = append(nodes, nodeInfo{name: pod.Name, addr: addr})
	}
	return nodes
}

// buildNodeTags builds the base label map for a given node, including metadata labels.
func (c *Collector) buildNodeTags(nodeName string) map[string]string {
	tags := c.buildCommonMetadataTags()
	tags[labelInstanceId] = nodeName
	return tags
}

// buildCommonMetadataTags builds labels common to all metrics: cluster, namespace, plus metadata.
func (c *Collector) buildCommonMetadataTags() map[string]string {
	tags := map[string]string{
		labelCluster:   c.clusterName,
		labelNamespace: c.namespace,
	}
	for k, v := range c.runtimeConfig.MetricsLabels() {
		tags[k] = v
	}
	return tags
}

func (c *Collector) reconcileMetricsLabelSchema() {
	currentLabels := c.runtimeConfig.MetricsLabels()
	currentKeys := sortedMetricsLabelKeys(currentLabels)

	if !c.metricsLabelsLoaded {
		c.metricsLabelsLoaded = true
		c.metricsLabels = currentLabels
		c.metricsLabelKeys = currentKeys
		return
	}

	if !slices.Equal(c.metricsLabelKeys, currentKeys) {
		if c.manager != nil {
			if c.manager.ResetRegistry() {
				c.logMetricsLabelChange("Reset RedKey metrics registry after metrics label keys changed", currentKeys)
			} else {
				c.manager.ResetMetrics()
				if c.logger != nil {
					c.logger.Warn("Metrics label keys changed but registry is not resettable; keeping existing metric label schema", "oldKeys", c.metricsLabelKeys, "newKeys", currentKeys)
				}
			}
		}
		c.metricsLabels = currentLabels
		c.metricsLabelKeys = currentKeys
		return
	}

	if !equalStringMaps(c.metricsLabels, currentLabels) {
		if c.manager != nil {
			c.manager.ResetMetrics()
		}
		c.logMetricsLabelChange("Reset RedKey metric values after metrics label values changed", currentKeys)
		c.metricsLabels = currentLabels
	}
}

func (c *Collector) logMetricsLabelChange(message string, newKeys []string) {
	if c.logger == nil {
		return
	}
	c.logger.Info(message, "oldKeys", c.metricsLabelKeys, "newKeys", newKeys)
}

func sortedMetricsLabelKeys(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func equalStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, aValue := range a {
		if b[key] != aValue {
			return false
		}
	}
	return true
}

// collectNode collects INFO ALL from a single Redis node and updates metrics.
func (c *Collector) collectNode(ctx context.Context, node nodeInfo, password string, infoKeys []string) error {
	rc := redis.NewClient(node.addr, password)
	defer func() {
		if err := rc.Close(); err != nil {
			c.logger.Debug("Error closing Redis client", "node", node.name, "error", err)
		}
	}()

	raw, err := rc.GetInfo(ctx)
	if err != nil {
		return err
	}

	info := redis.ParseInfoAll(raw)
	tags := c.buildNodeTags(node.name)

	// Separate keyspace entries from regular metrics.
	keyspaces := make(map[string]string)
	for key, value := range info {
		if strings.HasPrefix(key, "db") {
			keyspaces[key] = value
		}
	}

	for key, value := range info {
		// Skip keyspace entries — handled separately.
		if strings.HasPrefix(key, "db") {
			continue
		}
		// Skip values containing "slave0" (replication info noise).
		if strings.Contains(value, "slave0") {
			continue
		}

		normKey := strings.ReplaceAll(key, "-", "_")
		if !slices.Contains(infoKeys, normKey) {
			continue
		}

		// Compound value (contains "=") → split into sub-metrics.
		if strings.Contains(value, "=") {
			c.processSubmetrics(normKey, value, tags)
			continue
		}

		// Try numeric, then string-as-gauge.
		c.processSingleMetric(normKey, value, tags)
	}

	// Process keyspace metrics (e.g., db0:keys=1000,expires=50).
	c.processKeyspaceMetrics(keyspaces, tags)

	return nil
}

// processSubmetrics handles compound values like "calls=100,usec=500,usec_per_call=5.0".
func (c *Collector) processSubmetrics(metricName, rawValue string, tags map[string]string) {
	parts := strings.Split(rawValue, ",")
	for _, part := range parts {
		subKey, subVal, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		subMetricName := metricName + "_" + strings.ReplaceAll(subKey, "-", "_")
		c.processSingleMetric(subMetricName, subVal, tags)
	}
}

// processSingleMetric processes a single metric value:
// - integer → float gauge
// - float → float gauge
// - string → gauge=-1 with the string as an additional label
func (c *Collector) processSingleMetric(name, rawValue string, tags map[string]string) {
	if intVal, err := strconv.Atoi(rawValue); err == nil {
		c.manager.UpdateDynamicMetric(name, tags, nil, float64(intVal))
		return
	}
	if floatVal, err := strconv.ParseFloat(rawValue, 64); err == nil {
		c.manager.UpdateDynamicMetric(name, tags, nil, floatVal)
		return
	}
	// String value: expose as gauge=-1 with the metric name as an additional label key.
	additionalLabels := map[string]string{name: rawValue}
	c.manager.UpdateDynamicMetric(name, tags, additionalLabels, -1)
}

// processKeyspaceMetrics handles keyspace entries like "db0:keys=1000,expires=50,avg_ttl=1000".
func (c *Collector) processKeyspaceMetrics(keyspaces map[string]string, tags map[string]string) {
	for dbName, value := range keyspaces {
		additionalLabels := map[string]string{"database": dbName}
		parts := strings.Split(value, ",")
		for _, part := range parts {
			subKey, subVal, ok := strings.Cut(part, "=")
			if !ok {
				continue
			}
			metricName := "keyspace_" + subKey
			if floatVal, err := strconv.ParseFloat(subVal, 64); err == nil {
				c.manager.UpdateDynamicMetric(metricName, tags, additionalLabels, floatVal)
			}
		}
	}
}

// collectClusterMetrics collects CLUSTER INFO, CLUSTER NODES, and health metrics.
func (c *Collector) collectClusterMetrics(ctx context.Context, nodes []nodeInfo, password string) {
	if len(nodes) == 0 {
		return
	}

	c.ensureHealthChecker()

	report, err := c.healthChecker.Check(ctx, c.toHealthNodes(nodes), password)
	if err != nil {
		c.logger.Warn("Cluster health check reported errors", "error", err)
	}
	if report == nil {
		return
	}

	if report.ClusterInfo != nil {
		c.processClusterInfo(report.ClusterInfo)
	}
	if len(report.ClusterNodes) > 0 {
		c.processClusterNodes(report.ClusterNodes)
	}
	c.processClusterHealthMetrics(report)
}

func (c *Collector) ensureHealthChecker() {
	if c.logger == nil {
		c.logger = slog.Default().With("component", "metrics-collector", "cluster", c.clusterName)
	}
	if c.healthChecker == nil {
		c.healthChecker = health.NewChecker(c.logger.With("subcomponent", "cluster-health"), func(addr, password string) health.ClusterClient {
			return redis.NewClient(addr, password)
		}, 5*time.Second)
	}
}

func (c *Collector) toHealthNodes(nodes []nodeInfo) []health.Node {
	healthNodes := make([]health.Node, 0, len(nodes))
	for _, node := range nodes {
		healthNodes = append(healthNodes, health.Node{Name: node.name, Addr: node.addr})
	}
	return healthNodes
}

// processClusterInfo exposes CLUSTER INFO fields as metrics with SetToCurrentTime.
func (c *Collector) processClusterInfo(info *redis.ClusterInfo) {
	tags := c.buildCommonMetadataTags()

	// Expose all raw CLUSTER INFO fields as labels on a single metric.
	for k, v := range info.Raw() {
		tags[k] = v
	}

	labelKeys := make([]string, 0, len(tags))
	for k := range tags {
		labelKeys = append(labelKeys, k)
	}

	c.manager.UpdateDynamicMetricWithTime("cluster_metrics", tags, labelKeys)
}

// processClusterNodes exposes per-node information from CLUSTER NODES.
func (c *Collector) processClusterNodes(nodes []redis.ClusterNode) {
	for _, node := range nodes {
		tags := c.buildCommonMetadataTags()
		tags["nodeId"] = node.ID
		tags["nodeIp"] = node.IP
		tags["role"] = node.Flags
		tags["slots"] = node.Slots
		tags["primaryId"] = node.Primary
		tags["state"] = node.State

		labelKeys := make([]string, 0, len(tags))
		for k := range tags {
			labelKeys = append(labelKeys, k)
		}

		c.manager.UpdateDynamicMetricWithTime("nodes_metrics", tags, labelKeys)
	}
}

func (c *Collector) processClusterHealthMetrics(report *health.Report) {
	tags := c.buildCommonMetadataTags()

	c.manager.UpdateDynamicMetric("cluster_membership_ok", tags, nil, boolToFloat(report.MembershipOK))
	c.manager.UpdateDynamicMetric("cluster_slots_covered_ok", tags, nil, boolToFloat(report.SlotsCoveredOK))
	c.manager.UpdateDynamicMetric("cluster_balanced_ok", tags, nil, boolToFloat(report.BalancedOK))
	c.manager.UpdateDynamicMetric("cluster_healthy", tags, nil, boolToFloat(report.Healthy))
	c.manager.UpdateDynamicMetric("cluster_check_errors", tags, nil, float64(len(report.ClusterCheckErrors)))
	c.manager.UpdateDynamicMetric("cluster_check_warnings", tags, nil, float64(len(report.ClusterCheckWarnings)))
	c.manager.UpdateDynamicMetric("cluster_check_command_output_code", tags, nil, float64(report.ClusterCheckCommandOutputCode))
}

func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// getPassword reads the Redis password from the Kubernetes Secret referenced
// in the RuntimeConfig (originally from RedkeyClusterConfig.spec.auth.secret).
// It caches the result and re-reads when the secret name changes.
func (c *Collector) getPassword(ctx context.Context) (string, error) {
	authSecret := c.runtimeConfig.AuthSecret()

	// If the secret name changed, invalidate cache.
	if authSecret != c.cachedAuthSecret {
		c.passwordLoaded = false
		c.cachedAuthSecret = authSecret
	}

	if c.passwordLoaded {
		return c.cachedPassword, nil
	}

	if authSecret == "" {
		c.passwordLoaded = true
		c.cachedPassword = ""
		return "", nil
	}

	var secret corev1.Secret
	if err := c.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: c.namespace,
		Name:      authSecret,
	}, &secret); err != nil {
		return "", fmt.Errorf("reading auth secret %s/%s: %w", c.namespace, authSecret, err)
	}

	pw, ok := secret.Data[secretPasswordKey]
	if !ok {
		return "", fmt.Errorf("auth secret %s/%s missing key %q", c.namespace, authSecret, secretPasswordKey)
	}

	c.cachedPassword = string(pw)
	c.passwordLoaded = true
	return c.cachedPassword, nil
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/redis"
)

// metricNameReplacer transforms metric names (e.g., replacing hyphens).
var metricNameReplacer = strings.NewReplacer("-", "_")

// MetricsPoller is an interface that represents a metrics poller.
type MetricsPoller interface {
	// Start starts the poller.
	Start(ctx context.Context)
}

// MetricsPollerDelegate is an interface that represents a metrics poller delegate.
type MetricsPollerDelegate interface {
	// doPollMetrics polls the metrics.
	doPollMetrics(ctx context.Context) error
}

// NewMetricsPoller creates a new metrics poller. It returns a standalone or cluster metrics poller based on the cluster type.
func NewMetricsPoller(cluster cluster.Cluster) (MetricsPoller, error) {
	if cluster.IsStandalone() {
		return NewRedisStandaloneMetricsPoller(cluster)
	} else {
		return NewRedisClusterMetricsPoller(cluster)
	}
}

// basePoller is a base struct for all pollers.
type basePoller struct {
	logger         *slog.Logger
	metricsManager *MetricsManager
	cluster        cluster.Cluster
	delegate       MetricsPollerDelegate
}

// Start begins the polling loop.
func (bp *basePoller) Start(ctx context.Context) {
	if bp.delegate == nil {
		bp.logger.Error("Metrics poller delegate must be set")
		return
	}

	timeout := time.Duration(bp.cluster.GetMetricsInterval()) * time.Second

	for {
		select {
		case <-ctx.Done():
			bp.logger.Info("Context canceled, stopping polling")
			return
		case <-time.After(timeout):
			bp.pollMetrics(ctx)
		}
	}
}

func (bp *basePoller) pollMetrics(ctx context.Context) {
	// Do nothing if the cluster is not ready
	if bp.cluster.GetStatus() != cluster.Ready {
		return
	}

	bp.logger.Info("Polling metrics")

	if err := bp.delegate.doPollMetrics(ctx); err != nil {
		bp.logger.Error("Error polling metrics", "error", err)
	}
}

// buildNodeTags constructs a map of standard config tags plus an instance ID derived from the node.
func (bp *basePoller) buildNodeTags(nodeName string) map[string]string {
	tags := bp.buildCommonMetadataTags()
	tags[InstanceId] = nodeName
	return tags
}

// buildCommonMetadataTags returns a shared map of metadata from p.conf.
func (bp *basePoller) buildCommonMetadataTags() map[string]string {
	tags := map[string]string{
		Cluster:   bp.cluster.GetName(),
		Namespace: bp.cluster.GetNamespace(),
	}
	maps.Copy(tags, bp.cluster.GetMetadata())
	return tags
}

// createRedisClusterClient abstracts out creating a Redis client for the cluster service address.
func (p *basePoller) createRedisClusterClient(ctx context.Context) *redis.RedisClient {
	return redis.NewRedisClient(ctx, p.cluster.GetAddress(), os.Getenv("REDISAUTH"), 0)
}

// createRedisClient abstracts Redis client creation for arbitrary addresses.
func (p *basePoller) createRedisClient(ctx context.Context, addr string) *redis.RedisClient {
	return redis.NewRedisClient(ctx, addr, os.Getenv("REDISAUTH"), 0)
}

// fetchRedisInfo retrieves "info all" from the given client.
func (p *basePoller) fetchRedisInfo(redisClient *redis.RedisClient) (*redis.RedisInfo, error) {
	err := redisClient.CheckConnection(p.cluster.GetClusterMaxRetries(), p.cluster.GetClusterBackOff())
	if err != nil {
		return nil, fmt.Errorf("error checking connection: %w", err)
	}

	return redisClient.GetInfo()
}

// closeRedisClient safely closes a Redis client, logging any errors.
func (p *basePoller) closeRedisClient(redisClient *redis.RedisClient) error {
	if err := redisClient.Close(); err != nil {
		return fmt.Errorf("error closing Redis client: %w", err)
	}
	return nil
}

// pollRedisNodeLevelMetrics orchestrates node-level metric polling by fetching "info all"
// from each node in the configured Redis Cluster.
func (p *basePoller) pollRedisNodeLevelMetrics(ctx context.Context) error {
	for _, node := range p.cluster.GetNodes() {
		if err := p.pollRedisInfoAllMetrics(ctx, node.Addr, node.Name); err != nil {
			p.logger.Error("Error polling Redis metrics", "node", node.Name, "error", err)
		}
	}
	return nil
}

// pollRedisInfoAllMetrics fetches the Redis "info all" from an individual node.
func (p *basePoller) pollRedisInfoAllMetrics(ctx context.Context, nodeAddr, nodeName string) error {
	redisClient := p.createRedisClient(ctx, nodeAddr)
	defer p.closeRedisClient(redisClient)
	tags := p.buildNodeTags(nodeName)

	redisInfo, err := p.fetchRedisInfo(redisClient)
	if err != nil {
		return err
	}

	p.addPromMetrics(redisInfo, tags, p.cluster.GetMetricsRedisInfoKeys(), redisInfo.Keyspace)
	return nil
}

// ============================================================================
//  PROM METRICS LOGIC
// ============================================================================

// addPromMetrics processes the RedisInfo object, filters the fields by redisInfoKeys,
// and updates metrics in the provided metricsManager. Then processes the keyspaces map.
func (bp *basePoller) addPromMetrics(
	redisInfo *redis.RedisInfo,
	tags map[string]string,
	redisInfoKeys []string,
	keyspaces map[string]string,
) {
	fields := gatherAllFieldsAsStrings(redisInfo)

	for rawMetricName, rawMetricValue := range fields {
		metricName := metricNameReplacer.Replace(rawMetricName)

		if !slices.Contains(redisInfoKeys, metricName) {
			continue
		}
		if strings.Contains(rawMetricValue, "slave0") {
			continue
		}
		if strings.Contains(rawMetricValue, "=") {
			bp.processSubmetrics(metricName, rawMetricValue, tags)
			continue
		}
		bp.processSingleMetric(metricName, rawMetricValue, tags)
	}

	// Keyspaces
	for name, line := range keyspaces {
		if strings.Contains(line, "keys=") {
			additionalTags := map[string]string{"database": name}
			dbParts := strings.Split(line, ",")
			for _, dbPart := range dbParts {
				kv := strings.SplitN(dbPart, "=", 2)
				if len(kv) != 2 {
					continue
				}
				val, err := strconv.ParseFloat(kv[1], 32)
				if err == nil {
					metricName := "keyspace_" + kv[0]
					bp.metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, val)
				}
			}
		}
	}
}

// processSubmetrics attempts to parse subfields as int, float, or label.
func (bp *basePoller) processSubmetrics(
	metricName, rawMetricValue string,
	tags map[string]string,
) {
	subMetrics := strings.Split(rawMetricValue, ",")
	for _, metricPart := range subMetrics {
		additionalTags := make(map[string]string)
		composedMetric := metricName + "_" + metricPart
		subSlice := strings.SplitN(composedMetric, "=", 2)
		if len(subSlice) != 2 {
			continue
		}
		subMetricName := metricNameReplacer.Replace(subSlice[0])
		subMetricValue := subSlice[1]

		if intVal, err := strconv.Atoi(subMetricValue); err == nil {
			bp.metricsManager.UpdateDynamicMetric(subMetricName, tags, additionalTags, float64(intVal))
			continue
		}
		if floatVal, err := strconv.ParseFloat(subMetricValue, 64); err == nil {
			bp.metricsManager.UpdateDynamicMetric(subMetricName, tags, additionalTags, floatVal)
		} else {
			// submetric is a string => store as label
			additionalTags[subMetricName] = subMetricValue
			bp.metricsManager.UpdateDynamicMetric(subMetricName, tags, additionalTags, -1)
		}
	}
}

// processSingleMetric tries to parse a single metric as int, float, or label.
func (bp *basePoller) processSingleMetric(
	metricName, rawMetricValue string,
	tags map[string]string,
) {
	additionalTags := make(map[string]string)
	if intVal, err := strconv.Atoi(rawMetricValue); err == nil {
		bp.metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, float64(intVal))
		return
	}
	if floatVal, err := strconv.ParseFloat(rawMetricValue, 64); err == nil {
		bp.metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, floatVal)
	} else {
		additionalTags[metricName] = rawMetricValue
		bp.metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, -1)
	}
}

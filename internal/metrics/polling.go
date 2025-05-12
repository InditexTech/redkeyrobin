// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"maps"

	"github.com/inditextech/redisrobin/internal/redis"
)

// nodeInfoReplacer is used to clean up node info strings.
var nodeInfoReplacer = strings.NewReplacer("{", "", "}", "", "\r", "", "[", "", "]", "")

// metricNameReplacer transforms metric names (e.g., replacing hyphens).
var metricNameReplacer = strings.NewReplacer("-", "_")



// RedisPollMetrics is responsible for orchestrating the continuous polling of Redis 
type RedisPollMetrics struct {
	redisCluster   *redis.RedisCluster
	metricsManager *MetricsManager
	clusterMgr     *ClusterManager
}

// NewRedisPollMetrics constructs a RedisPollMetrics by delegating the K8s client retrieval,
// storing the given config & metrics manager, etc.
func NewRedisPollMetrics(redisCluster *redis.RedisCluster, metricsManager *MetricsManager) (*RedisPollMetrics, error) {
	// Create a cluster manager for IP tracking & reset logic.
	clusterMgr := NewClusterManager(metricsManager)

	return &RedisPollMetrics{
		redisCluster:           redisCluster,
		metricsManager: metricsManager,
		clusterMgr:     clusterMgr,
	}, nil
}

// Start begins the polling loop.
func (p *RedisPollMetrics) Start(ctx context.Context) {
	for {
		log.Printf("Polling metrics %s.", p.redisCluster.Conf.Redis.Cluster.Name)

		err := p.pollRedisMetrics(ctx)
		if err != nil {
			log.Printf("Error polling Redis metrics: %v", err)
		}
		time.Sleep(time.Second * time.Duration(p.redisCluster.Conf.Redis.Metrics.IntervalSeconds))
	}
}

// pollRedisMetrics retrieves all nodes in the configured namespace and labelSelector, then
// polls both cluster-level metrics and Redis INFO per node.
func (p *RedisPollMetrics) pollRedisMetrics(ctx context.Context) error {
	// cluster-level info (GetNodesInfo, GetClusterInfo)
	if err := p.pollRedisClusterMetrics(ctx); err != nil {
		return err
	}

	// cluster --check
	if err := p.pollClusterCheckMetrics(ctx); err != nil {
		return err
	}

	// node-level info (GetInfo)
	if err := p.pollRedisNodeLevelMetrics(ctx); err != nil {
		return err
	}

	return nil
}

// pollRedisClusterMetrics orchestrates cluster-level metric polling by:
//  1. Polling cluster node data
//  2. Polling overall cluster info
func (p *RedisPollMetrics) pollRedisClusterMetrics(ctx context.Context) error {
	// Create a Redis client scoped to the cluster service
	redisClient := p.createRedisClusterClient(ctx)
	defer p.closeRedisClient(redisClient)

	// Poll cluster nodes
	if err := p.pollClusterNodes(redisClient); err != nil {
		return err
	}

	// Poll cluster info
	if err := p.pollClusterInfo(redisClient); err != nil {
		return err
	}

	return nil
}

// pollClusterNodes obtains node information from Redis, updates membership if changed,
// and then stores each node's data in the metrics manager.
func (p *RedisPollMetrics) pollClusterNodes(redisClient *redis.RedisClient) error {
	err := redisClient.CheckConnection(p.redisCluster.Conf.Redis.Cluster.MaxRetries, p.redisCluster.Conf.Redis.Cluster.BackOff)
	if err != nil {
		return fmt.Errorf("error checking connection: %w", err)
	}

	nodesInfo, err := redisClient.GetNodesInfo()
	if err != nil {
		return fmt.Errorf("error getting nodes info: %w", err)
	}

	// Check if cluster membership changed; reset metrics if needed
	p.clusterMgr.CheckClusterNodes(nodesInfo)

	// For each node, generate relevant tags and update node info metrics
	for _, v := range nodesInfo {
		nodeInfoStringList := strings.Split(nodeInfoReplacer.Replace(fmt.Sprintf("%v", v)), " ")

		// Build standard tags
		nodeTags := p.buildCommonMetadataTags()
		// Add node-specific fields if present
		if len(nodeInfoStringList) > 0 {
			nodeTags[NodeID] = nodeInfoStringList[0]
		}
		if len(nodeInfoStringList) > 1 {
			nodeTags[NodeIP] = nodeInfoStringList[1]
		}
		if len(nodeInfoStringList) > 2 {
			nodeTags[Role] = nodeInfoStringList[2]
		}
		if len(nodeInfoStringList) > 3 {
			nodeTags[Slots] = nodeInfoStringList[3]
		}
		if len(nodeInfoStringList) > 4 {
			nodeTags[MasterID] = nodeInfoStringList[4]
		}
		if len(nodeInfoStringList) > 5 {
			nodeTags[NodeFailures] = nodeInfoStringList[5]
		}

		p.metricsManager.UpdateNodeInfo(nodeTags)
	}

	return nil
}

// pollClusterInfo obtains overall cluster details from Redis and updates them in the metrics manager.
func (p *RedisPollMetrics) pollClusterInfo(redisClient *redis.RedisClient) error {
	err := redisClient.CheckConnection(p.redisCluster.Conf.Redis.Cluster.MaxRetries, p.redisCluster.Conf.Redis.Cluster.BackOff)
	if err != nil {
		return fmt.Errorf("error checking connection: %w", err)
	}
	clusterInfo, err := redisClient.GetClusterInfo()
	if err != nil {
		return fmt.Errorf("error getting cluster info: %w", err)
	}

	// Build standard tags
	tags := p.buildCommonMetadataTags()
	// Insert cluster info fields
	tags[redis.ClusterState] = clusterInfo.State
	tags[redis.ClusterSlotsAssigned] = strconv.Itoa(clusterInfo.SlotsAssigned)
	tags[redis.ClusterSlotsOk] = strconv.Itoa(clusterInfo.SlotsOK)
	tags[redis.ClusterSlotsPFail] = strconv.Itoa(clusterInfo.SlotsPFail)
	tags[redis.ClusterSlotsFail] = strconv.Itoa(clusterInfo.SlotsFail)
	tags[redis.ClusterKnownNodes] = strconv.Itoa(clusterInfo.KnownNodes)
	tags[redis.ClusterSize] = strconv.Itoa(clusterInfo.ClusterSize)
	tags[redis.ClusterCurrentEpoch] = strconv.Itoa(clusterInfo.CurrentEpoch)
	tags[redis.ClusterMyEpoch] = strconv.Itoa(clusterInfo.MyEpoch)

	// Update cluster info in the metrics manager
	p.metricsManager.UpdateClusterInfo(tags)

	return nil
}

// pollRedisNodeLevelMetrics orchestrates node-level metric polling by fetching "info all"
// from each node in the configured Redis Cluster.
func (p *RedisPollMetrics) pollRedisNodeLevelMetrics(ctx context.Context) error {
	for _, node := range p.redisCluster.Nodes {
		if err := p.pollRedisInfoAllMetrics(ctx, node.Addr, node.Name); err != nil {
			log.Printf("Error polling Redis metrics for node %s: %v", node.Name, err)
		}
	}
	// for i := range p.redisCluster.Conf.Redis.Cluster.Replicas {
	// 	nodeName := fmt.Sprintf("%s-%d", p.redisCluster.Conf.Redis.Cluster.Name, i)
	// 	nodeAddr := fmt.Sprintf("%s.%s", nodeName, p.redisCluster.Conf.Redis.Cluster.Name)

	// 	if err := p.pollRedisInfoAllMetrics(ctx, nodeAddr, nodeName); err != nil {
	// 		log.Printf("Error polling Redis metrics for node %s: %v", nodeName, err)
	// 	}
	// }
	return nil
}

// pollRedisInfoAllMetrics fetches the Redis "info all" from an individual node.
func (p *RedisPollMetrics) pollRedisInfoAllMetrics(ctx context.Context, nodeAddr, nodeName string) error {
	redisClient := p.createRedisClient(ctx, nodeAddr)
	defer p.closeRedisClient(redisClient)
	tags := p.buildNodeTags(nodeName)

	redisInfo, err := p.fetchRedisInfo(redisClient)
	if err != nil {
		return err
	}

	p.processPromMetrics(redisInfo, tags)
	return nil
}

// buildNodeTags constructs a map of standard config tags plus an instance ID derived from the node.
func (p *RedisPollMetrics) buildNodeTags(nodeName string) map[string]string {
	tags := p.buildCommonMetadataTags()
	tags[InstanceId] = nodeName
	return tags
}

// buildCommonMetadataTags returns a shared map of metadata from p.conf.
func (p *RedisPollMetrics) buildCommonMetadataTags() map[string]string {
	tags := map[string]string{
		Cluster:   p.redisCluster.Conf.Redis.Cluster.Name,
		Namespace: p.redisCluster.Conf.Redis.Cluster.Namespace,
	}
	maps.Copy(tags, p.redisCluster.Conf.Metadata)
	return tags
}

// createRedisClusterClient abstracts out creating a Redis client for the cluster service address.
func (p *RedisPollMetrics) createRedisClusterClient(ctx context.Context) *redis.RedisClient {
	return redis.NewRedisClient(ctx, p.redisCluster.GetAddress(), os.Getenv("REDISAUTH"), 0)
}

// createRedisClient abstracts Redis client creation for arbitrary addresses.
func (p *RedisPollMetrics) createRedisClient(ctx context.Context, addr string) *redis.RedisClient {
	return redis.NewRedisClient(ctx, addr, os.Getenv("REDISAUTH"), 0)
}

// fetchRedisInfo retrieves "info all" from the given client.
func (p *RedisPollMetrics) fetchRedisInfo(redisClient *redis.RedisClient) (*redis.RedisInfo, error) {
	err := redisClient.CheckConnection(p.redisCluster.Conf.Redis.Cluster.MaxRetries, p.redisCluster.Conf.Redis.Cluster.BackOff)
	if err != nil {
		return nil, fmt.Errorf("error checking connection: %w", err)
	}

	return redisClient.GetInfo()
}

// closeRedisClient safely closes a Redis client, logging any errors.
func (p *RedisPollMetrics) closeRedisClient(redisClient *redis.RedisClient) error {
	if err := redisClient.Close(); err != nil {
		return fmt.Errorf("error closing Redis client: %w", err)
	}
	return nil
}

// processPromMetrics handles extra Prometheus-style metrics from the RedisInfo.
func (p *RedisPollMetrics) processPromMetrics(
	redisInfo *redis.RedisInfo,
	tags map[string]string,
) {
	addPromMetrics(redisInfo, tags, p.redisCluster.Conf.Redis.Metrics.RedisInfoKeys, redisInfo.Keyspace, p.metricsManager)
}

// pollClusterCheckMetrics fetches the "redis-cli --cluster check" info and updates 
func (p *RedisPollMetrics) pollClusterCheckMetrics(ctx context.Context) error {
	redisClient := p.createRedisClusterClient(ctx)
	defer p.closeRedisClient(redisClient)
	err := redisClient.CheckConnection(p.redisCluster.Conf.Redis.Cluster.MaxRetries, p.redisCluster.Conf.Redis.Cluster.BackOff)
	if err != nil {
		return fmt.Errorf("error checking connection: %w", err)
	}

	clusterCheck, err := redisClient.ClusterCheck(ctx)
	if err != nil {
		return fmt.Errorf("error checking cluster: %w", err)
	}

	// Build standard tags (e.g., cluster, slot, tenant, etc.).
	baseTags := p.buildCommonMetadataTags()

	p.metricsManager.UpdateDynamicMetric(
		redis.ClusterCheckErrors,
		baseTags,
		nil,
		float64(len(clusterCheck.Errors)),
	)

	// Warnings
	p.metricsManager.UpdateDynamicMetric(
		redis.ClusterCheckWarnings,
		baseTags,
		nil,
		float64(len(clusterCheck.Warnings)),
	)

	p.metricsManager.UpdateDynamicMetric(
		redis.ClusterCheckCommandOutputCode,
		baseTags,
		nil, // no additional labels
		float64(clusterCheck.CommandCodeOutput),
	)

	return nil
}

// ============================================================================
//  PROM METRICS LOGIC
// ============================================================================

// addPromMetrics processes the RedisInfo object, filters the fields by redisInfoKeys,
// and updates metrics in the provided metricsManager. Then processes the keyspaces map.
func addPromMetrics(
	redisInfo *redis.RedisInfo,
	tags map[string]string,
	redisInfoKeys []string,
	keyspaces map[string]string,
	metricsManager *MetricsManager,
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
			processSubmetrics(metricName, rawMetricValue, tags, metricsManager)
			continue
		}
		processSingleMetric(metricName, rawMetricValue, tags, metricsManager)
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
					metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, val)
				}
			}
		}
	}
}

// processSubmetrics attempts to parse subfields as int, float, or label.
func processSubmetrics(
	metricName, rawMetricValue string,
	tags map[string]string,
	metricsManager *MetricsManager,
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
			metricsManager.UpdateDynamicMetric(subMetricName, tags, additionalTags, float64(intVal))
			continue
		}
		if floatVal, err := strconv.ParseFloat(subMetricValue, 64); err == nil {
			metricsManager.UpdateDynamicMetric(subMetricName, tags, additionalTags, floatVal)
		} else {
			// submetric is a string => store as label
			additionalTags[subMetricName] = subMetricValue
			metricsManager.UpdateDynamicMetric(subMetricName, tags, additionalTags, -1)
		}
	}
}

// processSingleMetric tries to parse a single metric as int, float, or label.
func processSingleMetric(
	metricName, rawMetricValue string,
	tags map[string]string,
	metricsManager *MetricsManager,
) {
	additionalTags := make(map[string]string)
	if intVal, err := strconv.Atoi(rawMetricValue); err == nil {
		metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, float64(intVal))
		return
	}
	if floatVal, err := strconv.ParseFloat(rawMetricValue, 64); err == nil {
		metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, floatVal)
	} else {
		additionalTags[metricName] = rawMetricValue
		metricsManager.UpdateDynamicMetric(metricName, tags, additionalTags, -1)
	}
}

// gatherAllFieldsAsStrings flattens RedisInfo numeric fields into strings.
func gatherAllFieldsAsStrings(ri *redis.RedisInfo) map[string]string {
	allFields := make(map[string]string)

	// Copy maps that are string->string
	maps.Copy(allFields, ri.Server)
	maps.Copy(allFields, ri.Persistence)
	maps.Copy(allFields, ri.Replication)
	maps.Copy(allFields, ri.Cluster)
	maps.Copy(allFields, ri.ErrorStats)
	maps.Copy(allFields, ri.LatencyStats)
	maps.Copy(allFields, ri.Memory)
	maps.Copy(allFields, ri.CommandStats)
	maps.Copy(allFields, ri.Stats)
	// Convert int fields to strings
	for k, v := range ri.Clients {
		allFields[k] = strconv.Itoa(int(v))
	}

	// Convert float fields to strings
	for k, v := range ri.CPU {
		allFields[k] = fmt.Sprintf("%f", v)
	}
	return allFields
}

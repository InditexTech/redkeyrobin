// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"strconv"

	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/inditextech/redisrobin/internal/util"
)

const (
	redkeyClusterMetrics = "redis_cluster_metrics"
	redisNodesMetrics    = "redis_nodes_metrics"
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

// RedKeyClusterMetricsPoller is responsible for orchestrating the continuous polling of Redis
type RedKeyClusterMetricsPoller struct {
	basePoller
	clusterMgr *ClusterManager
}

// NewRedKeyClusterMetricsPoller constructs a RedisPollMetrics by delegating the K8s client retrieval,
// storing the given config & metrics manager, etc.
func NewRedKeyClusterMetricsPoller(redkeyCluster cluster.Cluster) (*RedKeyClusterMetricsPoller, error) {
	// Create a metrics manager with the appropriate dynamic metrics
	metricsManager := NewMetricsManager()

	for k := range redkeyCluster.GetMetadata() {
		clusterInfoLabelKeys = append(clusterInfoLabelKeys, k)
		nodeInfoLabelKeys = append(nodeInfoLabelKeys, k)
	}

	metricsManager.RegisterDynamicMetric(redkeyClusterMetrics, "RedKey cluster  metrics", clusterInfoLabelKeys)
	metricsManager.RegisterDynamicMetric(redisNodesMetrics, "Redis nodes metrics", nodeInfoLabelKeys)

	// Create a cluster manager for IP tracking & reset logic.
	clusterMgr := NewClusterManager(metricsManager)

	// Create the poller
	poller := &RedKeyClusterMetricsPoller{
		basePoller: basePoller{
			logger:         util.GetLogger("cluster-metrics"),
			cluster:        redkeyCluster,
			metricsManager: metricsManager,
		},
		clusterMgr: clusterMgr,
	}
	poller.delegate = poller

	return poller, nil
}

// doPollMetrics retrieves all nodes in the configured namespace and labelSelector, then
// polls both cluster-level metrics and Redis INFO per node.
func (p *RedKeyClusterMetricsPoller) doPollMetrics(ctx context.Context) error {
	// cluster-level info (GetNodesInfo, GetClusterInfo)
	if err := p.pollRedKeyClusterMetrics(ctx); err != nil {
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

// pollRedKeyClusterMetrics orchestrates cluster-level metric polling by:
//  1. Polling cluster node data
//  2. Polling overall cluster info
func (p *RedKeyClusterMetricsPoller) pollRedKeyClusterMetrics(ctx context.Context) error {
	// Create a Redis client scoped to the cluster service
	redisClient := p.createRedKeyClusterClient(ctx)
	defer p.closeRedisClient(redisClient)

	// Poll cluster nodes
	if err := p.pollClusterNodes(); err != nil {
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
func (p *RedKeyClusterMetricsPoller) pollClusterNodes() error {
	nodesInfo := p.cluster.GetNodes()

	// Check if cluster membership changed; reset metrics if needed
	p.clusterMgr.CheckClusterNodes(nodesInfo)

	// For each node, generate relevant tags and update node info metrics
	for _, node := range nodesInfo {
		// Build standard tags
		nodeTags := p.buildCommonMetadataTags()

		nodeTags[NodeID] = node.ID
		nodeTags[NodeIP] = node.IP
		nodeTags[Role] = node.Flags
		nodeTags[Slots] = fmt.Sprintf("%v", node.Slots)
		nodeTags[MasterID] = node.MasterID
		nodeTags[NodeFailures] = fmt.Sprintf("%v", node.Failures)

		p.metricsManager.UpdateDynamicMetricWithTime(redisNodesMetrics, nodeTags, nodeInfoLabelKeys, false)
	}

	return nil
}

// pollClusterInfo obtains overall cluster details from Redis and updates them in the metrics manager.
func (p *RedKeyClusterMetricsPoller) pollClusterInfo(redisClient *redis.RedisClient) error {
	err := redisClient.CheckConnection(p.cluster.GetClusterMaxRetries(), p.cluster.GetClusterBackOff())
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
	p.metricsManager.UpdateDynamicMetricWithTime(redkeyClusterMetrics, tags, clusterInfoLabelKeys, true)

	return nil
}

// pollClusterCheckMetrics fetches the "redis-cli --cluster check" info and updates
func (p *RedKeyClusterMetricsPoller) pollClusterCheckMetrics(ctx context.Context) error {
	redisClient := p.createRedKeyClusterClient(ctx)
	defer p.closeRedisClient(redisClient)
	err := redisClient.CheckConnection(p.cluster.GetClusterMaxRetries(), p.cluster.GetClusterBackOff())
	if err != nil {
		return fmt.Errorf("error checking connection: %w", err)
	}

	clusterCheck, err := redisClient.ClusterCheck(ctx)
	if err != nil {
		return fmt.Errorf("error checking cluster: %w", err)
	}

	// Build standard tags (e.g., cluster, slot, tenant, etc.).
	baseTags := p.buildCommonMetadataTags()

	// Errors
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

	// Command code output
	p.metricsManager.UpdateDynamicMetric(
		redis.ClusterCheckCommandOutputCode,
		baseTags,
		nil, // no additional labels
		float64(clusterCheck.CommandCodeOutput),
	)

	return nil
}

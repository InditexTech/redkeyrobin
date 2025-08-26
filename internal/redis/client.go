// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/inditextech/redisrobin/internal/util"
	redisgo "github.com/redis/go-redis/v9"
)

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------
const (
	RedisPort          = 6379
	SectionServer      = "server"
	SectionClients     = "clients"
	SectionMemory      = "memory"
	SectionPersistence = "persistence"
	SectionStats       = "stats"
	SectionReplication = "replication"
	SectionCPU         = "cpu"
	SectionCluster     = "cluster"
	SectionKeyspace    = "keyspace"
	SectionCmdStats    = "commandstats"
	SectionErrorStats  = "errorstats"
	SectionLatency     = "latencystats"
)

// Label keys for metrics.
const (
	ClusterState                       = "cluster_state"
	ClusterSlotsAssigned               = "cluster_slots_assigned"
	ClusterSlotsOk                     = "cluster_slots_ok"
	ClusterSlotsPFail                  = "cluster_slots_pfail"
	ClusterSlotsFail                   = "cluster_slots_fail"
	ClusterKnownNodes                  = "cluster_known_nodes"
	ClusterSize                        = "cluster_size"
	ClusterCurrentEpoch                = "cluster_current_epoch"
	ClusterMyEpoch                     = "cluster_my_epoch"
	ClusterStatsMMS                    = "cluster_stats_messages_meet_sent"
	ClusterStatsMMR                    = "cluster_stats_messages_meet_received"
	ClusterStatsMS                     = "cluster_stats_messages_sent"
	ClusterStatsMR                     = "cluster_stats_messages_received"
	ClusterStatsMPS                    = "cluster_stats_messages_ping_sent"
	ClusterStatsMPR                    = "cluster_stats_messages_ping_received"
	ClusterStatsMPongS                 = "cluster_stats_messages_pong_sent"
	ClusterStatsMPongR                 = "cluster_stats_messages_pong_received"
	ClusterCheckErrors                 = "cluster_check_errors"
	ClusterCheckCommandOutputCode      = "cluster_check_command_output_code"
	ClusterCheckWarnings               = "cluster_check_warnings"
	ClusterCheckSlotCoverageMessage    = "cluster_check_slot_coverage_message"
	ClusterCheckAgreementMessage       = "cluster_check_agreement_message"
	ClusterCheckPerformedUsingPod      = "cluster_check_performed_using_pod"
	ClusterStatsMessagesUpdateSent     = "cluster_stats_messages_update_sent"
	ClusterStatsMessagesUpdateReceived = "cluster_stats_messages_update_received"
	ClusterStatsMessagesFailReceived   = "cluster_stats_messages_fail_received"
	TotalClusterLinksBufEx             = "total_cluster_links_buffer_limit_exceeded"
)

// -----------------------------------------------------------------------------
// Redis Client
// -----------------------------------------------------------------------------

// RedisClient encapsulates a connection to Redis.
type RedisClient struct {
	logger *slog.Logger
	client *redisgo.Client
	ctx    context.Context
}

// RedisInfo represents structured Redis INFO output.
type RedisInfo struct {
	Server       map[string]string
	Clients      map[string]int64
	Memory       map[string]string
	Persistence  map[string]string
	Stats        map[string]string
	Replication  map[string]string
	CPU          map[string]float64
	Cluster      map[string]string
	Keyspace     map[string]string
	CommandStats map[string]string
	ErrorStats   map[string]string
	LatencyStats map[string]string
}

// ClusterInfo represents structured RedKey cluster  information.
type ClusterInfo struct {
	State                        string
	SlotsAssigned                int
	SlotsOK                      int
	SlotsPFail                   int
	SlotsFail                    int
	KnownNodes                   int
	ClusterSize                  int
	CurrentEpoch                 int
	MyEpoch                      int
	MessagesPingSent             int
	MessagesPongSent             int
	MessagesMeetSent             int
	MessagesSent                 int
	MessagesPingReceived         int
	MessagesPongReceived         int
	MessagesMeetReceived         int
	MessagesReceived             int
	TotalClusterLinksBufferLimit int
	MessagesUpdateSent           int
	MessagesUpdateReceived       int
	MessagesFailReceived         int
}

// NewRedisClient creates a new RedisClient for the given address.
func NewRedisClient(ctx context.Context, addr, password string, db int) *RedisClient {
	client := redisgo.NewClient(&redisgo.Options{
		Addr:     fmt.Sprintf("%s:%d", addr, RedisPort),
		Password: password,
		DB:       db,
	})
	return &RedisClient{
		logger: util.GetLogger("redis-cluster"),
		client: client,
		ctx:    ctx,
	}
}

// Close closes the Redis connection.
func (rc *RedisClient) Close() error {
	return rc.client.Close()
}

// CheckConnection pings the Redis server until a connection is established.
func (rc *RedisClient) CheckConnection(maxRetries int, backoff time.Duration) error {
	if maxRetries <= 0 {
		return fmt.Errorf("maxRetries must be greater than 0")
	}
	if backoff <= 0 {
		return fmt.Errorf("backoff must be greater than 0")
	}
	for range maxRetries {
		if _, err := rc.client.Ping(rc.ctx).Result(); err == nil {
			return nil
		}
		time.Sleep(backoff)
	}
	return fmt.Errorf("failed to connect after %d retries", maxRetries)
}

// GetInfo retrieves and parses the Redis INFO output for the given IP.
func (rc *RedisClient) GetInfo() (*RedisInfo, error) {
	info, err := rc.client.Info(rc.ctx, "all").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get info from %s: %v", rc.client.Options().Addr, err)
	}

	// Parse the response into a structured format
	return parseRedisInfo(info), nil
}

// GetClusterInfo retrieves and parses cluster information from Redis.
func (rc *RedisClient) GetClusterInfo() (*ClusterInfo, error) {
	info, err := rc.client.ClusterInfo(rc.ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster info from %s: %v", rc.client.Options().Addr, err)
	}
	if len(info) == 0 {
		return nil, fmt.Errorf("empty cluster info response")
	}

	// Parse response into a structured format
	clusterInfo := &ClusterInfo{}
	lines := strings.Split(strings.TrimSpace(info), "\n")

	for _, line := range lines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			rc.logger.Info("Skipping malformed cluster info", "line", line)
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Map values to struct fields
		switch key {
		case ClusterState:
			clusterInfo.State = value
		case ClusterSlotsAssigned:
			clusterInfo.SlotsAssigned = util.ParseInt(value)
		case ClusterSlotsOk:
			clusterInfo.SlotsOK = util.ParseInt(value)
		case ClusterSlotsPFail:
			clusterInfo.SlotsPFail = util.ParseInt(value)
		case ClusterSlotsFail:
			clusterInfo.SlotsFail = util.ParseInt(value)
		case ClusterKnownNodes:
			clusterInfo.KnownNodes = util.ParseInt(value)
		case ClusterSize:
			clusterInfo.ClusterSize = util.ParseInt(value)
		case ClusterCurrentEpoch:
			clusterInfo.CurrentEpoch = util.ParseInt(value)
		case ClusterMyEpoch:
			clusterInfo.MyEpoch = util.ParseInt(value)
		case ClusterStatsMMS:
			clusterInfo.MessagesMeetSent = util.ParseInt(value)
		case ClusterStatsMMR:
			clusterInfo.MessagesMeetReceived = util.ParseInt(value)
		case ClusterStatsMS:
			clusterInfo.MessagesSent = util.ParseInt(value)
		case ClusterStatsMR:
			clusterInfo.MessagesReceived = util.ParseInt(value)
		case ClusterStatsMPS:
			clusterInfo.MessagesPingSent = util.ParseInt(value)
		case ClusterStatsMPR:
			clusterInfo.MessagesPingReceived = util.ParseInt(value)
		case ClusterStatsMPongS:
			clusterInfo.MessagesPongSent = util.ParseInt(value)
		case ClusterStatsMPongR:
			clusterInfo.MessagesPongReceived = util.ParseInt(value)
		case TotalClusterLinksBufEx:
			clusterInfo.TotalClusterLinksBufferLimit = util.ParseInt(value)
		case ClusterStatsMessagesUpdateSent:
			clusterInfo.MessagesUpdateSent = util.ParseInt(value)
		case ClusterStatsMessagesUpdateReceived:
			clusterInfo.MessagesUpdateReceived = util.ParseInt(value)
		case ClusterStatsMessagesFailReceived:
			clusterInfo.MessagesFailReceived = util.ParseInt(value)
		default:
		}
	}

	return clusterInfo, nil
}

// GetNodesInfo retrieves and parses the cluster nodes information.
func (rc *RedisClient) GetNodesInfo() ([]RedisNode, error) {
	result, err := rc.client.ClusterNodes(rc.ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster nodes info from %s: %v", rc.client.Options().Addr, err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty cluster nodes response")
	}

	// Split response into lines
	lines := strings.Split(strings.TrimSpace(result), "\n")

	var nodes []RedisNode
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			rc.logger.Info("Skipping malformed", "line", line)
			continue
		}

		// Extract Node Details
		nodeID := fields[0]
		ipPort := fields[1]
		flags := fields[2]
		masterID := fields[3] // "-" if master, otherwise Master ID
		sent := util.ParseInt(fields[4])
		recv := util.ParseInt(fields[5])
		linkStatus := fields[7]

		// Extract slot information (if available)
		slots := []RedisSlotRange{}
		if len(fields) > 8 {
			slots = parseRedisSlotRange(fields[8:]...)
		}

		// Retrieve failure count
		failures := 0
		failureStr, err := rc.client.ClusterCountFailureReports(rc.ctx, nodeID).Result()
		if err == nil {
			failures = int(failureStr)
		}

		// Construct Node struct
		node := RedisNode{
			ID:         nodeID,
			IP:         strings.Split(ipPort, ":")[0], // Extract only IP
			Flags:      flags,
			Slots:      slots,
			MasterID:   masterID,
			Failures:   failures,
			Sent:       sent,
			Recv:       recv,
			LinkStatus: linkStatus,
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

// GetMyID retrieves the ID of the current Redis node.
func (rc *RedisClient) GetMyID() (string, error) {
	result, err := rc.client.Do(rc.ctx, "CLUSTER", "MYID").Result()
	if err != nil {
		return "", fmt.Errorf("failed to get cluster my ID from %s: %v", rc.client.Options().Addr, err)
	}

	return result.(string), nil
}

// ClusterForgetNode removes a node from the cluster.
func (rc *RedisClient) ClusterForgetNode(nodeID string) error {
	_, err := rc.client.ClusterForget(rc.ctx, nodeID).Result()
	if err != nil {
		return fmt.Errorf("failed to forget node %s: %v", nodeID, err)
	}
	return nil
}

// ClusterMeet instructs the current node to meet the specified node.
func (rc *RedisClient) ClusterMeet(ip string, port int) error {
	_, err := rc.client.ClusterMeet(rc.ctx, ip, strconv.Itoa(port)).Result()
	if err != nil {
		return fmt.Errorf("failed to meet node %s: %v", ip, err)
	}
	return nil
}

// ClusterReplicate instructs the current node to replicate the specified master.
func (rc *RedisClient) ClusterReplicate(nodeID string) error {
	_, err := rc.client.ClusterReplicate(rc.ctx, nodeID).Result()
	if err != nil {
		return fmt.Errorf("failed to replicate node %s: %v", nodeID, err)
	}
	return nil
}

// ClusterReset instructs the current node to reset.
func (rc *RedisClient) ClusterReset(hard bool) error {
	var err error

	if hard {
		_, err = rc.client.ClusterResetHard(rc.ctx).Result()
	} else {
		_, err = rc.client.ClusterResetSoft(rc.ctx).Result()
	}
	if err != nil {
		return fmt.Errorf("failed to reset cluster node: %v", err)
	}
	return nil
}

// ClusterForget removes a node in the current node from the cluster.
func (rc *RedisClient) ClusterForget(nodeID string) error {
	_, err := rc.client.ClusterForget(rc.ctx, nodeID).Result()
	if err != nil {
		return fmt.Errorf("failed to forget node %s: %v", nodeID, err)
	}
	return nil
}

// ClusterAddSlots adds the specified slots to the current node.
func (rc *RedisClient) ClusterAddSlots(slots ...int) error {
	_, err := rc.client.ClusterAddSlots(rc.ctx, slots...).Result()
	if err != nil {
		return fmt.Errorf("failed to add slots %v: %v", slots, err)
	}
	return nil
}

// ClusterFailover triggers a manual failover of the current node.
func (rc *RedisClient) ClusterFailover() error {
	_, err := rc.client.ClusterFailover(rc.ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to failover node: %v", err)
	}
	return nil
}

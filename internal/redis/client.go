// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
	"strconv"

	"slices"

	"github.com/go-logr/logr"
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

var redisClientLogger logr.Logger

// -----------------------------------------------------------------------------
// Redis Client
// -----------------------------------------------------------------------------

// RedisClient encapsulates a connection to Redis.
type RedisClient struct {
	client *redisgo.Client
	ctx    context.Context
}

// NewRedisClient creates a new RedisClient for the given address.
func NewRedisClient(ctx context.Context, addr, password string, db int) *RedisClient {
	redisClientLogger = util.GetLogger("redis-cluster")
	client := redisgo.NewClient(&redisgo.Options{
		Addr:     fmt.Sprintf("%s:%d", addr, RedisPort),
		Password: password,
		DB:       db,
	})
	return &RedisClient{
		client: client,
		ctx:    ctx,
	}
}

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

// GetInfo retrieves and parses the Redis INFO output for the given IP.
func (rc *RedisClient) GetInfo() (*RedisInfo, error) {
	info, err := rc.client.Info(rc.ctx, "all").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get info from %s: %w", rc.client.Options().Addr, err)
	}

	// Parse the response into a structured format
	return parseRedisInfo(info), nil
}

// parseRedisInfo processes Redis INFO output into a structured format.
func parseRedisInfo(info string) *RedisInfo {
	parsedInfo := &RedisInfo{
		Server:       make(map[string]string),
		Clients:      make(map[string]int64),
		Memory:       make(map[string]string),
		Persistence:  make(map[string]string),
		Stats:        make(map[string]string),
		Replication:  make(map[string]string),
		CPU:          make(map[string]float64),
		Cluster:      make(map[string]string),
		Keyspace:     make(map[string]string),
		CommandStats: make(map[string]string),
		ErrorStats:   make(map[string]string),
		LatencyStats: make(map[string]string),
	}

	section := ""
	lines := strings.Split(strings.TrimSpace(info), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Identify the current section if line starts with '#'
		if strings.HasPrefix(line, "#") {
			section = strings.ToLower(strings.TrimPrefix(line, "#"))
			section = strings.TrimSpace(section)
			continue
		}

		// Parse "key:value" structure
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			redisClientLogger.Info("Skipping malformed line", "line", line)
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Populate parsedInfo based on the recognized section
		switch section {
		case SectionServer:
			parsedInfo.Server[key] = value
		case SectionClients:
			intVal := util.ParseInt64(value)
			parsedInfo.Clients[key] = intVal
		case SectionMemory:
			parsedInfo.Memory[key] = value
		case SectionPersistence:
			parsedInfo.Persistence[key] = value
		case SectionStats:
			parsedInfo.Stats[key] = value
		case SectionReplication:
			parsedInfo.Replication[key] = value
		case SectionCPU:
			floatVal := util.ParseFloat(value)
			parsedInfo.CPU[key] = floatVal
		case SectionCluster:
			parsedInfo.Cluster[key] = value
		case SectionKeyspace:
			parsedInfo.Keyspace[key] = value
		case SectionCmdStats:
			parsedInfo.CommandStats[key] = value
		case SectionErrorStats:
			parsedInfo.ErrorStats[key] = value
		case SectionLatency:
			parsedInfo.LatencyStats[key] = value
		default:
			redisClientLogger.Info("Ignoring unknown redis info", "section", section, "key", key)
		}
	}

	return parsedInfo
}

// ClusterInfo represents structured Redis cluster information.
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

// GetClusterInfo retrieves and parses cluster information from Redis.
func (rc *RedisClient) GetClusterInfo() (*ClusterInfo, error) {
	info, err := rc.client.ClusterInfo(rc.ctx).Result()
	if err != nil {
		redisClientLogger.Error(err, "Error fetching cluster info")
		return nil, err
	}

	// Parse response into a structured format
	clusterInfo := &ClusterInfo{}
	lines := strings.Split(strings.TrimSpace(info), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty cluster info response")
	}

	for _, line := range lines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			redisClientLogger.Info("Skipping malformed cluster info", "line", line)
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

func (rc *RedisClient) GetMyID() (string, error) {
	result, err := rc.client.Do(rc.ctx, "CLUSTER", "MYID").Result()
	if err != nil {
		return "", err
	}

	return result.(string), nil
}

// GetNodesInfo retrieves and parses the cluster nodes information.
func (rc *RedisClient) GetNodesInfo() ([]RedisNode, error) {
	result, err := rc.client.ClusterNodes(rc.ctx).Result()
	if err != nil {
		return nil, err
	}

	// Split response into lines
	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty cluster nodes response")
	}

	var nodes []RedisNode
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			redisClientLogger.Info("Skipping malformed", "line", line)
			continue
		}

		// Extract Node Details
		nodeID := fields[0]
		ipPort := fields[1]
		role := fields[2]     // First flag usually indicates role
		masterID := fields[3] // "-" if master, otherwise Master ID

		// Extract slot information (if available)
		var slots []RedisSlotRange
		if len(fields) > 8 {
			slots = parseRedisSlotRange(fields[8:]...)
		}

		// Validate role
		if role != "master" && role != "slave" && role != "myself,master" && role != "myself,slave" {
			continue
		}

		// Retrieve failure count
		failures := 0
		failureStr, err := rc.client.ClusterCountFailureReports(rc.ctx, nodeID).Result()
		if err == nil {
			failures = int(failureStr)
		}

		// Construct Node struct
		node := RedisNode{
			ID:       nodeID,
			IP:       strings.Split(ipPort, ":")[0], // Extract only IP
			Role:     role,
			Slots:    slots,
			MasterID: masterID,
			Failures: failures,
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

type RedisCLICommand struct {
	cmd      *exec.Cmd
	stdout   *bytes.Buffer
	stderr   *bytes.Buffer
	ExitCode int
	Err      error
}

func NewRedisCLICommand(ctx context.Context, command string) *RedisCLICommand {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	return &RedisCLICommand{
		cmd:    cmd,
		stdout: &stdout,
		stderr: &stderr,
	}
}

func (rcc *RedisCLICommand) Run() {
	rcc.Err = rcc.cmd.Run()
	rcc.CheckStatusCode()
}

func (rcc *RedisCLICommand) Start() {
	rcc.Err = rcc.cmd.Start()
}

func (rcc *RedisCLICommand) CheckStatusCode() {
	exitCode := -1
	if rcc.cmd.ProcessState != nil {
		exitCode = rcc.cmd.ProcessState.ExitCode()
	}

	rcc.ExitCode = exitCode
}

func (rcc *RedisCLICommand) Wait() {
	rcc.Err = nil

	// Wait for command to finish
	rcc.Err = rcc.cmd.Wait()
	rcc.CheckStatusCode()
}

func (rcc *RedisCLICommand) GetStdout() string {
	return rcc.stdout.String()
}

func (rcc *RedisCLICommand) GetStderr() string {
	return rcc.stderr.String()
}

func (rcc *RedisCLICommand) GetCombinedOutput() string {
	return rcc.GetStdout() + rcc.GetStderr()
}

func (rc *RedisClient) runRedisCLICommand(ctx context.Context, command string) *RedisCLICommand {
	// Build the command
	cmd := NewRedisCLICommand(ctx, command)

	// Run the command
	cmd.Run()

	return cmd
}

func (rc *RedisClient) runRedisCLICommandAsync(ctx context.Context, command string) *RedisCLICommand {
	// Build the command
	cmd := NewRedisCLICommand(ctx, command)

	// Run the command asynchronously
	cmd.Start()

	return cmd
}

// ClusterCheckResult aggregates the overall cluster state similar to "redis-cli --cluster check".
type ClusterCheckResult struct {
	CommandCodeOutput int
	Errors            []string
	Warnings          []string
}

// -----------------------------------------------------------------------------
// ClusterCheck executes "redis-cli --cluster check <addr>" and parses its output.
// -----------------------------------------------------------------------------
func (rc *RedisClient) ClusterCheck(ctx context.Context) (*ClusterCheckResult, error) {
	// Build the command: "redis-cli --cluster check <host:port>"
	command := fmt.Sprintf("redis-cli --cluster check %s", rc.client.Options().Addr)

	// Execute the command and parse the output
	cmd := rc.runRedisCLICommand(ctx, command)
	if cmd.Err != nil {
		// If the context was canceled or timed out, return immediately.
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis-cli command canceled or timed out: %w", ctx.Err())
		}

		redisClientLogger.Error(cmd.Err, "Error executing 'redis-cli --cluster check'", "output", cmd.GetCombinedOutput())
	}

	// Parse the CLI output (whether complete or partial) to fill a ClusterCheckResult.
	result := parseClusterCheckOutput(cmd.GetStdout())
	result.CommandCodeOutput = cmd.ExitCode

	return result, nil
}

func parseClusterCheckOutput(output string) *ClusterCheckResult {
	scanner := bufio.NewScanner(strings.NewReader(output))

	result := &ClusterCheckResult{
		Errors:   []string{},
		Warnings: []string{},
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Capture errors
		if strings.Contains(line, "[ERR]") {
			if !slices.Contains(result.Errors, line) {
				result.Errors = append(result.Errors, line)
			}
		}

		// Capture general warnings
		if strings.Contains(line, "[WARNING]") {
			if !slices.Contains(result.Warnings, line) {
				result.Warnings = append(result.Warnings, line)
			}
		}
	}

	// Check for any scanning error.
	if err := scanner.Err(); err != nil {
		redisClientLogger.Error(err, "Error reading cluster check output")
	}

	return result
}

func (rc *RedisClient) ReshardNode(ctx context.Context, source, target RedisNode, slots int) *RedisCLICommand {
	if slots == 0 {
		redisClientLogger.Info("No slots to reshard")
		return nil
	}

	// Build the command: "redis-cli --cluster reshard <host:port>"
	command := fmt.Sprintf("redis-cli --cluster reshard %s --cluster-from %s --cluster-to %s --cluster-slots %v --cluster-yes", rc.client.Options().Addr, source.ID, target.ID, slots)

	// Execute the command and return command reference
	return rc.runRedisCLICommandAsync(ctx, command)
}

func (rc *RedisClient) ClusterFix(ctx context.Context) *RedisCLICommand {
	// Build the command: "redis-cli --cluster fix <host:port>"
	command := fmt.Sprintf("redis-cli --cluster fix %s --cluster-yes", rc.client.Options().Addr)

	// Execute the command and return command reference
	return rc.runRedisCLICommandAsync(ctx, command)
}

func (rc *RedisClient) ClusterRebalance(ctx context.Context) *RedisCLICommand {
	// Build the command: "redis-cli --cluster rebalance <host:port>"
	command := fmt.Sprintf("redis-cli --cluster rebalance %s --cluster-use-empty-masters", rc.client.Options().Addr)

	// Execute the command and return command reference
	return rc.runRedisCLICommandAsync(ctx, command)
}


func parseRedisSlotRange(values ...string) []RedisSlotRange {
	slots := []RedisSlotRange{}

	for _, value := range values {
		for _, slotRange := range strings.Split(value, " ") {
			if strings.Contains(slotRange, "-") {
				splitSlotRange := strings.Split(slotRange, "-")
				start, err := strconv.Atoi(splitSlotRange[0])
				if err != nil {
					fmt.Printf("could not parse int from %q: %v\n", slotRange, err)
					continue
				}
				end, err := strconv.Atoi(splitSlotRange[1])
				if err != nil {
					fmt.Printf("could not parse int from %q: %v\n", slotRange, err)
					continue
				}
				slots = append(slots, RedisSlotRange{Start: start, End: end})
			} else {
				slot, err := strconv.Atoi(slotRange)
				if err != nil {
					fmt.Printf("could not parse int from %q: %v\n", slotRange, err)
					continue
				}
				slots = append(slots, RedisSlotRange{Start: slot, End: slot})
			}
		}
	}

	return slots
}
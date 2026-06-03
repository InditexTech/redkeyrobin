// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	// DefaultPort is the default Redis port.
	DefaultPort = 6379
)

// Client wraps a go-redis client for a single Redis node.
type Client struct {
	client   *goredis.Client
	addr     string
	password string
}

// NewClient creates a new Redis client connected to the given address.
// Pool is limited to a single connection since Robin uses clients sequentially.
func NewClient(addr, password string) *Client {
	opts := &goredis.Options{
		Addr:         addr,
		Password:     password,
		PoolSize:     1,
		MaxIdleConns: 1,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	return &Client{
		client:   goredis.NewClient(opts),
		addr:     addr,
		password: password,
	}
}

// GetInfo executes INFO ALL and returns the raw response.
// Falls back to INFO (no section) if the server doesn't support section arguments.
func (c *Client) GetInfo(ctx context.Context) (string, error) {
	result, err := c.client.Info(ctx, "all").Result()
	if err != nil {
		// Fallback: some servers (e.g. miniredis) don't support INFO <section>.
		result, err = c.client.Info(ctx).Result()
		if err != nil {
			return "", fmt.Errorf("INFO on %s: %w", c.addr, err)
		}
	}
	return result, nil
}

// ClusterInfo holds parsed CLUSTER INFO fields.
type ClusterInfo struct {
	State       string
	SlotsOK     int
	SlotsFail   int
	KnownNodes  int
	ClusterSize int
	raw         map[string]string
}

// Raw returns all parsed key-value pairs from CLUSTER INFO.
func (ci *ClusterInfo) Raw() map[string]string {
	return ci.raw
}

// GetClusterInfo executes CLUSTER INFO and returns the parsed result.
func (c *Client) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	result, err := c.client.ClusterInfo(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("CLUSTER INFO on %s: %w", c.addr, err)
	}

	info := &ClusterInfo{raw: make(map[string]string)}
	for _, line := range strings.Split(strings.TrimSpace(result), "\n") {
		line = strings.TrimSpace(line)
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		info.raw[key] = value

		switch key {
		case "cluster_state":
			info.State = value
		case "cluster_slots_ok":
			info.SlotsOK, _ = strconv.Atoi(value)
		case "cluster_slots_fail":
			info.SlotsFail, _ = strconv.Atoi(value)
		case "cluster_known_nodes":
			info.KnownNodes, _ = strconv.Atoi(value)
		case "cluster_size":
			info.ClusterSize, _ = strconv.Atoi(value)
		}
	}
	return info, nil
}

// ClusterNode represents a single node from CLUSTER NODES output.
type ClusterNode struct {
	ID       string
	Addr     string
	IP       string
	Flags    string
	Primary  string
	PingSent int
	PongRecv int
	Epoch    int
	State    string
	Slots    string
}

// GetClusterNodes executes CLUSTER NODES and returns the parsed result.
func (c *Client) GetClusterNodes(ctx context.Context) ([]ClusterNode, error) {
	result, err := c.client.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("CLUSTER NODES on %s: %w", c.addr, err)
	}
	return ParseClusterNodesOutput(result), nil
}

// ParseClusterNodesOutput parses the raw text output of CLUSTER NODES into a slice of ClusterNode.
func ParseClusterNodesOutput(output string) []ClusterNode {
	var nodes []ClusterNode
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}

		// addr format is ip:port@cport or ip:port
		addr := parts[1]
		ip := addr
		if idx := strings.Index(addr, ":"); idx >= 0 {
			ip = addr[:idx]
		}

		pingSent, _ := strconv.Atoi(parts[4])
		pongRecv, _ := strconv.Atoi(parts[5])
		epoch, _ := strconv.Atoi(parts[6])

		slots := ""
		if len(parts) > 8 {
			slots = strings.Join(parts[8:], " ")
		}

		nodes = append(nodes, ClusterNode{
			ID:       parts[0],
			Addr:     addr,
			IP:       ip,
			Flags:    parts[2],
			Primary:  parts[3],
			PingSent: pingSent,
			PongRecv: pongRecv,
			Epoch:    epoch,
			State:    parts[7],
			Slots:    slots,
		})
	}
	return nodes
}

// CheckConnection pings Redis with retry and backoff.
func (c *Client) CheckConnection(ctx context.Context, maxRetries int, backoff time.Duration) error {
	var lastErr error
	for i := range maxRetries {
		if err := c.client.Ping(ctx).Err(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return fmt.Errorf("failed to connect to %s after %d retries: %w", c.addr, maxRetries, lastErr)
}

// Close closes the Redis client connection.
func (c *Client) Close() error {
	return c.client.Close()
}

// Addr returns the address of the Redis node.
func (c *Client) Addr() string {
	return c.addr
}

// Password returns the configured password (may be empty).
func (c *Client) Password() string {
	return c.password
}

// ClusterMyID executes CLUSTER MYID and returns the node's unique cluster ID.
func (c *Client) ClusterMyID(ctx context.Context) (string, error) {
	result, err := c.client.Do(ctx, "CLUSTER", "MYID").Text()
	if err != nil {
		return "", fmt.Errorf("CLUSTER MYID on %s: %w", c.addr, err)
	}
	return strings.TrimSpace(result), nil
}

// ClusterMeet sends CLUSTER MEET to introduce a node to the cluster.
func (c *Client) ClusterMeet(ctx context.Context, ip string, port int) error {
	err := c.client.ClusterMeet(ctx, ip, fmt.Sprintf("%d", port)).Err()
	if err != nil {
		return fmt.Errorf("CLUSTER MEET %s:%d on %s: %w", ip, port, c.addr, err)
	}
	return nil
}

// ClusterAddSlots assigns slots to the current node.
func (c *Client) ClusterAddSlots(ctx context.Context, slots ...int) error {
	err := c.client.ClusterAddSlots(ctx, slots...).Err()
	if err != nil {
		return fmt.Errorf("CLUSTER ADDSLOTS on %s: %w", c.addr, err)
	}
	return nil
}

// ClusterReplicate makes the current node a replica of the given primary node ID.
func (c *Client) ClusterReplicate(ctx context.Context, primaryID string) error {
	err := c.client.ClusterReplicate(ctx, primaryID).Err()
	if err != nil {
		return fmt.Errorf("CLUSTER REPLICATE %s on %s: %w", primaryID, c.addr, err)
	}
	return nil
}

// ClusterReset resets the cluster node. If hard is true, performs a HARD reset.
func (c *Client) ClusterReset(ctx context.Context, hard bool) error {
	mode := "SOFT"
	if hard {
		mode = "HARD"
	}
	err := c.client.Do(ctx, "CLUSTER", "RESET", mode).Err()
	if err != nil {
		return fmt.Errorf("CLUSTER RESET %s on %s: %w", mode, c.addr, err)
	}
	return nil
}

// ClusterForget removes a node from the cluster's node table.
func (c *Client) ClusterForget(ctx context.Context, nodeID string) error {
	err := c.client.ClusterForget(ctx, nodeID).Err()
	if err != nil {
		return fmt.Errorf("CLUSTER FORGET %s on %s: %w", nodeID, c.addr, err)
	}
	return nil
}

// ClusterSetSlotStable marks a slot as stable, clearing any importing/migrating state.
func (c *Client) ClusterSetSlotStable(ctx context.Context, slot int) error {
	err := c.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "STABLE").Err()
	if err != nil {
		return fmt.Errorf("CLUSTER SETSLOT %d STABLE on %s: %w", slot, c.addr, err)
	}
	return nil
}

// ClusterSetSlotNode assigns a slot to the given node, forcing configuration agreement.
func (c *Client) ClusterSetSlotNode(ctx context.Context, slot int, nodeID string) error {
	err := c.client.Do(ctx, "CLUSTER", "SETSLOT", fmt.Sprintf("%d", slot), "NODE", nodeID).Err()
	if err != nil {
		return fmt.Errorf("CLUSTER SETSLOT %d NODE %s on %s: %w", slot, nodeID, c.addr, err)
	}
	return nil
}

// ShutdownSave instructs the node to persist its dataset and shut down cleanly.
// Redis closes the connection as part of SHUTDOWN, so the "connection closed" /
// EOF responses returned by go-redis are treated as success.
func (c *Client) ShutdownSave(ctx context.Context) error {
	err := c.client.Do(ctx, "SHUTDOWN", "SAVE").Err()
	if err == nil || isShutdownConnError(err) {
		return nil
	}
	return fmt.Errorf("SHUTDOWN SAVE on %s: %w", c.addr, err)
}

// isShutdownConnError reports whether the error returned by a SHUTDOWN command is the
// expected connection teardown rather than a genuine failure.
func isShutdownConnError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "connection closed") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "use of closed network connection")
}

// HasInFlightSlots reports whether any of the given nodes currently has slots in a
// migrating ("[<slot>->-<id>]") or importing ("[<slot>-<-<id>]") state. A rebalance is
// only considered complete once no node reports in-flight slots.
func HasInFlightSlots(nodes []ClusterNode) bool {
	for _, n := range nodes {
		if strings.Contains(n.Slots, "[") {
			return true
		}
	}
	return false
}

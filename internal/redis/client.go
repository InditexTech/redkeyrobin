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
	client *goredis.Client
	addr   string
}

// NewClient creates a new Redis client connected to the given address.
func NewClient(addr, password string) *Client {
	opts := &goredis.Options{
		Addr:     addr,
		Password: password,
	}
	return &Client{
		client: goredis.NewClient(opts),
		addr:   addr,
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

	var nodes []ClusterNode
	for _, line := range strings.Split(strings.TrimSpace(result), "\n") {
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
		if idx := strings.Index(addr, ":"); idx > 0 {
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
	return nodes, nil
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

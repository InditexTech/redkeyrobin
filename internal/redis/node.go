// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/inditextech/redkeyrobin/internal/util"
)

// RedisSlotRange represents a range of Redis slots.
type RedisSlotRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// RedisNode represents a RedKey cluster node.
type RedisNode struct {
	Name       string           `json:"name"`
	ID         string           `json:"id"`
	Addr       string           `json:"-"`
	IP         string           `json:"ip"`
	Flags      string           `json:"flags"`
	Slots      []RedisSlotRange `json:"slots"`
	PrimaryID  string           `json:"masterId"`
	Failures   int              `json:"failures"`
	Sent       int              `json:"sent"`
	Recv       int              `json:"recv"`
	LinkStatus string           `json:"linkStatus"`
	MaxRetries int              `json:"-"`
	Backoff    time.Duration    `json:"-"`
	Migrating  map[int]string   `json:"migrating,omitempty"`
	Importing  map[int]string   `json:"importing,omitempty"`

	clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error)
}

// Default client factory (global, no closure)
func defaultClientFactory(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
	redisClient := NewRedisClient(ctx, addr, os.Getenv("REDISAUTH"), 0)
	if err := redisClient.CheckConnection(maxRetries, backoff); err != nil {
		return nil, err
	}
	return redisClient, nil
}

// NewRedisNode creates a new RedisNode with default client factory
func NewRedisNode(name, addr string, maxRetries int, backoff time.Duration) *RedisNode {
	return &RedisNode{
		Name:          name,
		Addr:          addr,
		MaxRetries:    maxRetries,
		Backoff:       backoff,
		clientFactory: defaultClientFactory,
	}
}

// NewFakeRedisNode creates a new RedisNode with a custom client factory (for testing)
func NewFakeRedisNode(name string, factory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error)) *RedisNode {
	return &RedisNode{
		Name:          name,
		clientFactory: factory,
	}
}

// WithClientFactory allows to configure a custom function to obtain clients (used for testing)
func (rn *RedisNode) WithClientFactory(factory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error)) *RedisNode {
	rn.clientFactory = factory
	return rn
}

// String returns a formatted string of the Redis node.
func (rn *RedisNode) String() string {
	return fmt.Sprintf("Node(ID: %s, Name: %s, IP: %s)", rn.ID, rn.Name, rn.IP)
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------- GETTERS AND SETTERS ---------------------------------------
// ----------------------------------------------------------------------------------------------------

// GetNumberOfSlots returns the number of slots of the Redis node.
func (rn *RedisNode) GetNumberOfSlots() int {
	slots := 0

	for _, slotRange := range rn.Slots {
		slots += slotRange.End - slotRange.Start + 1
	}

	return slots
}

// SetID sets the ID of the Redis node.
func (rn *RedisNode) SetID(id string) {
	rn.ID = id
	rn.ResetSlots()
}

// SetIP sets the IP of the Redis node.
func (rn *RedisNode) SetIP(ip string) {
	rn.IP = ip
	rn.ResetSlots()
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- ASKERS ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// IsPrimary returns true if the Redis node is a primary.
func (rn *RedisNode) IsPrimary() bool {
	return rn.hasFlag("master")
}

// IsReplica returns true if the Redis node is a replica.
func (rn *RedisNode) IsReplica() bool {
	return rn.hasFlag("slave")
}

// IsConnected returns true if the Redis node is connected.
func (rn *RedisNode) IsConnected() bool {
	return rn.LinkStatus == "connected"
}

// IsDisconnected returns true if the Redis node is disconnected.
func (rn *RedisNode) IsDisconnected() bool {
	return rn.LinkStatus == "disconnected"
}

// HasSlots returns true if the Redis node has slots.
func (rn *RedisNode) HasSlots() bool {
	return rn.GetNumberOfSlots() > 0
}

// ShouldBeRemoved returns true if the Redis node should be removed.
func (rn *RedisNode) ShouldBeRemoved() bool {
	return rn.hasFlag("fail") || rn.hasFlag("noaddr")
}

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PUBLIC  ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// Init initializes the Redis node.
func (rn *RedisNode) Init(ctx context.Context) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	// Get node info
	nodeID, err := redisClient.GetMyID()
	if err != nil {
		return err
	}

	nodeIP, err := util.GetIPFromAddress(rn.Addr)
	if err != nil {
		return err
	}

	rn.ID = nodeID
	rn.IP = nodeIP
	return nil
}

func (rn *RedisNode) InitStandalone(ctx context.Context) error {
	nodeIP, err := util.GetIPFromAddress(rn.Addr)
	if err != nil {
		return err
	}

	rn.IP = nodeIP
	return nil
}

// CheckConnection checks the connection to the Redis node.
func (rn *RedisNode) CheckConnection(ctx context.Context) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()
	return nil
}

// UpdateInfo updates the Redis node information.
func (rn *RedisNode) UpdateInfo(nodeInfo RedisNode) {
	if nodeInfo.IP != "" {
		rn.IP = nodeInfo.IP
	}
	rn.Flags = nodeInfo.Flags
	rn.Slots = nodeInfo.Slots
	rn.PrimaryID = nodeInfo.PrimaryID
	rn.Failures = nodeInfo.Failures
	rn.Sent = nodeInfo.Sent
	rn.Recv = nodeInfo.Recv
	rn.LinkStatus = nodeInfo.LinkStatus
	rn.Migrating = nodeInfo.Migrating
	rn.Importing = nodeInfo.Importing
}

// ResetSlots resets the Redis node slots.
func (rn *RedisNode) ResetSlots() {
	rn.Slots = []RedisSlotRange{}
}

// GetClusterNodes gets the cluster nodes known by the Redis node.
func (rn *RedisNode) GetClusterNodes(ctx context.Context) ([]RedisNode, error) {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return nil, err
	}
	defer redisClient.Close()

	return redisClient.GetNodesInfo()
}

// ReplicateNode replicates the Redis node.
func (rn *RedisNode) ReplicateNode(ctx context.Context, primary RedisNode) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterReplicate(primary.ID)
}

// Reset resets the Redis node.
func (rn *RedisNode) Reset(ctx context.Context) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterReset(false)
}

// MeetNode meets the Redis node with the current node.
func (rn *RedisNode) MeetNode(ctx context.Context, node RedisNode) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterMeet(node.IP, RedisPort)
}

// ForgetNode forgets the Redis node in the current node.
func (rn *RedisNode) ForgetNode(ctx context.Context, node RedisNode) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterForget(node.ID)
}

// AddSlots adds slots to the Redis node.
func (rn *RedisNode) AddSlots(ctx context.Context, slots ...int) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterAddSlots(slots...)
}

// Failover triggers a failover in the Redis node.
func (rn *RedisNode) Failover(ctx context.Context) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterFailover()
}

// StabilizeSlot sets a slot as stable in the Redis node.
func (rn *RedisNode) StabilizeSlot(ctx context.Context, nodeIP string, slot int) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return nil
	}
	defer redisClient.Close()

	cmd := redisClient.StabilizeSlot(ctx, nodeIP, slot)
	if cmd.Err != nil {
		return fmt.Errorf("error stabilizing slot: %v", cmd.Err)
	}
	return nil
}

// ----------------------------------------------------------------------------------------------------
// --------------------------------------------- PRIVATE ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// GetClient returns a Redis client for the node.
func (rn *RedisNode) getClient(ctx context.Context) (RedisClientInterface, error) {
	return rn.clientFactory(ctx, rn.Addr, rn.MaxRetries, rn.Backoff)
}

// hasFlag checks if the Redis node has a specific flag.
func (rn *RedisNode) hasFlag(flag string) bool {
	return strings.Contains(rn.Flags, flag)
}

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

	"github.com/inditextech/redisrobin/internal/util"
)

// RedisNode represents a Redis cluster node.
type RedisNode struct {
	Name       string           `json:"name"`
	ID         string           `json:"id"`
	Addr       string           `json:"-"`
	IP         string           `json:"ip"`
	Flags      string           `json:"flags"`
	Slots      []RedisSlotRange `json:"slots"`
	MasterID   string           `json:"masterId"`
	Failures   int              `json:"failures"`
	Sent       int              `json:"sent"`
	Recv       int              `json:"recv"`
	LinkStatus string           `json:"linkStatus"`
	MaxRetries int              `json:"-"`
	Backoff    time.Duration    `json:"-"`
}

type RedisSlotRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

func (rn *RedisNode) String() string {
	return fmt.Sprintf("Node(ID: %s, Name: %s, IP: %s)", rn.ID, rn.Name, rn.IP)
}

func (rn *RedisNode) GetNumberOfSlots() int {
	slots := 0

	for _, slotRange := range rn.Slots {
		slots += slotRange.End - slotRange.Start + 1
	}

	return slots
}

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

func (rn *RedisNode) getClient(ctx context.Context) (*RedisClient, error) {
	redisClient := NewRedisClient(ctx, rn.Addr, os.Getenv("REDISAUTH"), 0)
	if err := redisClient.CheckConnection(rn.MaxRetries, rn.Backoff); err != nil {
		return nil, err
	}
	return redisClient, nil
}

func (rn *RedisNode) CheckConnection(ctx context.Context) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()
	return nil
}

func (rn *RedisNode) UpdateInfo(nodeInfo RedisNode) {
	if nodeInfo.IP != "" {
		rn.IP = nodeInfo.IP
	}
	rn.Flags = nodeInfo.Flags
	rn.Slots = nodeInfo.Slots
	rn.MasterID = nodeInfo.MasterID
	rn.Failures = nodeInfo.Failures
	rn.Sent = nodeInfo.Sent
	rn.Recv = nodeInfo.Recv
	rn.LinkStatus = nodeInfo.LinkStatus
}

func (rn *RedisNode) ResetSlots() {
	rn.Slots = []RedisSlotRange{}
}

func (rn *RedisNode) SetID(id string) {
	rn.ID = id
	rn.ResetSlots()
}

func (rn *RedisNode) SetIP(ip string) {
	rn.IP = ip
	rn.ResetSlots()
}

func (rn *RedisNode) IsMaster() bool {
	return strings.Contains(rn.Flags, "master")
}

func (rn *RedisNode) IsConnected() bool {
	return rn.LinkStatus == "connected"
}

func (rn *RedisNode) IsDisconnected() bool {
	return rn.LinkStatus == "disconnected"
}

func (rn *RedisNode) IsReplica() bool {
	return rn.hasFlag("slave")
}

func (rn *RedisNode) HasSlots() bool {
	return rn.GetNumberOfSlots() > 0
}

func (rn *RedisNode) ShouldBeRemoved() bool {
	return rn.hasFlag("fail") || rn.hasFlag("noaddr")
}

func (rn *RedisNode) hasFlag(flag string) bool {
	return strings.Contains(rn.Flags, flag)
}

func (rn *RedisNode) GetClusterNodes(ctx context.Context) ([]RedisNode, error) {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return nil, err
	}
	defer redisClient.Close()

	return redisClient.GetNodesInfo()
}

func (rn *RedisNode) ReplicateNode(ctx context.Context, master RedisNode) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterReplicate(master.ID)
}

func (rn *RedisNode) Reset(ctx context.Context) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterReset(false)
}

func (rn *RedisNode) MeetNode(ctx context.Context, node RedisNode) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterMeet(node.IP, RedisPort)
}

func (rn *RedisNode) ForgetNode(ctx context.Context, node RedisNode) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterForget(node.ID)
}

func (rn *RedisNode) AddSlots(ctx context.Context, slots ...int) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterAddSlots(slots...)
}

func (rn *RedisNode) Failover(ctx context.Context) error {
	redisClient, err := rn.getClient(ctx)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	return redisClient.ClusterFailover()
}

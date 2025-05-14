// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"os"
	"time"
)

// RedisNode represents a Redis cluster node.
type RedisNode struct {
	Name     string
	ID       string
	Addr     string
	IP       string
	Role     string
	Slots    []RedisSlotRange
	MasterID string
	Failures int
}

type RedisSlotRange struct {
	Start int
	End   int
}

func (rn *RedisNode) GetNumberOfSlots() int {
	slots := 0

	for _, slotRange := range rn.Slots {
		slots += slotRange.End - slotRange.Start + 1
	}

	return slots
}

func (rn *RedisNode) Init() error {
	redisClient := NewRedisClient(context.Background(), rn.Addr, os.Getenv("REDISAUTH"), 0)
	defer redisClient.Close()

	// Check connection
	if err := redisClient.CheckConnection(3, 1*time.Second); err != nil {
		return err
	}

	// Get node info
	nodeID, err := redisClient.GetMyID()
	if err != nil {
		return err
	}

	rn.ID = nodeID
	return nil
}

func (rn *RedisNode) UpdateInfo(nodeInfo RedisNode) {
	rn.IP = nodeInfo.IP
	rn.Role = nodeInfo.Role
	rn.Slots = nodeInfo.Slots
	rn.MasterID = nodeInfo.MasterID
	rn.Failures = nodeInfo.Failures
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"os"
	"context"
	"time"
	"log"
)

// Node represents a Redis cluster node.
type Node struct {
	Name 	 string
	ID       string
	Addr     string
	IP       string
	Role     string
	Slots    [][]int
	MasterID string
	Failures int
}

func (rn *Node) GetNumberOfSlots() int {
	slots := 0

	for _, slotRange := range rn.Slots {
		slots += slotRange[1] - slotRange[0] + 1
	}

	return slots
}

func (rn *Node) Init() error {
	redisClient := NewRedisClient(context.Background(), rn.Addr, os.Getenv("REDISAUTH"), 0)
	defer redisClient.Close()

	// Check connection
	if err := redisClient.CheckConnection(3, 1*time.Second); err != nil {
		log.Printf("Error connecting to Redis: %v", err)
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

func (rn *Node) UpdateInfo(nodeInfo Node) {
	rn.IP = nodeInfo.IP
	rn.Role = nodeInfo.Role
	rn.Slots = nodeInfo.Slots
	rn.MasterID = nodeInfo.MasterID
	rn.Failures = nodeInfo.Failures
}
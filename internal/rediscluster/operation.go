// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package rediscluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/inditextech/redisrobin/internal/redis"
)

type RedisBaseOperationInterface interface {
	Run() error
	Cancel()
	GetDuration() time.Duration
	GetElapsedTime() time.Duration
	GetElapsedTimeFromEnd() time.Duration
	GetStatus() string
	GetName() string
	GetNodeFrom() *redis.RedisNode
	GetNodeTo() *redis.RedisNode
}

type RedisOperation interface {
	RedisBaseOperationInterface
	Launch() error
	Wait() error
}

// RedisOperationBase represents an operation in the Redis cluster, such as rebalancing or resharding
type RedisOperationBase struct {
	name          string
	status        string
	redisCluster  *RedisCluster
	nodeFrom      *redis.RedisNode
	nodeTo        *redis.RedisNode
	initTimestamp time.Time
	endTimestamp  time.Time
	cmd           redis.RedisCommand
	logger        *slog.Logger
	ctx           context.Context
}

// Wait waits for the command to finish and updates the operation status
func (ro *RedisOperationBase) Run() error {
	// Wait for command to finish
	ro.cmd.Wait()
	ro.endTimestamp = time.Now()

	// Check if command failed
	if err := ro.cmd.Error(); err != nil {
		ro.status = "Error"
		return err
	}

	// Command finished successfully
	ro.status = "Finished"
	return nil
}

// Cancel cancels the operation
func (ro *RedisOperationBase) Cancel() {
	ro.cmd.Cancel()
	ro.status = "Cancelled"
}

// GetDuration returns the duration of the operation
func (ro *RedisOperationBase) GetDuration() time.Duration {
	return ro.endTimestamp.Sub(ro.initTimestamp)
}

// GetElapsedTime returns the elapsed time since the operation started
func (ro *RedisOperationBase) GetElapsedTime() time.Duration {
	return time.Since(ro.initTimestamp)
}

// GetElapsedTimeFromEnd returns the elapsed time since the operation ended
func (ro *RedisOperationBase) GetElapsedTimeFromEnd() time.Duration {
	if ro.endTimestamp.IsZero() {
		return time.Duration(0)
	}

	return time.Since(ro.endTimestamp)
}

// GetStatus returns the status of the operation
func (ro *RedisOperationBase) GetStatus() string {
	return ro.status
}

// GetName returns the name of the operation
func (ro *RedisOperationBase) GetName() string {
	return ro.name
}

// GetNodeFrom returns the source node of the operation
func (ro *RedisOperationBase) GetNodeFrom() *redis.RedisNode {
	return ro.nodeFrom
}

// GetNodeTo returns the destination node of the operation
func (ro *RedisOperationBase) GetNodeTo() *redis.RedisNode {
	return ro.nodeTo
}

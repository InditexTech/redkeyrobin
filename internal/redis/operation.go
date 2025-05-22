// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"time"
)

// RedisOperation represents an operation in the Redis cluster, such as rebalancing or resharding
type RedisOperation struct {
	Name          string
	Status        string
	NodeFrom      *RedisNode
	NodeTo        *RedisNode
	InitTimestamp time.Time
	EndTimestamp  time.Time
	Cmd           RedisCommand
}

// Wait waits for the command to finish and updates the operation status
func (ro *RedisOperation) Wait() error {
	// Wait for command to finish
	ro.Cmd.Wait()
	ro.EndTimestamp = time.Now()

	// Check if command failed
	if err := ro.Cmd.Error(); err != nil {
		ro.Status = Error
		return err
	}

	// Command finished successfully
	ro.Status = "Finished"
	return nil
}

// Cancel cancels the operation
func (ro *RedisOperation) Cancel() {
	ro.Cmd.Cancel()
	ro.Status = "Cancelled"
}

// GetDuration returns the duration of the operation
func (ro *RedisOperation) GetDuration() time.Duration {
	return ro.EndTimestamp.Sub(ro.InitTimestamp)
}

// GetElapsedTime returns the elapsed time since the operation started
func (ro *RedisOperation) GetElapsedTime() time.Duration {
	return time.Since(ro.InitTimestamp)
}

// GetElapsedTimeFromEnd returns the elapsed time since the operation ended
func (ro *RedisOperation) GetElapsedTimeFromEnd() time.Duration {
	if ro.EndTimestamp.IsZero() {
		return time.Duration(0)
	}

	return time.Since(ro.EndTimestamp)
}

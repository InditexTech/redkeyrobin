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
	Cmd           *RedisCLICommand
}

// Wait waits for the command to finish and updates the operation status
func (ro *RedisOperation) Wait() error {
	// Wait for command to finish
	ro.Cmd.Wait()
	ro.EndTimestamp = time.Now()

	// Check if command failed
	if err := ro.Cmd.Err; err != nil {
		ro.Status = Error
		return err
	}

	// Command finished successfully
	ro.Status = "Finished"
	return nil
}

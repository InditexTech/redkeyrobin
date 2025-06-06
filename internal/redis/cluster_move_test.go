// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TODO: Test launch. Implement a redisCluster mock and redisClient mock
func TestRedisOperationMoveWait(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
		err			error
	}{
		{
			name: "move error",
			cmd:  NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: ReshardingError,
			err: fmt.Errorf("error moving slots from node 'test-0' to node 'node2': "),
		},
		{
			name: "good",
			cmd:  NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := NewFakeRedisOperationMove(context.Background(), redisCluster, "Running", node1, node2, 10, time.Time{})
			operation.cmd = tt.cmd

			tt.cmd.cmd.Start()
			err := operation.Wait()
			if tt.err != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.err.Error(), err.Error())
			}

			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}
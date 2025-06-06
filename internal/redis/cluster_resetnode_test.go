// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: Test launch. Implement a redisCluster mock and redisClient mock
func TestRedisOperatioResetNodeWait(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		err			error
	}{
		{
			name: "scale up error",
			cmd:  NewRedisCLICommand(t.Context(), "exit 1"),
			err: fmt.Errorf("error resetting cluster node 'test-0': "),
		},
		{
			name: "good",
			cmd:  NewRedisCLICommand(t.Context(), "exit 0"),
			err: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := NewFakeRedisOperationResetNode(context.Background(), redisCluster, "Running", node1)
			operation.cmd = tt.cmd

			tt.cmd.cmd.Start()
			err := operation.Wait()
			if tt.err != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.err.Error(), err.Error())
			}
		})
	}
}
// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/stretchr/testify/assert"
)

// TODO: Test launch. Implement a redkeyCluster mock and redisClient mock
func TestRedisOperationFixWait(t *testing.T) {
	tests := []struct {
		name string
		cmd  *redis.RedisCLICommand
		err  error
	}{
		{
			name: "fix error",
			cmd:  redis.NewRedisCLICommand(t.Context(), "exit 1"),
			err:  fmt.Errorf("error fixing cluster: "),
		},
		{
			name: "good",
			cmd:  redis.NewRedisCLICommand(t.Context(), "exit 0"),
			err:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := NewFakeRedisOperationFix(context.Background(), redkeyCluster, "Running")
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()
			if tt.err != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.err.Error(), err.Error())
			}
		})
	}
}

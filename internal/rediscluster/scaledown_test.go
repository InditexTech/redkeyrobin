// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package rediscluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/stretchr/testify/assert"
)

// TODO: Test launch. Implement a redisCluster mock and redisClient mock
func TestRedisOperationScaleDownWait(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *redis.RedisCLICommand
		expectedStatus string
		err            error
	}{
		{
			name:           "scale up error",
			cmd:            redis.NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: ScalingDownError,
			err:            fmt.Errorf("error scaling down cluster: "),
		},
		{
			name:           "good",
			cmd:            redis.NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
			err:            nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := NewFakeRedisOperationScaleDown(context.Background(), redisCluster, "Running")
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()
			if tt.err != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.err.Error(), err.Error())
			}

			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

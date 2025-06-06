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
func TestRedisOperationUpgradeWait(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *redis.RedisCLICommand
		expectedStatus string
		err            error
	}{
		{
			name:           "scale up error",
			cmd:            redis.NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: UpgradingError,
			err:            fmt.Errorf("error upgrading cluster: "),
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
			operation := NewFakeRedisOperationUpgrade(context.Background(), redisCluster, "Running")
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()
			if tt.err != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.err, err)
			}

			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

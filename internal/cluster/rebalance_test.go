// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedisOperationRebalanceLaunch(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockRedKeyCluster)
		expectedError error
	}{
		{
			name: "ensureNodesAreUp fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.EnsureNodesAreUpError = fmt.Errorf("nodes are not up")
			},
			expectedError: fmt.Errorf("error ensuring nodes are up: nodes are not up"),
		},
		{
			name: "getAndCheckRedisClient fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.GetAndCheckRedisClientError = fmt.Errorf("redis client error")
			},
			expectedError: fmt.Errorf("error getting and checking Redis client: redis client error"),
		},
		{
			name: "ClusterRebalance fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.SetRedisClientError("ClusterRebalance", fmt.Errorf("cluster rebalance failed"))
			},
			expectedError: fmt.Errorf("error rebalancing cluster: cluster rebalance failed"),
		},
		{
			name: "success",
			setupMock: func(mock *MockRedKeyCluster) {
				// No errors configured - everything should succeed
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)

			// Create mocked operation using NewFakeRedisOperationRebalance
			operation := NewFakeRedisOperationRebalance(context.Background(), mockCluster, "Pending", time.Time{})
			operation.weights = map[string]int{"node1": 1, "node2": 2}

			err := operation.Launch()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.Nil(t, err)
				assert.Equal(t, "Running", operation.GetStatus())
				assert.False(t, operation.initTimestamp.IsZero())
				assert.NotNil(t, operation.cmd)
			}
		})
	}
}

func TestRedisOperationRebalanceWait(t *testing.T) {
	tests := []struct {
		name          string
		cmd           *redis.RedisCLICommand
		setupMock     func(*MockRedKeyCluster)
		expectedError error
	}{
		{
			name: "command error",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 1"),
			setupMock: func(mock *MockRedKeyCluster) {
				// No additional setup needed
			},
			expectedError: fmt.Errorf("error rebalancing cluster: "),
		},
		{
			name: "refreshNodes fails",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RefreshNodesError = fmt.Errorf("refresh nodes failed")
			},
			expectedError: fmt.Errorf("refresh nodes failed"),
		},
		{
			name: "success",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock: func(mock *MockRedKeyCluster) {
				// No errors configured
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)

			// Create operation
			operation := NewFakeRedisOperationRebalance(context.Background(), mockCluster, "Running", time.Time{})
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

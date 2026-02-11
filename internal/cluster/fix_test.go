// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/stretchr/testify/assert"
)

func TestRedisOperationFixLaunch(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockRedKeyCluster)
		expectedError error
	}{
		{
			name: "ensureNodesAreUp fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.EnsureNodesAreUpError = fmt.Errorf("nodes are down")
			},
			expectedError: fmt.Errorf("error ensuring nodes are up: nodes are down"),
		},
		{
			name: "getAndCheckRedisClient fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.GetAndCheckRedisClientError = fmt.Errorf("redis client error")
			},
			expectedError: fmt.Errorf("error getting and checking Redis client: redis client error"),
		},
		{
			name: "ClusterFix fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.SetRedisClientError("ClusterFix", fmt.Errorf("cluster fix failed"))
			},
			expectedError: fmt.Errorf("error fixing cluster: cluster fix failed"),
		},
		{
			name: "success",
			setupMock: func(mock *MockRedKeyCluster) {
				// All methods should succeed - no errors to set
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)

			// Create mocked operation
			operation := NewFakeRedisOperationFix(context.Background(), mockCluster, "Pending")

			err := operation.Launch()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, "Running", operation.GetStatus())
				assert.False(t, operation.initTimestamp.IsZero())
				assert.NotNil(t, operation.cmd)
			}
		})
	}
}

func TestRedisOperationFixWait(t *testing.T) {
	tests := []struct {
		name          string
		cmd           *redis.RedisCLICommand
		setupMock     func(*MockRedKeyCluster)
		expectedError error
	}{
		{
			name: "fix command error",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 1"),
			setupMock: func(mock *MockRedKeyCluster) {
				// refreshNodes shouldn't be called when fix fails
			},
			expectedError: fmt.Errorf("error fixing cluster: "),
		},
		{
			name: "refreshNodes fails",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock: func(mock *MockRedKeyCluster) {
				// Make refreshNodes fail
				mock.RefreshNodesError = fmt.Errorf("failed to refresh nodes")
			},
			expectedError: fmt.Errorf("failed to refresh nodes"),
		},
		{
			name: "success",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock: func(mock *MockRedKeyCluster) {
				// All methods should succeed - no errors to set
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)

			operation := NewFakeRedisOperationFix(context.Background(), mockCluster, "Running")
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}

			// Note: Fix operation does not change cluster status, so we don't validate it
		})
	}
}

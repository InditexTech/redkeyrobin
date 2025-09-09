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

func TestRedisOperationResetNodeLaunch(t *testing.T) {
	// Create operation
	node := redis.NewFakeRedisNode("test-node", mockClientFactory)
	operation := NewRedisOperationResetNode(context.Background(), redkeyCluster, node)

	err := operation.Launch()

	assert.Nil(t, err)
	assert.Equal(t, "Running", operation.GetStatus())
	assert.False(t, operation.initTimestamp.IsZero())
	assert.NotNil(t, operation.cmd)
}

func TestRedisOperationResetNodeWait(t *testing.T) {
	tests := []struct {
		name          string
		cmd           *redis.RedisLibraryCommand
		expectedError error
	}{
		{
			name:          "error",
			cmd:           redis.NewRedisLibraryCommand(context.Background(), func(ctx context.Context) error { return fmt.Errorf("reset failed") }),
			expectedError: fmt.Errorf("error resetting cluster node 'test-node': reset failed"),
		},
		{
			name:          "success",
			cmd:           redis.NewRedisLibraryCommand(context.Background(), func(ctx context.Context) error { return nil }),
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create operation
			node := redis.NewFakeRedisNode("test-node", mockClientFactory)
			operation := NewFakeRedisOperationResetNode(context.Background(), redkeyCluster, "Running", node)
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestRedisOperationResetNodeDoResetNode(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockRedKeyCluster)
		nodeName      string
		expectedError error
	}{
		{
			name: "node not found in context",
			setupMock: func(mock *MockRedKeyCluster) {
				// No setup needed - context will be empty
			},
			expectedError: fmt.Errorf("node name not found in context"),
		},
		{
			name: "node not found",
			setupMock: func(mock *MockRedKeyCluster) {
				// No setup needed - GetNode will return nil for non-existent node
			},
			nodeName:      "non-existent-node",
			expectedError: fmt.Errorf("node 'non-existent-node' not found"),
		},
		{
			name: "node reset fails",
			setupMock: func(mock *MockRedKeyCluster) {
				// No setup needed - Reset will fail due to mock client
			},
			nodeName:      "test-0",
			expectedError: fmt.Errorf("error creating client"),
		},
		{
			name: "forgetNode fails (ephemeral cluster)",
			setupMock: func(mock *MockRedKeyCluster) {
				// Configure forgetNode to fail
				mock.ForgetNodeError = fmt.Errorf("forget node failed")
				// Make the cluster ephemeral
				mock.SetEphemeral(true)
			},
			nodeName:      "test-1",
			expectedError: fmt.Errorf("forget node failed"),
		},
		{
			name: "checkNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.CheckNodesError = fmt.Errorf("check nodes failed")
			},
			nodeName:      "test-1",
			expectedError: fmt.Errorf("check nodes failed"),
		},
		{
			name: "removeOutdatedNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RemoveOutdatedNodesError = fmt.Errorf("remove outdated nodes failed")
			},
			nodeName:      "test-1",
			expectedError: fmt.Errorf("remove outdated nodes failed"),
		},
		{
			name: "meetNodesIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.MeetNodesIfNeededError = fmt.Errorf("meet nodes failed")
			},
			nodeName:      "test-1",
			expectedError: fmt.Errorf("meet nodes failed"),
		},
		{
			name: "ensureClusterRatio fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.EnsureClusterRatioError = fmt.Errorf("cluster ratio failed")
			},
			nodeName:      "test-1",
			expectedError: fmt.Errorf("cluster ratio failed"),
		},
		{
			name: "refreshNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RefreshNodesError = fmt.Errorf("refresh nodes failed")
			},
			nodeName:      "test-1",
			expectedError: fmt.Errorf("refresh nodes failed"),
		},
		{
			name: "success",
			setupMock: func(mock *MockRedKeyCluster) {
				// All methods should succeed - no errors to set
			},
			nodeName:      "test-1",
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)

			// Create operation
			operation := NewFakeRedisOperationResetNode(context.Background(), mockCluster, "Pending", node1)

			// Create context with or without node name based on test case
			var ctx context.Context
			if tt.nodeName != "" {
				ctx = context.WithValue(context.Background(), "nodeName", tt.nodeName)
			} else {
				ctx = context.Background()
			}

			err := operation.doResetNode(ctx)

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

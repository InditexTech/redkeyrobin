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
	"github.com/inditextech/redkeyrobin/internal/config"
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
		node *redis.RedisNode
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
			node:      redis.NewFakeRedisNode("non-existent-node", mockClientFactory),
			expectedError: fmt.Errorf("node 'non-existent-node' not found"),
		},
		{
			name: "node reset fails",
			setupMock: func(mock *MockRedKeyCluster) {
				// No setup needed - Reset will fail due to mock client
			},
			node:      redis.NewFakeRedisNode("test-1", mockClientFactoryError),
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
			node:      redis.NewFakeRedisNode("test-0", mockClientFactory),
			expectedError: fmt.Errorf("forget node failed"),
		},
		{
			name: "checkNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.CheckNodesError = fmt.Errorf("check nodes failed")
			},
			node: 	redis.NewFakeRedisNode("test-0", mockClientFactory),
			expectedError: fmt.Errorf("check nodes failed"),
		},
		{
			name: "removeOutdatedNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RemoveOutdatedNodesError = fmt.Errorf("remove outdated nodes failed")
			},
			node: 	redis.NewFakeRedisNode("test-0", mockClientFactory),
			expectedError: fmt.Errorf("remove outdated nodes failed"),
		},
		{
			name: "meetNodesIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.MeetNodesIfNeededError = fmt.Errorf("meet nodes failed")
			},
			node: 	redis.NewFakeRedisNode("test-0", mockClientFactory),
			expectedError: fmt.Errorf("meet nodes failed"),
		},
		{
			name: "ensureClusterRatio fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.EnsureClusterRatioError = fmt.Errorf("cluster ratio failed")
			},
			node: 	redis.NewFakeRedisNode("test-0", mockClientFactory),
			expectedError: fmt.Errorf("cluster ratio failed"),
		},
		{
			name: "refreshNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RefreshNodesError = fmt.Errorf("refresh nodes failed")
			},
			node: 	redis.NewFakeRedisNode("test-0", mockClientFactory),
			expectedError: fmt.Errorf("refresh nodes failed"),
		},
		{
			name: "success",
			setupMock: func(mock *MockRedKeyCluster) {
				// All methods should succeed - no errors to set
			},
			node: 	redis.NewFakeRedisNode("test-0", mockClientFactory),
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:       "test-cluster",
							MaxRetries: 1,
							BackOff:    time.Microsecond * 10,
						},
					},
				},
				"Ready",
				map[string]*redis.RedisNode{
					"test-0": redis.NewRedisNode("test-0", "id1", 0, time.Duration(0)).WithClientFactory(mockClientFactory),
					"test-1": redis.NewRedisNode("test-1", "id2", 0, time.Duration(0)).WithClientFactory(mockClientFactoryError),
				},
				map[string][]RedisOperation{},
				make(chan struct{}, 1),
			).WithClientFactory(mockClientFactory)

			mockCluster := NewMockRedKeyCluster(cluster)
			tt.setupMock(mockCluster)

			// Create operation
			operation := NewFakeRedisOperationResetNode(context.Background(), mockCluster, "Pending", tt.node)

			// Create context with or without node name based on test case
			var ctx context.Context
			if tt.node != nil {
				ctx = context.WithValue(context.Background(), nodeNameKey, tt.node.Name)
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

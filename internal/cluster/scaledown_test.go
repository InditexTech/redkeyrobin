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

func TestRedisOperationScaleDownLaunch(t *testing.T) {
	// Create operation
	operation := NewRedisOperationScaleDown(context.Background(), redkeyCluster)

	err := operation.Launch()

	assert.Nil(t, err)
	assert.Equal(t, "Running", operation.GetStatus())
	assert.False(t, operation.initTimestamp.IsZero())
	assert.NotNil(t, operation.cmd)
}

func TestRedisOperationScaleDownWait(t *testing.T) {
	tests := []struct {
		name          string
		cmd           *redis.RedisLibraryCommand
		expectedError error
	}{
		{
			name:          "error",
			cmd:           redis.NewRedisLibraryCommand(context.Background(), func(ctx context.Context) error { return fmt.Errorf("scale down failed") }),
			expectedError: fmt.Errorf("error scaling down cluster: scale down failed"),
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
			operation := NewFakeRedisOperationScaleDown(context.Background(), redkeyCluster, "Running")
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
				assert.Equal(t, ScalingDownError, redkeyCluster.GetStatus())
			} else {
				assert.Nil(t, err)
				assert.Equal(t, Ready, redkeyCluster.GetStatus())
			}
		})
	}
}

func TestRedisOperationScaleDownDoScaleDown(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockRedKeyCluster)
		expectedError error
	}{
		{
			name: "checkNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.CheckNodesError = fmt.Errorf("check nodes failed")
			},
			expectedError: fmt.Errorf("check nodes failed"),
		},
		{
			name: "removeNodesIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RemoveNodesIfNeededError = fmt.Errorf("remove nodes failed")
			},
			expectedError: fmt.Errorf("remove nodes failed"),
		},
		{
			name: "removeOutdatedNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RemoveOutdatedNodesError = fmt.Errorf("remove outdated nodes failed")
			},
			expectedError: nil, // This error is logged but does not cause failure
		},
		{
			name: "meetNodesIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.MeetNodesIfNeededError = fmt.Errorf("meet nodes failed")
			},
			expectedError: fmt.Errorf("meet nodes failed"),
		},
		{
			name: "ensureClusterRatio fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.EnsureClusterRatioError = fmt.Errorf("cluster ratio failed")
			},
			expectedError: fmt.Errorf("cluster ratio failed"),
		},
		{
			name: "assignMissingSlotsIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.AssignMissingSlotsIfNeededError = fmt.Errorf("assign slots failed")
			},
			expectedError: fmt.Errorf("assign slots failed"),
		},
		{
			name: "fixClusterIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.FixClusterIfNeededError = fmt.Errorf("fix cluster failed")
			},
			expectedError: fmt.Errorf("fix cluster failed"),
		},
		{
			name: "balanceClusterIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.BalanceClusterIfNeededError = fmt.Errorf("balance cluster failed")
			},
			expectedError: fmt.Errorf("balance cluster failed"),
		},
		{
			name: "refreshNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RefreshNodesError = fmt.Errorf("refresh nodes failed")
			},
			expectedError: fmt.Errorf("refresh nodes failed"),
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

			// Create operation
			operation := NewFakeRedisOperationScaleDown(context.Background(), mockCluster, "Pending")

			err := operation.doScaleDown(context.Background())

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

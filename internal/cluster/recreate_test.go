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

func TestRedisOperationRecreateLaunch(t *testing.T) {
	mockCluster := NewMockRedKeyCluster(redkeyCluster)
	operation := NewRedisOperationRecreate(context.Background(), mockCluster)

	err := operation.Launch()

	assert.Nil(t, err)
	assert.Equal(t, "Running", operation.GetStatus())
	assert.False(t, operation.initTimestamp.IsZero())
	assert.NotNil(t, operation.cmd)
}

func TestRedisOperationRecreateWait(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *redis.RedisCLICommand
		setupMock      func(*MockRedKeyCluster)
		expectedStatus string
		expectedError  error
	}{
		{
			name:           "error",
			cmd:            redis.NewRedisCLICommand(context.Background(), "exit 1"),
			setupMock:      func(mock *MockRedKeyCluster) {},
			expectedStatus: RecreatingError,
			expectedError:  fmt.Errorf("error recreating cluster: "),
		},
		{
			name:           "success",
			cmd:            redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock:      func(mock *MockRedKeyCluster) {},
			expectedStatus: Ready,
			expectedError:  nil,
		},
		{
			name: "refreshNodesInfo fails after success",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RefreshNodesError = fmt.Errorf("refresh nodes failed")
			},
			expectedStatus: Ready,
			expectedError:  fmt.Errorf("refresh nodes failed"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)
			operation := NewFakeRedisOperationRecreate(context.Background(), mockCluster, "Running", time.Time{})
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, mockCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

func TestRedisOperationRecreateDoRecreateCluster(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockRedKeyCluster)
		expectedError error
	}{
		{
			name: "clearNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.ClearNodesError = fmt.Errorf("clear nodes failed")
			},
			expectedError: fmt.Errorf("clear nodes failed"),
		},
		{
			name: "Init fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.InitError = fmt.Errorf("init failed")
			},
			expectedError: fmt.Errorf("init failed"),
		},
		{
			name: "removeOutdatedNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.RemoveOutdatedNodesError = fmt.Errorf("remove outdated nodes failed")
			},
			expectedError: fmt.Errorf("remove outdated nodes failed"),
		},
		{
			name: "checkNodes fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.CheckNodesError = fmt.Errorf("check nodes failed")
			},
			expectedError: fmt.Errorf("check nodes failed"),
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
			name: "balanceClusterIfNeeded fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.BalanceClusterIfNeededError = fmt.Errorf("balance cluster failed")
			},
			expectedError: fmt.Errorf("balance cluster failed"),
		},
		{
			name: "refreshNodesInfo fails",
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
			operation := NewRedisOperationRecreate(context.Background(), mockCluster)

			err := operation.doRecreateCluster(context.Background())

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

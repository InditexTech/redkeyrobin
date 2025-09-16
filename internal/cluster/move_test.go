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

func TestRedisOperationMoveLaunch(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(*MockRedKeyCluster)
		expectedError error
		nodeTo        *redis.RedisNode
	}{
		{
			name: "ensureNodesAreUp fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.EnsureNodesAreUpError = fmt.Errorf("nodes are down")
			},
			expectedError: fmt.Errorf("error ensuring nodes are up: nodes are down"),
			nodeTo:        node1,
		},
		{
			name: "convertNodesToMaster fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.ConvertNodesToMasterError = fmt.Errorf("conversion failed")
			},
			expectedError: fmt.Errorf("error converting node 'node3' to master: conversion failed"),
			nodeTo:        node3, // replica node to trigger conversion
		},
		{
			name: "getAndCheckRedisClient fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.GetAndCheckRedisClientError = fmt.Errorf("redis client error")
			},
			expectedError: fmt.Errorf("error getting and checking Redis client: redis client error"),
			nodeTo:        node1,
		},
		{
			name: "ReshardNode fails",
			setupMock: func(mock *MockRedKeyCluster) {
				mock.SetRedisClientError("ReshardNode", fmt.Errorf("reshard failed"))
			},
			expectedError: fmt.Errorf("error moving slots: reshard failed"),
			nodeTo:        node1,
		},
		{
			name: "success with master node",
			setupMock: func(mock *MockRedKeyCluster) {
				// All methods should succeed - no errors to set
			},
			expectedError: nil,
			nodeTo:        node1, // node1 is already a master
		},
		{
			name: "success with replica conversion",
			setupMock: func(mock *MockRedKeyCluster) {
				// All methods should succeed - no errors to set
			},
			expectedError: nil,
			nodeTo:        node3, // node3 is a replica that needs conversion
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)

			// Create mocked operation
			operation := NewFakeRedisOperationMove(context.Background(), mockCluster, "Pending", node1, tt.nodeTo, 10, time.Time{})

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

func TestRedisOperationMoveWait(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *redis.RedisCLICommand
		setupMock      func(*MockRedKeyCluster)
		expectedStatus string
		expectedError  error
	}{
		{
			name: "reshard command error",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 1"),
			setupMock: func(mock *MockRedKeyCluster) {
				// refreshNodes shouldn't be called when reshard fails
			},
			expectedStatus: ReshardingError,
			expectedError:  fmt.Errorf("error moving slots from node 'test-0' to node 'node2': "),
		},
		{
			name: "refreshNodes fails",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock: func(mock *MockRedKeyCluster) {
				// Make refreshNodes fail
				mock.RefreshNodesError = fmt.Errorf("failed to refresh nodes")
			},
			expectedStatus: Ready,
			expectedError:  fmt.Errorf("failed to refresh nodes"),
		},
		{
			name: "success",
			cmd:  redis.NewRedisCLICommand(context.Background(), "exit 0"),
			setupMock: func(mock *MockRedKeyCluster) {
				// All methods should succeed - no errors to set
			},
			expectedStatus: Ready,
			expectedError:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock cluster
			mockCluster := NewMockRedKeyCluster(redkeyCluster)
			tt.setupMock(mockCluster)

			operation := NewFakeRedisOperationMove(context.Background(), mockCluster, "Running", node1, node2, 10, time.Time{})
			operation.cmd = tt.cmd

			tt.cmd.Start()
			err := operation.Wait()

			if tt.expectedError != nil {
				assert.NotNil(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, mockCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

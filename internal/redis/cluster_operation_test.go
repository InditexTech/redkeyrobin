// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"testing"
	"time"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestRedisClusterAddOperation(t *testing.T) {
	tests := []struct {
		name               string
		operationName      string
		operation 	   	   RedisOperation
		expectedOperations int
	}{
		{
			name:               "add operation rebalancing",
			operationName:      Rebalancing,
			operation: NewFakeRedisOperationRebalance(t.Context(), redisCluster, "Running", time.Time{}),
			expectedOperations: 1,
		},
		{
			name:               "add operation resharding",
			operationName:      Resharding,
			operation: NewFakeRedisOperationMove(t.Context(), redisCluster, "Running", node1, node3, 10, time.Time{}),
			expectedOperations: 1,
		},
		{
			name:               "add operation resharding",
			operationName:      Resharding,
			operation: NewFakeRedisOperationMove(t.Context(), redisCluster, "Finished", node1, node2, 10, time.Time{}),
			expectedOperations: 2,
		},
		{
			name:               "add operation fixing",
			operationName:      Fixing,
			operation: NewFakeRedisOperationFix(t.Context(), redisCluster, "Finished"),
			expectedOperations: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redisCluster.addOperation(tt.operationName, tt.operation)

			assert.NotNil(t, redisCluster.operations[tt.operationName])
			assert.Len(t, redisCluster.operations[tt.operationName], tt.expectedOperations)
		})
	}
}

func TestRedisClusterGetOperation(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		expectedResult  RedisOperation
	}{
		{
			name:           "operation not found",
			operationName:  Fixing,
			expectedResult: nil,
		},
		{
			name:            "no operation with status",
			operationName:   Fixing,
			operationStatus: "Error",
			expectedResult:  nil,
		},
		{
			name:            "operation with status",
			operationName:   Fixing,
			operationStatus: "Finished",
			expectedResult: NewFakeRedisOperationFix(t.Context(), redisCluster, "Finished"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redisCluster.getOperation(tt.operationName, tt.operationStatus)

			if tt.expectedResult != nil {
				assert.NotNil(t, result)
				assert.Equal(t, result.GetStatus(), tt.operationStatus)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestRedisClusterHasOperation(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		expectedResult  bool
	}{
		{
			name:           "operation not found",
			operationName:  Fixing,
			expectedResult: false,
		},
		{
			name:            "no operation with status",
			operationName:   Fixing,
			operationStatus: "Error",
			expectedResult:  false,
		},
		{
			name:            "operation with status",
			operationName:   Fixing,
			operationStatus: "Finished",
			expectedResult:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redisCluster.hasOperation(tt.operationName, tt.operationStatus)
			assert.Equal(t, result, tt.expectedResult)
		})
	}
}

func TestRedisClusterHasOperationInNode(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		node            RedisNode
		expectedResult  bool
	}{
		{
			name:           "operation not found",
			operationName:  Fixing,
			expectedResult: false,
		},
		{
			name:            "no operation in nodes",
			operationName:   Rebalancing,
			operationStatus: "Running",
			node:            *node1,
			expectedResult:  false,
		},
		{
			name:            "operation in nodes",
			operationName:   Resharding,
			operationStatus: "Running",
			node:            *node1,
			expectedResult:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redisCluster.hasOperationInNode(tt.operationName, tt.operationStatus, tt.node)
			assert.Equal(t, result, tt.expectedResult)
		})
	}
}

func TestRedisClusterHasOperationBetweenNodes(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		nodeFrom        RedisNode
		nodeTo          RedisNode
		expectedResult  bool
	}{
		{
			name:           "operation not found",
			operationName:  Fixing,
			expectedResult: false,
		},
		{
			name:            "no operation in nodes",
			operationName:   Rebalancing,
			operationStatus: "Running",
			nodeFrom:        *node1,
			nodeTo:          *node2,
			expectedResult:  false,
		},
		{
			name:            "operation in nodes, bad status",
			operationName:   Resharding,
			operationStatus: "Running",
			nodeFrom:        *node1,
			nodeTo:          *node2,
			expectedResult:  false,
		},
		{
			name:            "operation in nodes",
			operationName:   Resharding,
			operationStatus: "Running",
			nodeFrom:        *node1,
			nodeTo:          *node3,
			expectedResult:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redisCluster.hasOperationBetweenNodes(tt.operationName, tt.operationStatus, tt.nodeFrom, tt.nodeTo)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestRedisClusterRemoveOutdatedOperations(t *testing.T) {
	tests := []struct {
		name               string
		operations         map[string][]RedisOperation
		expectedOperations map[string]int
	}{
		{
			name: "no expired operations",
			operations: map[string][]RedisOperation{
				Rebalancing: {
					NewFakeRedisOperationRebalance(t.Context(), redisCluster, "Running", time.Time{}),
				},
				Resharding: {
					NewFakeRedisOperationMove(t.Context(), redisCluster, "Finished", node1, node3, 10, time.Now().Add(-time.Second * 2)),
				},
			},
			expectedOperations: map[string]int{
				Rebalancing: 1,
				Resharding:  1,
			},
		},
		{
			name: "one expired operation",
			operations: map[string][]RedisOperation{
				Rebalancing: {
					NewFakeRedisOperationRebalance(t.Context(), redisCluster, "Running", time.Time{}),
					NewFakeRedisOperationRebalance(t.Context(), redisCluster, "Finished", time.Now().Add(-time.Second * 100)),
				},
				Resharding: {
					NewFakeRedisOperationMove(t.Context(), redisCluster, "Finished", node1, node3, 10, time.Now().Add(-time.Second * 2)),
				},
			},
			expectedOperations: map[string]int{
				Rebalancing: 1,
				Resharding:  1,
			},
		},
		{
			name: "several expired operations",
			operations: map[string][]RedisOperation{
				Rebalancing: {
					NewFakeRedisOperationRebalance(t.Context(), redisCluster, "Finished", time.Now().Add(-time.Second * 1000)),
					NewFakeRedisOperationRebalance(t.Context(), redisCluster, "Running", time.Time{}),
					NewFakeRedisOperationRebalance(t.Context(), redisCluster, "Finished", time.Now().Add(-time.Second * 100)),
				},
				Resharding: {
					NewFakeRedisOperationMove(t.Context(), redisCluster, "Finished", node1, node3, 10, time.Now().Add(-time.Second * 20)),
				},
			},
			expectedOperations: map[string]int{
				Rebalancing: 1,
				Resharding:  0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdcl := NewFakeRedisCluster(
				t.Context(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Reconciler: config.RedisReconcilerConfig{
							OperationCleanupIntervalSeconds: 10,
						},
					},
				},
				"Unknown",
				map[string]*RedisNode{},
				tt.operations,
				make(chan struct{}, 5),
			)

			rdcl.RemoveOutdatedOperations()

			for operation, ops := range rdcl.operations {
				assert.Len(t, ops, tt.expectedOperations[operation], "operation %s", operation)
			}
		})
	}
}

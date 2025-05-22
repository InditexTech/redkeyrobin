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
		nodeFrom           *RedisNode
		nodeTo             *RedisNode
		expectedOperations int
		newState           string
	}{
		{
			name:               "add operation rebalancing",
			operationName:      Rebalancing,
			expectedOperations: 1,
		},
		{
			name:               "add operation resharding",
			operationName:      Resharding,
			nodeFrom:           node1,
			nodeTo:             node3,
			expectedOperations: 1,
		},
		{
			name:               "add operation resharding",
			operationName:      Resharding,
			nodeFrom:           node1,
			nodeTo:             node2,
			expectedOperations: 2,
			newState:           "Finished",
		},
		{
			name:               "add operation fixing",
			operationName:      Fixing,
			nodeFrom:           node1,
			expectedOperations: 1,
			newState:           "Finished",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := redisCluster.addOperation(tt.operationName, nil, tt.nodeFrom, tt.nodeTo)

			assert.NotNil(t, redisCluster.operations[tt.operationName])
			assert.Len(t, redisCluster.operations[tt.operationName], tt.expectedOperations)
			assert.Equal(t, operation.Name, tt.operationName)
			assert.Equal(t, operation.NodeFrom, tt.nodeFrom)
			assert.Equal(t, operation.NodeTo, tt.nodeTo)
			assert.Equal(t, operation.Status, "Running")

			if tt.newState != "" {
				operation.Status = tt.newState
				assert.Equal(t, operation.Status, tt.newState)
			}
		})
	}
}

func TestRedisClusterGetOperation(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		expectedResult  *RedisOperation
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
			expectedResult:  &RedisOperation{
				Name:   Fixing,
				Status: "Finished",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redisCluster.getOperation(tt.operationName, tt.operationStatus)

			if tt.expectedResult != nil {
				assert.NotNil(t, result)
				assert.Equal(t, result.Name, tt.operationName)
				assert.Equal(t, result.Status, tt.operationStatus)
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
			assert.Equal(t, result, tt.expectedResult)
		})
	}
}

func TestRedisClusterRemoveOutdatedOperations(t *testing.T) {
	tests := []struct {
		name               string
		operations         map[string][]*RedisOperation
		expectedOperations map[string]int
	}{
		{
			name: "no expired operations",
			operations: map[string][]*RedisOperation{
				Rebalancing: {
					{
						Name:   Rebalancing,
						Status: "Running",
					},
				},
				Resharding: {
					{
						Name:         Resharding,
						Status:       "Finished",
						EndTimestamp: time.Now().Add(-time.Second * 2),
					},
				},
			},
			expectedOperations: map[string]int{
				Rebalancing: 1,
				Resharding:  1,
			},
		},
		{
			name: "one expired operation",
			operations: map[string][]*RedisOperation{
				Rebalancing: {
					{
						Name:   Rebalancing,
						Status: "Running",
					},
					{
						Name:         Rebalancing,
						Status:       "Finished",
						EndTimestamp: time.Now().Add(-time.Second * 100),
					},
				},
				Resharding: {
					{
						Name:         Resharding,
						Status:       "Finished",
						EndTimestamp: time.Now().Add(-time.Second * 2),
					},
				},
			},
			expectedOperations: map[string]int{
				Rebalancing: 1,
				Resharding:  1,
			},
		},
		{
			name: "several expired operations",
			operations: map[string][]*RedisOperation{
				Rebalancing: {
					{
						Name:         Rebalancing,
						Status:       "Finished",
						EndTimestamp: time.Now().Add(-time.Second * 1000),
					},
					{
						Name:   Rebalancing,
						Status: "Running",
					},
					{
						Name:         Rebalancing,
						Status:       "Finished",
						EndTimestamp: time.Now().Add(-time.Second * 100),
					},
				},
				Resharding: {
					{
						Name:         Resharding,
						Status:       "Finished",
						EndTimestamp: time.Now().Add(-time.Second * 20),
					},
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
			)

			rdcl.RemoveOutdatedOperations()

			for operation, ops := range rdcl.operations {
				assert.Len(t, ops, tt.expectedOperations[operation], "operation %s", operation)
			}
		})
	}
}

func TestRedisClusterWaitForReshardToFinish(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
	}{
		{
			name:           "resharding error",
			cmd:            NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: ReshardingError,
		},
		{
			name:           "good",
			cmd:            NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := &RedisOperation{
				Name:     Resharding,
				Status:   "Running",
				NodeFrom: node1,
				NodeTo:   node3,
				Cmd:      tt.cmd,
			}
			tt.cmd.cmd.Start()
			redisCluster.waitForReshardToFinish(operation)

			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}


func TestRedisClusterWaitForRebalanceToFinish(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
	}{
		{
			name:           "rebalancing error",
			cmd:            NewRedisCLICommand(t.Context(), "exit 1"),
		},
		{
			name:           "good",
			cmd:            NewRedisCLICommand(t.Context(), "exit 0"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := &RedisOperation{
				Name:   Rebalancing,
				Status: "Running",
				Cmd:    tt.cmd,
			}

			tt.cmd.cmd.Start()
			redisCluster.waitForRebalanceToFinish(operation)
		})
	}
}

func TestRedisClusterWaitForFixToFinish(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
	}{
		{
			name:           "fix error",
			cmd:            NewRedisCLICommand(t.Context(), "exit 1"),
		},
		{
			name:           "good",
			cmd:            NewRedisCLICommand(t.Context(), "exit 0"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := &RedisOperation{
				Name:   Fixing,
				Status: "Running",
				Cmd:    tt.cmd,
			}

			tt.cmd.cmd.Start()
			redisCluster.waitForFixToFinish(operation)
		})
	}
}

func TestRedisClusterWaitForReconcileToFinish(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
	}{
		{
			name:           "reconcile error",
			cmd:            NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: ReconcilingError,
		},
		{
			name:           "good",
			cmd:            NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := &RedisOperation{
				Name:   Reconciling,
				Status: "Running",
				Cmd:    tt.cmd,
			}

			tt.cmd.cmd.Start()
			redisCluster.waitForReconcileToFinish(operation)
			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

func TestRedisClusterWaitForScaleUpeToFinish(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
	}{
		{
			name:           "scale up error",
			cmd:            NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: ScalingUpError,
		},
		{
			name:           "good",
			cmd:            NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := &RedisOperation{
				Name:   ScalingDown,
				Status: "Running",
				Cmd:    tt.cmd,
			}

			tt.cmd.cmd.Start()
			redisCluster.waitForScaleUpToFinish(operation)
			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

func TestRedisClusterWaitForScaleDownToFinish(t *testing.T) {
	tests := []struct {
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
	}{
		{
			name:           "scale down error",
			cmd:            NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: ScalingDownError,
		},
		{
			name:           "good",
			cmd:            NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operation := &RedisOperation{
				Name:   ScalingDown,
				Status: "Running",
				Cmd:    tt.cmd,
			}

			tt.cmd.cmd.Start()
			redisCluster.waitForScaleDownToFinish(operation)
			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}
// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/stretchr/testify/assert"
)

var node1 = &RedisNode{
	Name: "node1",
	ID: "1234567890",
	Addr: "node1",
	IP: "1.1.1.1",
	Role: "master",
	Slots: []RedisSlotRange{
			{
				Start: 0,
				End: 4,
			},
		},
	MasterID: "",
}
var node2 = &RedisNode{
	Name: "node2",
	ID: "0987654321",
	Addr: "node2",
	IP: "2.2.2.2",
	Role: "master",
	Slots: []RedisSlotRange{
			{
				Start: 5,
				End: 7,
			},
		},
	MasterID: "",
}
var node3 = &RedisNode{
	Name: "node3",
	ID: "0987654321",
	Addr: "node2",
	IP: "2.2.2.2",
	Role: "master",
	Slots: []RedisSlotRange{
			{
				Start: 7,
				End: 10,
			},
		},
	MasterID: "",
}

var redisCluster = NewFakeRedisCluster(
	&config.Configuration{
		Redis: config.RedisConfig{
			Cluster: config.RedisClusterConfig{
				Status: "Ready",
				Replicas: 3,
				Name: "test",
				Namespace: "test",
				MaxRetries: 1,
				BackOff: time.Microsecond * 10,
				HealingTimeSeconds: 55,
				HealthProbePeriodSeconds: 40,
			},
			Reconciler: config.RedisReconcilerConfig{
				IntervalSeconds: 10,
			},
			Metrics: config.RedisMetricsConfig{
				IntervalSeconds: 110,
				RedisInfoKeys: []string{"test"},
			},
		},
		Metadata: map[string]string{
			"test": "test",
		},
	},
	"Unknown",
	map[string]*RedisNode{
		"test-0": node1,
		"test-1": node2,
		"test-2": node3,
	},
	map[string][]*RedisOperation{},
	)

func TestRedisClusterGetters(t *testing.T) {
	assert.Equal(t, redisCluster.GetRedisClusterStatus(), "Ready")
	assert.Equal(t, redisCluster.GetStatus(), "Unknown")
	assert.Equal(t, redisCluster.GetReplicas(), 3)
	assert.Equal(t, redisCluster.GetName(), "test")
	assert.Equal(t, redisCluster.GetNamespace(), "test")
	assert.Equal(t, redisCluster.GetAddress(), "test")
	assert.Equal(t, redisCluster.GetReconcilerInterval(), 10)
	assert.Equal(t, redisCluster.GetClusterMaxRetries(), 1)
	assert.Equal(t, redisCluster.GetClusterBackOff(), time.Microsecond * 10)
	assert.Equal(t, redisCluster.GetClusterHealingTime(), 55)
	assert.Equal(t, redisCluster.GetClusterHealthProbePeriod(), 40)
	assert.Equal(t, redisCluster.GetMetricsRedisInfoKeys(), []string{"test"})
	assert.Equal(t, redisCluster.GetMetricsInterval(), 110)
	assert.Equal(t, redisCluster.GetMetadata(), map[string]string{"test": "test"})
	assert.Equal(t, redisCluster.GetNode("test-0"), node1)
	assert.Nil(t, redisCluster.GetNode("node4"))

	nodes := redisCluster.GetNodes()
	assert.Len(t, nodes, 3)
	assert.Contains(t, nodes, node1)
	assert.Contains(t, nodes, node2)
	assert.Contains(t, nodes, node3)
}

func TestRedisClusterAddOperation(t *testing.T) {
	tests := []struct {
		name               string
		operationName	  string
		nodeFrom		  *RedisNode
		nodeTo			  *RedisNode
		expectedOperations int
		newState			  string
	}{
		{
			name: "add operation rebalancing",
			operationName: Rebalancing,
			expectedOperations: 1,
		},
		{
			name: "add operation resharding",
			operationName: Resharding,
			nodeFrom: node1,
			nodeTo: node3,
			expectedOperations: 1,
		},
		{
			name: "add operation resharding",
			operationName: Resharding,
			nodeFrom: node1,
			nodeTo: node2,
			expectedOperations: 2,
			newState: "Finished",
		},
		{
			name: "add operation forgetting",
			operationName: Forgetting,
			nodeFrom: node1,
			expectedOperations: 1,
			newState: "Finished",
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

func TestRedisClusterHasOperation(t *testing.T) {
	tests := []struct {
		name               string
		operationName	  string
		operationStatus	  string
		expectedResult bool
	}{
		{
			name: "operation not found",
			operationName: Fixing,
			expectedResult: false,
		},
		{
			name: "no operation with status",
			operationName: Forgetting,
			operationStatus: "Error",
			expectedResult: false,
		},
		{
			name: "operation with status",
			operationName: Forgetting,
			operationStatus: "Finished",
			expectedResult: true,
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
		name               string
		operationName	  string
		operationStatus	  string
		node 			 RedisNode
		expectedResult bool
	}{
		{
			name: "operation not found",
			operationName: Fixing,
			expectedResult: false,
		},
		{
			name: "no operation in node",
			operationName: Rebalancing,
			operationStatus: "Running",
			node: *node1,
			expectedResult: false,
		},
		{
			name: "operation in node",
			operationName: Forgetting,
			operationStatus: "Finished",
			node: *node1,
			expectedResult: true,
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
		name               string
		operationName	  string
		operationStatus	  string
		nodeFrom 			 RedisNode
		nodeTo 			 RedisNode
		expectedResult bool
	}{
		{
			name: "operation not found",
			operationName: Fixing,
			expectedResult: false,
		},
		{
			name: "no operation in nodes",
			operationName: Rebalancing,
			operationStatus: "Running",
			nodeFrom: *node1,
			nodeTo: *node2,
			expectedResult: false,
		},
		{
			name: "operation in nodes, bad status",
			operationName: Resharding,
			operationStatus: "Running",
			nodeFrom: *node1,
			nodeTo: *node2,
			expectedResult: false,
		},
		{
			name: "operation in nodes",
			operationName: Resharding,
			operationStatus: "Running",
			nodeFrom: *node1,
			nodeTo: *node3,
			expectedResult: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redisCluster.hasOperationBetweenNodes(tt.operationName, tt.operationStatus, tt.nodeFrom, tt.nodeTo)
			assert.Equal(t, result, tt.expectedResult)
		})
	}
}

func TestRedisClusterCheckOperations(t *testing.T) {
	assert.True(t, redisCluster.IsRebalancing())
	assert.False(t, redisCluster.HasBeenRebalanced())

	assert.False(t, redisCluster.IsResharding(*node1, *node2))
	assert.True(t, redisCluster.IsResharding(*node1, *node3))
	assert.True(t, redisCluster.HasBeenResharded(*node1, *node2))
	assert.False(t, redisCluster.HasBeenResharded(*node1, *node3))
}

func TestRedisClusterAddNode(t *testing.T) {
	tests := []struct {
		name               string
		nodeName	  string
		nodeAddr	  string
		expectedNodes int
	}{
		{
			name: "add node",
			nodeName: "node4",
			nodeAddr: "node4",
			expectedNodes: 4,
		},
		{
			name: "add existing node",
			nodeName: "node4",
			nodeAddr: "node4",
			expectedNodes: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := redisCluster.addNode(tt.nodeName, tt.nodeAddr)
			
			assert.NotNil(t, node)
			assert.Equal(t, node.Name, tt.nodeName)
			assert.Equal(t, node.Addr, tt.nodeAddr)
			assert.Len(t, redisCluster.nodes, tt.expectedNodes)
		})
	}
}

func TestRedisClusterRemoveNode(t *testing.T) {
	tests := []struct {
		name               string
		nodeName	  string
		expectedNodes int
	}{
		{
			name: "remove node",
			nodeName: "node4",
			expectedNodes: 3,
		},
		{
			name: "remove non-existing node",
			nodeName: "node4",
			expectedNodes: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redisCluster.removeNode(tt.nodeName)
			assert.Len(t, redisCluster.nodes, tt.expectedNodes)
		})
	}
}


func TestRedisClusterGetNodeFromID(t *testing.T) {
	tests := []struct {
		name               string
		nodeID	  string
		expectedNode *RedisNode
	}{
		{
			name: "node not found",
			nodeID: "55555555555",
			expectedNode: nil,
		},
		{
			name: "node found",
			nodeID: "1234567890",
			expectedNode: node1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := redisCluster.getNodeFromID(tt.nodeID)
			assert.Equal(t, node, tt.expectedNode)
		})
	}
}

func TestRedisClusterUpdateNodesInfo(t *testing.T) {
	tests := []struct {
		name               string
		nodesInfo 		[]RedisNode
	}{
		{
			name: "update nodes info",
			nodesInfo: []RedisNode{
				{
					Name: "node1",
					ID: "1234567890",
					Addr: "node1",
					IP: "9.9.9.9",
					Role: "master",
				},
				{
					Name: "node2",
					ID: "6666666666",
					Addr: "node2",
					IP: "change not affecting",
					Role: "slave",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redisCluster.updateNodesInfo(tt.nodesInfo)
		})
	}
}

func TestRedisClusterGetAndCheckRedisClient(t *testing.T) {
	tests := []struct {
		name               string
		close 			  bool
		expectedError 	error
		expectedClient 	*RedisClient
		expectedCtx 		context.Context
	}{
		{
			name: "get bad redis client",
			close: true,
			expectedError: fmt.Errorf("failed to connect after 1 retries"),
			expectedClient: nil,
			expectedCtx: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, client, err := redisCluster.getAndCheckRedisClient(tt.close)
			
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Nil(t, client)
				assert.Nil(t, ctx)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, client, tt.expectedClient)
				assert.Equal(t, ctx, tt.expectedCtx)
			}
		})
	}
}

func TestRedisClusterRefreshNodes(t *testing.T) {
	tests := []struct {
		name               string
		expectedError 	error
	}{
		{
			name: "get bad redis client",
			expectedError: fmt.Errorf("failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redisCluster.RefreshNodes()
			
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterInit(t *testing.T) {
	tests := []struct {
		name               string
		expectedError 	error
	}{
		{
			name: "get bad redis client",
			expectedError: fmt.Errorf("Error refreshing nodes info: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redisCluster.Init()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterSetReplicas(t *testing.T) {
	tests := []struct {
		name               string
		replicas int
		expectedError 	error
	}{
		{
			name: "same replicas",
			replicas: 3,
			expectedError: &OperationCompletedError{Operation: "SetReplicas"},
		},
		{
			name: "less replicas",
			replicas: 2,
			expectedError: nil,
		},
		{
			name: "more replicas",
			replicas: 4,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redisCluster.SetReplicas(tt.replicas)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, redisCluster.GetReplicas(), tt.replicas)
			assert.Len(t, redisCluster.nodes, tt.replicas)
		})
	}
}

func TestRedisClusterSetRedisClusterStatus(t *testing.T) {
	tests := []struct {
		name               string
		status string
		expectedError 	error
	}{
		{
			name: "good status",
			status: "Ready",
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redisCluster.SetRedisClusterStatus(tt.status)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}


func TestRedisClusterRebalance(t *testing.T) {
	tests := []struct {
		name               string
		prepareTest 	func()
		expectedError 	error
	}{
		{
			name: "rebalancing",
			prepareTest: func() {
				redisCluster.operations[Rebalancing] = []*RedisOperation{
					{
						Name: Rebalancing,
						Status: "Running",
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "Rebalance"},
		},
		{
			name: "rebalanced",
			prepareTest: func() {
				redisCluster.operations[Rebalancing] = []*RedisOperation{
					{
						Name: Rebalancing,
						Status: "Finished",
					},
				}
			},
			expectedError: &OperationCompletedError{Operation: "Rebalance"},
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redisCluster.operations[Rebalancing] = []*RedisOperation{}
			},
			expectedError: fmt.Errorf("error getting and checking Redis client: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.Rebalance(true)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterWaitForRebalanceToFinish(t *testing.T) {
	tests := []struct {
		name               string
		cmd *RedisCLICommand
		expectedStatus string
	}{
		{
			name: "rebalancing error",
			cmd: NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: RebalancingError,
		},
		{
			name: "good",
			cmd: NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cmd.cmd.Start()
			redisCluster.waitForRebalanceToFinish(tt.cmd)

			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

func TestRedisClusterMoveSlots(t *testing.T) {
	tests := []struct {
		name               string
		prepareTest 	func()
		expectedError 	error
	}{
		{
			name: "moving",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{
					{
						Name: Resharding,
						Status: "Running",
						NodeFrom: node1,
						NodeTo: node3,
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "Resharding"},
		},
		{
			name: "moved",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{
					{
						Name: Rebalancing,
						Status: "Finished",
						NodeFrom: node1,
						NodeTo: node3,
					},
				}
			},
			expectedError: &OperationCompletedError{Operation: "Resharding"},
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{}
			},
			expectedError: fmt.Errorf("error getting and checking Redis client: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.MoveSlots(node1, node3, 10)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterWaitForReshardToFinish(t *testing.T) {
	tests := []struct {
		name               string
		cmd *RedisCLICommand
		expectedStatus string
	}{
		{
			name: "resharding error",
			cmd: NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: ReshardingError,
		},
		{
			name: "good",
			cmd: NewRedisCLICommand(t.Context(), "exit 0"),
			expectedStatus: Ready,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cmd.cmd.Start()
			redisCluster.waitForReshardToFinish(tt.cmd, node1, node3)

			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

func TestRedisClusterCheck(t *testing.T) {
	tests := []struct {
		name               string
	
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			
		})
	}
}


func TestRedisClusterFix(t *testing.T) {
	tests := []struct {
		name               string
	
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			
		})
	}
}

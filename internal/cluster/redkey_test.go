// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/stretchr/testify/assert"
)

var mockClientFactory = func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
	client := redis.MockRedisClient{}
	return client, nil
}

var mockClientFactoryError = func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
	return nil, fmt.Errorf("error creating client")
}

// Helper functions for testing
func createNodeWithSlots(name, id string, slots []redis.RedisSlotRange) *redis.RedisNode {
	node := redis.NewFakeRedisNode(name, mockClientFactory)
	node.ID = id
	node.Flags = "master"
	node.Slots = slots
	return node
}

func createNodeWithoutSlots(name, id string) *redis.RedisNode {
	node := redis.NewFakeRedisNode(name, mockClientFactory)
	node.ID = id
	node.Flags = "master"
	node.Slots = []redis.RedisSlotRange{}
	return node
}

var node1 = redis.NewFakeRedisNode("test-0", mockClientFactoryError)
var node2 = redis.NewFakeRedisNode("node2", mockClientFactory)
var node3 = redis.NewFakeRedisNode("node3", mockClientFactory)

// Create mock client factory that fails on refreshNodes (getClient call)
var errRefresh = fmt.Errorf("failed to connect for refresh")
var mockClientFactoryRefreshError = func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
	return nil, errRefresh
}

func init() {
	// Configure node1 properties
	node1.Addr = "node1"
	node1.MaxRetries = 1
	node1.Backoff = time.Microsecond * 10
	node1.ID = "1234567890"
	node1.IP = "1.1.1.1"
	node1.Flags = "master"
	node1.Slots = []redis.RedisSlotRange{
		{
			Start: 1,
			End:   5461,
		},
	}
	node1.PrimaryID = ""

	// Configure node2 properties
	node2.Addr = "node2"
	node2.MaxRetries = 1
	node2.Backoff = time.Microsecond * 10
	node2.ID = "0987654321"
	node2.IP = "2.2.2.2"
	node2.Flags = "master, addr"
	node2.Slots = []redis.RedisSlotRange{
		{
			Start: 5462,
			End:   10922,
		},
	}
	node2.PrimaryID = ""

	// Configure node3 properties
	node3.Addr = "node2"
	node3.MaxRetries = 1
	node3.Backoff = time.Microsecond * 10
	node3.ID = "0987654321"
	node3.IP = "2.2.2.2"
	node3.Flags = "slave"
	node3.Slots = []redis.RedisSlotRange{
		{
			Start: 10923,
			End:   16384,
		},
	}
	node3.PrimaryID = "1234567890"
}

var redkeyCluster = NewFakeRedKeyCluster(
	context.TODO(),
	&config.Configuration{
		Redis: config.RedisConfig{
			Cluster: config.RedKeyClusterConfig{
				Status:                   "Ready",
				Primaries:                3,
				Name:                     "test",
				Namespace:                "test",
				MaxRetries:               1,
				BackOff:                  time.Microsecond * 10,
				HealingTimeSeconds:       55,
				HealthProbePeriodSeconds: 40,
			},
			Reconciler: config.RedisReconcilerConfig{
				IntervalSeconds: 10,
			},
			Metrics: config.RedisMetricsConfig{
				IntervalSeconds: 110,
				RedisInfoKeys:   []string{"test"},
			},
		},
		Metadata: map[string]string{
			"test": "test",
		},
	},
	"Unknown",
	map[string]*redis.RedisNode{
		"test-0": node1,
		"test-1": node2,
		"test-2": node3,
	},
	map[string][]RedisOperation{},
	make(chan struct{}, 5),
)

func getIntPointer(val int) *int {
	return &val
}

func TestRedKeyClusterAddOperation(t *testing.T) {
	tests := []struct {
		name               string
		operationName      string
		operation          RedisOperation
		expectedOperations int
	}{
		{
			name:               "add operation rebalancing",
			operationName:      Rebalancing,
			operation:          NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Running", time.Time{}),
			expectedOperations: 1,
		},
		{
			name:               "add operation resharding",
			operationName:      Resharding,
			operation:          NewFakeRedisOperationMove(t.Context(), redkeyCluster, "Running", node1, node3, 10, time.Time{}),
			expectedOperations: 1,
		},
		{
			name:               "add operation resharding",
			operationName:      Resharding,
			operation:          NewFakeRedisOperationMove(t.Context(), redkeyCluster, "Finished", node1, node2, 10, time.Time{}),
			expectedOperations: 2,
		},
		{
			name:               "add operation fixing",
			operationName:      Fixing,
			operation:          NewFakeRedisOperationFix(t.Context(), redkeyCluster, "Finished"),
			expectedOperations: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redkeyCluster.addOperation(tt.operationName, tt.operation)

			assert.NotNil(t, redkeyCluster.operations[tt.operationName])
			assert.Len(t, redkeyCluster.operations[tt.operationName], tt.expectedOperations)
		})
	}
}

func TestRedKeyClusterGetOperation(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		expectedResult  RedisOperation
	}{
		{
			name:           "no operations",
			operationName:  "AntiOperation",
			expectedResult: nil,
		},
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
			expectedResult:  NewFakeRedisOperationFix(t.Context(), redkeyCluster, "Finished"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redkeyCluster.getOperation(tt.operationName, tt.operationStatus)

			if tt.expectedResult != nil {
				assert.NotNil(t, result)
				assert.Equal(t, result.GetStatus(), tt.operationStatus)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}

func TestRedKeyClusterHasOperation(t *testing.T) {
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
			result := redkeyCluster.hasOperation(tt.operationName, tt.operationStatus)
			assert.Equal(t, result, tt.expectedResult)
		})
	}
}

func TestRedKeyClusterHasOperationInNode(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		node            redis.RedisNode
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
			result := redkeyCluster.hasOperationInNode(tt.operationName, tt.operationStatus, tt.node)
			assert.Equal(t, result, tt.expectedResult)
		})
	}
}

func TestRedKeyClusterHasOperationBetweenNodes(t *testing.T) {
	tests := []struct {
		name            string
		operationName   string
		operationStatus string
		nodeFrom        redis.RedisNode
		nodeTo          redis.RedisNode
		expectedResult  bool
	}{
		{
			name:           "operation not found",
			operationName:  Fixing,
			expectedResult: false,
		},
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
			result := redkeyCluster.hasOperationBetweenNodes(tt.operationName, tt.operationStatus, tt.nodeFrom, tt.nodeTo)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestRedKeyClusterDoRemoveOutdatedOperations(t *testing.T) {
	tests := []struct {
		name               string
		operations         map[string][]RedisOperation
		expectedOperations map[string]int
	}{
		{
			name: "no expired operations",
			operations: map[string][]RedisOperation{
				Rebalancing: {
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Running", time.Time{}),
				},
				Resharding: {
					NewFakeRedisOperationMove(t.Context(), redkeyCluster, "Finished", node1, node3, 10, time.Now().Add(-time.Second*2)),
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
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Running", time.Time{}),
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Finished", time.Now().Add(-time.Second*100)),
				},
				Resharding: {
					NewFakeRedisOperationMove(t.Context(), redkeyCluster, "Finished", node1, node3, 10, time.Now().Add(-time.Second*2)),
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
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Finished", time.Now().Add(-time.Second*1000)),
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Running", time.Time{}),
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Finished", time.Now().Add(-time.Second*100)),
				},
				Resharding: {
					NewFakeRedisOperationMove(t.Context(), redkeyCluster, "Finished", node1, node3, 10, time.Now().Add(-time.Second*20)),
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
			rdcl := NewFakeRedKeyCluster(
				t.Context(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Reconciler: config.RedisReconcilerConfig{
							OperationCleanupIntervalSeconds: 10,
						},
					},
				},
				"Unknown",
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 5),
			)

			rdcl.doRemoveOutdatedNodes()

			for operation, ops := range rdcl.operations {
				assert.Len(t, ops, tt.expectedOperations[operation], "operation %s", operation)
			}
		})
	}
}

func TestRedKeyClusterGetters(t *testing.T) {
	assert.Equal(t, redkeyCluster.GetRedKeyClusterStatus(), "Ready")
	assert.Equal(t, redkeyCluster.GetPrimaries(), 3)
	assert.Equal(t, redkeyCluster.GetReplicasPerPrimary(), 0)
	assert.Equal(t, redkeyCluster.GetName(), "test")
	assert.Equal(t, redkeyCluster.GetNamespace(), "test")
	assert.Equal(t, redkeyCluster.GetAddress(), "test")
	assert.Equal(t, redkeyCluster.IsEphemeral(), false)
	assert.Equal(t, redkeyCluster.GetReconcilerInterval(), 10)
	assert.Equal(t, redkeyCluster.GetReconcilerOperationCleanupInterval(), 0)
	assert.Equal(t, redkeyCluster.GetClusterMaxRetries(), 1)
	assert.Equal(t, redkeyCluster.GetClusterBackOff(), time.Microsecond*10)
	assert.Equal(t, redkeyCluster.GetClusterHealingTime(), 55)
	assert.Equal(t, redkeyCluster.GetClusterHealthProbePeriod(), 40)
	assert.Equal(t, redkeyCluster.GetMetricsRedisInfoKeys(), []string{"test"})
	assert.Equal(t, redkeyCluster.GetMetricsInterval(), 110)
	assert.Equal(t, redkeyCluster.GetMetadata(), map[string]string{"test": "test"})
	assert.Equal(t, redkeyCluster.GetNode("0"), node1)
	assert.Nil(t, redkeyCluster.GetNode("node4"))

	nodes := redkeyCluster.GetNodes()
	assert.Len(t, nodes, 3)
	assert.Contains(t, nodes, node1)
	assert.Contains(t, nodes, node2)
	assert.Contains(t, nodes, node3)

	primaryNodes := redkeyCluster.GetPrimaryNodes()
	assert.Len(t, primaryNodes, 2)
	assert.Contains(t, primaryNodes, node1)
	assert.Contains(t, primaryNodes, node2)

	replicaNodes := redkeyCluster.GetReplicaNodes()
	assert.Len(t, replicaNodes, 1)
	assert.Contains(t, replicaNodes, node3)

	replicasOfPrimary := redkeyCluster.GetReplicasOfNode(node1)
	assert.Len(t, replicasOfPrimary, 1)
	assert.Contains(t, replicasOfPrimary, node3)
	replicasOfPrimary = redkeyCluster.GetReplicasOfNode(node2)
	assert.Len(t, replicasOfPrimary, 0)
	replicasOfPrimary = redkeyCluster.GetReplicasOfNode(node3)
	assert.Len(t, replicasOfPrimary, 0)
}

func TestRedKeyClusterGetNodeFromID(t *testing.T) {
	tests := []struct {
		name         string
		nodeID       string
		expectedNode *redis.RedisNode
	}{
		{
			name:         "node not found",
			nodeID:       "55555555555",
			expectedNode: nil,
		},
		{
			name:         "node found",
			nodeID:       "1234567890",
			expectedNode: node1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := redkeyCluster.GetNodeFromID(tt.nodeID)
			assert.Equal(t, node, tt.expectedNode)
		})
	}
}

func TestRedKeyClusterAskers(t *testing.T) {
	assert.True(t, redkeyCluster.IsRebalancing())

	assert.False(t, redkeyCluster.IsReshardingNodes(*node1, *node2))
	assert.True(t, redkeyCluster.IsReshardingNodes(*node1, *node3))
	assert.True(t, redkeyCluster.IsResharding())

	assert.False(t, redkeyCluster.IsFixing())
	assert.False(t, redkeyCluster.IsCheckingIntegrity())
	assert.False(t, redkeyCluster.IsScalingUp())
	assert.False(t, redkeyCluster.IsScalingDown())
	assert.False(t, redkeyCluster.IsUpgrading())

	assert.False(t, redkeyCluster.IsResettingNode(*node1))
	assert.False(t, redkeyCluster.IsResetting())

	assert.False(t, redkeyCluster.IsScaled())
	assert.False(t, redkeyCluster.IsUpgraded())
	assert.False(t, redkeyCluster.CanBeUpgraded())
	assert.False(t, redkeyCluster.CanBeChecked())

	assert.False(t, redkeyCluster.HasBeenRebalanced())
	assert.False(t, redkeyCluster.HasMissingSlots())
	assert.False(t, redkeyCluster.HasDesiredReplicas())

	assert.True(t, redkeyCluster.HasBeenResharded(*node1, *node2))
	assert.False(t, redkeyCluster.HasBeenResharded(*node1, *node3))

	assert.True(t, redkeyCluster.HasNode("test-0"))
	assert.False(t, redkeyCluster.HasNode("notfound"))

	assert.True(t, redkeyCluster.NodeHasReplicas(node1))
	assert.False(t, redkeyCluster.NodeHasReplicas(node2))
	assert.False(t, redkeyCluster.NodeHasReplicas(node3))

	assert.False(t, redkeyCluster.needsUpscale())
	assert.False(t, redkeyCluster.needsDownscale())
}

func TestRedKeyClusterIsBalanced(t *testing.T) {
	tests := []struct {
		name     string
		nodes    map[string]*redis.RedisNode
		expected bool
	}{
		{
			name: "node without slots",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "id1"),
			},
			expected: false,
		},
		{
			name: "node with more slots",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "id1", []redis.RedisSlotRange{
					{Start: 0, End: 5000},
					{Start: 10000, End: 16384},
				}),
			},
			expected: false,
		},
		{
			name:     "no nodes",
			nodes:    map[string]*redis.RedisNode{},
			expected: true,
		},
		{
			name: "balanced nodes",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "id1", []redis.RedisSlotRange{
					{Start: 0, End: 5460},
				}),
				"test-cluster-1": createNodeWithSlots("test-cluster-1", "id2", []redis.RedisSlotRange{
					{Start: 5461, End: 10922},
				}),
				"test-cluster-2": createNodeWithSlots("test-cluster-2", "id3", []redis.RedisSlotRange{
					{Start: 10923, End: 16384},
				}),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			)

			ret := cluster.IsBalanced()
			assert.Equal(t, tt.expected, ret)
		})
	}
}

func TestRedKeyClusterAddNode(t *testing.T) {
	tests := []struct {
		name          string
		nodeName      string
		nodeAddr      string
		expectedNodes int
	}{
		{
			name:          "add node",
			nodeName:      "node4",
			nodeAddr:      "node4",
			expectedNodes: 4,
		},
		{
			name:          "add existing node",
			nodeName:      "node4",
			nodeAddr:      "node4",
			expectedNodes: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := redkeyCluster.addNode(tt.nodeName, tt.nodeAddr)

			assert.NotNil(t, node)
			assert.Equal(t, node.Name, tt.nodeName)
			assert.Equal(t, node.Addr, tt.nodeAddr)
			assert.Len(t, redkeyCluster.nodes, tt.expectedNodes)
		})
	}
}

func TestRedKeyClusterRemoveNode(t *testing.T) {
	tests := []struct {
		name          string
		nodeName      string
		expectedNodes int
		expectedError error
		node          *redis.RedisNode
	}{
		{
			name:          "remove node",
			nodeName:      "node4",
			expectedNodes: 3,
			node:          redis.NewFakeRedisNode("node4", mockClientFactory),
		},
		{
			name:          "remove non-existing node",
			nodeName:      "node4",
			expectedNodes: 3,
			expectedError: fmt.Errorf("node node4 not found"),
			node:          redis.NewFakeRedisNode("node4", mockClientFactory),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.removeNode(t.Context(), *tt.node)
			assert.Len(t, redkeyCluster.nodes, tt.expectedNodes)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterForgetNode(t *testing.T) {
	tests := []struct {
		name          string
		node          *redis.RedisNode
		expectedError error
	}{
		{
			name:          "node does not exist",
			node:          redis.NewFakeRedisNode("node4", mockClientFactory),
			expectedError: fmt.Errorf("node node4 not found"),
		},
		{
			name:          "get bad redis client",
			node:          redis.NewFakeRedisNode("test-1", mockClientFactoryError),
			expectedError: fmt.Errorf("error forgetting node"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.forgetNode(t.Context(), *tt.node)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterRefreshNodes(t *testing.T) {
	tests := []struct {
		name          string
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name:          "get bad redis client",
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("error creating client"),
		},
		{
			name: "error getting nodes info",
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				client := redis.MockRedisClient{}
				client.GetNodesInfoError = fmt.Errorf("error getting nodes info")
				return client, nil
			},
			expectedError: fmt.Errorf("error getting nodes info"),
		},
		{
			name:          "success",
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.WithClientFactory(tt.clientFactory).refreshNodesInfo()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterCheckNodes(t *testing.T) {
	tests := []struct {
		name          string
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name:          "get bad redis client",
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("error creating client"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.WithClientFactory(tt.clientFactory).checkNodes(true)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterUpdateNodesInfo(t *testing.T) {
	tests := []struct {
		name      string
		nodesInfo []redis.RedisNode
	}{
		{
			name: "update nodes info",
			nodesInfo: []redis.RedisNode{
				func() redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.Addr = "node1"
					node.MaxRetries = 1
					node.Backoff = time.Microsecond * 10
					node.ID = "1234567890"
					node.IP = "9.9.9.9"
					node.Flags = "master"
					return *node
				}(),
				func() redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.Addr = "node2"
					node.MaxRetries = 1
					node.Backoff = time.Microsecond * 10
					node.ID = "6666666666"
					node.IP = "change not affecting"
					node.Flags = "slave"
					return *node
				}(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redkeyCluster.updateNodesInfo(tt.nodesInfo)
		})
	}
}

func TestRedKeyClusterNeedsMeet(t *testing.T) {
	tests := []struct {
		name           string
		clusterNodes   map[string]*redis.RedisNode
		mockNodesInfo  map[string][]redis.RedisNode // Mocked cluster nodes response for each node
		mockErrors     map[string]error             // Mocked errors for GetNodesInfo calls
		expectedResult bool
		expectedError  error
	}{
		{
			name: "error getting cluster nodes",
			clusterNodes: map[string]*redis.RedisNode{
				"node1": {
					Name: "node1",
					ID:   "id1",
					IP:   "1.1.1.1",
					Addr: "node1:6379",
				},
			},
			mockErrors: map[string]error{
				"node1": fmt.Errorf("connection error"),
			},
			expectedResult: false,
			expectedError:  fmt.Errorf("connection error"),
		},
		{
			name: "unknown IP",
			clusterNodes: map[string]*redis.RedisNode{
				"node1": {
					Name: "node1",
					ID:   "id1",
					IP:   "1.1.1.1",
					Addr: "node1:6379",
				},
				"node2": {
					Name: "node2",
					ID:   "id2",
					IP:   "2.2.2.2",
					Addr: "node2:6379",
				},
			},
			mockNodesInfo: map[string][]redis.RedisNode{
				"node1": {
					{ID: "id1", IP: "1.1.1.1"},
					{ID: "id2", IP: "2.2.2.2"},
					{ID: "id3", IP: "3.3.3.3"}, // Unknown IP
				},
				"node2": {
					{ID: "id1", IP: "1.1.1.1"},
					{ID: "id2", IP: "2.2.2.2"},
				},
			},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name: "fewer peers than expected",
			clusterNodes: map[string]*redis.RedisNode{
				"node1": {
					Name: "node1",
					ID:   "id1",
					IP:   "1.1.1.1",
					Addr: "node1:6379",
				},
				"node2": {
					Name: "node2",
					ID:   "id2",
					IP:   "2.2.2.2",
					Addr: "node2:6379",
				},
				"node3": {
					Name: "node3",
					ID:   "id3",
					IP:   "3.3.3.3",
					Addr: "node3:6379",
				},
			},
			mockNodesInfo: map[string][]redis.RedisNode{
				"node1": {
					{ID: "id1", IP: "1.1.1.1"},
					// Missing node2 and node3 - only knows itself
				},
				"node2": {
					{ID: "id1", IP: "1.1.1.1"},
					{ID: "id2", IP: "2.2.2.2"},
					{ID: "id3", IP: "3.3.3.3"},
				},
				"node3": {
					{ID: "id1", IP: "1.1.1.1"},
					{ID: "id2", IP: "2.2.2.2"},
					{ID: "id3", IP: "3.3.3.3"},
				},
			},
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name: "good",
			clusterNodes: map[string]*redis.RedisNode{
				"node1": {
					Name: "node1",
					ID:   "id1",
					IP:   "1.1.1.1",
					Addr: "node1:6379",
				},
				"node2": {
					Name: "node2",
					ID:   "id2",
					IP:   "2.2.2.2",
					Addr: "node2:6379",
				},
			},
			mockNodesInfo: map[string][]redis.RedisNode{
				"node1": {
					{ID: "id1", IP: "1.1.1.1"},
					{ID: "id2", IP: "2.2.2.2"},
				},
				"node2": {
					{ID: "id1", IP: "1.1.1.1"},
					{ID: "id2", IP: "2.2.2.2"},
				},
			},
			expectedResult: false,
			expectedError:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock client factory for each node
			clientFactories := make(map[string]func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error))

			for nodeName := range tt.clusterNodes {
				nodeName := nodeName // Capture for closure
				clientFactories[nodeName] = func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
					mockClient := &redis.MockRedisClient{}

					// Set up mock response for GetNodesInfo
					if mockError, hasError := tt.mockErrors[nodeName]; hasError {
						mockClient.GetNodesInfoError = mockError
					} else if mockNodes, hasNodes := tt.mockNodesInfo[nodeName]; hasNodes {
						mockClient.MockNodesInfo = mockNodes
					}

					return mockClient, nil
				}
			}

			// Set up nodes with their mock client factories
			nodes := make(map[string]*redis.RedisNode)
			for nodeName, nodeData := range tt.clusterNodes {
				node := redis.NewFakeRedisNode(nodeData.Name, clientFactories[nodeName])
				node.ID = nodeData.ID
				node.IP = nodeData.IP
				node.Addr = nodeData.Addr
				nodes[nodeName] = node
			}

			// Create test cluster
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{},
				"Ready",
				nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			)

			// Call the method under test
			result, err := cluster.needsMeet(context.Background())

			// Verify results
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestRedKeyClusterNeedsFix(t *testing.T) {
	// Mock client factory that returns a MockRedisClient with ClusterCheck error
	mockClientFactoryClusterCheckError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		return &redis.MockRedisClient{
			ClusterCheckError: fmt.Errorf("cluster check failed"),
		}, nil
	}

	// Mock client factory that returns a MockRedisClient with successful check but needs fix
	mockClientFactoryNeedsFix := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		return &redis.MockRedisClient{
			MockClusterCheck: &redis.ClusterCheckResult{
				CommandCodeOutput: 1, // Non-zero indicates issues
				Errors:            []string{"Some cluster error"},
				Warnings:          []string{},
			},
		}, nil
	}

	// Mock client factory that returns a MockRedisClient with successful check and no fix needed
	mockClientFactoryNoFixNeeded := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		return &redis.MockRedisClient{
			MockClusterCheck: &redis.ClusterCheckResult{
				CommandCodeOutput: 0, // Zero indicates no issues
				Errors:            []string{},
				Warnings:          []string{},
			},
		}, nil
	}

	tests := []struct {
		name           string
		cluster        *RedKeyCluster
		expectedResult bool
		expectedError  error
	}{
		{
			name: "error getting redis client",
			cluster: NewFakeRedKeyCluster(
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
					"node1": redis.NewFakeRedisNode("node1", mockClientFactoryClusterCheckError),
				},
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(mockClientFactoryError),
			expectedResult: false,
			expectedError:  fmt.Errorf("error getting and checking Redis client: error creating client"),
		},
		{
			name: "error checking cluster",
			cluster: NewFakeRedKeyCluster(
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
					"node1": redis.NewFakeRedisNode("node1", mockClientFactoryClusterCheckError),
				},
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(mockClientFactoryClusterCheckError),
			expectedResult: false,
			expectedError:  fmt.Errorf("error checking cluster: cluster check failed"),
		},
		{
			name: "command code output not zero",
			cluster: NewFakeRedKeyCluster(
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
					"node1": redis.NewFakeRedisNode("node1", mockClientFactoryNeedsFix),
				},
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(mockClientFactoryNeedsFix),
			expectedResult: true,
			expectedError:  nil,
		},
		{
			name: "command code output zero",
			cluster: NewFakeRedKeyCluster(
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
					"node1": redis.NewFakeRedisNode("node1", mockClientFactoryNoFixNeeded),
				},
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(mockClientFactoryNoFixNeeded),
			expectedResult: false,
			expectedError:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.cluster.needsFix(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestRedKeyClusterMeetNodesIfNeeded(t *testing.T) {
	tests := []struct {
		name             string
		nodes            map[string]*redis.RedisNode
		operationFactory *OperationFactory
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError    error
	}{
		{
			name: "error in needsMeet - GetClusterNodes fails",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", mockClientFactoryError),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error creating client"),
		},
		{
			name: "error in meetNodes - ClusterMeet fails",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
					client := redis.MockRedisClient{
						MockNodesInfo:    []redis.RedisNode{}, // Empty list triggers needsMeet=true
						ClusterMeetError: fmt.Errorf("meet failed"),
					}
					return client, nil
				}),
				"test-cluster-1": redis.NewFakeRedisNode("test-cluster-1", mockClientFactory), // This will fail on MeetNode
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error in ClusterMeet between 'test-cluster-0' and 'test-cluster-1': meet failed"),
		},
		{
			name: "error in meetNodes - refreshNodes fails",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
					client := redis.MockRedisClient{
						MockNodesInfo: []redis.RedisNode{}, // Empty list triggers needsMeet=true
					}
					return client, nil
				}),
				"test-cluster-1": redis.NewFakeRedisNode("test-cluster-1", mockClientFactory),
			},
			clientFactory: mockClientFactoryError, // This makes refreshNodes fail
			expectedError: fmt.Errorf("error refreshing nodes info: error creating client"),
		},
		{
			name: "success - nodes met successfully",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
					client := redis.MockRedisClient{
						MockNodesInfo: []redis.RedisNode{}, // Empty list triggers needsMeet=true
					}
					return client, nil
				}),
				"test-cluster-1": redis.NewFakeRedisNode("test-cluster-1", mockClientFactory),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.meetNodesIfNeeded(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterAsignMissingSlotsIfNeeded(t *testing.T) {
	tests := []struct {
		name             string
		nodes            map[string]*redis.RedisNode
		operationFactory *OperationFactory
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError    error
	}{
		{
			name: "no missing slots",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 8191}}),
				"test-cluster-1": createNodeWithSlots("test-cluster-1", "0000000002", []redis.RedisSlotRange{{Start: 8192, End: 16383}}),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "error in assignMissingSlots - AddSlots fails",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactoryError)
					node.ID = "0000000001"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 8191}}
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error creating client"),
		},
		{
			name: "error in assignMissingSlots - refreshNodes fails",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 8191}}),
			},
			clientFactory: mockClientFactoryError, // Esto hará que refreshNodes falle
			expectedError: fmt.Errorf("error refreshing nodes info: error creating client"),
		},
		{
			name: "success - missing slots assigned",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 8191}}),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.assignMissingSlotsIfNeeded(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterBalanceClusterIfNeeded(t *testing.T) {
	tests := []struct {
		name             string
		nodes            map[string]*redis.RedisNode
		operationFactory *OperationFactory
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError    error
	}{
		{
			name: "cluster already balanced",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 8191}}),
				"test-cluster-1": createNodeWithSlots("test-cluster-1", "0000000002", []redis.RedisSlotRange{{Start: 8192, End: 16383}}),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "cluster needs balance but no primary nodes",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": func() *redis.RedisNode {
					node := createNodeWithoutSlots("test-cluster-0", "0000000001")
					node.Flags = "slave"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil, // IsBalanced returns true when no primary nodes (division by zero case)
		},
		{
			name: "error in Rebalance operation",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 100}}),
				"test-cluster-1": createNodeWithSlots("test-cluster-1", "0000000002", []redis.RedisSlotRange{{Start: 101, End: 16383}}),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.SetRedisClientError("ClusterRebalance", fmt.Errorf("rebalance operation failed"))
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("rebalance operation failed"),
		},
		{
			name: "success - cluster rebalanced successfully",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 100}}),
				"test-cluster-1": createNodeWithSlots("test-cluster-1", "0000000002", []redis.RedisSlotRange{{Start: 101, End: 16383}}),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					return NewFakeRedisOperationRebalance(ctx, cluster, "Finished", time.Time{})
				},
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.balanceClusterIfNeeded(map[string]int{})

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterFixClusterIfNeeded(t *testing.T) {
	tests := []struct {
		name             string
		nodes            map[string]*redis.RedisNode
		operationFactory *OperationFactory
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError    error
	}{
		{
			name: "error in needsFix - error getting Redis client",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
			},
			clientFactory: mockClientFactoryError,
			expectedError: nil, // fixClusterIfNeeded returns nil when needsFix fails
		},
		{
			name: "error in needsFix - error in cluster check",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
			},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				client := redis.MockRedisClient{
					ClusterCheckError: fmt.Errorf("cluster check failed"),
				}
				return client, nil
			},
			expectedError: nil, // fixClusterIfNeeded returns nil when needsFix fails
		},
		{
			name: "no fix needed - cluster check returns code 0",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
			},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				client := redis.MockRedisClient{
					MockClusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
				}
				return client, nil
			},
			expectedError: nil,
		},
		{
			name: "error in Fix operation",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", mockClientFactoryError),
			},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				client := redis.MockRedisClient{
					MockClusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 1},
				}
				return client, nil
			},
			operationFactory: &OperationFactory{
				NewFix: func(ctx context.Context, cluster Cluster) *RedisOperationFix {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.MockRedisClient.ClusterFixError = fmt.Errorf("fix failed")
					return NewFakeRedisOperationFix(ctx, mockCluster, "Finished")
				},
			},
			expectedError: fmt.Errorf("error fixing cluster: fix failed"),
		},
		{
			name: "success - cluster fixed successfully",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", mockClientFactoryError),
			},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				client := redis.MockRedisClient{
					MockClusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 1},
				}
				return client, nil
			},
			operationFactory: &OperationFactory{
				NewFix: func(ctx context.Context, cluster Cluster) *RedisOperationFix {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationFix(ctx, mockCluster, "Finished")
				},
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.fixClusterIfNeeded(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterAddNewNodesIfNeeded(t *testing.T) {
	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		replicas      int
		expectedError error
	}{
		{
			name:     "no upscale needed",
			replicas: 2,
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
				"test-cluster-1": createNodeWithoutSlots("test-cluster-1", "0000000002"),
			},
			expectedError: nil,
		},
		{
			name:     "success",
			replicas: 2,
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:       "test-cluster",
							MaxRetries: 1,
							BackOff:    time.Microsecond * 10,
							Primaries:  tt.replicas,
						},
					},
				},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			)

			err := cluster.addNewNodesIfNeeded()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, len(cluster.nodes), tt.replicas)
			}
		})
	}
}

func TestRedKeyClusterRemoveNodesIfNeeded(t *testing.T) {
	mockClientFactoryForgetError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		redisClientMock := &redis.MockRedisClient{}
		redisClientMock.ClusterForgetError = fmt.Errorf("no forget")
		return redisClientMock, nil
	}

	tests := []struct {
		name             string
		nodes            map[string]*redis.RedisNode
		replicas         int
		operationFactory *OperationFactory
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError    error
	}{
		{
			name:     "error in getNodesToRemove",
			replicas: 1,
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
				"test-cluster-1": createNodeWithoutSlots("test-cluster-1", "0000000002"),
			},
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("error meeting nodes: error refreshing nodes info: error creating client"),
		},
		{
			name:     "error in removeSlotsFromNodes",
			replicas: 1,
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithSlots("test-cluster-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 100}}),
				"test-cluster-1": createNodeWithSlots("test-cluster-1", "0000000002", []redis.RedisSlotRange{{Start: 101, End: 200}}),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.EnsureNodesAreUpError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error ensuring nodes are up: nodes are not up"),
		},
		{
			name:     "error in forgetAndRemoveNodes",
			replicas: 1,
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", mockClientFactoryForgetError),
				"test-cluster-1": redis.NewFakeRedisNode("test-cluster-1", mockClientFactoryForgetError),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					return NewFakeRedisOperationRebalance(ctx, cluster, "Finished", time.Time{})
				},
			},
			clientFactory: mockClientFactoryForgetError,
			expectedError: fmt.Errorf("error forgetting node test-cluster-0 from node test-cluster-1: no forget"),
		},
		{
			name:     "no downscale needed",
			replicas: 3,
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
				"test-cluster-1": createNodeWithoutSlots("test-cluster-1", "0000000002"),
				"test-cluster-2": createNodeWithoutSlots("test-cluster-2", "0000000003"),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name:     "success",
			replicas: 1,
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": createNodeWithoutSlots("test-cluster-0", "0000000001"),
				"test-cluster-1": createNodeWithoutSlots("test-cluster-1", "0000000002"),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					return NewFakeRedisOperationRebalance(ctx, cluster, "Finished", time.Time{})
				},
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:       "test-cluster",
							Primaries:  tt.replicas,
							MaxRetries: 1,
							BackOff:    time.Microsecond * 10,
						},
					},
				},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.removeNodesIfNeeded(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterRemoveSlotsFromNodes(t *testing.T) {
	tests := []struct {
		name             string
		nodes            []*redis.RedisNode
		operationFactory *OperationFactory
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError    error
	}{
		{
			name:          "no nodes with slots",
			nodes:         []*redis.RedisNode{createNodeWithoutSlots("node-0", "0000000001")},
			expectedError: nil,
		},
		{
			name: "maximum rebalance retries reached",
			nodes: []*redis.RedisNode{
				createNodeWithSlots("node-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 100}}),
				createNodeWithSlots("node-1", "0000000002", []redis.RedisSlotRange{{Start: 101, End: 200}}),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.SetRedisClientError("ClusterRebalance", fmt.Errorf("ERR Please use SETSLOT only with masters"))
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			expectedError: fmt.Errorf("error rebalancing cluster: ERR Please use SETSLOT only with masters"),
		},
		{
			name:  "no error set slot",
			nodes: []*redis.RedisNode{createNodeWithSlots("node-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 100}})},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.EnsureNodesAreUpError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			expectedError: fmt.Errorf("error ensuring nodes are up: nodes are not up"),
		},
		{
			name: "refresh error",
			nodes: []*redis.RedisNode{
				createNodeWithSlots("node-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 100}}),
				createNodeWithoutSlots("node-1", "0000000002"),
				createNodeWithSlots("node-2", "0000000003", []redis.RedisSlotRange{{Start: 101, End: 200}}),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("error refreshing nodes info: error creating client"),
		},
		{
			name: "success",
			nodes: []*redis.RedisNode{
				createNodeWithSlots("node-0", "0000000001", []redis.RedisSlotRange{{Start: 0, End: 100}}),
				createNodeWithoutSlots("node-1", "0000000002"),
				createNodeWithSlots("node-2", "0000000003", []redis.RedisSlotRange{{Start: 101, End: 200}}),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.removeSlotsFromNodes(tt.nodes)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterForgetAndRemoveNodes(t *testing.T) {
	// Create mock client factories for different error scenarios
	mockClientFactoryForgetError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterForgetError: fmt.Errorf("forget error"),
		}
		return client, nil
	}

	tests := []struct {
		name          string
		nodes         []*redis.RedisNode
		clusterNodes  map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		// Flow 1: ForgetNode error - node not found (first error in loop)
		{
			name: "ForgetNodeNotFound",
			nodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("nonexistent", mockClientFactory)
					node.ID = "nonexistent-id"
					node.Flags = "master"
					return node
				}(),
			},
			clusterNodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("node nonexistent not found"),
		},
		// Flow 2: ForgetNode error - Redis ForgetNode fails (second error in forgetNode)
		{
			name: "ForgetNodeRedisError",
			nodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clusterNodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactoryForgetError)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error forgetting node node2 from node node1: forget error"),
		},
		// Flow 3: Empty nodes list - no action needed (skip loop, go to sleep, return nil)
		{
			name:          "EmptyNodesList",
			nodes:         []*redis.RedisNode{},
			clusterNodes:  map[string]*redis.RedisNode{},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		// Flow 4: RemoveNode error - node not found (error after forgetNode succeeds)
		{
			name: "RemoveNodeNotFound",
			nodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clusterNodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil, // In practice, this should pass as removeNode will find the node
		},
		// Flow 5: Single node success (forgetNode succeeds, removeNode succeeds, sleep, return nil)
		{
			name: "SingleNodeSuccess",
			nodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clusterNodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		// Flow 6: Multiple nodes success (loop multiple times, all succeed, sleep, return nil)
		{
			name: "MultipleNodesSuccess",
			nodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "replica"
					return node
				}(),
			},
			clusterNodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "replica"
					return node
				}(),
				"node3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:                       "test-cluster",
							MaxRetries:                 1,
							BackOff:                    time.Microsecond * 10,
							ClusterMeetWaitTimeSeconds: 0,
						},
					},
				},
				"Ready",
				tt.clusterNodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			err := cluster.forgetAndRemoveNodes(context.Background(), tt.nodes)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterGetNodesToRemove(t *testing.T) {
	// Create mock client factory that fails on Reset for convertNodesToPrimary
	mockClientFactoryResetError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterResetError: fmt.Errorf("reset error"),
		}
		return client, nil
	}

	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedNodes []*redis.RedisNode
		expectedError error
	}{
		{
			name: "NotEnoughNodesToRemove",
			nodes: map[string]*redis.RedisNode{
				"node0-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactory)
					node.ID = "node0-id"
					node.Flags = "master"
					return node
				}(),
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedNodes: nil,
			expectedError: fmt.Errorf("not enough nodes to remove"),
		},
		{
			name: "ConvertNodesToPrimaryError",
			nodes: map[string]*redis.RedisNode{
				"node0-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactoryResetError)
					node.ID = "node0-id"
					node.Flags = "slave"
					return node
				}(),
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
				"node3-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedNodes: nil,
			expectedError: fmt.Errorf("reset error"),
		},
		{
			name: "SuccessRemoveSingleNode",
			nodes: map[string]*redis.RedisNode{
				"node0-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactory)
					node.ID = "node0-id"
					node.Flags = "master"
					return node
				}(),
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
				"node3-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedNodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
			},
			expectedError: nil,
		},
		{
			name: "SuccessRemoveMultipleNodes",
			nodes: map[string]*redis.RedisNode{
				"node0-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactory)
					node.ID = "node0-id"
					node.Flags = "master"
					return node
				}(),
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
				"node3-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
				"node4-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-4", mockClientFactory)
					node.ID = "node4-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedNodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-4", mockClientFactory)
					node.ID = "node4-id"
					node.Flags = "master"
					return node
				}(),
			},
			expectedError: nil,
		},
		{
			name: "SuccessOrderedByOrdinal",
			nodes: map[string]*redis.RedisNode{
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				"node0-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactory)
					node.ID = "node0-id"
					node.Flags = "master"
					return node
				}(),
				"node3-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedNodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "master"
					return node
				}(),
			},
			expectedError: nil,
		},
		{
			name: "SuccessMixedNodeTypes",
			nodes: map[string]*redis.RedisNode{
				"node0-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactory)
					node.ID = "node0-id"
					node.Flags = "master"
					return node
				}(),
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
				"node3-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "slave"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedNodes: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-3", mockClientFactory)
					node.ID = "node3-id"
					node.Flags = "slave"
					return node
				}(),
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:                       "test-cluster",
							Primaries:                  3,
							ReplicasPerPrimary:         0,
							ClusterMeetWaitTimeSeconds: 0,
							NodeResetWaitTimeSeconds:   0,
							MaxRetries:                 1,
							BackOff:                    time.Microsecond * 10,
						},
					},
				},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			nodes, err := cluster.getNodesToRemove(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				assert.Len(t, nodes, len(tt.expectedNodes))
				for i, expectedNode := range tt.expectedNodes {
					assert.Equal(t, expectedNode.Name, nodes[i].Name)
					assert.Equal(t, expectedNode.ID, nodes[i].ID)
				}
			}
		})
	}
}

func TestRedKeyClusterMeetNodes(t *testing.T) {
	// Define error variables for reuse
	meetNodeError := fmt.Errorf("failed to meet node")

	// Create mock client factory that fails on MeetNode
	mockClientFactoryMeetError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterMeetError: meetNodeError,
		}
		return client, nil
	}

	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name:          "empty cluster",
			nodes:         map[string]*redis.RedisNode{},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("there are no nodes in the cluster"),
		},
		{
			name: "MeetNode fails",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactoryMeetError)
					node.ID = "id1"
					node.Flags = "master"
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "id2"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error in ClusterMeet between 'primary1' and 'primary2': %v", meetNodeError),
		},
		{
			name: "refreshNodes fails",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "id1"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactoryRefreshError,
			expectedError: fmt.Errorf("error refreshing nodes info: failed to connect for refresh"),
		},
		{
			name: "single node success",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "id1"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "multiple nodes success",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "id1"
					node.Flags = "master"
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "id2"
					node.Flags = "master"
					return node
				}(),
				"primary3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary3", mockClientFactory)
					node.ID = "id3"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:                       "test-cluster",
							MaxRetries:                 1,
							BackOff:                    time.Microsecond * 10,
							ClusterMeetWaitTimeSeconds: 0, // Set to 0 to speed up tests
						},
					},
				},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			err := cluster.meetNodes(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterRemoveOutdatedNodes(t *testing.T) {
	// Create mock client factories for different error scenarios
	mockClientFactoryGetNodesError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			GetNodesInfoError: fmt.Errorf("get nodes info error"),
		}
		return client, nil
	}

	mockClientFactoryForgetNodeError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		// Create nodes with "fail" flag to be removed
		nodeToRemove := redis.NewFakeRedisNode("outdated-node", mockClientFactory)
		nodeToRemove.ID = "outdated-id"
		nodeToRemove.Flags = "fail"

		client := redis.MockRedisClient{
			MockNodesInfo:      []redis.RedisNode{*nodeToRemove},
			ClusterForgetError: fmt.Errorf("forget node error"),
		}
		return client, nil
	}

	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name: "GetClusterNodesError",
			nodes: map[string]*redis.RedisNode{
				"node1": redis.NewFakeRedisNode("node1", mockClientFactoryGetNodesError),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error getting cluster nodes from node node1: get nodes info error"),
		},
		{
			name: "ForgetNodeError",
			nodes: map[string]*redis.RedisNode{
				"node1": redis.NewFakeRedisNode("node1", mockClientFactoryForgetNodeError),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error forgetting node outdated-id from node node1: forget node error"),
		},
		{
			name: "RefreshNodesError",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					// Create a node that returns healthy nodes (no removal needed)
					healthyNode := redis.NewFakeRedisNode("healthy-node", mockClientFactory)
					healthyNode.ID = "healthy-id"
					healthyNode.Flags = "master"

					mockClientFactoryHealthy := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{*healthyNode},
						}
						return client, nil
					}
					return redis.NewFakeRedisNode("node1", mockClientFactoryHealthy)
				}(),
			},
			clientFactory: mockClientFactoryRefreshError,
			expectedError: fmt.Errorf("error refreshing nodes info: %v", errRefresh),
		},
		{
			name: "SuccessNoOutdatedNodes",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					healthyNode := redis.NewFakeRedisNode("healthy-node", mockClientFactory)
					healthyNode.ID = "healthy-id"
					healthyNode.Flags = "master"

					mockClientFactoryHealthy := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{*healthyNode},
						}
						return client, nil
					}
					return redis.NewFakeRedisNode("node1", mockClientFactoryHealthy)
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessFailFlagNode",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					failedNode := redis.NewFakeRedisNode("failed-node", mockClientFactory)
					failedNode.ID = "failed-id"
					failedNode.Flags = "fail"

					mockClientFactoryWithFailedNode := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{*failedNode},
						}
						return client, nil
					}
					return redis.NewFakeRedisNode("node1", mockClientFactoryWithFailedNode)
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessNoAddrFlagNode",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					noaddrNode := redis.NewFakeRedisNode("noaddr-node", mockClientFactory)
					noaddrNode.ID = "noaddr-id"
					noaddrNode.Flags = "noaddr"

					mockClientFactoryWithNoAddrNode := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{*noaddrNode},
						}
						return client, nil
					}
					return redis.NewFakeRedisNode("node1", mockClientFactoryWithNoAddrNode)
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessMultipleNodes",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					failedNode := redis.NewFakeRedisNode("failed-node", mockClientFactory)
					failedNode.ID = "failed-id"
					failedNode.Flags = "fail"

					noaddrNode := redis.NewFakeRedisNode("noaddr-node", mockClientFactory)
					noaddrNode.ID = "noaddr-id"
					noaddrNode.Flags = "noaddr"

					healthyNode := redis.NewFakeRedisNode("healthy-node", mockClientFactory)
					healthyNode.ID = "healthy-id"
					healthyNode.Flags = "master"

					mockClientFactoryMultiple := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{*failedNode, *noaddrNode, *healthyNode},
						}
						return client, nil
					}
					return redis.NewFakeRedisNode("node1", mockClientFactoryMultiple)
				}(),
				"node2": func() *redis.RedisNode {
					healthyNode := redis.NewFakeRedisNode("healthy-node2", mockClientFactory)
					healthyNode.ID = "healthy-id2"
					healthyNode.Flags = "master"

					mockClientFactoryHealthy := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{*healthyNode},
						}
						return client, nil
					}
					return redis.NewFakeRedisNode("node2", mockClientFactoryHealthy)
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			err := cluster.removeOutdatedNodes(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterEnsureClusterRatio(t *testing.T) {
	// Create mock client factories for different error scenarios
	mockClientFactoryReplicateError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterReplicateError: fmt.Errorf("replicate error"),
		}
		return client, nil
	}

	mockClientFactoryResetError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterResetError: fmt.Errorf("reset error"),
		}
		return client, nil
	}

	tests := []struct {
		name               string
		nodes              map[string]*redis.RedisNode
		primaries          int
		replicasPerPrimary int
		clientFactory      func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		operationFactory   *OperationFactory
		expectedError      error
	}{
		// Flow 1: len(activePrimaries) == desiredPrimaries -> ensureReplicaSpread
		{
			name:               "CorrectRatioCallsEnsureReplicaSpread",
			primaries:          3,
			replicasPerPrimary: 1,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 5461}}
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "primary2-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 5462, End: 10922}}
					return node
				}(),
				"primary3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary3", mockClientFactory)
					node.ID = "primary3-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 10923, End: 16383}}
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
				"replica2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica2", mockClientFactory)
					node.ID = "replica2-id"
					node.Flags = "slave"
					node.PrimaryID = "primary2-id"
					return node
				}(),
				"replica3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica3", mockClientFactory)
					node.ID = "replica3-id"
					node.Flags = "slave"
					node.PrimaryID = "primary3-id"
					return node
				}(),
			},
			clientFactory:    mockClientFactory,
			operationFactory: nil,
			expectedError:    nil,
		},
		// Flow 2: len(activePrimaries) > desiredPrimaries -> convertNodesToReplica -> ensureReplicaSpread
		{
			name:               "TooManyPrimariesConvertToReplicas",
			primaries:          2,
			replicasPerPrimary: 1,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 8191}}
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "primary2-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 8192, End: 16383}}
					return node
				}(),
				"primary3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary3", mockClientFactory)
					node.ID = "primary3-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 100}}
					return node
				}(),
				"primary4": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary4", mockClientFactory)
					node.ID = "primary4-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{}
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
				"replica2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica2", mockClientFactory)
					node.ID = "replica2-id"
					node.Flags = "slave"
					node.PrimaryID = "primary2-id"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			expectedError: nil,
		},
		{
			name:               "ConvertNodesToReplicaError",
			primaries:          2,
			replicasPerPrimary: 1,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 8191}}
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "primary2-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 8192, End: 16383}}
					return node
				}(),
				"primary3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary3", mockClientFactoryReplicateError)
					node.ID = "primary3-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 100}}
					return node
				}(),
				"primary4": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary4", mockClientFactoryReplicateError)
					node.ID = "primary4-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{}
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
				"replica2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica2", mockClientFactory)
					node.ID = "replica2-id"
					node.Flags = "slave"
					node.PrimaryID = "primary2-id"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			expectedError: fmt.Errorf("replicate error"),
		},
		// Flow 3: len(activePrimaries) < desiredPrimaries && desiredReplicas > 0 -> convertNodesToPrimary -> ensureReplicaSpread
		{
			name:               "FewPrimariesPromoteReplicas",
			primaries:          3,
			replicasPerPrimary: 1,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 16383}}
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
				"replica2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica2", mockClientFactory)
					node.ID = "replica2-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
			},
			clientFactory:    mockClientFactory,
			operationFactory: nil,
			expectedError:    nil,
		},
		{
			name:               "ConvertNodesToPrimaryError",
			primaries:          3,
			replicasPerPrimary: 1,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 16383}}
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactoryResetError)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
				"replica2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica2", mockClientFactory)
					node.ID = "replica2-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
			},
			clientFactory:    mockClientFactory,
			operationFactory: nil,
			expectedError:    fmt.Errorf("reset error"),
		},
		// Flow 4: len(activeReplicas) > 0 && desiredReplicas == 0 -> convertNodesToPrimary -> ensureReplicaSpread
		{
			name:               "UnwantedReplicasPromoteAll",
			primaries:          3,
			replicasPerPrimary: 0,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 8191}}
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "primary2-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 8192, End: 16383}}
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
			},
			clientFactory:    mockClientFactory,
			operationFactory: nil,
			expectedError:    nil,
		},
		{
			name:               "UnwantedReplicasPromoteAllWithFail",
			primaries:          3,
			replicasPerPrimary: 0,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactoryResetError)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 8191}}
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "primary2-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 8192, End: 16383}}
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactoryResetError)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
			},
			clientFactory:    mockClientFactoryResetError,
			operationFactory: nil,
			expectedError:    fmt.Errorf("reset error"),
		},
		// Flow 5: Default case -> return nil
		{
			name:               "DefaultCaseNoAction",
			primaries:          2,
			replicasPerPrimary: -1,
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 16383}}
					return node
				}(),
			},
			clientFactory:    mockClientFactory,
			operationFactory: nil,
			expectedError:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:               "test-cluster",
							Primaries:          tt.primaries,
							ReplicasPerPrimary: tt.replicasPerPrimary,
							MaxRetries:         1,
							BackOff:            time.Microsecond * 10,
						},
					},
				},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.ensureClusterRatio(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterEnsureReplicaSpread(t *testing.T) {
	// Create mock client factories for different error scenarios
	mockClientFactoryReplicateError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterReplicateError: fmt.Errorf("replicate error"),
		}
		return client, nil
	}

	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name: "NotEnoughReplicas",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "primary2-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "unknown-primary-id" // Points to unknown primary, needs move
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("there are not enough replicas to convert. primaries=%d replicas=%d", 2, 1),
		},
		{
			name: "ReplicateNodeError",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactoryReplicateError)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "unknown-primary-id" // Points to unknown primary, needs move
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error promoting replica replica1 to primary primary1: replicate error"),
		},
		{
			name: "RefreshNodesError",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "unknown-primary-id" // Points to unknown primary, needs move
					return node
				}(),
			},
			clientFactory: mockClientFactoryRefreshError,
			expectedError: fmt.Errorf("error refreshing nodes info: %v", errRefresh),
		},
		{
			name: "SuccessNoChangesNeeded",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id" // Points to correct primary
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessReplicaPointingToUnknownPrimary",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "unknown-primary-id" // Points to unknown primary, needs move
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessReplicaPointingToPrimaryName",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1" // Points to primary name, should be auto-corrected to primary ID
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessReplicaPointingToReplica",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
				"replica2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica2", mockClientFactory)
					node.ID = "replica2-id"
					node.Flags = "slave"
					node.PrimaryID = "replica1-id" // Points to replica, needs move
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessTooManyReplicasForPrimary",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master"
					return node
				}(),
				"primary2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary2", mockClientFactory)
					node.ID = "primary2-id"
					node.Flags = "master"
					return node
				}(),
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id"
					return node
				}(),
				"replica2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica2", mockClientFactory)
					node.ID = "replica2-id"
					node.Flags = "slave"
					node.PrimaryID = "primary1-id" // Two replicas for primary1, one should move to primary2
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:               "test-cluster",
							MaxRetries:         1,
							BackOff:            time.Microsecond * 10,
							ReplicasPerPrimary: 1,
						},
					},
				},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			err := cluster.ensureReplicaSpread(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterConvertNodesToReplica(t *testing.T) {
	// Create mock client factories for different error scenarios
	mockClientFactoryRemoveSlotsError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterRebalanceError: fmt.Errorf("remove slots error"),
		}
		return client, nil
	}

	mockClientFactoryReplicateError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterReplicateError: fmt.Errorf("replicate error"),
		}
		return client, nil
	}

	tests := []struct {
		name             string
		nodesToConvert   []*redis.RedisNode
		nodesToKeep      []*redis.RedisNode
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		operationFactory *OperationFactory
		expectedError    error
	}{
		{
			name: "RemoveSlotsError",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactoryRemoveSlotsError)
					node.ID = "node1-id"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{{Start: 0, End: 100}}
					return node
				}(),
			},
			nodesToKeep: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper1", mockClientFactory)
					node.ID = "keeper1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.EnsureNodesAreUpError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			expectedError: fmt.Errorf("error ensuring nodes are up: nodes are not up"),
		},
		{
			name: "ReplicateNodeError",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactoryReplicateError)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			nodesToKeep: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper1", mockClientFactory)
					node.ID = "keeper1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("replicate error"),
		},
		{
			name: "RefreshNodesError",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			nodesToKeep: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper1", mockClientFactory)
					node.ID = "keeper1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactoryRefreshError,
			expectedError: fmt.Errorf("error refreshing nodes info: %v", errRefresh),
		},
		{
			name: "SuccessSkipReplicas",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave" // Already a replica, should be skipped
					return node
				}(),
			},
			nodesToKeep: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper1", mockClientFactory)
					node.ID = "keeper1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessSingleConversion",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			nodesToKeep: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper1", mockClientFactory)
					node.ID = "keeper1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessMultipleConversions",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
			},
			nodesToKeep: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper1", mockClientFactory)
					node.ID = "keeper1-id"
					node.Flags = "master"
					return node
				}(),
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper2", mockClientFactory)
					node.ID = "keeper2-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessMixedNodes",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1-id"
					node.Flags = "slave" // Already replica, should be skipped
					return node
				}(),
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactory)
					node.ID = "primary1-id"
					node.Flags = "master" // Should be converted
					return node
				}(),
			},
			nodesToKeep: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("keeper1", mockClientFactory)
					node.ID = "keeper1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory).WithOperationFactory(tt.operationFactory)

			err := cluster.convertNodesToReplica(context.Background(), tt.nodesToConvert, tt.nodesToKeep)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterConvertNodesToPrimary(t *testing.T) {
	// Create mock client factories for different error scenarios
	mockClientFactoryResetError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterResetError: fmt.Errorf("reset error"),
		}
		return client, nil
	}

	mockClientFactoryMeetError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterMeetError: fmt.Errorf("meet error"),
		}
		return client, nil
	}

	tests := []struct {
		name           string
		nodesToConvert []*redis.RedisNode
		nodes          map[string]*redis.RedisNode
		clientFactory  func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError  error
	}{
		{
			name: "ResetError",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactoryResetError)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
			},
			nodes: map[string]*redis.RedisNode{
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactoryResetError)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
			},
			clientFactory: mockClientFactoryResetError,
			expectedError: fmt.Errorf("reset error"),
		},
		{
			name: "MeetNodesError",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
			},
			nodes:         map[string]*redis.RedisNode{},
			clientFactory: mockClientFactoryMeetError,
			expectedError: fmt.Errorf("error meeting nodes: there are no nodes in the cluster"),
		},
		{
			name: "SuccessSkipPrimaries",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			nodes: map[string]*redis.RedisNode{
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessConvertSingleNode",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
			},
			nodes: map[string]*redis.RedisNode{
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessConvertMultipleNodes",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "slave"
					return node
				}(),
			},
			nodes: map[string]*redis.RedisNode{
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "slave"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "SuccessMixedNodes",
			nodesToConvert: []*redis.RedisNode{
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
				func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
			},
			nodes: map[string]*redis.RedisNode{
				"node1-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", mockClientFactory)
					node.ID = "node1-id"
					node.Flags = "slave"
					return node
				}(),
				"node2-id": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "node2-id"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{
					Redis: config.RedisConfig{
						Cluster: config.RedKeyClusterConfig{
							Name:                       "test-cluster",
							Primaries:                  3,
							ReplicasPerPrimary:         2,
							ClusterMeetWaitTimeSeconds: 0,
							NodeResetWaitTimeSeconds:   0,
							MaxRetries:                 1,
							BackOff:                    time.Microsecond * 10,
						},
					},
				},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			err := cluster.convertNodesToPrimary(context.Background(), tt.nodesToConvert)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterPromoteReplicasOfNode(t *testing.T) {
	tests := []struct {
		name          string
		node          *redis.RedisNode
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name:          "node no primary",
			node:          node3,
			expectedError: fmt.Errorf("node node3 is not a primary"),
		},
		{
			name: "node no replicas",
			node: node1,
			nodes: map[string]*redis.RedisNode{
				"node-0": node2,
			},
			expectedError: fmt.Errorf("node test-0 has no replicas to promote"),
		},
		{
			name: "failover failed",
			node: node1,
			nodes: map[string]*redis.RedisNode{
				"node-0": node1,
				"node-1": func() *redis.RedisNode {
					n := redis.NewFakeRedisNode("node-1", mockClientFactoryError)
					n.ID = "0000000002"
					n.Flags = "slave"
					n.PrimaryID = "1234567890"
					return n
				}(),
			},
			expectedError: fmt.Errorf("error promoting replica node-1 to primary test-0: error creating client"),
		},
		{
			name: "refresh error",
			node: node1,
			nodes: map[string]*redis.RedisNode{
				"node-0": node1,
				"node-1": func() *redis.RedisNode {
					n := redis.NewFakeRedisNode("node-1", mockClientFactory)
					n.ID = "0000000002"
					n.Flags = "slave"
					n.PrimaryID = "1234567890"
					return n
				}(),
			},
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("error refreshing nodes info: error creating client"),
		},
		{
			name: "success",
			node: node1,
			nodes: map[string]*redis.RedisNode{
				"node-0": node1,
				"node-1": func() *redis.RedisNode {
					n := redis.NewFakeRedisNode("node-1", mockClientFactory)
					n.ID = "0000000002"
					n.Flags = "slave"
					n.PrimaryID = "1234567890"
					return n
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			err := cluster.promoteReplicaOfNode(t.Context(), tt.node)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterEnsureNodesAreUp(t *testing.T) {
	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		expectedError error
	}{
		{
			name:          "no nodes",
			nodes:         map[string]*redis.RedisNode{},
			expectedError: nil,
		},
		{
			name: "node error",
			nodes: map[string]*redis.RedisNode{
				"node-0": redis.NewFakeRedisNode("node-0", mockClientFactoryError),
			},
			expectedError: fmt.Errorf("error creating client"),
		},
		{
			name: "success",
			nodes: map[string]*redis.RedisNode{
				"node-0": redis.NewFakeRedisNode("node-0", mockClientFactory),
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			)

			err := cluster.ensureNodesAreUp(t.Context())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterAssignMissingSlots(t *testing.T) {
	// Define error variables for reuse
	addSlotsError := fmt.Errorf("failed to add slots")

	// Create mock client factory that fails on AddSlots
	mockClientFactoryAddSlotsError := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
		client := redis.MockRedisClient{
			ClusterAddSlotsError: addSlotsError,
		}
		return client, nil
	}

	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name: "no primaries only replicas",
			nodes: map[string]*redis.RedisNode{
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1"
					node.Flags = "slave"
					node.PrimaryID = "primary1"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "AddSlots fails",
			nodes: map[string]*redis.RedisNode{
				"primary1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("primary1", mockClientFactoryAddSlotsError)
					node.ID = "id1"
					node.Flags = "master"
					node.Slots = []redis.RedisSlotRange{} // No slots, needs assignment
					return node
				}(),
			},
			clientFactory: mockClientFactory, // Cluster client factory is successful
			expectedError: addSlotsError,
		},
		{
			name: "refreshNodes fails",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithoutSlots("primary1", "id1"),
			},
			clientFactory: mockClientFactoryRefreshError,
			expectedError: fmt.Errorf("error refreshing nodes info: %v", errRefresh),
		},
		{
			name: "single primary no slots",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithoutSlots("primary1", "id1"),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "multiple primaries no slots",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithoutSlots("primary1", "id1"),
				"primary2": createNodeWithoutSlots("primary2", "id2"),
				"primary3": createNodeWithoutSlots("primary3", "id3"),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "primaries with partial slots",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithSlots("primary1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5000}}),
				"primary2": createNodeWithSlots("primary2", "id2", []redis.RedisSlotRange{{Start: 5001, End: 10000}}),
				"primary3": createNodeWithoutSlots("primary3", "id3"), // This one needs slots
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "mixed primaries some full some partial",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithSlots("primary1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5461}}),    // Has enough slots (5462 slots)
				"primary2": createNodeWithSlots("primary2", "id2", []redis.RedisSlotRange{{Start: 5462, End: 8000}}), // Needs more slots
				"primary3": createNodeWithoutSlots("primary3", "id3"),                                                // Needs all slots
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "last primary gets remaining slots",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithSlots("primary1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5000}}),
				"primary2": createNodeWithoutSlots("primary2", "id2"), // Last primary gets remaining slots
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "gaps in slot ranges",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithSlots("primary1", "id1", []redis.RedisSlotRange{{Start: 0, End: 1000}, {Start: 2000, End: 3000}}),
				"primary2": createNodeWithSlots("primary2", "id2", []redis.RedisSlotRange{{Start: 5000, End: 6000}}),
				"primary3": createNodeWithoutSlots("primary3", "id3"),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "all primaries have enough slots",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithSlots("primary1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5461}}),
				"primary2": createNodeWithSlots("primary2", "id2", []redis.RedisSlotRange{{Start: 5462, End: 10922}}),
				"primary3": createNodeWithSlots("primary3", "id3", []redis.RedisSlotRange{{Start: 10923, End: 16383}}),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "single primary with all slots",
			nodes: map[string]*redis.RedisNode{
				"primary1": createNodeWithSlots("primary1", "id1", []redis.RedisSlotRange{{Start: 0, End: 16383}}),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			err := cluster.assignMissingSlots(context.Background())

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterGetAndCheckRedisClient(t *testing.T) {
	tests := []struct {
		name          string
		close         bool
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name:          "get bad redis client",
			close:         true,
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("error creating client"),
		},
		{
			name:          "get good redis client",
			close:         true,
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			client, err := cluster.getRedisClient(tt.close)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}

func TestRedKeyClusterInit(t *testing.T) {
	tests := []struct {
		name          string
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name:          "get bad redis client",
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("Error refreshing nodes info: error creating client"),
		},
		{
			name:          "success",
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.WithClientFactory(tt.clientFactory).Init()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterSetReplicas(t *testing.T) {
	tests := []struct {
		name               string
		primaries          int
		replicasPerPrimary *int
		expectedError      error
	}{
		{
			name:          "same replicas",
			primaries:     3,
			expectedError: &OperationCompletedError{Operation: "SetReplicas"},
		},
		{
			name:               "same replicas per primary",
			primaries:          3,
			replicasPerPrimary: getIntPointer(0),
			expectedError:      &OperationCompletedError{Operation: "SetReplicas"},
		},
		{
			name:          "less replicas",
			primaries:     2,
			expectedError: nil,
		},
		{
			name:               "more replicas per primary",
			primaries:          2,
			replicasPerPrimary: getIntPointer(1),
			expectedError:      nil,
		},
		{
			name:          "more replicas",
			primaries:     4,
			expectedError: nil,
		},
		{
			name:               "less replicas per primary",
			primaries:          4,
			replicasPerPrimary: getIntPointer(0),
			expectedError:      nil,
		},
		{
			name:               "both replicas and replicas per primary",
			primaries:          3,
			replicasPerPrimary: getIntPointer(1),
			expectedError:      nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.SetReplicas(tt.primaries, tt.replicasPerPrimary)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, redkeyCluster.GetPrimaries(), tt.primaries)

			if tt.replicasPerPrimary != nil {
				assert.Equal(t, redkeyCluster.GetReplicasPerPrimary(), *tt.replicasPerPrimary)
			}
		})
	}
}

func TestRedKeyClusterSetRedKeyClusterStatus(t *testing.T) {
	tests := []struct {
		name          string
		status        string
		expectedError error
	}{
		{
			name:          "good status",
			status:        "Ready",
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.SetRedKeyClusterStatus(tt.status)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterRebalance(t *testing.T) {
	tests := []struct {
		name             string
		operations       map[string][]RedisOperation
		operationFactory *OperationFactory
		force            bool
		expectedError    error
	}{
		{
			name: "rebalancing",
			operations: map[string][]RedisOperation{
				Rebalancing: {NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Running", time.Time{})},
			},
			expectedError: &OperationInProgressError{Operation: "Rebalance"},
		},
		{
			name: "rebalanced",
			operations: map[string][]RedisOperation{
				Rebalancing: {NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Finished", time.Time{})},
			},
			expectedError: &OperationCompletedError{Operation: "Rebalance"},
		},
		{
			name: "operation failed with cancel",
			operations: map[string][]RedisOperation{
				Rebalancing: func() []RedisOperation {
					op := NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Running", time.Time{})
					cmd := redis.NewRedisCLICommand(t.Context(), "exit 0")
					cmd.Start()
					op.cmd = cmd
					return []RedisOperation{op}
				}(),
			},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.SetRedisClientError("ClusterRebalance", fmt.Errorf("rebalance operation failed"))
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			force:         true,
			expectedError: fmt.Errorf("error rebalancing cluster: rebalance operation failed"),
		},
		{
			name:       "success",
			operations: map[string][]RedisOperation{},
			operationFactory: &OperationFactory{
				NewRebalance: func(ctx context.Context, cluster Cluster, weights map[string]int) *RedisOperationRebalance {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationRebalance(ctx, mockCluster, "Running", time.Time{})
				},
			},
			force:         true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory)

			err := cluster.Rebalance(false, nil, tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterMoveSlots(t *testing.T) {
	tests := []struct {
		name             string
		prepareTest      func(cluster *RedKeyCluster)
		operationFactory *OperationFactory
		clientFactory    func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		operations       map[string][]RedisOperation
		from             *redis.RedisNode
		to               *redis.RedisNode
		expectedError    error
	}{
		{
			name: "moving",
			operations: map[string][]RedisOperation{
				Resharding: {NewFakeRedisOperationMove(t.Context(), redkeyCluster, "Running", node1, node3, 10, time.Time{})},
			},
			from:          node1,
			to:            node3,
			expectedError: &OperationInProgressError{Operation: "Resharding"},
		},
		{
			name: "origin has no slots",
			operations: map[string][]RedisOperation{
				Resharding: {},
			},
			prepareTest: func(cluster *RedKeyCluster) {
				node1.Slots = []redis.RedisSlotRange{}
			},
			from:          node1,
			to:            node3,
			expectedError: &OperationCompletedError{Operation: "Resharding", Reason: "Origin node has no slots"},
		},
		{
			name:          "node is a replica",
			operations:    map[string][]RedisOperation{},
			from:          node3,
			to:            node1,
			expectedError: &OperationCompletedError{Operation: "Resharding", Reason: "Origin node is a replica"},
		},
		{
			name: "failed to promote replicas of node",
			operations: map[string][]RedisOperation{
				Resharding: {},
			},
			prepareTest: func(cluster *RedKeyCluster) {
				node1.Slots = []redis.RedisSlotRange{
					{
						Start: 1,
						End:   5461,
					},
				}

				node1.ID = "1234567890"
				node1.Flags = "master"
				node3.PrimaryID = "1234567890"
				node3.Flags = "slave"

				cluster.nodes = map[string]*redis.RedisNode{
					"test-0": node1,
					"test-1": node2,
					"test-2": node3,
				}
			},
			clientFactory: mockClientFactoryError,
			from:          node1,
			to:            node3,
			expectedError: fmt.Errorf("error promoting replica of node 'test-0': error refreshing nodes info: error creating client"),
		},
		{
			name: "promoted replica node",
			operations: map[string][]RedisOperation{
				Resharding: {},
			},
			prepareTest: func(cluster *RedKeyCluster) {
				node1.Slots = []redis.RedisSlotRange{
					{
						Start: 1,
						End:   5461,
					},
				}

				node1.ID = "1234567890"
				node1.Flags = "master"
				node3.PrimaryID = "1234567890"
				node3.Flags = "slave"

				cluster.nodes = map[string]*redis.RedisNode{
					"test-0": node1,
					"test-1": node2,
					"test-2": node3.WithClientFactory(mockClientFactory),
				}
			},
			clientFactory: mockClientFactory,
			from:          node1,
			to:            node3,
			expectedError: &OperationCompletedError{Operation: "Resharding", Reason: "Promoted replica of origin node"},
		},
		{
			name: "success",
			operations: map[string][]RedisOperation{
				Resharding: {},
			},
			operationFactory: &OperationFactory{
				NewMove: func(ctx context.Context, cluster Cluster, from *redis.RedisNode, to *redis.RedisNode, slots int) *RedisOperationMove {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationMove(ctx, mockCluster, "Running", from, to, slots, time.Time{})
				},
			},
			from:          node2,
			to:            node3,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory).WithClientFactory(tt.clientFactory)

			if tt.prepareTest != nil {
				tt.prepareTest(cluster)
			}

			err := cluster.MoveSlots(tt.from, tt.to, 10)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterCheck(t *testing.T) {
	tests := []struct {
		name           string
		clientFactory  func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError  error
		expectedResult *redis.ClusterCheckResult
	}{
		{
			name:          "bad redis client",
			clientFactory: mockClientFactoryError,
			expectedError: fmt.Errorf("error getting and checking Redis client: error creating client"),
		},
		{
			name: "cluster check fails",
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				return &redis.MockRedisClient{
					ClusterCheckError: fmt.Errorf("cluster check failed"),
				}, nil
			},
			expectedError: fmt.Errorf("error checking cluster: cluster check failed"),
		},
		{
			name: "success",
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				return &redis.MockRedisClient{}, nil
			},
			expectedError: nil,
			expectedResult: &redis.ClusterCheckResult{
				CommandCodeOutput: 0,
				Errors:            []string{},
				Warnings:          []string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				map[string][]RedisOperation{},
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			result, err := cluster.Check()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				if tt.expectedResult != nil {
					assert.Equal(t, tt.expectedResult.CommandCodeOutput, result.CommandCodeOutput)
					assert.Equal(t, tt.expectedResult.Errors, result.Errors)
					assert.Equal(t, tt.expectedResult.Warnings, result.Warnings)
				}
			}
		})
	}
}

func TestRedKeyClusterFix(t *testing.T) {
	tests := []struct {
		name             string
		operationFactory *OperationFactory
		operations       map[string][]RedisOperation
		force            bool
		expectedError    error
	}{
		{
			name: "fixing",
			operations: map[string][]RedisOperation{
				Fixing: {NewFakeRedisOperationFix(t.Context(), redkeyCluster, "Running")},
			},
			expectedError: &OperationInProgressError{Operation: "Fixing"},
		},
		{
			name: "cancel fixing with error",
			operations: map[string][]RedisOperation{
				Fixing: func() []RedisOperation {
					op := NewFakeRedisOperationFix(t.Context(), redkeyCluster, "Running")
					cmd := redis.NewRedisCLICommand(t.Context(), "exit 0")
					cmd.Start()
					op.cmd = cmd
					return []RedisOperation{op}
				}(),
			},
			operationFactory: &OperationFactory{
				NewFix: func(ctx context.Context, cluster Cluster) *RedisOperationFix {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.EnsureNodesAreUpError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationFix(ctx, mockCluster, "Running")
				},
			},
			force:         true,
			expectedError: fmt.Errorf("error ensuring nodes are up: nodes are not up"),
		},
		{
			name: "good",
			operations: map[string][]RedisOperation{
				Fixing: {},
			},
			operationFactory: &OperationFactory{
				NewFix: func(ctx context.Context, cluster Cluster) *RedisOperationFix {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationFix(ctx, mockCluster, "Running")
				},
			},
			force:         true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory)

			err := cluster.Fix(false, tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterCheckIntegrity(t *testing.T) {
	tests := []struct {
		name             string
		operationFactory *OperationFactory
		operations       map[string][]RedisOperation
		force            bool
		expectedError    error
	}{
		{
			name: "checking integrity",
			operations: map[string][]RedisOperation{
				CheckingIntegrity: {NewFakeRedisOperationCheckIntegrity(t.Context(), redkeyCluster, "Running")},
			},
			expectedError: &OperationInProgressError{Operation: "CheckingIntegrity"},
		},
		{
			name: "cancel checking integrity with error",
			operations: map[string][]RedisOperation{
				CheckingIntegrity: func() []RedisOperation {
					op := NewFakeRedisOperationCheckIntegrity(t.Context(), redkeyCluster, "Running")
					cmd := redis.NewRedisCLICommand(t.Context(), "exit 0")
					cmd.Start()
					op.cmd = cmd
					return []RedisOperation{op}
				}(),
			},
			operationFactory: &OperationFactory{
				NewCheckIntegrity: func(ctx context.Context, cluster Cluster) *RedisOperationCheckIntegrity {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.CheckNodesError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationCheckIntegrity(ctx, mockCluster, "Running")
				},
			},
			force:         true,
			expectedError: fmt.Errorf("error checking cluster integrity: nodes are not up"),
		},
		{
			name: "good",
			operations: map[string][]RedisOperation{
				CheckingIntegrity: {},
			},
			operationFactory: &OperationFactory{
				NewCheckIntegrity: func(ctx context.Context, cluster Cluster) *RedisOperationCheckIntegrity {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationCheckIntegrity(ctx, mockCluster, "Running")
				},
			},
			force:         true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory)

			err := cluster.CheckIntegrity(false, tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterScaleUp(t *testing.T) {
	tests := []struct {
		name             string
		operationFactory *OperationFactory
		operations       map[string][]RedisOperation
		force            bool
		expectedError    error
	}{
		{
			name: "scaling up",
			operations: map[string][]RedisOperation{
				ScalingUp: {NewFakeRedisOperationScaleUp(t.Context(), redkeyCluster, "Running")},
			},
			expectedError: &OperationInProgressError{Operation: "ScaleUp"},
		},
		{
			name: "fail",
			operations: map[string][]RedisOperation{
				ScalingUp: {NewFakeRedisOperationScaleUp(t.Context(), redkeyCluster, "Running")},
			},
			operationFactory: &OperationFactory{
				NewScaleUp: func(ctx context.Context, cluster Cluster) *RedisOperationScaleUp {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.CheckNodesError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationScaleUp(ctx, mockCluster, "Running")
				},
			},
			force:         true,
			expectedError: fmt.Errorf("error scaling up cluster: nodes are not up"),
		},
		{
			name: "good",
			operations: map[string][]RedisOperation{
				ScalingUp: {NewFakeRedisOperationScaleUp(t.Context(), redkeyCluster, "Running")},
			},
			operationFactory: &OperationFactory{
				NewScaleUp: func(ctx context.Context, cluster Cluster) *RedisOperationScaleUp {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationScaleUp(ctx, mockCluster, "Running")
				},
			},
			force:         true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory)

			err := cluster.ScaleUp(tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterScaleDown(t *testing.T) {
	tests := []struct {
		name             string
		operationFactory *OperationFactory
		operations       map[string][]RedisOperation
		force            bool
		expectedError    error
	}{
		{
			name: "scaling down",
			operationFactory: &OperationFactory{
				NewScaleDown: func(ctx context.Context, cluster Cluster) *RedisOperationScaleDown {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationScaleDown(ctx, mockCluster, "Running")
				},
			},
			operations: map[string][]RedisOperation{
				ScalingDown: {NewFakeRedisOperationScaleDown(t.Context(), redkeyCluster, "Running")},
			},
			expectedError: &OperationInProgressError{Operation: "ScaleDown"},
		},
		{
			name: "fail",
			operationFactory: &OperationFactory{
				NewScaleDown: func(ctx context.Context, cluster Cluster) *RedisOperationScaleDown {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.CheckNodesError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationScaleDown(ctx, mockCluster, "Running")
				},
			},
			operations: map[string][]RedisOperation{
				ScalingDown: {},
			},
			force:         true,
			expectedError: fmt.Errorf("error scaling down cluster: nodes are not up"),
		},
		{
			name: "good",
			operationFactory: &OperationFactory{
				NewScaleDown: func(ctx context.Context, cluster Cluster) *RedisOperationScaleDown {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationScaleDown(ctx, mockCluster, "Running")
				},
			},
			operations: map[string][]RedisOperation{
				ScalingDown: {},
			},
			force:         true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory)

			err := cluster.ScaleDown(tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterUpgrade(t *testing.T) {
	tests := []struct {
		name             string
		operationFactory *OperationFactory
		operations       map[string][]RedisOperation
		force            bool
		expectedError    error
	}{
		{
			name: "upgrading",
			operationFactory: &OperationFactory{
				NewUpgrade: func(ctx context.Context, cluster Cluster) *RedisOperationUpgrade {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationUpgrade(ctx, mockCluster, "Running")
				},
			},
			operations: map[string][]RedisOperation{
				Upgrading: {NewFakeRedisOperationUpgrade(t.Context(), redkeyCluster, "Running")},
			},
			expectedError: &OperationInProgressError{Operation: "Upgrade"},
		},
		{
			name: "fail",
			operationFactory: &OperationFactory{
				NewUpgrade: func(ctx context.Context, cluster Cluster) *RedisOperationUpgrade {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					mockCluster.CheckNodesError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationUpgrade(ctx, mockCluster, "Running")
				},
			},
			operations: map[string][]RedisOperation{
				Upgrading: {NewFakeRedisOperationUpgrade(t.Context(), redkeyCluster, "Running")},
			},
			force:         true,
			expectedError: fmt.Errorf("error upgrading cluster: nodes are not up"),
		},
		{
			name: "good",
			operationFactory: &OperationFactory{
				NewUpgrade: func(ctx context.Context, cluster Cluster) *RedisOperationUpgrade {
					mockCluster := NewMockRedKeyCluster(redkeyCluster)
					return NewFakeRedisOperationUpgrade(ctx, mockCluster, "Running")
				},
			},
			operations: map[string][]RedisOperation{
				Upgrading: {NewFakeRedisOperationUpgrade(t.Context(), redkeyCluster, "Running")},
			},
			force:         true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				map[string]*redis.RedisNode{},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory)

			err := cluster.Upgrade(tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterResetNode(t *testing.T) {
	tests := []struct {
		name             string
		operationFactory *OperationFactory
		operations       map[string][]RedisOperation
		node             *redis.RedisNode
		force            bool
		expectedError    error
	}{
		{
			name: "resetting",
			operations: map[string][]RedisOperation{
				Resetting: {NewFakeRedisOperationResetNode(t.Context(), redkeyCluster, "Running", node1)},
			},
			node:          node1,
			expectedError: &OperationInProgressError{Operation: "Resetting"},
		},
		{
			name: "fail",
			operationFactory: &OperationFactory{
				NewResetNode: func(ctx context.Context, cluster Cluster, node *redis.RedisNode) *RedisOperationResetNode {
					mockCluster := NewMockRedKeyCluster(cluster.(*RedKeyCluster))
					mockCluster.CheckNodesError = fmt.Errorf("nodes are not up")
					return NewFakeRedisOperationResetNode(ctx, mockCluster, "Running", node.WithClientFactory(mockClientFactory))
				},
			},
			operations: map[string][]RedisOperation{
				Resetting: {},
			},
			node:          redis.NewRedisNode("test-0", "id1", 0, time.Duration(0)).WithClientFactory(mockClientFactory),
			force:         true,
			expectedError: fmt.Errorf("error resetting cluster node 'test-0': nodes are not up"),
		},
		{
			name: "good",
			operationFactory: &OperationFactory{
				NewResetNode: func(ctx context.Context, cluster Cluster, node *redis.RedisNode) *RedisOperationResetNode {
					mockCluster := NewMockRedKeyCluster(cluster.(*RedKeyCluster))
					return NewFakeRedisOperationResetNode(ctx, mockCluster, "Running", node.WithClientFactory(mockClientFactory))
				},
			},
			operations: map[string][]RedisOperation{
				Resetting: {},
			},
			node:          redis.NewRedisNode("test-0", "id1", 0, time.Duration(0)).WithClientFactory(mockClientFactory),
			force:         true,
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				},
				tt.operations,
				make(chan struct{}, 1),
			).WithOperationFactory(tt.operationFactory).WithClientFactory(mockClientFactory)

			err := cluster.ResetNode(tt.node)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterStabilizeOpenSlots(t *testing.T) {
	tests := []struct {
		name         string
		nodes        map[string]*redis.RedisNode
		initialCount map[int]int
		threshold    int
		expected     map[int]int
		expectError  bool
	}{
		{
			name: "no open slots",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{{ID: "nodeId1", IP: "1.1.1.1", Migrating: map[int]string{}}},
						}
						return client, nil
					})
					node.ID = "nodeId1"
					return node
				}(),
			},
			initialCount: map[int]int{},
			threshold:    2,
			expected:     map[int]int{},
			expectError:  false,
		},
		{
			name: "increment counter for open slot",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{{ID: "nodeId1", IP: "1.1.1.1", Migrating: map[int]string{5: "nodeId2"}}},
						}
						return client, nil
					})
					node.ID = "nodeId1"
					return node
				}(),
				// include destination node so GetNodeById can find it (not strictly required when not stabilizing)
				"node2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", mockClientFactory)
					node.ID = "nodeId2"
					return node
				}(),
			},
			initialCount: map[int]int{},
			threshold:    1,
			expected:     map[int]int{5: 1},
			expectError:  false,
		},
		{
			name: "stabilize when threshold reached",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{{ID: "nodeId1", IP: "10.0.0.1", Migrating: map[int]string{7: "nodeId2"}}},
						}
						return client, nil
					})
					node.ID = "nodeId1"
					node.IP = "10.0.0.1"
					return node
				}(),
				"node2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{},
						}
						return client, nil
					})
					node.ID = "nodeId2"
					node.IP = "10.0.0.2"
					return node
				}(),
			},
			initialCount: map[int]int{7: 1},
			threshold:    1,
			expected:     map[int]int{},
			expectError:  false,
		},
		{
			name: "error getting cluster nodes",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{GetNodesInfoError: fmt.Errorf("Error getting cluster nodes")}
						return client, nil
					})
					node.ID = "nodeId1"
					return node
				}(),
			},
			initialCount: map[int]int{},
			threshold:    1,
			expected:     nil,
			expectError:  true,
		},
		{
			name: "multiple open slots across nodes",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						// node1 is migrating slots 1 and 2 to node2 and node3 respectively
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{{ID: "nodeId1", IP: "10.0.0.1", Migrating: map[int]string{1: "nodeId2", 2: "nodeId3"}}},
						}
						return client, nil
					})
					node.ID = "nodeId1"
					node.IP = "10.0.0.1"
					return node
				}(),
				"node2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{MockNodesInfo: []redis.RedisNode{{ID: "nodeId2", IP: "10.0.0.2"}}}
						return client, nil
					})
					node.ID = "nodeId2"
					node.IP = "10.0.0.2"
					return node
				}(),
				"node3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node3", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						client := redis.MockRedisClient{MockNodesInfo: []redis.RedisNode{{ID: "nodeId3", IP: "10.0.0.3"}}}
						return client, nil
					})
					node.ID = "nodeId3"
					node.IP = "10.0.0.3"
					return node
				}(),
			},
			initialCount: map[int]int{1: 1, 2: 0},
			threshold:    1,
			// slot 1 already had count 1 -> threshold reached -> stabilized; slot 2 will be incremented to 1
			expected:    map[int]int{2: 1},
			expectError: false,
		},
		{
			name: "partial stabilize slot error",
			nodes: map[string]*redis.RedisNode{
				"node1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node1", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						// node1 has a migrating slot to node2
						client := redis.MockRedisClient{
							MockNodesInfo: []redis.RedisNode{{ID: "nodeId1", IP: "10.1.0.1", Migrating: map[int]string{11: "nodeId2"}}},
							// StabilizeSlot will fail on the node1
							StabilizeSlotError: fmt.Errorf("stabilize from failed"),
						}
						return client, nil
					})
					node.ID = "nodeId1"
					node.IP = "10.1.0.1"
					return node
				}(),
				"node2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("node2", func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
						// node2 returns success for StabilizeSlot
						client := redis.MockRedisClient{MockNodesInfo: []redis.RedisNode{{ID: "nodeId2", IP: "10.1.0.2"}}}
						return client, nil
					})
					node.ID = "nodeId2"
					node.IP = "10.1.0.2"
					return node
				}(),
			},
			initialCount: map[int]int{11: 1},
			threshold:    1,
			// Even though from.StabilizeSlot will error, stabilizeOpenSlots should log the error and continue.
			expected:    map[int]int{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewFakeRedKeyCluster(
				context.Background(),
				&config.Configuration{Redis: config.RedisConfig{Cluster: config.RedKeyClusterConfig{MaxRetries: 1, BackOff: time.Microsecond * 10}}},
				"Ready",
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			)

			updated, err := cluster.stabilizeOpenSlots(context.Background(), tt.initialCount, tt.threshold)

			if tt.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, updated)
		})
	}
}

func TestRedKeyClusterGetClient(t *testing.T) {
	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{
		{
			name:  "empty nodes falls back to cluster address",
			nodes: map[string]*redis.RedisNode{},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				// Verify that we receive the cluster address when no nodes exist
				assert.Equal(t, "test-cluster", addr)
				return &redis.MockRedisClient{}, nil
			},
			expectedError: nil,
		},
		{
			name:  "error from client factory when nodes empty",
			nodes: map[string]*redis.RedisNode{},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				return nil, fmt.Errorf("connection failed")
			},
			expectedError: fmt.Errorf("connection failed"),
		},
		{
			name: "uses node with addr when available",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("test-cluster-0", mockClientFactory)
					node.Addr = "redis-node-0.example.com:6379"
					return node
				}(),
			},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				// Should receive the node's address, not the cluster address
				assert.Equal(t, "redis-node-0.example.com:6379", addr)
				return &redis.MockRedisClient{}, nil
			},
			expectedError: nil,
		},
		{
			name: "falls back to cluster address when nodes have no addr",
			nodes: map[string]*redis.RedisNode{
				"test-cluster-0": redis.NewFakeRedisNode("test-cluster-0", mockClientFactory),
			},
			clientFactory: func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
				// Should fall back to cluster address
				assert.Equal(t, "test-cluster", addr)
				return &redis.MockRedisClient{}, nil
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				tt.nodes,
				make(map[string][]RedisOperation),
				make(chan struct{}, 1),
			).WithClientFactory(tt.clientFactory)

			client, err := cluster.getClient()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, client)
			}
		})
	}
}

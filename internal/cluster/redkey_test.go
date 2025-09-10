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
	node1.MasterID = ""

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
	node2.MasterID = ""

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
	node3.MasterID = "1234567890"
}

var redkeyCluster = NewFakeRedKeyCluster(
	context.TODO(),
	&config.Configuration{
		Redis: config.RedisConfig{
			Cluster: config.RedKeyClusterConfig{
				Status:                   "Ready",
				Replicas:                 3,
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
	assert.Equal(t, redkeyCluster.GetStatus(), "Ready")
	assert.Equal(t, redkeyCluster.GetReplicas(), 3)
	assert.Equal(t, redkeyCluster.GetReplicasPerMaster(), 0)
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
	assert.Equal(t, redkeyCluster.GetNode("test-0"), node1)
	assert.Nil(t, redkeyCluster.GetNode("node4"))

	nodes := redkeyCluster.GetNodes()
	assert.Len(t, nodes, 3)
	assert.Contains(t, nodes, node1)
	assert.Contains(t, nodes, node2)
	assert.Contains(t, nodes, node3)

	masterNodes := redkeyCluster.GetMasterNodes()
	assert.Len(t, masterNodes, 2)
	assert.Contains(t, masterNodes, node1)
	assert.Contains(t, masterNodes, node2)

	replicaNodes := redkeyCluster.GetReplicaNodes()
	assert.Len(t, replicaNodes, 1)
	assert.Contains(t, replicaNodes, node3)

	replicasOfMaster := redkeyCluster.GetReplicasOfNode(node1)
	assert.Len(t, replicasOfMaster, 1)
	assert.Contains(t, replicasOfMaster, node3)
	replicasOfMaster = redkeyCluster.GetReplicasOfNode(node2)
	assert.Len(t, replicasOfMaster, 0)
	replicasOfMaster = redkeyCluster.GetReplicasOfNode(node3)
	assert.Len(t, replicasOfMaster, 0)
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
	assert.False(t, redkeyCluster.IsBalanced())
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
	}{
		{
			name:          "remove node",
			nodeName:      "node4",
			expectedNodes: 3,
		},
		{
			name:          "remove non-existing node",
			nodeName:      "node4",
			expectedNodes: 3,
			expectedError: fmt.Errorf("node node4 not found"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.removeNode(tt.nodeName)
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
		expectedError error
	}{
		{
			name:          "get bad redis client",
			expectedError: fmt.Errorf("failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.refreshNodes()

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
		expectedError error
	}{
		{
			name:          "get bad redis client",
			expectedError: fmt.Errorf("failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.checkNodes()

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
			name:           "error getting redis client",
			cluster:        redkeyCluster, // Uses mockClientFactoryError which fails connection
			expectedResult: false,
			expectedError:  fmt.Errorf("error getting and checking Redis client: failed to connect after 1 retries"),
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
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedKeyClusterAsignMissingSlotsIfNeeded(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedKeyClusterBalanceNodesIfNeeded(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedKeyClusterFixClusterIfNeeded(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedKeyClusterAddNewNodesIfNeeded(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedKeyClusterRemoveNodesIfNeeded(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

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
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedKeyClusterGetNodesToRemove(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

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
				"master1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master1", mockClientFactoryMeetError)
					node.ID = "id1"
					node.Flags = "master"
					return node
				}(),
				"master2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master2", mockClientFactory)
					node.ID = "id2"
					node.Flags = "master"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: fmt.Errorf("error in ClusterMeet between 'master1' and 'master2': %w", meetNodeError),
		},
		{
			name: "refreshNodes fails",
			nodes: map[string]*redis.RedisNode{
				"master1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master1", mockClientFactory)
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
				"master1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master1", mockClientFactory)
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
				"master1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master1", mockClientFactory)
					node.ID = "id1"
					node.Flags = "master"
					return node
				}(),
				"master2": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master2", mockClientFactory)
					node.ID = "id2"
					node.Flags = "master"
					return node
				}(),
				"master3": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master3", mockClientFactory)
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
	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{}

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
	tests := []struct {
		name string
	}{
		{
			name: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedKeyClusterEnsureReplicaSpread(t *testing.T) {
	tests := []struct {
		name          string
		nodes         map[string]*redis.RedisNode
		clientFactory func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError error
	}{}

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
	tests := []struct {
		name           string
		nodesToConvert []*redis.RedisNode
		nodesToKeep    []*redis.RedisNode
		clientFactory  func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError  error
	}{}

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

func TestRedKeyClusterConvertNodesToMaster(t *testing.T) {
	tests := []struct {
		name           string
		nodesToConvert []*redis.RedisNode
		nodes          map[string]*redis.RedisNode
		clientFactory  func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error)
		expectedError  error
	}{}

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

			err := cluster.convertNodesToMaster(context.Background(), tt.nodesToConvert)

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
			name:          "node no master",
			node:          node3,
			expectedError: fmt.Errorf("node node3 is not a master"),
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
					n.MasterID = "1234567890"
					return n
				}(),
			},
			expectedError: fmt.Errorf("error promoting replica node-1 to master test-0: error creating client"),
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
					n.MasterID = "1234567890"
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
					n.MasterID = "1234567890"
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
			name: "no masters only replicas",
			nodes: map[string]*redis.RedisNode{
				"replica1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("replica1", mockClientFactory)
					node.ID = "replica1"
					node.Flags = "slave"
					node.MasterID = "master1"
					return node
				}(),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "AddSlots fails",
			nodes: map[string]*redis.RedisNode{
				"master1": func() *redis.RedisNode {
					node := redis.NewFakeRedisNode("master1", mockClientFactoryAddSlotsError)
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
				"master1": createNodeWithoutSlots("master1", "id1"),
			},
			clientFactory: mockClientFactoryRefreshError,
			expectedError: fmt.Errorf("error refreshing nodes info: %w", errRefresh),
		},
		{
			name: "single master no slots",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithoutSlots("master1", "id1"),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "multiple masters no slots",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithoutSlots("master1", "id1"),
				"master2": createNodeWithoutSlots("master2", "id2"),
				"master3": createNodeWithoutSlots("master3", "id3"),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "masters with partial slots",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithSlots("master1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5000}}),
				"master2": createNodeWithSlots("master2", "id2", []redis.RedisSlotRange{{Start: 5001, End: 10000}}),
				"master3": createNodeWithoutSlots("master3", "id3"), // This one needs slots
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "mixed masters some full some partial",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithSlots("master1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5461}}),    // Has enough slots (5462 slots)
				"master2": createNodeWithSlots("master2", "id2", []redis.RedisSlotRange{{Start: 5462, End: 8000}}), // Needs more slots
				"master3": createNodeWithoutSlots("master3", "id3"),                                                // Needs all slots
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "last master gets remaining slots",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithSlots("master1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5000}}),
				"master2": createNodeWithoutSlots("master2", "id2"), // Last master gets remaining slots
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "gaps in slot ranges",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithSlots("master1", "id1", []redis.RedisSlotRange{{Start: 0, End: 1000}, {Start: 2000, End: 3000}}),
				"master2": createNodeWithSlots("master2", "id2", []redis.RedisSlotRange{{Start: 5000, End: 6000}}),
				"master3": createNodeWithoutSlots("master3", "id3"),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "all masters have enough slots",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithSlots("master1", "id1", []redis.RedisSlotRange{{Start: 0, End: 5461}}),
				"master2": createNodeWithSlots("master2", "id2", []redis.RedisSlotRange{{Start: 5462, End: 10922}}),
				"master3": createNodeWithSlots("master3", "id3", []redis.RedisSlotRange{{Start: 10923, End: 16383}}),
			},
			clientFactory: mockClientFactory,
			expectedError: nil,
		},
		{
			name: "single master with all slots",
			nodes: map[string]*redis.RedisNode{
				"master1": createNodeWithSlots("master1", "id1", []redis.RedisSlotRange{{Start: 0, End: 16383}}),
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
		name           string
		close          bool
		expectedError  error
		expectedClient *redis.RedisClient
	}{
		{
			name:           "get bad redis client",
			close:          true,
			expectedError:  fmt.Errorf("failed to connect after 1 retries"),
			expectedClient: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := redkeyCluster.getAndCheckRedisClient(tt.close)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
				assert.Nil(t, client)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, client, tt.expectedClient)
			}
		})
	}
}

func TestRedKeyClusterInit(t *testing.T) {
	tests := []struct {
		name          string
		expectedError error
	}{
		{
			name:          "get bad redis client",
			expectedError: fmt.Errorf("Error refreshing nodes info: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.Init()

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
		name              string
		replicas          int
		replicasPerMaster *int
		expectedError     error
	}{
		{
			name:          "same replicas",
			replicas:      3,
			expectedError: &OperationCompletedError{Operation: "SetReplicas"},
		},
		{
			name:              "same replicas per master",
			replicas:          3,
			replicasPerMaster: getIntPointer(0),
			expectedError:     &OperationCompletedError{Operation: "SetReplicas"},
		},
		{
			name:          "less replicas",
			replicas:      2,
			expectedError: nil,
		},
		{
			name:              "more replicas per master",
			replicas:          2,
			replicasPerMaster: getIntPointer(1),
			expectedError:     nil,
		},
		{
			name:          "more replicas",
			replicas:      4,
			expectedError: nil,
		},
		{
			name:              "less replicas per master",
			replicas:          4,
			replicasPerMaster: getIntPointer(0),
			expectedError:     nil,
		},
		{
			name:              "both replicas and replicas per master",
			replicas:          3,
			replicasPerMaster: getIntPointer(1),
			expectedError:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redkeyCluster.SetReplicas(tt.replicas, tt.replicasPerMaster)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, redkeyCluster.GetReplicas(), tt.replicas)

			if tt.replicasPerMaster != nil {
				assert.Equal(t, redkeyCluster.GetReplicasPerMaster(), *tt.replicasPerMaster)
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
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "rebalancing",
			prepareTest: func() {
				redkeyCluster.operations[Rebalancing] = []RedisOperation{
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Running", time.Time{}),
				}
			},
			expectedError: &OperationInProgressError{Operation: "Rebalance"},
		},
		{
			name: "rebalanced",
			prepareTest: func() {
				redkeyCluster.operations[Rebalancing] = []RedisOperation{
					NewFakeRedisOperationRebalance(t.Context(), redkeyCluster, "Finished", time.Time{}),
				}
			},
			expectedError: &OperationCompletedError{Operation: "Rebalance"},
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redkeyCluster.operations[Rebalancing] = []RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error ensuring nodes are up: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.Rebalance(true, nil, tt.force)

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
		name          string
		prepareTest   func()
		from          *redis.RedisNode
		to            *redis.RedisNode
		expectedError error
	}{
		{
			name: "moving",
			prepareTest: func() {
				redkeyCluster.operations[Resharding] = []RedisOperation{
					NewFakeRedisOperationMove(t.Context(), redkeyCluster, "Running", node1, node3, 10, time.Time{}),
				}
			},
			from:          node1,
			to:            node3,
			expectedError: &OperationInProgressError{Operation: "Resharding"},
		},
		{
			name: "origin has no slots",
			prepareTest: func() {
				redkeyCluster.operations[Resharding] = []RedisOperation{}
				node1.Slots = []redis.RedisSlotRange{}
			},
			from:          node1,
			to:            node3,
			expectedError: &OperationCompletedError{Operation: "Resharding", Reason: "Origin node has no slots"},
		},
		{
			name: "node is a replica",
			prepareTest: func() {
				redkeyCluster.operations[Resharding] = []RedisOperation{}
			},
			from:          node3,
			to:            node1,
			expectedError: &OperationCompletedError{Operation: "Resharding", Reason: "Origin node is a replica"},
		},
		{
			name: "node has replicas",
			prepareTest: func() {
				redkeyCluster.conf.Redis.Cluster.MaxRetries = 1
				redkeyCluster.operations[Resharding] = []RedisOperation{}
				node1.Slots = []redis.RedisSlotRange{
					{
						Start: 1,
						End:   5461,
					},
				}
				node1.ID = "1234567890"
				node1.Flags = "master"
				node3.MasterID = "1234567890"
				node3.Flags = "slave"

				redkeyCluster.nodes = map[string]*redis.RedisNode{
					"test-0": node1,
					"test-1": node2,
					"test-2": node3,
				}
				node1.MaxRetries = 1
				node1.Backoff = time.Microsecond * 10
				node3.MaxRetries = 1
				node3.Backoff = time.Microsecond * 10
			},
			from:          node1,
			to:            node3,
			expectedError: fmt.Errorf("error promoting replica of node 'test-0': error refreshing nodes info: failed to connect after 1 retries"),
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redkeyCluster.operations[Resharding] = []RedisOperation{}
				node2.MaxRetries = 1
				node2.Backoff = time.Microsecond * 10
				node3.MaxRetries = 1
				node3.Backoff = time.Microsecond * 10
			},
			from:          node2,
			to:            node3,
			expectedError: fmt.Errorf("error ensuring nodes are up: error creating client"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.MoveSlots(tt.from, tt.to, 10)

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
		name          string
		expectedError error
	}{
		{
			name:          "bad redis client",
			expectedError: fmt.Errorf("error getting and checking Redis client: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := redkeyCluster.Check()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterFix(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "fixing",
			prepareTest: func() {
				redkeyCluster.operations[Fixing] = []RedisOperation{
					NewFakeRedisOperationFix(t.Context(), redkeyCluster, "Running"),
				}
			},
			expectedError: &OperationInProgressError{Operation: "Fixing"},
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redkeyCluster.operations[Fixing] = []RedisOperation{}
				redkeyCluster.conf.Redis.Cluster.MaxRetries = 1
			},
			force:         true,
			expectedError: fmt.Errorf("error ensuring nodes are up: error creating client"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.Fix(true, tt.force)

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
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "checking integrity",
			prepareTest: func() {
				redkeyCluster.operations[CheckingIntegrity] = []RedisOperation{
					NewFakeRedisOperationCheckIntegrity(t.Context(), redkeyCluster, "Running"),
				}
			},
			expectedError: &OperationInProgressError{Operation: "CheckingIntegrity"},
		},
		{
			name: "good",
			prepareTest: func() {
				redkeyCluster.operations[CheckingIntegrity] = []RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error checking cluster integrity: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.CheckIntegrity(false, tt.force)

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
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "scaling up",
			prepareTest: func() {
				redkeyCluster.operations[ScalingUp] = []RedisOperation{
					NewFakeRedisOperationScaleUp(t.Context(), redkeyCluster, "Running"),
				}
			},
			expectedError: &OperationInProgressError{Operation: "ScaleUp"},
		},
		{
			name: "good",
			prepareTest: func() {
				redkeyCluster.operations[ScalingUp] = []RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error scaling up cluster: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.ScaleUp(tt.force)

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
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "scaling down",
			prepareTest: func() {
				redkeyCluster.operations[ScalingDown] = []RedisOperation{
					NewFakeRedisOperationScaleDown(t.Context(), redkeyCluster, "Running"),
				}
			},
			expectedError: &OperationInProgressError{Operation: "ScaleDown"},
		},
		{
			name: "good",
			prepareTest: func() {
				redkeyCluster.operations[ScalingDown] = []RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error scaling down cluster: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.ScaleDown(tt.force)

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
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "upgrading",
			prepareTest: func() {
				redkeyCluster.operations[Upgrading] = []RedisOperation{
					NewFakeRedisOperationUpgrade(t.Context(), redkeyCluster, "Running"),
				}
			},
			expectedError: &OperationInProgressError{Operation: "Upgrade"},
		},
		{
			name: "good",
			prepareTest: func() {
				redkeyCluster.operations[Upgrading] = []RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error upgrading cluster: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.Upgrade(tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedKeyClusterReset(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "resetting",
			prepareTest: func() {
				redkeyCluster.operations[Resetting] = []RedisOperation{
					NewFakeRedisOperationResetNode(t.Context(), redkeyCluster, "Running", node1),
				}
			},
			expectedError: &OperationInProgressError{Operation: "Resetting"},
		},
		{
			name: "good",
			prepareTest: func() {
				redkeyCluster.operations[Resetting] = []RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error resetting cluster node 'test-0': error creating client"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redkeyCluster.ResetNode(node1)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

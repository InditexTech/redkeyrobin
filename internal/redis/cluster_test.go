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
	Name:  "node1",
	ID:    "1234567890",
	Addr:  "node1",
	IP:    "1.1.1.1",
	Flags: "master",
	Slots: []RedisSlotRange{
		{
			Start: 1,
			End:   5461,
		},
	},
	MasterID: "",
}
var node2 = &RedisNode{
	Name:  "node2",
	ID:    "0987654321",
	Addr:  "node2",
	IP:    "2.2.2.2",
	Flags: "master",
	Slots: []RedisSlotRange{
		{
			Start: 5462,
			End:   10922,
		},
	},
	MasterID: "",
}
var node3 = &RedisNode{
	Name:  "node3",
	ID:    "0987654321",
	Addr:  "node2",
	IP:    "2.2.2.2",
	Flags: "master",
	Slots: []RedisSlotRange{
		{
			Start: 10923,
			End:   16384,
		},
	},
	MasterID: "",
}

var redisCluster = NewFakeRedisCluster(
	context.TODO(),
	&config.Configuration{
		Redis: config.RedisConfig{
			Cluster: config.RedisClusterConfig{
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
	assert.Equal(t, redisCluster.GetClusterBackOff(), time.Microsecond*10)
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

func TestRedisClusterAskers(t *testing.T) {
	assert.True(t, redisCluster.IsRebalancing())
	assert.False(t, redisCluster.HasBeenRebalanced())

	assert.False(t, redisCluster.IsResharding(*node1, *node2))
	assert.True(t, redisCluster.IsResharding(*node1, *node3))
	assert.True(t, redisCluster.HasBeenResharded(*node1, *node2))
	assert.False(t, redisCluster.HasBeenResharded(*node1, *node3))

	assert.False(t, redisCluster.IsFixing())
	assert.False(t, redisCluster.IsReconciling())
	assert.False(t, redisCluster.HasMissingSlots())
	assert.True(t, redisCluster.IsBalanced())
	assert.True(t, redisCluster.HasDesiredReplicas())
	assert.True(t, redisCluster.IsScaled())
}

func TestRedisClusterAddNode(t *testing.T) {
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
		name          string
		nodeName      string
		expectedNodes int
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
		name         string
		nodeID       string
		expectedNode *RedisNode
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
			node := redisCluster.GetNodeFromID(tt.nodeID)
			assert.Equal(t, node, tt.expectedNode)
		})
	}
}

func TestRedisClusterUpdateNodesInfo(t *testing.T) {
	tests := []struct {
		name      string
		nodesInfo []RedisNode
	}{
		{
			name: "update nodes info",
			nodesInfo: []RedisNode{
				{
					Name:  "node1",
					ID:    "1234567890",
					Addr:  "node1",
					IP:    "9.9.9.9",
					Flags: "master",
				},
				{
					Name:  "node2",
					ID:    "6666666666",
					Addr:  "node2",
					IP:    "change not affecting",
					Flags: "slave",
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
		name           string
		close          bool
		expectedError  error
		expectedClient *RedisClient
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
			client, err := redisCluster.getAndCheckRedisClient(tt.close)

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

func TestRedisClusterRefreshNodes(t *testing.T) {
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
			err := redisCluster.refreshNodes()

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
		name          string
		replicas      int
		expectedError error
	}{
		{
			name:          "same replicas",
			replicas:      3,
			expectedError: &OperationCompletedError{Operation: "SetReplicas"},
		},
		{
			name:          "less replicas",
			replicas:      2,
			expectedError: nil,
		},
		{
			name:          "more replicas",
			replicas:      4,
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
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "rebalancing",
			prepareTest: func() {
				redisCluster.operations[Rebalancing] = []*RedisOperation{
					{
						Name:   Rebalancing,
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
						Name:   Rebalancing,
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
			force:         true,
			expectedError: fmt.Errorf("error getting and checking Redis client: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.Rebalance(true, nil, tt.force)

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
		name           string
		cmd            *RedisCLICommand
		expectedStatus string
	}{
		{
			name:           "rebalancing error",
			cmd:            NewRedisCLICommand(t.Context(), "exit 1"),
			expectedStatus: RebalancingError,
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
				Name:   Rebalancing,
				Status: "Running",
				Cmd:    tt.cmd,
			}

			tt.cmd.cmd.Start()
			redisCluster.waitForRebalanceToFinish(operation)

			assert.Equal(t, redisCluster.GetStatus(), tt.expectedStatus)
		})
	}
}

func TestRedisClusterMoveSlots(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		from          *RedisNode
		to            *RedisNode
		expectedError error
	}{
		{
			name: "moving",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{
					{
						Name:     Resharding,
						Status:   "Running",
						NodeFrom: node1,
						NodeTo:   node3,
					},
				}
			},
			from:          node1,
			to:            node3,
			expectedError: &OperationInProgressError{Operation: "Resharding"},
		},
		{
			name: "moved",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{
					{
						Name:     Rebalancing,
						Status:   "Finished",
						NodeFrom: node1,
						NodeTo:   node3,
					},
				}
			},
			from:          node1,
			to:            node3,
			expectedError: &OperationCompletedError{Operation: "Resharding"},
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{}
			},
			from:          node2,
			to:            node3,
			expectedError: fmt.Errorf("error getting and checking Redis client: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.MoveSlots(tt.from, tt.to, 10)

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

func TestRedisClusterCheck(t *testing.T) {
	tests := []struct {
		name string
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

func TestRedisClusterFix(t *testing.T) {
	tests := []struct {
		name string
	}{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

		})
	}
}

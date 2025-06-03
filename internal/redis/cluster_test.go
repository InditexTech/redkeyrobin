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
	Name:  "test-0",
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
	Flags: "master, addr",
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
	Flags: "slave",
	Slots: []RedisSlotRange{
		{
			Start: 10923,
			End:   16384,
		},
	},
	MasterID: "1234567890",
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
	make(chan struct{}, 5),
)

func getIntPointer(val int) *int {
	return &val
}

func TestRedisClusterGetters(t *testing.T) {
	assert.Equal(t, redisCluster.GetRedisClusterStatus(), "Ready")
	assert.Equal(t, redisCluster.GetStatus(), "Ready")
	assert.Equal(t, redisCluster.GetReplicas(), 3)
	assert.Equal(t, redisCluster.GetReplicasPerMaster(), 0)
	assert.Equal(t, redisCluster.GetName(), "test")
	assert.Equal(t, redisCluster.GetNamespace(), "test")
	assert.Equal(t, redisCluster.GetAddress(), "test")
	assert.Equal(t, redisCluster.IsEphemeral(), false)
	assert.Equal(t, redisCluster.GetReconcilerInterval(), 10)
	assert.Equal(t, redisCluster.GetReconcilerOperationCleanupInterval(), 0)
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

	masterNodes := redisCluster.GetMasterNodes()
	assert.Len(t, masterNodes, 2)
	assert.Contains(t, masterNodes, node1)
	assert.Contains(t, masterNodes, node2)

	replicaNodes := redisCluster.GetReplicaNodes()
	assert.Len(t, replicaNodes, 1)
	assert.Contains(t, replicaNodes, node3)

	replicasOfMaster := redisCluster.GetReplicasOfNode(node1)
	assert.Len(t, replicasOfMaster, 1)
	assert.Contains(t, replicasOfMaster, node3)
	replicasOfMaster = redisCluster.GetReplicasOfNode(node2)
	assert.Len(t, replicasOfMaster, 0)
	replicasOfMaster = redisCluster.GetReplicasOfNode(node3)
	assert.Len(t, replicasOfMaster, 0)
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

func TestRedisClusterAskers(t *testing.T) {
	assert.True(t, redisCluster.IsBalanced())
	assert.True(t, redisCluster.IsRebalancing())

	assert.False(t, redisCluster.IsReshardingNodes(*node1, *node2))
	assert.True(t, redisCluster.IsReshardingNodes(*node1, *node3))
	assert.True(t, redisCluster.IsResharding())

	assert.False(t, redisCluster.IsFixing())
	assert.False(t, redisCluster.IsCheckingIntegrity())
	assert.False(t, redisCluster.IsScalingUp())
	assert.False(t, redisCluster.IsScalingDown())
	assert.False(t, redisCluster.IsUpgrading())

	assert.False(t, redisCluster.IsResettingNode(*node1))
	assert.False(t, redisCluster.IsResetting())

	assert.False(t, redisCluster.IsScaled())
	assert.False(t, redisCluster.IsUpgraded())
	assert.False(t, redisCluster.CanBeUpgraded())

	assert.False(t, redisCluster.HasBeenRebalanced())
	assert.False(t, redisCluster.HasMissingSlots())
	assert.False(t, redisCluster.HasDesiredReplicas())

	assert.True(t, redisCluster.HasBeenResharded(*node1, *node2))
	assert.False(t, redisCluster.HasBeenResharded(*node1, *node3))

	assert.True(t, redisCluster.HasNode("test-0"))
	assert.False(t, redisCluster.HasNode("notfound"))

	assert.True(t, redisCluster.NodeHasReplicas(node1))
	assert.False(t, redisCluster.NodeHasReplicas(node2))
	assert.False(t, redisCluster.NodeHasReplicas(node3))

	assert.False(t, redisCluster.needsUpscale())
	assert.False(t, redisCluster.needsDownscale())
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
			err := redisCluster.removeNode(tt.nodeName)
			assert.Len(t, redisCluster.nodes, tt.expectedNodes)
			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterForgetNode(t *testing.T) {
	tests := []struct {
		name          string
		node          *RedisNode
		expectedError error
	}{
		{
			name: "node does not exist",
			node: &RedisNode{
				Name: "node4",
			},
			expectedError: fmt.Errorf("node node4 not found"),
		},
		{
			name:          "get bad redis client",
			node:          node1,
			expectedError: fmt.Errorf("error forgetting node"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := redisCluster.forgetNode(t.Context(), *tt.node)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
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

func TestRedisClusterCheckNodes(t *testing.T) {
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
			err := redisCluster.checkNodes()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
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

func TestRedisClusterNeedsMeet(t *testing.T) {
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

func TestRedisClusterNeedsFix(t *testing.T) {
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

func TestRedisClusterMeetNodesIfNeeded(t *testing.T) {
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

func TestRedisClusterAsignMissingSlotsIfNeeded(t *testing.T) {
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

func TestRedisClusterBalanceNodesIfNeeded(t *testing.T) {
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

func TestRedisClusterFixClusterIfNeeded(t *testing.T) {
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

func TestRedisClusterAddNewNodesIfNeeded(t *testing.T) {
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

func TestRedisClusterRemoveNodesIfNeeded(t *testing.T) {
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

func TestRedisClusterRemoveSlotsFromNodes(t *testing.T) {
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

func TestRedisClusterForgetAndRemoveNodes(t *testing.T) {
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

func TestRedisClusterGetNodesToRemove(t *testing.T) {
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

func TestRedisClusterMeetNodes(t *testing.T) {
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

func TestRedisClusterRemoveOutdatedNodes(t *testing.T) {
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

func TestRedisClusterEnsureClusterRatio(t *testing.T) {
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

func TestRedisClusterEnsureReplicaSpread(t *testing.T) {
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

func TestRedisClusterConvertNodesToReplica(t *testing.T) {
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

func TestRedisClusterPromoteReplicasOfNode(t *testing.T) {
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

func TestRedisClusterConvertNodesToMaster(t *testing.T) {
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

func TestRedisClusterAssignMissingSlots(t *testing.T) {
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
			err := redisCluster.SetReplicas(tt.replicas, tt.replicasPerMaster)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, redisCluster.GetReplicas(), tt.replicas)

			if tt.replicasPerMaster != nil {
				assert.Equal(t, redisCluster.GetReplicasPerMaster(), *tt.replicasPerMaster)
			}
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
			expectedError: fmt.Errorf("error ensuring nodes are up: failed to connect after 1 retries"),
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
			name: "origin has no slots",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{}
				node1.Slots = []RedisSlotRange{}
			},
			from:          node1,
			to:            node3,
			expectedError: &OperationCompletedError{Operation: "Resharding", Reason: "Origin node has no slots"},
		},
		{
			name: "node is a replica",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{}
			},
			from:          node3,
			to:            node1,
			expectedError: &OperationCompletedError{Operation: "Resharding", Reason: "Origin node is a replica"},
		},
		{
			name: "node has replicas",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{}
				node1.Slots = []RedisSlotRange{
					{
						Start: 1,
						End:   5461,
					},
				}
				node1.ID = "1234567890"
				node1.Flags = "master"
				node3.MasterID = "1234567890"
				node3.Flags = "slave"

				redisCluster.nodes = map[string]*RedisNode{
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
			expectedError: fmt.Errorf("error promoting replica of node 'test-0': failed to connect after 1 retries"),
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redisCluster.operations[Resharding] = []*RedisOperation{}
			},
			from:          node2,
			to:            node3,
			expectedError: fmt.Errorf("error ensuring nodes are up: failed to connect after 1 retries"),
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

func TestRedisClusterCheck(t *testing.T) {
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
			_, err := redisCluster.Check()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterFix(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "fixing",
			prepareTest: func() {
				redisCluster.operations[Fixing] = []*RedisOperation{
					{
						Name:   Fixing,
						Status: "Running",
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "Fixing"},
		},
		{
			name: "bad redis client",
			prepareTest: func() {
				redisCluster.operations[Fixing] = []*RedisOperation{
					{
						Name:   Fixing,
						Status: "Running",
						Cmd:    NewRedisLibraryCommand(t.Context(), func(ctx context.Context) error { return nil }),
					},
				}
			},
			force:         true,
			expectedError: fmt.Errorf("error ensuring nodes are up: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.Fix(true, tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterCheckIntegrity(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "checking integrity",
			prepareTest: func() {
				redisCluster.operations[CheckingIntegrity] = []*RedisOperation{
					{
						Name:   CheckingIntegrity,
						Status: "Running",
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "CheckingIntegrity"},
		},
		{
			name: "good",
			prepareTest: func() {
				redisCluster.operations[CheckingIntegrity] = []*RedisOperation{
					{
						Name:   CheckingIntegrity,
						Status: "Running",
						Cmd:    NewRedisLibraryCommand(t.Context(), func(ctx context.Context) error { return nil }),
					},
				}
			},
			force:         true,
			expectedError: fmt.Errorf("error checking cluster integrity: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.CheckIntegrity(false, tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterScaleUp(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "scaling up",
			prepareTest: func() {
				redisCluster.operations[ScalingUp] = []*RedisOperation{
					{
						Name:   ScalingUp,
						Status: "Running",
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "ScaleUp"},
		},
		{
			name: "good",
			prepareTest: func() {
				redisCluster.operations[ScalingUp] = []*RedisOperation{
					{
						Name:   ScalingUp,
						Status: "Running",
						Cmd:    NewRedisLibraryCommand(t.Context(), func(ctx context.Context) error { return nil }),
					},
				}
			},
			force:         true,
			expectedError: fmt.Errorf("error scaling up cluster: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.ScaleUp(tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterScaleDown(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "scaling down",
			prepareTest: func() {
				redisCluster.operations[ScalingDown] = []*RedisOperation{
					{
						Name:   ScalingDown,
						Status: "Running",
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "ScaleDown"},
		},
		{
			name: "good",
			prepareTest: func() {
				redisCluster.operations[ScalingDown] = []*RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error scaling down cluster: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.ScaleDown(tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterUpgrade(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "upgrading",
			prepareTest: func() {
				redisCluster.operations[Upgrading] = []*RedisOperation{
					{
						Name:   Upgrading,
						Status: "Running",
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "Upgrade"},
		},
		{
			name: "good",
			prepareTest: func() {
				redisCluster.operations[Upgrading] = []*RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error upgrading cluster: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.Upgrade(tt.force)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRedisClusterReset(t *testing.T) {
	tests := []struct {
		name          string
		prepareTest   func()
		force         bool
		expectedError error
	}{
		{
			name: "resetting",
			prepareTest: func() {
				redisCluster.operations[Resetting] = []*RedisOperation{
					{
						Name:     Resetting,
						Status:   "Running",
						NodeFrom: node1,
					},
				}
			},
			expectedError: &OperationInProgressError{Operation: "Resetting"},
		},
		{
			name: "good",
			prepareTest: func() {
				redisCluster.operations[Resetting] = []*RedisOperation{}
			},
			force:         true,
			expectedError: fmt.Errorf("error resetting cluster node 'test-0': failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepareTest()
			err := redisCluster.ResetNode(node1)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

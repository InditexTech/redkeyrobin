// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNodeGetNumberOfSlots(t *testing.T) {
	tests := []struct {
		name          string
		node          *RedisNode
		expectedSlots int
	}{
		{
			name: "zero slots",
			node: &RedisNode{
				Slots: []RedisSlotRange{},
			},
			expectedSlots: 0,
		},
		{
			name: "one slot",
			node: &RedisNode{
				Slots: []RedisSlotRange{
					{
						Start: 1,
						End:   1,
					},
				},
			},
			expectedSlots: 1,
		},
		{
			name: "several slots",
			node: &RedisNode{
				Slots: []RedisSlotRange{
					{
						Start: 1,
						End:   10,
					},
					{
						Start: 20,
						End:   20,
					},
				},
			},
			expectedSlots: 11,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.node.GetNumberOfSlots()
			assert.Equal(t, tt.expectedSlots, actual)
		})
	}
}

func TestNodeSetters(t *testing.T) {
	node := NewFakeRedisNode("node1", nil)
	assert.Equal(t, "", node.ID)
	node.SetID("node-id-1")
	assert.Equal(t, "node-id-1", node.ID)

	assert.Equal(t, "", node.IP)
	node.SetIP("192.168.1.1")
	assert.Equal(t, "192.168.1.1", node.IP)
}

func TestNodeAskers(t *testing.T) {
	node := NewFakeRedisNode("node1", nil)
	assert.False(t, node.IsConnected())
	assert.False(t, node.IsDisconnected())

	node.LinkStatus = "connected"
	assert.True(t, node.IsConnected())
	assert.False(t, node.IsDisconnected())

	node.LinkStatus = "disconnected"
	assert.False(t, node.IsConnected())
	assert.True(t, node.IsDisconnected())

	assert.False(t, node.HasSlots())
	node.Slots = []RedisSlotRange{
		{
			Start: 1,
			End:   10,
		},
	}
	assert.True(t, node.HasSlots())
	node.ResetSlots()
	assert.False(t, node.HasSlots())

	assert.False(t, node.ShouldBeRemoved())
	node.Flags = "fail"
	assert.True(t, node.ShouldBeRemoved())
	node.Flags = "noaddr"
	assert.True(t, node.ShouldBeRemoved())
	node.Flags = "master"
	assert.False(t, node.ShouldBeRemoved())
}

func TestNodeIsMaster(t *testing.T) {
	tests := []struct {
		name     string
		node     *RedisNode
		expected bool
	}{
		{
			name: "master node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: true,
		},
		{
			name: "master node with flags",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master,noaddr,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: true,
		},
		{
			name: "slave node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "slave",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: false,
		},
		{
			name: "slave node with flags",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "fail,slave,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.node.IsMaster()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestNodeIsReplica(t *testing.T) {
	tests := []struct {
		name     string
		node     *RedisNode
		expected bool
	}{
		{
			name: "master node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: false,
		},
		{
			name: "master node with flags",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master,noaddr,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: false,
		},
		{
			name: "slave node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "slave",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: true,
		},
		{
			name: "slave node with flags",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "fail,slave,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.node.IsReplica()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestNodeInit(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		expectedError bool
		validateNode  func(*testing.T, *RedisNode)
	}{
		{
			name: "success initialization",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					MockMyID: "test-node-id-123",
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: false,
			validateNode: func(t *testing.T, node *RedisNode) {
				assert.Equal(t, "test-node-id-123", node.ID)
				assert.Equal(t, "127.0.0.1", node.IP)
			},
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
		{
			name: "get my id error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					GetMyIDError: fmt.Errorf("unable to get node ID"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
		{
			name: "invalid address error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					MockMyID: "test-node-id-123",
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "invalid-address-that-does-not-resolve",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.Init(context.Background())

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				if tt.validateNode != nil {
					tt.validateNode(t, node)
				}
			}
		})
	}
}

func TestNodeInitStandalone(t *testing.T) {
	tests := []struct {
		name          string
		node          *RedisNode
		expectedError bool
		validateNode  func(*testing.T, *RedisNode)
	}{
		{
			name: "valid standalone node",
			node: &RedisNode{
				Addr:  "127.0.0.1",
				Flags: "master",
			},
			expectedError: false,
			validateNode: func(t *testing.T, node *RedisNode) {
				assert.Equal(t, "127.0.0.1", node.IP)
			},
		},
		{
			name: "valid standalone node with hostname",
			node: &RedisNode{
				Addr:  "localhost",
				Flags: "master",
			},
			expectedError: false,
			validateNode: func(t *testing.T, node *RedisNode) {
				assert.NotEmpty(t, node.IP)
			},
		},
		{
			name: "invalid standalone node address",
			node: &RedisNode{
				Addr:  "invalid-address-that-does-not-resolve",
				Flags: "master",
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.node.InitStandalone(context.Background())

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				if tt.validateNode != nil {
					tt.validateNode(t, tt.node)
				}
			}
		})
	}
}

func TestNodeCheckConnection(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		expectedError bool
	}{
		{
			name: "successful connection check",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.CheckConnection(context.Background())

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestNodeUpdateInfo(t *testing.T) {
	tests := []struct {
		name   string
		node   *RedisNode
		update RedisNode
	}{
		{
			name: "zero slots",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			update: RedisNode{
				IP:    "bbb",
				Flags: "slave",
				Slots: []RedisSlotRange{
					{
						Start: 1,
						End:   1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.node.UpdateInfo(tt.update)
			assert.Equal(t, tt.node.IP, tt.update.IP)
			assert.Equal(t, tt.node.Flags, tt.update.Flags)
			assert.Equal(t, tt.node.Slots, tt.update.Slots)
			assert.Equal(t, tt.node.MasterID, tt.update.MasterID)
			assert.Equal(t, tt.node.Failures, tt.update.Failures)
		})
	}
}

func TestNodeGetClusterNodes(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		expectedNodes int
		expectedError bool
	}{
		{
			name: "successful get cluster nodes",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					MockNodesInfo: []RedisNode{
						{
							ID:         "node1",
							IP:         "127.0.0.1",
							Flags:      "master",
							Slots:      []RedisSlotRange{{Start: 0, End: 5461}},
							LinkStatus: "connected",
						},
						{
							ID:         "node2",
							IP:         "127.0.0.2",
							Flags:      "master",
							Slots:      []RedisSlotRange{{Start: 5462, End: 10922}},
							LinkStatus: "connected",
						},
					},
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedNodes: 2,
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedNodes: 0,
			expectedError: true,
		},
		{
			name: "get nodes info error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					GetNodesInfoError: fmt.Errorf("cluster nodes command failed"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedNodes: 0,
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			nodes, err := node.GetClusterNodes(context.Background())

			if tt.expectedError {
				assert.NotNil(t, err)
				assert.Nil(t, nodes)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, tt.expectedNodes, len(nodes))
			}
		})
	}
}

func TestNodeReplicateNode(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		master        RedisNode
		expectedError bool
	}{
		{
			name: "successful replication",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "replica-node",
					Addr:          "127.0.0.2",
					clientFactory: factory,
				}
			},
			master: RedisNode{
				ID:    "master-node-id-123",
				IP:    "127.0.0.1",
				Flags: "master",
			},
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "replica-node",
					Addr:          "127.0.0.2",
					clientFactory: factory,
				}
			},
			master: RedisNode{
				ID:    "master-node-id-123",
				IP:    "127.0.0.1",
				Flags: "master",
			},
			expectedError: true,
		},
		{
			name: "cluster replicate error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					ClusterReplicateError: fmt.Errorf("replicate command failed"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "replica-node",
					Addr:          "127.0.0.2",
					clientFactory: factory,
				}
			},
			master: RedisNode{
				ID:    "master-node-id-123",
				IP:    "127.0.0.1",
				Flags: "master",
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.ReplicateNode(context.Background(), tt.master)

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestNodeReset(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		expectedError bool
	}{
		{
			name: "successful reset",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
		{
			name: "cluster reset error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					ClusterResetError: fmt.Errorf("reset command failed"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.Reset(context.Background())

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestNodeMeetNode(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		destination   RedisNode
		expectedError bool
	}{
		{
			name: "successful meet node",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			destination: RedisNode{
				ID:    "destination-node-id",
				IP:    "127.0.0.2",
				Flags: "master",
			},
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			destination: RedisNode{
				ID:    "destination-node-id",
				IP:    "127.0.0.2",
				Flags: "master",
			},
			expectedError: true,
		},
		{
			name: "cluster meet error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					ClusterMeetError: fmt.Errorf("meet command failed"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			destination: RedisNode{
				ID:    "destination-node-id",
				IP:    "127.0.0.2",
				Flags: "master",
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.MeetNode(context.Background(), tt.destination)

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestNodeForgetNode(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		destination   RedisNode
		expectedError bool
	}{
		{
			name: "successful forget node",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			destination: RedisNode{
				ID:    "destination-node-id",
				IP:    "127.0.0.2",
				Flags: "master",
			},
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			destination: RedisNode{
				ID:    "destination-node-id",
				IP:    "127.0.0.2",
				Flags: "master",
			},
			expectedError: true,
		},
		{
			name: "cluster forget error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					ClusterForgetError: fmt.Errorf("forget command failed"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			destination: RedisNode{
				ID:    "destination-node-id",
				IP:    "127.0.0.2",
				Flags: "master",
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.ForgetNode(context.Background(), tt.destination)

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestNodeAddSlots(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		slots         []int
		expectedError bool
	}{
		{
			name: "successful add single slot",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			slots:         []int{1},
			expectedError: false,
		},
		{
			name: "successful add multiple slots",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			slots:         []int{1, 2, 3, 4, 5},
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			slots:         []int{1, 2, 3},
			expectedError: true,
		},
		{
			name: "cluster add slots error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					ClusterAddSlotsError: fmt.Errorf("add slots command failed"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "test-node",
					Addr:          "127.0.0.1",
					clientFactory: factory,
				}
			},
			slots:         []int{1, 2, 3},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.AddSlots(context.Background(), tt.slots...)

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestNodeFailover(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func() *MockRedisClient
		setupNode     func(*MockRedisClient) *RedisNode
		expectedError bool
	}{
		{
			name: "successful failover",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "replica-node",
					Addr:          "127.0.0.2",
					Flags:         "slave",
					clientFactory: factory,
				}
			},
			expectedError: false,
		},
		{
			name: "client factory error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return nil, fmt.Errorf("connection failed")
				}
				return &RedisNode{
					Name:          "replica-node",
					Addr:          "127.0.0.2",
					Flags:         "slave",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
		{
			name: "cluster failover error",
			setupMock: func() *MockRedisClient {
				return &MockRedisClient{
					ClusterFailoverError: fmt.Errorf("failover command failed"),
				}
			},
			setupNode: func(mockClient *MockRedisClient) *RedisNode {
				factory := func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (RedisClientInterface, error) {
					return mockClient, nil
				}
				return &RedisNode{
					Name:          "replica-node",
					Addr:          "127.0.0.2",
					Flags:         "slave",
					clientFactory: factory,
				}
			},
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := tt.setupMock()
			node := tt.setupNode(mockClient)

			err := node.Failover(context.Background())

			if tt.expectedError {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestNodeHasFlag(t *testing.T) {
	tests := []struct {
		name     string
		node     *RedisNode
		flag     string
		expected bool
	}{
		{
			name: "master node has master flag",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag:     "master",
			expected: true,
		},
		{
			name: "master node with multiple flags has master flag",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master,noaddr,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag:     "master",
			expected: true,
		},
		{
			name: "slave node does not have master flag",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "slave",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag:     "master",
			expected: false,
		},
		{
			name: "node with multiple flags has fail flag",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "fail,slave,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag:     "fail",
			expected: true,
		},
		{
			name: "node with multiple flags has noaddr flag",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master,noaddr,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag:     "noaddr",
			expected: true,
		},
		{
			name: "node does not have requested flag",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag:     "slave",
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.node.hasFlag(tt.flag)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

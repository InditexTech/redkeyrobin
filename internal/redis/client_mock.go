package redis

import (
	"context"
	"time"
)

// MockClientProvider mock implementation of ClientProvider for testing
type MockClientProvider struct {
	mockClient     RedisClientInterface
	getClientError error
}

// NewMockClientProvider creates a new MockClientProvider with the given mock client
func NewMockClientProvider(mockClient RedisClientInterface) *MockClientProvider {
	return &MockClientProvider{
		mockClient: mockClient,
	}
}

// SetGetClientError configures the provider to return an error on getClient calls
func (mcp *MockClientProvider) SetGetClientError(err error) {
	mcp.getClientError = err
}

// MockRedisClient is a mock implementation of RedisClient for testing
type MockRedisClient struct {
	// Error control fields for each method
	CloseError             error
	CheckConnectionError   error
	GetInfoError           error
	GetClusterInfoError    error
	GetNodesInfoError      error
	GetMyIDError           error
	ClusterForgetNodeError error
	ClusterMeetError       error
	ClusterReplicateError  error
	ClusterResetError      error
	ClusterForgetError     error
	ClusterAddSlotsError   error
	ClusterFailoverError   error
	ClusterCheckError      error
	ClusterFixError        error
	ClusterRebalanceError  error
	ReshardNodeError       error

	// Mock data to return when no error
	MockRedisInfo    *RedisInfo
	MockClusterInfo  *ClusterInfo
	MockNodesInfo    []RedisNode
	MockMyID         string
	MockClusterCheck *ClusterCheckResult
}

// Close mocks the Close method
func (m MockRedisClient) Close() error {
	return m.CloseError
}

// CheckConnection mocks the CheckConnection method
func (m MockRedisClient) CheckConnection(maxRetries int, backoff time.Duration) error {
	return m.CheckConnectionError
}

// GetInfo mocks the GetInfo method
func (m MockRedisClient) GetInfo() (*RedisInfo, error) {
	if m.GetInfoError != nil {
		return nil, m.GetInfoError
	}
	if m.MockRedisInfo != nil {
		return m.MockRedisInfo, nil
	}
	// Return default mock data
	return &RedisInfo{
		Server:       make(map[string]string),
		Clients:      make(map[string]int64),
		Memory:       make(map[string]string),
		Persistence:  make(map[string]string),
		Stats:        make(map[string]string),
		Replication:  make(map[string]string),
		CPU:          make(map[string]float64),
		Cluster:      make(map[string]string),
		Keyspace:     make(map[string]string),
		CommandStats: make(map[string]string),
		ErrorStats:   make(map[string]string),
		LatencyStats: make(map[string]string),
	}, nil
}

// GetClusterInfo mocks the GetClusterInfo method
func (m MockRedisClient) GetClusterInfo() (*ClusterInfo, error) {
	if m.GetClusterInfoError != nil {
		return nil, m.GetClusterInfoError
	}
	if m.MockClusterInfo != nil {
		return m.MockClusterInfo, nil
	}
	// Return default mock data
	return &ClusterInfo{
		State:                        "ok",
		SlotsAssigned:                16384,
		SlotsOK:                      16384,
		SlotsPFail:                   0,
		SlotsFail:                    0,
		KnownNodes:                   3,
		ClusterSize:                  3,
		CurrentEpoch:                 1,
		MyEpoch:                      1,
		MessagesPingSent:             100,
		MessagesPongSent:             100,
		MessagesMeetSent:             0,
		MessagesSent:                 200,
		MessagesPingReceived:         100,
		MessagesPongReceived:         100,
		MessagesMeetReceived:         0,
		MessagesReceived:             200,
		TotalClusterLinksBufferLimit: 0,
		MessagesUpdateSent:           0,
		MessagesUpdateReceived:       0,
		MessagesFailReceived:         0,
	}, nil
}

// GetNodesInfo mocks the GetNodesInfo method
func (m MockRedisClient) GetNodesInfo() ([]RedisNode, error) {
	if m.GetNodesInfoError != nil {
		return nil, m.GetNodesInfoError
	}
	if m.MockNodesInfo != nil {
		return m.MockNodesInfo, nil
	}
	// Return default mock data
	return []RedisNode{
		{
			ID:         "node1",
			IP:         "127.0.0.1",
			Flags:      "master",
			Slots:      []RedisSlotRange{{Start: 0, End: 5461}},
			MasterID:   "-",
			Failures:   0,
			Sent:       100,
			Recv:       100,
			LinkStatus: "connected",
		},
	}, nil
}

// GetMyID mocks the GetMyID method
func (m MockRedisClient) GetMyID() (string, error) {
	if m.GetMyIDError != nil {
		return "", m.GetMyIDError
	}
	if m.MockMyID != "" {
		return m.MockMyID, nil
	}
	return "mock-node-id", nil
}

// ClusterForgetNode mocks the ClusterForgetNode method
func (m MockRedisClient) ClusterForgetNode(nodeID string) error {
	return m.ClusterForgetNodeError
}

// ClusterMeet mocks the ClusterMeet method
func (m MockRedisClient) ClusterMeet(ip string, port int) error {
	return m.ClusterMeetError
}

// ClusterReplicate mocks the ClusterReplicate method
func (m MockRedisClient) ClusterReplicate(nodeID string) error {
	return m.ClusterReplicateError
}

// ClusterReset mocks the ClusterReset method
func (m MockRedisClient) ClusterReset(hard bool) error {
	return m.ClusterResetError
}

// ClusterForget mocks the ClusterForget method
func (m MockRedisClient) ClusterForget(nodeID string) error {
	return m.ClusterForgetError
}

// ClusterAddSlots mocks the ClusterAddSlots method
func (m MockRedisClient) ClusterAddSlots(slots ...int) error {
	return m.ClusterAddSlotsError
}

// ClusterFailover mocks the ClusterFailover method
func (m MockRedisClient) ClusterFailover() error {
	return m.ClusterFailoverError
}

// ClusterCheck mocks the ClusterCheck method
func (m MockRedisClient) ClusterCheck(ctx context.Context) (*ClusterCheckResult, error) {
	if m.ClusterCheckError != nil {
		return nil, m.ClusterCheckError
	}
	if m.MockClusterCheck != nil {
		return m.MockClusterCheck, nil
	}
	// Return default mock data
	return &ClusterCheckResult{
		CommandCodeOutput: 0,
		Errors:            []string{},
		Warnings:          []string{},
	}, nil
}

// ClusterFix mocks the ClusterFix method
func (m MockRedisClient) ClusterFix(ctx context.Context) *RedisCLICommand {
	if m.ClusterFixError != nil {
		cmd := &RedisCLICommand{}
		cmd.Err = m.ClusterFixError
		return cmd
	}
	// Return a successful command by default
	command := NewRedisCLICommand(ctx, "exit 0")
	command.Start()
	return command
}

// ClusterRebalance mocks the ClusterRebalance method
func (m MockRedisClient) ClusterRebalance(ctx context.Context, weights map[string]int) *RedisCLICommand {
	if m.ClusterRebalanceError != nil {
		cmd := &RedisCLICommand{}
		cmd.Err = m.ClusterRebalanceError
		return cmd
	}
	// Return a successful command by default
	command := NewRedisCLICommand(ctx, "exit 0")
	command.Start()
	return command
}

// ReshardNode mocks the ReshardNode method
func (m MockRedisClient) ReshardNode(ctx context.Context, source, target RedisNode, slots int) *RedisCLICommand {
	if m.ReshardNodeError != nil {
		cmd := &RedisCLICommand{}
		cmd.Err = m.ReshardNodeError
		return cmd
	}

	// Return a successful command by default
	command := NewRedisCLICommand(ctx, "exit 0")
	command.Start()
	return command
}

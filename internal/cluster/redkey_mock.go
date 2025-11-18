// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"

	"github.com/inditextech/redkeyrobin/internal/redis"
)

// MockRedKeyCluster wraps a RedKeyCluster and allows mocking specific methods
type MockRedKeyCluster struct {
	*RedKeyCluster

	// Error control fields
	EnsureNodesAreUpError           error
	ConvertNodesToMasterError       error
	GetAndCheckRedisClientError     error
	RefreshNodesError               error
	CheckNodesError                 error
	RemoveOutdatedNodesError        error
	MeetNodesIfNeededError          error
	EnsureClusterRatioError         error
	AssignMissingSlotsIfNeededError error
	FixClusterIfNeededError         error
	BalanceClusterIfNeededError     error
	RemoveNodesIfNeededError        error
	AddNewNodesIfNeededError        error
	ForgetNodeError                 error
	StabilizeOpenSlotsError         error

	// Behavior control fields
	IsEphemeralValue bool

	// Mock Redis client configuration
	MockRedisClient *redis.MockRedisClient
}

// NewMockRedKeyCluster creates a new mock RedKey cluster
func NewMockRedKeyCluster(baseCluster *RedKeyCluster) *MockRedKeyCluster {
	return &MockRedKeyCluster{
		RedKeyCluster:                   baseCluster,
		EnsureNodesAreUpError:           nil,
		ConvertNodesToMasterError:       nil,
		GetAndCheckRedisClientError:     nil,
		RefreshNodesError:               nil,
		CheckNodesError:                 nil,
		RemoveOutdatedNodesError:        nil,
		MeetNodesIfNeededError:          nil,
		EnsureClusterRatioError:         nil,
		AssignMissingSlotsIfNeededError: nil,
		FixClusterIfNeededError:         nil,
		BalanceClusterIfNeededError:     nil,
		RemoveNodesIfNeededError:        nil,
		AddNewNodesIfNeededError:        nil,
		ForgetNodeError:                 nil,

		// Behavior control fields
		IsEphemeralValue: false,
		MockRedisClient:  &redis.MockRedisClient{},
	}
}

// ensureNodesAreUp mocks the ensureNodesAreUp method
func (m *MockRedKeyCluster) ensureNodesAreUp(ctx context.Context) error {
	return m.EnsureNodesAreUpError
}

// convertNodesToMaster mocks the convertNodesToMaster method
func (m *MockRedKeyCluster) convertNodesToMaster(ctx context.Context, nodes []*redis.RedisNode) error {
	return m.ConvertNodesToMasterError
}

// getRedisClient mocks the getRedisClient method and returns our configured mock client
func (m *MockRedKeyCluster) getRedisClient(close bool) (redis.RedisClientInterface, error) {
	if m.GetAndCheckRedisClientError != nil {
		return nil, m.GetAndCheckRedisClientError
	}

	// Return the configured mock client
	return m.MockRedisClient, nil
}

// refreshNodesInfo mocks the refreshNodesInfo method
func (m *MockRedKeyCluster) refreshNodesInfo() error {
	return m.RefreshNodesError
}

// checkNodes mocks the checkNodes method
// refreshNodesList indicates whether to refresh the internal nodes list or not
func (m *MockRedKeyCluster) checkNodes(refreshNodesList bool) error {
	return m.CheckNodesError
}

// removeOutdatedNodes mocks the removeOutdatedNodes method
func (m *MockRedKeyCluster) removeOutdatedNodes(ctx context.Context) error {
	return m.RemoveOutdatedNodesError
}

// meetNodesIfNeeded mocks the meetNodesIfNeeded method
func (m *MockRedKeyCluster) meetNodesIfNeeded(ctx context.Context) error {
	return m.MeetNodesIfNeededError
}

// ensureClusterRatio mocks the ensureClusterRatio method
func (m *MockRedKeyCluster) ensureClusterRatio(ctx context.Context) error {
	return m.EnsureClusterRatioError
}

// assignMissingSlotsIfNeeded mocks the assignMissingSlotsIfNeeded method
func (m *MockRedKeyCluster) assignMissingSlotsIfNeeded(ctx context.Context) error {
	return m.AssignMissingSlotsIfNeededError
}

// fixClusterIfNeeded mocks the fixClusterIfNeeded method
func (m *MockRedKeyCluster) fixClusterIfNeeded(ctx context.Context) error {
	return m.FixClusterIfNeededError
}

// balanceClusterIfNeeded mocks the balanceClusterIfNeeded method
func (m *MockRedKeyCluster) balanceClusterIfNeeded(weights map[string]int) error {
	return m.BalanceClusterIfNeededError
}

// stabilizeOpenSlots mocks the stabilizeOpenSlots method
func (m *MockRedKeyCluster) stabilizeOpenSlots(ctx context.Context, counter map[int]int, threshold int) (map[int]int, error) {
	return nil, m.StabilizeOpenSlotsError
}

// SetRedisClientError configures an error for a specific Redis client method
func (m *MockRedKeyCluster) SetRedisClientError(method string, err error) {
	switch method {
	case "Close":
		m.MockRedisClient.CloseError = err
	case "CheckConnection":
		m.MockRedisClient.CheckConnectionError = err
	case "GetInfo":
		m.MockRedisClient.GetInfoError = err
	case "GetClusterInfo":
		m.MockRedisClient.GetClusterInfoError = err
	case "GetNodesInfo":
		m.MockRedisClient.GetNodesInfoError = err
	case "GetMyID":
		m.MockRedisClient.GetMyIDError = err
	case "ClusterForgetNode":
		m.MockRedisClient.ClusterForgetNodeError = err
	case "ClusterMeet":
		m.MockRedisClient.ClusterMeetError = err
	case "ClusterReplicate":
		m.MockRedisClient.ClusterReplicateError = err
	case "ClusterReset":
		m.MockRedisClient.ClusterResetError = err
	case "ClusterForget":
		m.MockRedisClient.ClusterForgetError = err
	case "ClusterAddSlots":
		m.MockRedisClient.ClusterAddSlotsError = err
	case "ClusterFailover":
		m.MockRedisClient.ClusterFailoverError = err
	case "ClusterCheck":
		m.MockRedisClient.ClusterCheckError = err
	case "ClusterFix":
		m.MockRedisClient.ClusterFixError = err
	case "ClusterRebalance":
		m.MockRedisClient.ClusterRebalanceError = err
	case "ReshardNode":
		m.MockRedisClient.ReshardNodeError = err
	case "StabilizeSlot":
		m.MockRedisClient.StabilizeSlotError = err
	}
}

// SetRedisClientMockData configures mock data for Redis client methods
func (m *MockRedKeyCluster) SetRedisClientMockData(mockInfo *redis.RedisInfo, mockClusterInfo *redis.ClusterInfo, mockNodes []redis.RedisNode, mockID string, mockClusterCheck *redis.ClusterCheckResult) {
	if mockInfo != nil {
		m.MockRedisClient.MockRedisInfo = mockInfo
	}
	if mockClusterInfo != nil {
		m.MockRedisClient.MockClusterInfo = mockClusterInfo
	}
	if mockNodes != nil {
		m.MockRedisClient.MockNodesInfo = mockNodes
	}
	if mockID != "" {
		m.MockRedisClient.MockMyID = mockID
	}
	if mockClusterCheck != nil {
		m.MockRedisClient.MockClusterCheck = mockClusterCheck
	}
}

// removeNodesIfNeeded mock implementation
func (m *MockRedKeyCluster) removeNodesIfNeeded(ctx context.Context) error {
	if m.RemoveNodesIfNeededError != nil {
		return m.RemoveNodesIfNeededError
	}
	return m.RedKeyCluster.removeNodesIfNeeded(ctx)
}

// addNewNodesIfNeeded mock implementation
func (m *MockRedKeyCluster) addNewNodesIfNeeded() error {
	if m.AddNewNodesIfNeededError != nil {
		return m.AddNewNodesIfNeededError
	}
	return m.RedKeyCluster.addNewNodesIfNeeded()
}

// IsEphemeral mock implementation
func (m *MockRedKeyCluster) IsEphemeral() bool {
	return m.IsEphemeralValue
}

// SetEphemeral configures the ephemeral behavior for testing
func (m *MockRedKeyCluster) SetEphemeral(ephemeral bool) {
	m.IsEphemeralValue = ephemeral
}

// forgetNode mock implementation
func (m *MockRedKeyCluster) forgetNode(ctx context.Context, node redis.RedisNode) error {
	if m.ForgetNodeError != nil {
		return m.ForgetNodeError
	}
	return m.RedKeyCluster.forgetNode(ctx, node)
}

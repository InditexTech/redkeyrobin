// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/inditextech/redkeyrobin/internal/cluster"
	"github.com/stretchr/testify/assert"
)

func TestGetRedKeyClusterStatus(t *testing.T) {
	tests := []struct {
		name               string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: RedKeyClusterStatusResponse{
				Status: "Ready",
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(nil, 0, 0)
			testRequest(t, "GET", "/redkeycluster/status", "", "", nil, server.GetRedKeyClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestUpdateRedKeyClusterStatus(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name:    "bad request",
			request: "{",
			expectedBody: ErrorResponse{
				Error: "Invalid request: unexpected EOF",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "invalid request",
			request: `{"status": "Invalid"}`,
			expectedBody: ErrorResponse{
				Error: "Invalid request: invalid status 'Invalid'",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "good request",
			request: `{"status": "Ready"}`,
			expectedBody: RedKeyClusterStatusResponse{
				Status: "Ready",
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(nil, 0, 0)
			testRequest(t, "PUT", "/redkeycluster/status", tt.request, "", nil, server.UpdateRedKeyClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestGetRedKeyClusterReplicas(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: ClusterReplicasResponse{
				Primaries:          0,
				ReplicasPerPrimary: 0,
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(nil, 0, 0)
			testRequest(t, "GET", "/redkeycluster/replicas", tt.request, "", nil, server.GetRedKeyClusterReplicas, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestUpdateRedKeyClusterReplicas(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		primaries          int
		replicasPerPrimary int
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name:               "invalid request",
			request:            `{"primaries": -1}`,
			primaries:          0,
			replicasPerPrimary: 0,
			expectedBody: ErrorResponse{
				Error: "Invalid request: 'primaries' must be positive",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:               "invalid request replicas per primary",
			request:            `{"primaries": 0, "replicas_per_primary": -1}`,
			primaries:          0,
			replicasPerPrimary: 0,
			expectedBody: ErrorResponse{
				Error: "Invalid request: 'replicas_per_primary' must be positive",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:               "same primaries and replicas per primary",
			request:            `{"primaries": 0}`,
			primaries:          0,
			replicasPerPrimary: 0,
			expectedBody: ClusterReplicasResponse{
				Primaries:          0,
				ReplicasPerPrimary: 0,
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "good request",
			request:            `{"primaries": 3}`,
			primaries:          0,
			replicasPerPrimary: 0,
			expectedBody: ClusterReplicasResponse{
				Primaries:          3,
				ReplicasPerPrimary: 0,
			},
			expectedStatusCode: http.StatusCreated,
		},
		{
			name:               "same primaries and replicas per primary with values",
			request:            `{"primaries": 3, "replicas_per_primary": 2}`,
			primaries:          3,
			replicasPerPrimary: 2,
			expectedBody: ClusterReplicasResponse{
				Primaries:          3,
				ReplicasPerPrimary: 2,
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "good request with replicas per primary",
			request:            `{"primaries": 3, "replicas_per_primary": 2}`,
			primaries:          0,
			replicasPerPrimary: 0,
			expectedBody: ClusterReplicasResponse{
				Primaries:          3,
				ReplicasPerPrimary: 2,
			},
			expectedStatusCode: http.StatusCreated,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(nil, tt.primaries, tt.replicasPerPrimary)
			testRequest(t, "PUT", "/redkeycluster/replicas", tt.request, "", nil, server.UpdateRedKeyClusterReplicas, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestGetClusterStatus(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: ClusterStatusResponse{
				Status: "Unknown",
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(nil, 0, 0)
			testRequest(t, "GET", "/cluster/status", tt.request, "", nil, server.GetClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestMoveNodeSlots(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name:    "invalid request from",
			request: `{"invalid": -1}`,
			expectedBody: ErrorResponse{
				Error: "Invalid request: 'from' cannot be empty",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "invalid request to",
			request: `{"from": "0"}`,
			expectedBody: ErrorResponse{
				Error: "Invalid request: 'to' cannot be empty",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "same nodes",
			request: `{"from": "0", "to": "0"}`,
			expectedBody: ErrorResponse{
				Error: "Source and destination nodes cannot be the same",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "invalid request slots",
			request: `{"from": "0", "to": "1", "slots": -1}`,
			expectedBody: ErrorResponse{
				Error: "Invalid request: 'slots' must be positive",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "from node not found",
			request: `{"from": "3", "to": "1"}`,
			expectedBody: ErrorResponse{
				Error: "Node '3' not found",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "from node bad cluster name not found",
			request: `{"from": "novalid-3", "to": "1"}`,
			expectedBody: ErrorResponse{
				Error: "Node 'novalid-3' not found",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "to node not found",
			request: `{"from": "test-0", "to": "3"}`,
			expectedBody: ErrorResponse{
				Error: "Node '3' not found",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "to node good cluster name not found",
			request: `{"from": "test-0", "to": "test-3"}`,
			expectedBody: ErrorResponse{
				Error: "Node 'test-3' not found",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "resharding in progress",
			request: `{"from": "0", "to": "2"}`,
			expectedBody: ClusterMoveSlotsResponse{
				Status: "In progress",
			},
			expectedStatusCode: http.StatusAccepted,
		},
		{
			name:    "resharding completed",
			request: `{"from": "0", "to": "1"}`,
			expectedBody: ClusterMoveSlotsResponse{
				Status: "Completed",
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:    "unexpected error",
			request: `{"from": "1", "to": "2"}`,
			expectedBody: ErrorResponse{
				Error: "Internal server error: error getting and checking Redis client: maxRetries must be greater than 0",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(nil, 0, 0)
			testRequest(t, "PUT", "/v1/cluster/move", tt.request, "", nil, server.MoveNodeSlots, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestCheckCluster(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		operations         map[string][]cluster.RedisOperation
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "conflict",
			operations: map[string][]cluster.RedisOperation{
				"Resharding": {
					cluster.NewFakeRedisOperationMove(context.TODO(), &cluster.RedKeyCluster{}, "Running", nil, nil, 10, time.Time{}),
				},
			},
			expectedBody: ClusterCheckResponse{
				Errors: []string{
					"Operation CheckCluster conflicts with ongoing operation Move",
				},
			},
			expectedStatusCode: http.StatusConflict,
		},
		{
			name:       "unexpected error",
			operations: map[string][]cluster.RedisOperation{},
			expectedBody: ErrorResponse{
				Error: "Internal server error: error getting and checking Redis client: maxRetries must be greater than 0",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(tt.operations, 0, 0)
			testRequest(t, "GET", "/cluster/check", tt.request, "", nil, server.CheckCluster, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestFixCluster(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		operations         map[string][]cluster.RedisOperation
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name:       "good request",
			operations: nil, // No operations, fresh server
			expectedBody: ClusterFixResponse{
				Status: "In progress",
			},
			expectedStatusCode: http.StatusCreated,
		},
		{
			name: "fix in progress",
			operations: map[string][]cluster.RedisOperation{
				"CheckingIntegrity": {
					cluster.NewFakeRedisOperationCheckIntegrity(context.TODO(), &cluster.RedKeyCluster{}, "Running"),
				},
			},
			expectedBody: ClusterFixResponse{
				Status: "In progress",
			},
			expectedStatusCode: http.StatusAccepted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(tt.operations, 0, 0)
			testRequest(t, "PUT", "/cluster/fix", tt.request, "", nil, server.FixCluster, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestResetNode(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		pathValues         map[string]string
		operations         map[string][]cluster.RedisOperation
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name:       "node not found",
			operations: nil,
			expectedBody: ErrorResponse{
				Error: "Node 'notfound' not found",
			},
			pathValues: map[string]string{
				"nodeIndex": "notfound",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:       "reset node error",
			operations: nil,
			expectedBody: ErrorResponse{
				Error: "Internal server error: error resetting cluster node 'test-1': maxRetries must be greater than 0",
			},
			pathValues: map[string]string{
				"nodeIndex": "1",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
		{
			name: "reset in progress",
			operations: map[string][]cluster.RedisOperation{
				"Resetting": {
					cluster.NewFakeRedisOperationResetNode(context.TODO(), &cluster.RedKeyCluster{}, "Running", node2),
				},
			},
			expectedBody: ClusterResetNodeResponse{
				Status: "In progress",
			},
			pathValues: map[string]string{
				"nodeIndex": "1",
			},
			expectedStatusCode: http.StatusAccepted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(tt.operations, 0, 0)
			testRequest(t, "PUT", "/cluster/reset/1", tt.request, "", tt.pathValues, server.ResetNode, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestGetNodes(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: ClusterNodesResponse{
				Nodes: []RedisNode{
					{
						Name:       "node1",
						ID:         "1234567890",
						IP:         "1.1.1.1",
						Role:       "primary",
						PrimaryID:  "",
						Failures:   0,
						Sent:       0,
						Recv:       0,
						LinkStatus: "",
					},
					{
						Name:       "test-1",
						ID:         "0987654321",
						IP:         "2.2.2.2",
						Role:       "primary",
						PrimaryID:  "",
						Failures:   0,
						Sent:       0,
						Recv:       0,
						LinkStatus: "",
					},
					{
						Name:       "node3",
						ID:         "0987654321",
						IP:         "2.2.2.2",
						Role:       "primary",
						PrimaryID:  "",
						Failures:   0,
						Sent:       0,
						Recv:       0,
						LinkStatus: "",
					},
				},
			},
			expectedStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(nil, 0, 0)
			body := testRequest(t, "PUT", "/cluster/nodes", tt.request, "", nil, server.GetNodes, tt.expectedStatusCode, nil)

			var response ClusterNodesResponse
			if err := json.NewDecoder(body).Decode(&response); err != nil {
				t.Fatalf("Error decoding response: %v", err)
			}
			assert.NotNil(t, response.Nodes)

			expectedBody := tt.expectedBody.(ClusterNodesResponse)
			assert.Len(t, response.Nodes, len(expectedBody.Nodes))
			for _, node := range expectedBody.Nodes {
				assert.Contains(t, response.Nodes, node)
			}
		})
	}
}

func TestRecreateCluster(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		operations         map[string][]cluster.RedisOperation
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name:       "good request",
			operations: nil,
			expectedBody: ClusterRecreateResponse{
				Status: "In progress",
			},
			expectedStatusCode: http.StatusCreated,
		},
		{
			name: "recreate in progress",
			operations: map[string][]cluster.RedisOperation{
				"Recreating": {
					cluster.NewFakeRedisOperationRecreate(context.TODO(), &cluster.RedKeyCluster{}, "Running", time.Time{}),
				},
			},
			expectedBody: ClusterRecreateResponse{
				Status: "In progress",
			},
			expectedStatusCode: http.StatusAccepted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer(tt.operations, 0, 0)
			testRequest(t, "PUT", "/cluster/recreate", tt.request, "", nil, server.RecreateCluster, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

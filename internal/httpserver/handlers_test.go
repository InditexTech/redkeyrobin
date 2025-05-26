// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"net/http"
	"testing"
)

func TestGetRedisClusterStatus(t *testing.T) {
	tests := []struct {
		name               string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: RedisClusterStatusResponse{
				Status: "Ready",
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "GET", "/rediscluster/status", "", "", nil, server.GetRedisClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestUpdateRedisClusterStatus(t *testing.T) {
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
			expectedBody: RedisClusterStatusResponse{
				Status: "Ready",
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "POST", "/rediscluster/status", tt.request, "", nil, server.UpdateRedisClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestGetClusterReplicas(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: ClusterReplicasResponse{
				Replicas: 0,
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "GET", "/cluster/replicas", tt.request, "", nil, server.GetClusterReplicas, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestUpdateClusterReplicas(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name:    "invalid request",
			request: `{"replicas": -1}`,
			expectedBody: ErrorResponse{
				Error: "Invalid request: 'replicas' must be positive",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name:    "same replicas",
			request: `{"replicas": 0}`,
			expectedBody: ClusterReplicasResponse{
				Replicas: 0,
			},
			expectedStatusCode: http.StatusAccepted,
		},
		{
			name:    "good request",
			request: `{"replicas": 3}`,
			expectedBody: ClusterReplicasResponse{
				Replicas: 3,
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "PUT", "/cluster/replicas", tt.request, "", nil, server.UpdateClusterReplicas, tt.expectedStatusCode, tt.expectedBody)
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
				Error: "Error rebalancing cluster: error getting and checking Redis client: maxRetries must be greater than 0",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "PUT", "/cluster/move", tt.request, "", nil, server.MoveNodeSlots, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestCheckCluster(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "unexpected error",
			expectedBody: ErrorResponse{
				Error: "Error checking cluster: error getting and checking Redis client: maxRetries must be greater than 0",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "GET", "/cluster/check", tt.request, "", nil, server.CheckCluster, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestFixCluster(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: ClusterFixResponse{
				Status: "In progress",
			},
			expectedStatusCode: http.StatusCreated,
		},
		{
			name: "fix in progress",
			expectedBody: ClusterFixResponse{
				Status: "In progress",
			},
			expectedStatusCode: http.StatusAccepted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "PUT", "/cluster/fix", tt.request, "", nil, server.FixCluster, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestResetNode(t *testing.T) {
	tests := []struct {
		name               string
		request            string
		pathValues         map[string]string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "node not found",
			expectedBody: ErrorResponse{
				Error: "Node 'notfound' not found",
			},
			pathValues: map[string]string{
				"nodeIndex": "notfound",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "good request",
			expectedBody: ErrorResponse{
				Error: "Error reseting node: maxRetries must be greater than 0",
			},
			pathValues: map[string]string{
				"nodeIndex": "1",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "PUT", "/cluster/reset/1", tt.request, "", tt.pathValues, server.ResetNode, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

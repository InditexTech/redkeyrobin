package httpserver

import(
	"testing"
	"net/http"
)



func TestGetRedisClusterStatus(t *testing.T) {
	tests := []struct {
		name string
		expectedBody RedisClusterStatusResponse
		expectedStatusCode int
	}{
		{
			name: "good request",
			expectedBody: RedisClusterStatusResponse{
				Status: "OK",
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "GET", "/rediscluster/status", "", server.GetRedisClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestUpdateRedisClusterStatus(t *testing.T) {
	tests := []struct {
		name string
		request string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "bad request",
			request: "{",
			expectedBody: ErrorResponse{
				Error: "Invalid request: unexpected EOF",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "invalid request",
			request: `{"status": "Invalid"}`,
			expectedBody: ErrorResponse{
				Error: "Invalid request: invalid status 'Invalid'",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "good request",
			request: `{"status": "Ready"}`,
			expectedBody: RedisClusterStatusResponse{
				Status: "Ready",
			},
			expectedStatusCode: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "POST", "/rediscluster/status", tt.request, server.UpdateRedisClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestGetClusterReplicas(t *testing.T) {
	tests := []struct {
		name string
		request string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "GET", "/cluster/replicas", tt.request, server.GetClusterReplicas, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}


func TestUpdateClusterReplicas(t *testing.T) {
	tests := []struct {
		name string
		request string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "PUT", "/cluster/replicas", tt.request, server.UpdateClusterReplicas, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestGetClusterStatus(t *testing.T) {
	tests := []struct {
		name string
		request string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "GET", "/cluster/status", tt.request, server.GetClusterStatus, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestMoveNodeSlots(t *testing.T) {
	tests := []struct {
		name string
		request string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "PUT", "/cluster/move", tt.request, server.MoveNodeSlots, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestCheckCluster(t *testing.T) {
	tests := []struct {
		name string
		request string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "GET", "/cluster/check", tt.request, server.CheckCluster, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func TestFixCluster(t *testing.T) {
	tests := []struct {
		name string
		request string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testRequest(t, "PUT", "/cluster/fix", tt.request, server.FixCluster, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}
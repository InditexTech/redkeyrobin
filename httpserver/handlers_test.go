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
				Error: "Invalid request",
			},
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "invalid request",
			request: `{"status": "Invalid"}`,
			expectedBody: ErrorResponse{
				Error: "Invalid request",
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



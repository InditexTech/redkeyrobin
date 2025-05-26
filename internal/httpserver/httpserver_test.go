// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/stretchr/testify/assert"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlConfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

type MockedControllerManager struct {
	ctrl.Manager
}

func (m MockedControllerManager) Add(manager.Runnable) error {
	return nil
}

func (m MockedControllerManager) Elected() <-chan struct{} {
	return nil
}

func (m MockedControllerManager) AddMetricsServerExtraHandler(path string, handler http.Handler) error {
	if path == "/error" {
		return fmt.Errorf("error adding metrics server extra handler")
	}
	return nil
}

func (m MockedControllerManager) AddHealthzCheck(name string, check healthz.Checker) error {
	return nil
}

func (m MockedControllerManager) AddReadyzCheck(name string, check healthz.Checker) error {
	return nil
}

func (m MockedControllerManager) Start(ctx context.Context) error {
	return nil
}

func (m MockedControllerManager) GetWebhookServer() webhook.Server {
	return nil
}

func (m MockedControllerManager) GetLogger() logr.Logger {
	return logr.Discard()
}

func (m MockedControllerManager) GetControllerOptions() ctrlConfig.Controller {
	return ctrlConfig.Controller{}
}

var node1 = &redis.RedisNode{
	Name:     "node1",
	ID:       "1234567890",
	Addr:     "node1",
	IP:       "1.1.1.1",
	Flags:    "master",
	Slots:    []redis.RedisSlotRange{},
	MasterID: "",
}
var node2 = &redis.RedisNode{
	Name:  "test-1",
	ID:    "0987654321",
	Addr:  "node2",
	IP:    "2.2.2.2",
	Flags: "master",
	Slots: []redis.RedisSlotRange{
		{
			Start: 5,
			End:   7,
		},
	},
	MasterID: "",
}
var node3 = &redis.RedisNode{
	Name:  "node3",
	ID:    "0987654321",
	Addr:  "node2",
	IP:    "2.2.2.2",
	Flags: "master",
	Slots: []redis.RedisSlotRange{
		{
			Start: 7,
			End:   10,
		},
	},
	MasterID: "",
}

var server = Server{
	redisCluster: redis.NewFakeRedisCluster(
		context.TODO(),
		&config.Configuration{
			Redis: config.RedisConfig{
				Cluster: config.RedisClusterConfig{
					Status: "Ready",
					Name:   "test",
				},
			},
		},
		"Unknown",
		map[string]*redis.RedisNode{
			"test-0": node1,
			"test-1": node2,
			"test-2": node3,
		},
		map[string][]*redis.RedisOperation{
			"Resharding": {
				{
					Name:     "Resharding",
					Status:   "Running",
					NodeFrom: node1,
					NodeTo:   node3,
				},
				{
					Name:     "Resharding",
					Status:   "Finished",
					NodeFrom: node1,
					NodeTo:   node2,
				},
			},
		},
		make(chan struct{}, 5),
	),
}

func TestCheckPathConfiguration(t *testing.T) {
	tests := []struct {
		name              string
		pathConfiguration map[string]interface{}
		err               error
	}{
		{
			name:              "emtpy path configuration",
			pathConfiguration: map[string]interface{}{},
			err:               fmt.Errorf("no methods configured for path"),
		},
		{
			name: "invalid method configuration",
			pathConfiguration: map[string]interface{}{
				"get": "invalid",
			},
			err: fmt.Errorf("invalid method configuration for method get"),
		},
		{
			name: "no operationId",
			pathConfiguration: map[string]interface{}{
				"get": map[string]interface{}{
					"noperationId": 123,
				},
			},
			err: fmt.Errorf("invalid operationId for method get"),
		},
		{
			name: "invalid operationId",
			pathConfiguration: map[string]interface{}{
				"get": map[string]interface{}{
					"operationId": 123,
				},
			},
			err: fmt.Errorf("invalid operationId for method get"),
		},
		{
			name: "method does not exist",
			pathConfiguration: map[string]interface{}{
				"get": map[string]interface{}{
					"operationId": "InvalidOperationId",
				},
			},
			err: fmt.Errorf("method InvalidOperationId not found"),
		},
		{
			name: "method has invalid signature",
			pathConfiguration: map[string]interface{}{
				"get": map[string]interface{}{
					"operationId": "Init",
				},
			},
			err: fmt.Errorf("method Init has invalid signature"),
		},
		{
			name: "good request",
			pathConfiguration: map[string]interface{}{
				"get": map[string]interface{}{
					"operationId": "GetRedisClusterStatus",
				},
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := server.checkPathConfiguration(tt.pathConfiguration)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestInit(t *testing.T) {
	tests := []struct {
		name          string
		paths         map[string]map[string]interface{}
		expectedPaths map[string]map[string]interface{}
		err           error
	}{
		{
			name:          "empty endpoints",
			paths:         map[string]map[string]interface{}{},
			expectedPaths: map[string]map[string]interface{}{},
			err:           nil,
		},
		{
			name: "invalid path configuration",
			paths: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{},
			},
			expectedPaths: map[string]map[string]interface{}{},
			err:           nil,
		},
		{
			name: "metrics server extra handler error",
			paths: map[string]map[string]interface{}{
				"/error": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
			},
			expectedPaths: map[string]map[string]interface{}{},
			err:           fmt.Errorf("unable to attach /error handler: error adding metrics server extra handler"),
		},
		{
			name: "one path good, one path bad",
			paths: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
				"/rediscluster/health": map[string]interface{}{},
			},
			expectedPaths: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
			},
			err: nil,
		},
		{
			name: "good request",
			paths: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
				"/rediscluster/reset/{nodeId}": map[string]interface{}{
					"put": map[string]interface{}{
						"operationId": "ResetRedisNode",
					},
				},
			},
			expectedPaths: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
				"/rediscluster/reset/{nodeId}": map[string]interface{}{
					"put": map[string]interface{}{
						"operationId": "ResetRedisNode",
					},
				},
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := Server{
				config: config.APIConfig{
					Paths: tt.paths,
				},
				redisCluster: redis.NewRedisCluster(t.Context(), &config.Configuration{}, make(chan struct{})),
			}
			err := server.Init(MockedControllerManager{})
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestServeHTTP(t *testing.T) {
	tests := []struct {
		name               string
		config             config.APIConfig
		endpoint           string
		method             string
		pattern 		  string
		pathValues         map[string]string
		expectedBody       ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "endpoint not found",
			config: config.APIConfig{
				Paths: map[string]map[string]interface{}{
					"/rediscluster/status": map[string]interface{}{
						"get": map[string]interface{}{
							"operationId": "GetRedisClusterStatus",
						},
					},
				},
			},
			endpoint: "/notfound",
			method:   "GET",
			expectedBody: ErrorResponse{
				Error: "Unknown path /notfound",
			},
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name: "method not allowed",
			config: config.APIConfig{
				Paths: map[string]map[string]interface{}{
					"/rediscluster/status": map[string]interface{}{
						"get": map[string]interface{}{
							"operationId": "GetRedisClusterStatus",
						},
					},
				},
			},
			endpoint: "/rediscluster/status",
			method:   "POST",
			expectedBody: ErrorResponse{
				Error: "Method POST not allowed in path /rediscluster/status",
			},
			expectedStatusCode: http.StatusMethodNotAllowed,
		},
		{
			name: "good request",
			config: config.APIConfig{
				Paths: map[string]map[string]interface{}{
					"/rediscluster/status": map[string]interface{}{
						"get": map[string]interface{}{
							"operationId": "GetRedisClusterStatus",
						},
					},
				},
			},
			endpoint: "/rediscluster/status",
			method:   "GET",
			expectedBody: RedisClusterStatusResponse{
				Status: "Ready",
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "good request with parameters",
			config: config.APIConfig{
				Paths: map[string]map[string]interface{}{
					"/rediscluster/reset/{nodeIndex}": map[string]interface{}{
					"put": map[string]interface{}{
						"operationId": "ResetNode",
					},
				},
				},
			},
			endpoint: "/rediscluster/reset/1",
			method:   "PUT",
			pattern: "/rediscluster/reset/{nodeIndex}",
			pathValues: map[string]string{
				"nodeIndex": "1",
			},
			expectedBody: ErrorResponse{
				Error: "Error reseting node: maxRetries must be greater than 0",
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svr := Server{
				logger:       ctrl.Log.WithName("test"),
				config:       tt.config,
				redisCluster: server.redisCluster,
			}
			testRequest(t, tt.method, tt.endpoint, "", tt.pattern, tt.pathValues, svr.ServeHTTP, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func testRequest(t *testing.T, method string, endpoint string, body string, pattern string, pathValues map[string] string,handlerFunc http.HandlerFunc, expectedStatusCode int, expectedBody interface{}) {
	// Create a new request
	req, err := http.NewRequest(method, endpoint, strings.NewReader(body))
	if err != nil {
		t.Fatalf("Error creating request: %v", err)
	}
	if pattern != "" {
		req.Pattern = pattern
	}
	for key, value := range pathValues {
		req.SetPathValue(key, value)
	}

	// Create a new response recorder
	rr := httptest.NewRecorder()

	// Call the handler function
	handlerFunc(rr, req)

	// Check the status code
	if status := rr.Code; status != expectedStatusCode {
		t.Errorf("Handler returned wrong status code: got %v want %v", status, expectedStatusCode)
	}

	// Check the response body
	var response interface{}
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Fatalf("Error decoding response: %v", err)
	}

	bodyContent, _ := json.Marshal(response)
	bodyContentString := string(bodyContent[:])
	bodyExpectedContent, _ := json.Marshal(expectedBody)
	bodyExpectedContentString := string(bodyExpectedContent[:])

	if bodyContentString != bodyExpectedContentString {
		t.Errorf("Handler returned unexpected body: got %v want %v", bodyContentString, bodyExpectedContentString)
	}
}

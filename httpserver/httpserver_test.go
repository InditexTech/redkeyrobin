package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/inditextech/redisrobin/config"
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


var server = Server{}

func TestCheckPathConfiguration(t *testing.T) {
	tests := []struct {
		name string
		pathConfiguration map[string]interface{}
		err error
	}{
		{
			name: "emtpy path configuration",
			pathConfiguration: map[string]interface{}{},
			err: fmt.Errorf("no methods configured for path"),
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
		name string
		endpoints map[string]map[string]interface{}
		expectedEndpoints map[string]map[string]interface{}
		err error
	}{
		{
			name: "empty endpoints",
			endpoints: map[string]map[string]interface{}{},
			expectedEndpoints: map[string]map[string]interface{}{},
			err: nil,
		},
		{
			name: "invalid path configuration",
			endpoints: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{},
			},
			expectedEndpoints: map[string]map[string]interface{}{},
			err: nil,
		},
		{
			name: "metrics server extra handler error",
			endpoints: map[string]map[string]interface{}{
				"/error": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
			},
			expectedEndpoints: map[string]map[string]interface{}{},
			err: fmt.Errorf("unable to attach /error handler: error adding metrics server extra handler"),
		},
		{
			name: "one path good, one path bad",
			endpoints: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
				"/rediscluster/health": map[string]interface{}{},
			},
			expectedEndpoints: map[string]map[string]interface{}{
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
			endpoints: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
			},
			expectedEndpoints: map[string]map[string]interface{}{
				"/rediscluster/status": map[string]interface{}{
					"get": map[string]interface{}{
						"operationId": "GetRedisClusterStatus",
					},
				},
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := Server{
				Config: config.APIConfig{
					Endpoints: tt.endpoints,
				},
			}
			err := server.Init(MockedControllerManager{})
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestServeHTTP(t *testing.T) {
	tests := []struct {
		name string
		config config.APIConfig
		endpoint string
		method string
		expectedBody ResponseInterface
		expectedStatusCode int
	}{
		{
			name: "endpoint not found",
			config: config.APIConfig{
				Endpoints: map[string]map[string]interface{}{
					"/rediscluster/status": map[string]interface{}{
						"get": map[string]interface{}{
							"operationId": "GetRedisClusterStatus",
						},
					},
				},
			},
			endpoint: "/notfound",
			method: "GET",
			expectedBody: ErrorResponse{
				Error: "Unknown path /notfound",
			},
			expectedStatusCode: http.StatusNotFound,
		},
		{
			name: "method not allowed",
			config: config.APIConfig{
				Endpoints: map[string]map[string]interface{}{
					"/rediscluster/status": map[string]interface{}{
						"get": map[string]interface{}{
							"operationId": "GetRedisClusterStatus",
						},
					},
				},
			},
			endpoint: "/rediscluster/status",
			method: "POST",
			expectedBody: ErrorResponse{
				Error: "Method POST not allowed in path /rediscluster/status",
			},
			expectedStatusCode: http.StatusMethodNotAllowed,
		},
		{
			name: "good request",
			config: config.APIConfig{
				Endpoints: map[string]map[string]interface{}{
					"/rediscluster/status": map[string]interface{}{
						"get": map[string]interface{}{
							"operationId": "GetRedisClusterStatus",
						},
					},
				},
			},
			endpoint: "/rediscluster/status",
			method: "GET",
			expectedBody: RedisClusterStatusResponse{
				Status: "OK",
			},
			expectedStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := Server{
				Config: tt.config,
			}
			testRequest(t, tt.method, tt.endpoint, "", server.ServeHTTP, tt.expectedStatusCode, tt.expectedBody)
		})
	}
}

func testRequest(t *testing.T, method string, endpoint string, request string, handlerFunc http.HandlerFunc, expectedStatusCode int, expectedBody interface{}) {
	// Create a new request
	req, err := http.NewRequest(method, endpoint, strings.NewReader(request))
	if err != nil {
		t.Fatalf("Error creating request: %v", err)
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
	var body interface{}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("Error decoding response: %v", err)
	}

	bodyContent, _ := json.Marshal(body)
	bodyContentString := string(bodyContent[:])
	bodyExpectedContent, _ := json.Marshal(expectedBody)
	bodyExpectedContentString := string(bodyExpectedContent[:])

	if bodyContentString != bodyExpectedContentString {
		t.Errorf("Handler returned unexpected body: got %v want %v", bodyContentString, bodyExpectedContentString)
	}
}
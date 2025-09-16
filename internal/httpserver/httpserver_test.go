// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/inditextech/redkeyrobin/internal/cluster"
	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/inditextech/redkeyrobin/internal/util"
	"github.com/stretchr/testify/assert"
)

// mockClientFactory creates a mock client factory for testing
var mockClientFactory = func(ctx context.Context, addr string, maxRetries int, backoff time.Duration) (redis.RedisClientInterface, error) {
	return &redis.MockRedisClient{}, nil
}

var node1 = redis.NewFakeRedisNode("node1", mockClientFactory)
var node2 = redis.NewFakeRedisNode("test-1", mockClientFactory)
var node3 = redis.NewFakeRedisNode("node3", mockClientFactory)

func init() {
	// Configure node1 properties
	node1.Addr = "node1"
	node1.MaxRetries = 1
	node1.Backoff = time.Microsecond * 10
	node1.ID = "1234567890"
	node1.IP = "1.1.1.1"
	node1.Flags = "master"
	node1.Slots = []redis.RedisSlotRange{}
	node1.MasterID = ""

	// Configure node2 properties
	node2.Addr = "node2"
	node2.MaxRetries = 1
	node2.Backoff = time.Microsecond * 10
	node2.ID = "0987654321"
	node2.IP = "2.2.2.2"
	node2.Flags = "master"
	node2.Slots = []redis.RedisSlotRange{
		{
			Start: 5,
			End:   7,
		},
	}
	node2.MasterID = ""

	// Configure node3 properties
	node3.Addr = "node2"
	node3.MaxRetries = 1
	node3.Backoff = time.Microsecond * 10
	node3.ID = "0987654321"
	node3.IP = "2.2.2.2"
	node3.Flags = "master"
	node3.Slots = []redis.RedisSlotRange{
		{
			Start: 7,
			End:   10,
		},
	}
	node3.MasterID = ""
}

var server = Server{
	logger: util.GetLogger("http-server"),
	cluster: cluster.NewFakeRedKeyCluster(
		context.TODO(),
		&config.Configuration{
			Redis: config.RedisConfig{
				Cluster: config.RedKeyClusterConfig{
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
		map[string][]cluster.RedisOperation{
			"Resharding": {
				cluster.NewFakeRedisOperationMove(context.TODO(), &cluster.RedKeyCluster{}, "Running", node1, node3, 10, time.Time{}),
				cluster.NewFakeRedisOperationMove(context.TODO(), &cluster.RedKeyCluster{}, "Finished", node1, node2, 10, time.Time{}),
			},
		},
		make(chan struct{}, 5),
	),
}

func TestInit(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "good init",
			err:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewServer(cluster.NewRedKeyCluster(t.Context(), &config.Configuration{}, make(chan struct{})))
			err := server.Init(&util.Options{})
			assert.Equal(t, tt.err, err)
		})
	}
}

func testRequest(t *testing.T, method string, endpoint string, body string, pattern string, pathValues map[string]string, handlerFunc http.HandlerFunc, expectedStatusCode int, expectedBody interface{}) *bytes.Buffer {
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
	if expectedBody != nil {
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

	return rr.Body
}

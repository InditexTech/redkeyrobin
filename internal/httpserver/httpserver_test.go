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

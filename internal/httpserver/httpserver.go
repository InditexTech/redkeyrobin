// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"errors"
	"net/http"

	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/inditextech/redisrobin/internal/util"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server represents an HTTP server with a dependency on a ConfigProvider.
type Server struct {
	logger       logr.Logger
	redisCluster *redis.RedisCluster
	mux 		*http.ServeMux
	options 	*util.Options
}

func NewServer(options *util.Options, redisCluster *redis.RedisCluster) *Server {
	return &Server{
		logger:       util.GetLogger("http-server"),
		redisCluster: redisCluster,
		mux: http.NewServeMux(),
		options: options,
	}
}

// Init initializes the Server by attaching the handler to the metrics server.
func (s *Server) Init() error {
	// Metrics endpoint
	if !s.options.DisableMetrics {
		s.mux.Handle("GET /metrics", promhttp.Handler())
	}

	// Rediscluster endpoints
	s.mux.HandleFunc("GET /v1/rediscluster/status", s.GetRedisClusterStatus)
	s.mux.HandleFunc("PUT /v1/rediscluster/status", s.UpdateRedisClusterStatus)
	s.mux.HandleFunc("GET /v1/rediscluster/replicas", s.GetClusterReplicas)
	s.mux.HandleFunc("PUT /v1/rediscluster/replicas", s.UpdateClusterReplicas)

	// Cluster endpoints
	s.mux.HandleFunc("GET /v1/cluster/status", s.GetClusterStatus)
	s.mux.HandleFunc("PUT /v1/cluster/move", s.MoveNodeSlots)
	s.mux.HandleFunc("GET /v1/cluster/check", s.CheckCluster)
	s.mux.HandleFunc("PUT /v1/cluster/fix", s.FixCluster)
	s.mux.HandleFunc("PUT /v1/cluster/reset/{nodeIndex}", s.ResetNode)
	s.mux.HandleFunc("GET /v1/cluster/nodes", s.GetNodes)

	return nil
}

// Start starts the Server.
func (s *Server) Start() error {
	// Start the HTTP server
	if err := http.ListenAndServe(s.options.Address, s.mux); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) sendResponse(w http.ResponseWriter, code int, object ResponseInterface) {
	response := Response{
		Code:    code,
		Headers: map[string][]string{"Content-Type": {"application/json"}},
		Object:  object,
	}
	response.WriteResponse(w)
}

func (s *Server) sendError(w http.ResponseWriter, code int, message string) {
	response := ErrorResponse{
		Error: message,
	}
	s.sendResponse(w, code, response)
}

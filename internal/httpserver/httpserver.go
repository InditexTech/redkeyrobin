// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/util"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server represents an HTTP server with a dependency on a ConfigProvider.
type Server struct {
	logger  *slog.Logger
	cluster cluster.Cluster
	server  *http.Server
}

// NewServer creates a new Server with the provided ConfigProvider.
func NewServer(redkeyCluster cluster.Cluster) *Server {
	return &Server{
		logger:  util.GetLogger("http-server"),
		cluster: redkeyCluster,
	}
}

// Init initializes the Server by attaching the handler to the metrics server.
func (s *Server) Init(opts *util.Options) error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		s.sendResponse(w, http.StatusOK, HealthResponse{Status: "OK"})
	})

	// Metrics endpoint
	if !opts.DisableMetrics {
		mux.Handle("GET /metrics", promhttp.Handler())
	}

	if !s.cluster.IsStandalone() {
		// RedKeyCluster endpoints
		mux.HandleFunc("GET /v1/redkeycluster/status", s.GetRedKeyClusterStatus)
		mux.HandleFunc("PUT /v1/redkeycluster/status", s.UpdateRedKeyClusterStatus)
		mux.HandleFunc("GET /v1/redkeycluster/replicas", s.GetRedKeyClusterReplicas)
		mux.HandleFunc("PUT /v1/redkeycluster/replicas", s.UpdateRedKeyClusterReplicas)

		// Cluster endpoints
		mux.HandleFunc("PUT /v1/cluster/move", s.MoveNodeSlots)
		mux.HandleFunc("GET /v1/cluster/check", s.CheckCluster)
		mux.HandleFunc("PUT /v1/cluster/fix", s.FixCluster)
		mux.HandleFunc("PUT /v1/cluster/reset/{nodeIndex}", s.ResetNode)
		mux.HandleFunc("GET /v1/cluster/nodes", s.GetNodes)
	}

	// Create the server
	s.server = &http.Server{
		Addr:    opts.Address,
		Handler: mux,
	}

	return nil
}

// Start starts the Server.
func (s *Server) Start(ctx context.Context) error {
	if s.server == nil {
		return fmt.Errorf("server not initialized. You must call Init() first")
	}

	// Handle context cancellation
	go func() {
		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Context cancelled, stopping HTTP server")
				time.Sleep(1 * time.Second)

				if err := s.server.Shutdown(ctx); err != nil {
					s.logger.Error("HTTP shutdown error: %v", "error", err)
				}
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	// Start the HTTP server
	if err := s.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// SendResponse sends a JSON response with the specified status code and object.
func (s *Server) sendResponse(w http.ResponseWriter, code int, object ResponseInterface) {
	response := Response{
		Code:    code,
		Headers: map[string][]string{"Content-Type": {"application/json"}},
		Object:  object,
	}
	response.WriteResponse(w)
}

// SendError sends a JSON error response with the specified status code and message.
func (s *Server) sendError(w http.ResponseWriter, code int, message string) {
	response := ErrorResponse{
		Error: message,
	}
	s.sendResponse(w, code, response)
}

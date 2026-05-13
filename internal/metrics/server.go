// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is a Prometheus metrics HTTP server.
type Server struct {
	bindAddr string
	logger   *slog.Logger
}

// NewServer creates a new metrics Server.
func NewServer(bindAddr string) *Server {
	return &Server{
		bindAddr: bindAddr,
		logger:   slog.Default().With("component", "metrics-server"),
	}
}

// Start starts the metrics HTTP server. It blocks until the context is cancelled,
// then performs a graceful shutdown.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              s.bindAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Start serving in a goroutine
	errChan := make(chan error, 1)
	go func() {
		s.logger.Info("Starting metrics server", "addr", s.bindAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		s.logger.Info("Shutting down metrics server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

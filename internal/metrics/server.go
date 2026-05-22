// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is a Prometheus metrics HTTP server with optional hot-togglable pprof
// profiling endpoints.
type Server struct {
	bindAddr      string
	gatherer      prometheus.Gatherer
	runtimeConfig *config.RuntimeConfig
	logger        *slog.Logger
}

// NewServer creates a metrics Server that reads profiling state from the shared
// RuntimeConfig. Profiling can be toggled at runtime via the RedkeyClusterConfig
// CRD without restarting the pod. Pass nil gatherer to use prometheus.DefaultGatherer.
func NewServer(bindAddr string, gatherer prometheus.Gatherer, rc *config.RuntimeConfig) *Server {
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}
	if rc == nil {
		rc = config.NewRuntimeConfig()
	}
	return &Server{
		bindAddr:      bindAddr,
		gatherer:      gatherer,
		runtimeConfig: rc,
		logger:        slog.Default().With("component", "metrics-server"),
	}
}

// Start starts the metrics HTTP server. It blocks until the context is cancelled,
// then performs a graceful shutdown.
//
// Pprof endpoints are always registered but gated by a middleware that checks
// RuntimeConfig.ProfilingEnabled() on every request. This allows toggling
// profiling at runtime without restarting the server.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.gatherer, promhttp.HandlerOpts{}))

	// Register pprof handlers behind a guard that checks runtime config.
	// The pprofGuard middleware returns 404 when profiling is disabled,
	// allowing hot enable/disable via the RedkeyClusterConfig CRD.
	mux.HandleFunc("/debug/pprof/", s.pprofGuard(pprof.Index))
	mux.HandleFunc("/debug/pprof/cmdline", s.pprofGuard(pprof.Cmdline))
	mux.HandleFunc("/debug/pprof/profile", s.pprofGuard(pprof.Profile))
	mux.HandleFunc("/debug/pprof/symbol", s.pprofGuard(pprof.Symbol))
	mux.HandleFunc("/debug/pprof/trace", s.pprofGuard(pprof.Trace))

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

// pprofGuard wraps a pprof handler and returns 404 when profiling is disabled.
// It checks RuntimeConfig.ProfilingEnabled() (an atomic.Bool) on each request,
// providing zero-cost hot-toggling without mutex contention or server restart.
func (s *Server) pprofGuard(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.runtimeConfig.ProfilingEnabled() {
			http.NotFound(w, r)
			return
		}
		handler(w, r)
	}
}

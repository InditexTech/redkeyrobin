// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/inditextech/redkey-robin/internal/config"
	"github.com/prometheus/client_golang/prometheus"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("failed to close listener: %v", err)
	}
	return port
}

func TestNewServer(t *testing.T) {
	s := NewServer(":9090", nil, nil)
	if s.bindAddr != ":9090" {
		t.Fatalf("expected bindAddr ':9090', got '%s'", s.bindAddr)
	}
	if s.gatherer == nil {
		t.Fatal("expected non-nil gatherer")
	}
	if s.logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestServer_ServesMetrics(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	s := NewServer(addr, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for server to be ready
	var resp *http.Response
	var err error
	for range 20 {
		resp, err = http.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to reach metrics endpoint: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Graceful shutdown
	cancel()
	select {
	case sErr := <-errCh:
		if sErr != nil {
			t.Fatalf("unexpected error from Start: %v", sErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestServer_ServesCustomGatherer(t *testing.T) {
	reg := NewResettableRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redkey_custom_server_metric",
		Help: "Custom server gatherer test metric.",
	})
	reg.MustRegister(gauge)
	gauge.Set(7)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	s := NewServer(addr, reg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	var resp *http.Response
	var err error
	for range 20 {
		resp, err = http.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("failed to reach metrics endpoint: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("failed to close response body: %v", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read metrics response: %v", err)
	}
	if !strings.Contains(string(body), "redkey_custom_server_metric 7") {
		t.Fatalf("expected custom metric in response, got:\n%s", string(body))
	}

	cancel()
	select {
	case sErr := <-errCh:
		if sErr != nil {
			t.Fatalf("unexpected error from Start: %v", sErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}
}

func TestCombinedGathererIncludesDefaultAndRedKeyMetrics(t *testing.T) {
	reg := NewResettableRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redkey_combined_gatherer_test_metric",
		Help: "Combined gatherer test metric.",
	})
	reg.MustRegister(gauge)
	gauge.Set(1)

	families, err := prometheus.Gatherers{prometheus.DefaultGatherer, reg}.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	if metricFamilyByName(families, "redkey_combined_gatherer_test_metric") == nil {
		t.Fatal("expected RedKey metric from resettable registry")
	}
	if metricFamilyByName(families, "go_goroutines") == nil {
		t.Fatal("expected default Go runtime metrics")
	}
}

func TestServer_GracefulShutdown(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	s := NewServer(addr, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for server to be ready
	for range 20 {
		_, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Cancel and verify clean shutdown
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("expected clean shutdown, got error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server shutdown")
	}

	// Server should no longer accept connections
	_, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
	if err == nil {
		t.Fatal("expected connection error after shutdown")
	}
}

func TestServer_PprofEnabled(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	rc := config.NewRuntimeConfig()
	rc.SetProfilingEnabled(true)
	s := NewServer(addr, nil, rc)

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for server to be ready
	for range 20 {
		_, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", addr))
	if err != nil {
		t.Fatalf("failed to reach pprof endpoint: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /debug/pprof/, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read pprof response: %v", err)
	}
	if !strings.Contains(string(body), "heap") {
		t.Fatal("expected pprof index to contain 'heap'")
	}
}

func TestServer_PprofDisabled(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	rc := config.NewRuntimeConfig()
	rc.SetProfilingEnabled(false)
	s := NewServer(addr, nil, rc)

	ctx := t.Context()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for server to be ready
	for range 20 {
		_, err := http.Get(fmt.Sprintf("http://%s/metrics", addr))
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	resp, err := http.Get(fmt.Sprintf("http://%s/debug/pprof/", addr))
	if err != nil {
		t.Fatalf("failed to reach server: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 from /debug/pprof/ when disabled, got %d", resp.StatusCode)
	}
	_ = errCh
}

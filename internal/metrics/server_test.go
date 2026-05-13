// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
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
	s := NewServer(":9090")
	if s.bindAddr != ":9090" {
		t.Fatalf("expected bindAddr ':9090', got '%s'", s.bindAddr)
	}
	if s.logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestServer_ServesMetrics(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	s := NewServer(addr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for server to be ready
	var resp *http.Response
	var err error
	for i := 0; i < 20; i++ {
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

func TestServer_GracefulShutdown(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	s := NewServer(addr)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for server to be ready
	for i := 0; i < 20; i++ {
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

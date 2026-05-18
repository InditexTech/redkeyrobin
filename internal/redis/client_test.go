// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestCheckConnection_Success(t *testing.T) {
	mr := miniredis.RunT(t)

	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	err := c.CheckConnection(context.Background(), 3, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestCheckConnection_FailsAfterRetries(t *testing.T) {
	// Connect to a non-existent address.
	c := NewClient("127.0.0.1:1", "")
	c.client.Options().MaxRetries = 0 // Disable go-redis internal retries for speed.
	defer func() { _ = c.Close() }()

	start := time.Now()
	err := c.CheckConnection(context.Background(), 3, 50*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for unreachable address")
	}
	// Should have waited at least 2 backoff periods (between retries 1-2 and 2-3).
	if elapsed < 100*time.Millisecond {
		t.Fatalf("expected at least 100ms for 3 retries with 50ms backoff, got %v", elapsed)
	}
}

func TestCheckConnection_RespectsContextCancellation(t *testing.T) {
	// Connect to a non-existent address so retries will fail.
	c := NewClient("127.0.0.1:1", "")
	c.client.Options().MaxRetries = 0
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay — should abort before all retries complete.
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := c.CheckConnection(ctx, 10, 100*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error when context is cancelled")
	}
	// Should have returned much sooner than 10*100ms = 1s.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected early exit on context cancel, took %v", elapsed)
	}
}

func TestCheckConnection_SucceedsOnRetry(t *testing.T) {
	mr := miniredis.RunT(t)
	addr := mr.Addr()

	// Close the server, then restart after a short delay.
	mr.Close()

	c := NewClient(addr, "")
	c.client.Options().MaxRetries = 0
	defer func() { _ = c.Close() }()

	// Restart miniredis after 80ms so a later retry succeeds.
	go func() {
		time.Sleep(80 * time.Millisecond)
		_ = mr.Restart()
	}()

	err := c.CheckConnection(context.Background(), 5, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("expected success after server restart, got %v", err)
	}
}

func TestGetInfo_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	mr.Close()

	_, err := c.GetInfo(context.Background())
	if err == nil {
		t.Fatal("expected error when server is closed")
	}
}

func TestAddr(t *testing.T) {
	c := NewClient("10.0.0.1:6379", "")
	defer func() { _ = c.Close() }()

	if c.Addr() != "10.0.0.1:6379" {
		t.Fatalf("expected '10.0.0.1:6379', got %q", c.Addr())
	}
}

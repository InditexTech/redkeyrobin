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

func TestPassword(t *testing.T) {
	c := NewClient("10.0.0.1:6379", "secret")
	defer func() { _ = c.Close() }()

	if c.Password() != "secret" {
		t.Fatalf("expected 'secret', got %q", c.Password())
	}
}

func TestPassword_Empty(t *testing.T) {
	c := NewClient("10.0.0.1:6379", "")
	defer func() { _ = c.Close() }()

	if c.Password() != "" {
		t.Fatalf("expected empty, got %q", c.Password())
	}
}

func TestClose(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")

	// Should succeed
	err := c.Close()
	if err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}

	// After close, operations should fail
	_, err = c.GetInfo(context.Background())
	if err == nil {
		t.Fatal("expected error after closing client")
	}
}

func TestGetClusterInfo_FailsOnNonCluster(t *testing.T) {
	// miniredis doesn't support CLUSTER INFO, so it should return an error
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	_, err := c.GetClusterInfo(context.Background())
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestGetClusterNodes_ReturnsEmptyOnStandalone(t *testing.T) {
	// miniredis supports CLUSTER NODES but returns empty for standalone
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	nodes, err := c.GetClusterNodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Standalone returns empty node list
	if len(nodes) > 1 {
		t.Fatalf("expected 0 or 1 nodes from standalone, got %d", len(nodes))
	}
}

func TestClusterMyID_FailsOnNonCluster(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	_, err := c.ClusterMyID(context.Background())
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestClusterMeet_FailsOnNonCluster(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	err := c.ClusterMeet(context.Background(), "10.0.0.1", 6379)
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestClusterAddSlots_FailsOnNonCluster(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	err := c.ClusterAddSlots(context.Background(), 0, 1, 2)
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestClusterReplicate_FailsOnNonCluster(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	err := c.ClusterReplicate(context.Background(), "some-node-id")
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestClusterReset_FailsOnNonCluster(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	err := c.ClusterReset(context.Background(), false)
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestClusterForget_FailsOnNonCluster(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	err := c.ClusterForget(context.Background(), "some-node-id")
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestGetInfo_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	info, err := c.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info == "" {
		t.Fatal("expected non-empty INFO response")
	}
}

func TestClusterSetSlotStable_FailsOnNonCluster(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	err := c.ClusterSetSlotStable(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error from miniredis which doesn't support cluster commands")
	}
}

func TestClusterSetSlotStable_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	mr.Close()

	err := c.ClusterSetSlotStable(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error when server is closed")
	}
}

func TestParseClusterNodesOutput_ExtractsIP(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		wantIP     string
		wantAddr   string
		wantSlots  string
	}{
		{
			name:     "normal address with cport",
			line:     "abc123 10.244.0.45:6379@16379 master - 0 1716710000000 1 connected 0-5460",
			wantIP:   "10.244.0.45",
			wantAddr: "10.244.0.45:6379@16379",
			wantSlots: "0-5460",
		},
		{
			name:     "address without cport",
			line:     "abc123 10.244.0.45:6379 master - 0 1716710000000 1 connected 0-5460",
			wantIP:   "10.244.0.45",
			wantAddr: "10.244.0.45:6379",
			wantSlots: "0-5460",
		},
		{
			name:     "empty IP with cport",
			line:     "abc123 :6379@16379 master - 0 1716710000000 1 connected 0-5460",
			wantIP:   "",
			wantAddr: ":6379@16379",
			wantSlots: "0-5460",
		},
		{
			name:     "empty IP without cport",
			line:     "abc123 :6379 master - 0 1716710000000 1 connected",
			wantIP:   "",
			wantAddr: ":6379",
			wantSlots: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := ParseClusterNodesOutput(tt.line)
			if len(nodes) != 1 {
				t.Fatalf("expected 1 node, got %d", len(nodes))
			}
			if nodes[0].IP != tt.wantIP {
				t.Errorf("IP = %q, want %q", nodes[0].IP, tt.wantIP)
			}
			if nodes[0].Addr != tt.wantAddr {
				t.Errorf("Addr = %q, want %q", nodes[0].Addr, tt.wantAddr)
			}
			if nodes[0].Slots != tt.wantSlots {
				t.Errorf("Slots = %q, want %q", nodes[0].Slots, tt.wantSlots)
			}
		})
	}
}

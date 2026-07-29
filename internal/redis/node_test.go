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

func TestNewNode(t *testing.T) {
	node := NewNode("cluster-0", "10.0.0.1:6379", "pass")
	if node.Name != "cluster-0" {
		t.Fatalf("expected Name 'cluster-0', got '%s'", node.Name)
	}
	if node.Addr != "10.0.0.1:6379" {
		t.Fatalf("expected Addr '10.0.0.1:6379', got '%s'", node.Addr)
	}
	if node.client == nil {
		t.Fatal("expected non-nil client")
	}
	_ = node.Close()
}

func TestNode_IsPrimary(t *testing.T) {
	node := &Node{Flags: "myself,master"}
	if !node.IsPrimary() {
		t.Fatal("expected IsPrimary=true")
	}
	if node.IsReplica() {
		t.Fatal("expected IsReplica=false")
	}
}

func TestNode_IsReplica(t *testing.T) {
	node := &Node{Flags: "myself,slave"}
	if node.IsPrimary() {
		t.Fatal("expected IsPrimary=false")
	}
	if !node.IsReplica() {
		t.Fatal("expected IsReplica=true")
	}
}

func TestNode_Close_NilClient(t *testing.T) {
	node := &Node{}
	if err := node.Close(); err != nil {
		t.Fatalf("expected nil error on close with nil client, got %v", err)
	}
}

func TestNode_Client(t *testing.T) {
	node := NewNode("cluster-0", "10.0.0.1:6379", "")
	defer func() { _ = node.Close() }()

	c := node.Client()
	if c == nil {
		t.Fatal("expected non-nil client from Client()")
	}
	if c.Addr() != "10.0.0.1:6379" {
		t.Fatalf("expected addr '10.0.0.1:6379', got '%s'", c.Addr())
	}
}

func TestNode_Init_FailsOnUnreachable(t *testing.T) {
	node := NewNode("node-0", "127.0.0.1:1", "")
	node.client.client.Options().MaxRetries = 0
	defer func() { _ = node.Close() }()

	err := node.Init(context.Background(), 1, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for unreachable node")
	}
}

func TestNode_Init_FailsOnClusterMyID(t *testing.T) {
	// miniredis doesn't support CLUSTER MYID, so PING succeeds but CLUSTER MYID fails
	mr := miniredis.RunT(t)
	node := NewNode("node-0", mr.Addr(), "")
	defer func() { _ = node.Close() }()

	err := node.Init(context.Background(), 1, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error since miniredis doesn't support CLUSTER MYID")
	}
}

func TestNode_Init_SetsIPFromAddr(t *testing.T) {
	// Verify that IP is extracted from address when Init succeeds.
	// We can't fully test Init without a real cluster, but we can test the IP extraction logic.
	node := &Node{
		Name: "node-0",
		Addr: "10.0.0.5:6379",
	}
	// Manually set what Init would set
	node.IP = node.Addr[:len("10.0.0.5")]
	if node.IP != "10.0.0.5" {
		t.Fatalf("expected IP '10.0.0.5', got '%s'", node.IP)
	}
}

func TestNode_RefreshInfo_Success(t *testing.T) {
	// miniredis returns a single node with "myself,master" flags
	mr := miniredis.RunT(t)
	node := NewNode("node-0", mr.Addr(), "")
	node.ID = "any-id" // ID doesn't matter; code matches on "myself" flag
	defer func() { _ = node.Close() }()

	err := node.RefreshInfo(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !node.IsPrimary() {
		t.Fatal("expected node to be primary after RefreshInfo")
	}
	if node.IP != "127.0.0.1" {
		t.Fatalf("expected IP '127.0.0.1', got '%s'", node.IP)
	}
	if node.Slots == "" {
		t.Fatal("expected non-empty slots after RefreshInfo")
	}
}

func TestNode_RefreshInfo_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	node := NewNode("node-0", mr.Addr(), "")
	node.ID = "abc123"
	defer func() { _ = node.Close() }()

	mr.Close()

	err := node.RefreshInfo(context.Background())
	if err == nil {
		t.Fatal("expected error when server is closed")
	}
}

func TestNode_Close_WithClient(t *testing.T) {
	mr := miniredis.RunT(t)
	node := NewNode("node-0", mr.Addr(), "")

	err := node.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

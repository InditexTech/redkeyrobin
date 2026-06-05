// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestClient_Save_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	mr.Close()

	if err := c.Save(context.Background()); err == nil {
		t.Fatal("expected error when server is closed")
	}
}

func TestClient_FlushAll_RemovesKeys(t *testing.T) {
	mr := miniredis.RunT(t)
	if err := mr.Set("key1", "value1"); err != nil {
		t.Fatalf("seeding key failed: %v", err)
	}
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	if err := c.FlushAll(context.Background()); err != nil {
		t.Fatalf("unexpected error on FLUSHALL: %v", err)
	}
	if mr.Exists("key1") {
		t.Fatal("expected key1 to be removed after FLUSHALL")
	}
}

func TestClient_FlushAll_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	mr.Close()

	if err := c.FlushAll(context.Background()); err == nil {
		t.Fatal("expected error when server is closed")
	}
}

func TestClient_ReplicaLinkUp_StandaloneIsFalse(t *testing.T) {
	// A standalone node is not a replica, so INFO has no master_link_status:up.
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	up, err := c.ReplicaLinkUp(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if up {
		t.Fatal("expected standalone node to report replica link down")
	}
}

func TestClient_ReplicaLinkUp_FailsOnClosedServer(t *testing.T) {
	mr := miniredis.RunT(t)
	c := NewClient(mr.Addr(), "")
	defer func() { _ = c.Close() }()

	mr.Close()

	if _, err := c.ReplicaLinkUp(context.Background()); err == nil {
		t.Fatal("expected error when server is closed")
	}
}

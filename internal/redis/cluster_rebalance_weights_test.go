// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"slices"
	"testing"
)

func TestClient_ClusterRebalanceWithWeights_BuildsWeightArgs(t *testing.T) {
	originalFactory := newRedisCLICommand
	var seenArgs []string
	newRedisCLICommand = func(ctx context.Context, args []string, env map[string]string) *RedisCLICommand {
		seenArgs = append([]string(nil), args...)
		return newCLICommand(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterRebalanceWithWeights(context.Background(), map[string]int{
		"nodeB": 0,
		"nodeA": 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !slices.Contains(seenArgs, "--cluster-weight") {
		t.Fatalf("expected --cluster-weight in args, got %v", seenArgs)
	}
	if !slices.Contains(seenArgs, "--cluster-use-empty-masters") {
		t.Fatalf("expected --cluster-use-empty-masters in args, got %v", seenArgs)
	}
	if !slices.Contains(seenArgs, "--cluster-replace") {
		t.Fatalf("expected --cluster-replace in args, got %v", seenArgs)
	}
	if !slices.Contains(seenArgs, "--cluster-yes") {
		t.Fatalf("expected --cluster-yes in args, got %v", seenArgs)
	}
	// Weights must be sorted by node ID for deterministic ordering.
	idxA := slices.Index(seenArgs, "nodeA=1")
	idxB := slices.Index(seenArgs, "nodeB=0")
	if idxA == -1 || idxB == -1 {
		t.Fatalf("expected weight tokens in args, got %v", seenArgs)
	}
	if idxA > idxB {
		t.Fatalf("expected nodeA before nodeB (sorted), got %v", seenArgs)
	}
}

func TestClient_ClusterRebalanceWithWeights_EmptyFallsBackToEmptyMasters(t *testing.T) {
	originalFactory := newRedisCLICommand
	var seenArgs []string
	newRedisCLICommand = func(ctx context.Context, args []string, env map[string]string) *RedisCLICommand {
		seenArgs = append([]string(nil), args...)
		return newCLICommand(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterRebalanceWithWeights(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if slices.Contains(seenArgs, "--cluster-weight") {
		t.Fatalf("did not expect --cluster-weight for empty weights, got %v", seenArgs)
	}
	if !slices.Contains(seenArgs, "--cluster-replace") {
		t.Fatalf("expected --cluster-replace in args, got %v", seenArgs)
	}
}

func TestHasInFlightSlots(t *testing.T) {
	stable := []ClusterNode{{Slots: "0-5460"}, {Slots: "5461-10922"}}
	if HasInFlightSlots(stable) {
		t.Fatal("expected no in-flight slots for stable cluster")
	}

	migrating := []ClusterNode{{Slots: "0-5460"}, {Slots: "5461-10922 [5461->-abc123]"}}
	if !HasInFlightSlots(migrating) {
		t.Fatal("expected in-flight slots when a node is migrating")
	}
}

func TestSlotsFullyCovered(t *testing.T) {
	full := []ClusterNode{
		{Flags: "myself,master", Slots: "0-5460"},
		{Flags: "master", Slots: "5461-10922"},
		{Flags: "master", Slots: "10923-16383"},
	}
	if !SlotsFullyCovered(full) {
		t.Fatal("expected full coverage when all 16384 slots are owned")
	}

	// A gap (5461-10922 missing) left by a vanished primary must be detected.
	gap := []ClusterNode{
		{Flags: "myself,master", Slots: "0-5460"},
		{Flags: "master", Slots: "10923-16383"},
		{Flags: "master", Slots: ""},
	}
	if SlotsFullyCovered(gap) {
		t.Fatal("expected coverage gap to be detected")
	}

	// Replica slot data must be ignored; only masters count.
	replicaOnly := []ClusterNode{
		{Flags: "slave", Slots: "0-16383"},
	}
	if SlotsFullyCovered(replicaOnly) {
		t.Fatal("expected replicas not to count toward coverage")
	}

	// In-flight migration tokens must be ignored for stable coverage.
	covered := SlotsFullyCovered([]ClusterNode{
		{Flags: "myself,master", Slots: "0-5460 [5461-<-abc123]"},
		{Flags: "master", Slots: "5461-10922"},
		{Flags: "master", Slots: "10923-16383"},
	})
	if !covered {
		t.Fatal("expected coverage to be satisfied ignoring in-flight tokens")
	}
}

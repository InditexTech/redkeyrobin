// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/inditextech/redkey-robin/internal/redis"
)

// --- replicaSpreadOK tests ---

func TestReplicaSpreadOK_NoReplicasExpected_Pass(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "10922-16383"},
	}
	if !replicaSpreadOK(nodes, 3, 0) {
		t.Fatal("expected replicaSpreadOK=true when desiredReplicas=0 and no replicas exist")
	}
}

func TestReplicaSpreadOK_NoReplicasExpected_FailWhenReplicasExist(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", Flags: "slave", State: "connected", Primary: "id-1"},
	}
	if replicaSpreadOK(nodes, 3, 0) {
		t.Fatal("expected replicaSpreadOK=false when desiredReplicas=0 but replicas exist")
	}
}

func TestReplicaSpreadOK_OneReplicaPerPrimary_Pass(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-5", Flags: "slave", State: "connected", Primary: "id-2"},
		{ID: "id-6", Flags: "slave", State: "connected", Primary: "id-3"},
	}
	if !replicaSpreadOK(nodes, 3, 1) {
		t.Fatal("expected replicaSpreadOK=true with correct replica spread")
	}
}

func TestReplicaSpreadOK_OrphanedReplica(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-5", Flags: "slave", State: "connected", Primary: "id-2"},
		{ID: "id-6", Flags: "slave", State: "connected", Primary: "unknown-id"}, // orphaned
	}
	if replicaSpreadOK(nodes, 3, 1) {
		t.Fatal("expected replicaSpreadOK=false with orphaned replica")
	}
}

func TestReplicaSpreadOK_UnevenDistribution(t *testing.T) {
	// id-1 has 2 replicas, id-2 has 1, id-3 has 0
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-5", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-6", Flags: "slave", State: "connected", Primary: "id-2"},
	}
	if replicaSpreadOK(nodes, 3, 1) {
		t.Fatal("expected replicaSpreadOK=false with uneven distribution")
	}
}

func TestReplicaSpreadOK_PrimaryCountMismatch(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-8191"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "8192-16383"},
		{ID: "id-3", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-4", Flags: "slave", State: "connected", Primary: "id-2"},
	}
	// Expect 3 primaries but have 2
	if replicaSpreadOK(nodes, 3, 1) {
		t.Fatal("expected replicaSpreadOK=false when primary count doesn't match desired")
	}
}

// --- visibleNodeIDs tests ---

func TestVisibleNodeIDs_EmptyID(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "", Flags: "master", State: "connected"},
	}
	_, ok := visibleNodeIDs(nodes)
	if ok {
		t.Fatal("expected failure for node with empty ID")
	}
}

func TestVisibleNodeIDs_FailingFlag(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master,fail", State: "connected"},
	}
	_, ok := visibleNodeIDs(nodes)
	if ok {
		t.Fatal("expected failure for node with 'fail' flag")
	}
}

func TestVisibleNodeIDs_HandshakeFlag(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master,handshake", State: "connected"},
	}
	_, ok := visibleNodeIDs(nodes)
	if ok {
		t.Fatal("expected failure for node with 'handshake' flag")
	}
}

func TestVisibleNodeIDs_PfailFlag(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master,pfail", State: "connected"},
	}
	_, ok := visibleNodeIDs(nodes)
	if ok {
		t.Fatal("expected failure for node with 'pfail' flag")
	}
}

func TestVisibleNodeIDs_DisconnectedState(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "disconnected"},
	}
	_, ok := visibleNodeIDs(nodes)
	if ok {
		t.Fatal("expected failure for disconnected node")
	}
}

func TestVisibleNodeIDs_ValidNodes(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected"},
		{ID: "id-2", Flags: "slave", State: "connected"},
	}
	ids, ok := visibleNodeIDs(nodes)
	if !ok {
		t.Fatal("expected success for valid nodes")
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 IDs, got %d", len(ids))
	}
}

// --- slotsCovered tests ---

func TestSlotsCovered_OverlappingRanges(t *testing.T) {
	// Overlapping ranges should fail
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-8000"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "7000-16383"},
	}
	if slotsCovered(nodes) {
		t.Fatal("expected slotsCovered=false for overlapping ranges")
	}
}

func TestSlotsCovered_NoMasters(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "slave", State: "connected", Primary: "id-2"},
	}
	if slotsCovered(nodes) {
		t.Fatal("expected slotsCovered=false when no masters exist")
	}
}

func TestSlotsCovered_StartNotZero(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "1-16383"},
	}
	if slotsCovered(nodes) {
		t.Fatal("expected slotsCovered=false when first slot is not 0")
	}
}

func TestSlotsCovered_NotContiguous(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5000"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5002-16383"},
	}
	if slotsCovered(nodes) {
		t.Fatal("expected slotsCovered=false when there's a gap")
	}
}

func TestSlotsCovered_FullCoverage(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "10922-16383"},
	}
	if !slotsCovered(nodes) {
		t.Fatal("expected slotsCovered=true for full coverage")
	}
}

// --- balanced tests ---

func TestBalanced_EmptyPrimaries(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "slave", State: "connected"},
	}
	if balanced(nodes) {
		t.Fatal("expected balanced=false with no primaries")
	}
}

func TestBalanced_PrimaryWithZeroSlots(t *testing.T) {
	nodes := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-16383"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: ""},
	}
	if balanced(nodes) {
		t.Fatal("expected balanced=false when a primary has zero slots")
	}
}

// --- reconcileClients tests ---

func TestReconcileClients_PasswordChange(t *testing.T) {
	created := map[string]int{}
	factory := func(addr, password string) ClusterClient {
		created[addr]++
		return &fakeClusterClient{
			clusterInfo:  healthyClusterInfo(),
			clusterNodes: healthyClusterNodes(),
			clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
		}
	}
	checker := NewChecker(nil, factory, 2*time.Second)

	nodes := []Node{{Name: "n1", Addr: "addr1:6379"}}

	// First reconcile with password "a"
	checker.reconcileClients(nodes, "a")
	// Should have no clients yet (reconcile only removes stale ones)
	if len(checker.clients) != 0 {
		t.Fatalf("expected 0 clients before any probing, got %d", len(checker.clients))
	}

	// Simulate having a cached client
	checker.clients["addr1:6379"] = &fakeClusterClient{}
	checker.clientsPassword = "a"

	// Reconcile with different password should clear all clients
	checker.reconcileClients(nodes, "b")
	if len(checker.clients) != 0 {
		t.Fatalf("expected 0 clients after password change, got %d", len(checker.clients))
	}
}

func TestReconcileClients_RemovesStaleNodes(t *testing.T) {
	factory := func(addr, password string) ClusterClient {
		return &fakeClusterClient{
			clusterInfo:  healthyClusterInfo(),
			clusterNodes: healthyClusterNodes(),
			clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
		}
	}
	checker := NewChecker(nil, factory, 2*time.Second)

	// Add cached clients for 3 nodes
	checker.clients["addr1:6379"] = &fakeClusterClient{}
	checker.clients["addr2:6379"] = &fakeClusterClient{}
	checker.clients["addr3:6379"] = &fakeClusterClient{}
	checker.clientsPassword = "pass"

	// Reconcile with only 2 nodes → addr3 should be removed
	nodes := []Node{
		{Name: "n1", Addr: "addr1:6379"},
		{Name: "n2", Addr: "addr2:6379"},
	}
	checker.reconcileClients(nodes, "pass")

	if _, exists := checker.clients["addr3:6379"]; exists {
		t.Fatal("expected addr3 to be removed from cached clients")
	}
	if len(checker.clients) != 2 {
		t.Fatalf("expected 2 clients remaining, got %d", len(checker.clients))
	}
}

// --- Check edge cases ---

func TestChecker_CheckWithNoNodes(t *testing.T) {
	checker := NewChecker(nil, func(addr, password string) ClusterClient {
		return &fakeClusterClient{}
	}, 2*time.Second)

	report, err := checker.Check(context.Background(), nil, "", 3, 0)
	if err == nil {
		t.Fatal("expected error for nil nodes")
	}
	if report == nil {
		t.Fatal("expected non-nil report even on error")
	}
}

func TestChecker_CheckAllNodesUnreachable(t *testing.T) {
	nodes := testNodes()
	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		nodes[0].Addr: {clusterInfoErr: errors.New("conn refused")},
		nodes[1].Addr: {clusterInfoErr: errors.New("conn refused")},
		nodes[2].Addr: {clusterInfoErr: errors.New("conn refused")},
	}), 2*time.Second)

	_, err := checker.Check(context.Background(), nodes, "", 3, 0)
	if err == nil {
		t.Fatal("expected error when all nodes are unreachable")
	}
}

func TestChecker_ReplicaSpreadCheck_WithReplicas(t *testing.T) {
	nodes := []Node{
		{Name: "n0", Addr: "10.0.0.1:6379"},
		{Name: "n1", Addr: "10.0.0.2:6379"},
		{Name: "n2", Addr: "10.0.0.3:6379"},
		{Name: "n3", Addr: "10.0.0.4:6379"},
		{Name: "n4", Addr: "10.0.0.5:6379"},
		{Name: "n5", Addr: "10.0.0.6:6379"},
	}
	clusterNodes := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", IP: "10.0.0.4", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-5", IP: "10.0.0.5", Flags: "slave", State: "connected", Primary: "id-2"},
		{ID: "id-6", IP: "10.0.0.6", Flags: "slave", State: "connected", Primary: "id-3"},
	}
	info := &redis.ClusterInfo{State: "ok", SlotsOK: 16384, KnownNodes: 6, ClusterSize: 3}

	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		"10.0.0.1:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.2:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.3:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.4:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.5:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.6:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.ReplicaSpreadOK {
		t.Fatal("expected ReplicaSpreadOK=true with correct spread")
	}
	if !report.Healthy {
		t.Fatal("expected healthy=true")
	}
}

func TestChecker_ReplicaSpreadCheck_BadSpread(t *testing.T) {
	nodes := []Node{
		{Name: "n0", Addr: "10.0.0.1:6379"},
		{Name: "n1", Addr: "10.0.0.2:6379"},
		{Name: "n2", Addr: "10.0.0.3:6379"},
		{Name: "n3", Addr: "10.0.0.4:6379"},
		{Name: "n4", Addr: "10.0.0.5:6379"},
		{Name: "n5", Addr: "10.0.0.6:6379"},
	}
	// id-1 has 2 replicas, id-3 has none
	clusterNodes := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", IP: "10.0.0.4", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-5", IP: "10.0.0.5", Flags: "slave", State: "connected", Primary: "id-1"},
		{ID: "id-6", IP: "10.0.0.6", Flags: "slave", State: "connected", Primary: "id-2"},
	}
	info := &redis.ClusterInfo{State: "ok", SlotsOK: 16384, KnownNodes: 6, ClusterSize: 3}

	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		"10.0.0.1:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.2:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.3:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.4:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.5:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		"10.0.0.6:6379": {clusterInfo: info, clusterNodes: clusterNodes, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.ReplicaSpreadOK {
		t.Fatal("expected ReplicaSpreadOK=false with bad spread")
	}
	if report.Healthy {
		t.Fatal("expected healthy=false")
	}
}

func TestChecker_CloseAll(t *testing.T) {
	factory := func(addr, password string) ClusterClient {
		return &fakeClusterClient{}
	}
	checker := NewChecker(nil, factory, 2*time.Second)
	checker.clients["a"] = &fakeClusterClient{}
	checker.clients["b"] = &fakeClusterClient{}

	checker.CloseAll()
	if len(checker.clients) != 0 {
		t.Fatalf("expected 0 clients after CloseAll, got %d", len(checker.clients))
	}
}

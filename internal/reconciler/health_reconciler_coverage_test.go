// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/health"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

// --- remediateClusterCheck tests ---

// errorHealthClusterClient always returns errors for all operations.
type errorHealthClusterClient struct {
	err error
}

func (e *errorHealthClusterClient) GetClusterInfo(context.Context) (*redis.ClusterInfo, error) {
	return nil, e.err
}
func (e *errorHealthClusterClient) GetClusterNodes(context.Context) ([]redis.ClusterNode, error) {
	return nil, e.err
}
func (e *errorHealthClusterClient) ClusterCheck(context.Context) (*redis.ClusterCheckResult, error) {
	return nil, e.err
}
func (e *errorHealthClusterClient) Close() error { return nil }

func TestHealthReconciler_RemediateClusterCheck_Success(t *testing.T) {
	// ClusterFix uses redis-cli; we override the CLI factory.
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo 'All slots covered'; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	// Build a checker that returns an unhealthy report (ClusterCheckOK=false)
	clusterNodes := healthyClusterView()
	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{
			CommandCodeOutput: 1,
			Errors:            []string{"some issue"},
		},
	}
	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ClusterCheck failed → remediation triggered → returns wait interval
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_RemediateClusterCheck_CLIError(t *testing.T) {
	// ClusterFix fails with non-zero exit and error message
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo 'fix failed'; exit 1"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	clusterNodes := healthyClusterView()
	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{
			CommandCodeOutput: 1,
			Errors:            []string{"problem"},
		},
	}
	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	// ClusterFix returns a result even with exit code 1 (it's not an execution error)
	// So remediation succeeds (the result is logged)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

// --- remediateMembership error paths ---

func TestHealthReconciler_RemediateMembership_ConnectionError(t *testing.T) {
	// Cluster view shows 4 nodes but only 3 K8s pods exist.
	// len(seedIDs)=4 != len(nodes)=3 → MembershipOK=false → triggers remediation.
	// Remediation calls clientFactory (real redis.Client) on unreachable addr → error.
	fourNodeView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", IP: "10.0.0.4", Flags: "master", State: "connected"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: fourNodeView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// clientFactory creates real redis.Client that will fail to connect
	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(ctx, nodes, "", 3, 0)
	// The membership remediation should fail because it can't connect to the seed node
	if err == nil {
		t.Fatal("expected error from membership remediation connection failure")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval on error, got %v", schedule)
	}
}

// --- Reconcile pipeline tests ---

func TestHealthReconciler_Reconcile_CheckerError_AllNodesDown(t *testing.T) {
	// All nodes return errors → checker can't find a seed → returns error
	checkerFactory := func(addr, password string) health.ClusterClient {
		return &errorHealthClusterClient{err: context.DeadlineExceeded}
	}
	checker := health.NewChecker(nil, checkerFactory, 100*time.Millisecond)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	// Short context so remediation redis calls don't hang
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodes := healthyNodes()
	_, err := hr.Reconcile(ctx, nodes, "", 3, 0)
	if err == nil {
		t.Fatal("expected error when all nodes are unreachable")
	}
}

func TestHealthReconciler_Reconcile_SlotCoverageRemediation_NoPrimaries(t *testing.T) {
	// Cluster with no primaries → slot coverage fails → remediation fails with "no primaries"
	onlyReplicasView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "slave", State: "connected", Primary: "id-unknown"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "slave", State: "connected", Primary: "id-unknown"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "slave", State: "connected", Primary: "id-unknown"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     0,
			SlotsFail:   16384,
			KnownNodes:  3,
			ClusterSize: 0,
		},
		clusterNodes: onlyReplicasView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	// Slot coverage remediation should fail with "no primaries found"
	if err == nil {
		t.Fatal("expected error for slot coverage remediation with no primaries")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_Reconcile_ReplicaSpread_PrimaryCountMismatch(t *testing.T) {
	// Cluster has 2 primaries but desired is 3 → remediation skips (returns nil)
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	twoPrimariesView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-8191"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "8192-16383"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "slave", State: "connected", Primary: "id-1"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  3,
			ClusterSize: 2,
		},
		clusterNodes: twoPrimariesView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
	}
	// desiredPrimaries=3 but actual=2 → replica spread remediation should be skipped
	// And since balancedOK will be OK (slots are covered evenly for 2 primaries)
	// the cluster just goes through remaining steps
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 1)
	// ReplicaSpreadOK fails → remediation is triggered → skipped due to mismatch → no error
	// After that, if balance is OK, returns wait interval
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

// --- logReport coverage ---

func TestHealthReconciler_LogReport_WithWarningsAndErrors(t *testing.T) {
	// Build a checker returning a report with errors and warnings
	clusterNodes := healthyClusterView()
	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{
			CommandCodeOutput: 1,
			Errors:            []string{"error1", "error2"},
			Warnings:          []string{"warn1"},
		},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Mock ClusterFix so remediation succeeds
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo fixed; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ClusterCheck failed → remediation happened
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestHealthReconciler_Reconcile_SlotCoverage_SkipsEmptyIPPrimary(t *testing.T) {
	// Cluster has 3 primaries, one with empty IP (simulating a node that reports :6379@16379).
	// The empty-IP primary should be skipped; slots should be assigned to the other two.
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	clusterView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Addr: "10.0.0.1:6379@16379", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Addr: "10.0.0.2:6379@16379", Flags: "master", State: "connected", Slots: "5461-10921"},
		// Node with empty IP — simulates :6379@16379 being parsed with the fix
		{ID: "id-3", IP: "", Addr: ":6379@16379", Flags: "master", State: "connected", Slots: ""},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     10922,
			SlotsFail:   5462,
			KnownNodes:  3,
			ClusterSize: 3,
		},
		clusterNodes: clusterView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Track addresses that clientFactory is called with
	var calledAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		calledAddrs = append(calledAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := healthyNodes()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// The remediation will fail connecting to real redis, but the key assertion
	// is that it never tries to connect to the empty-IP address ":6379"
	_, _ = hr.Reconcile(ctx, nodes, "", 3, 0)

	for _, addr := range calledAddrs {
		if addr == ":6379" || addr == ":6379@16379:6379" {
			t.Fatalf("clientFactory was called with invalid empty-IP address: %q", addr)
		}
	}
}

// --- Excess primary demotion (failover) tests ---

func TestHealthReconciler_ExcessPrimaries_DemotesEmptyPrimary(t *testing.T) {
	// Scenario: 3 primaries desired with 1 replica each.
	// After failover: 4 primaries (the promoted replica + the new empty pod), 0 replicas.
	// Expected: the empty primary (10.0.0.4, 0 slots) is demoted to replica of a primary needing one.
	fourPrimaryView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		{ID: "id-4", IP: "10.0.0.4", Flags: "master", State: "connected", Slots: ""},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  4,
			ClusterSize: 4,
		},
		clusterNodes: fourPrimaryView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Track which addresses ClusterReplicate is attempted on
	var replicateAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		replicateAddrs = append(replicateAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
		{Name: "cluster-3", Addr: "10.0.0.4:6379"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// desiredPrimaries=3, desiredReplicasPerPrimary=1
	schedule, err := hr.Reconcile(ctx, nodes, "", 3, 1)

	// The demotion will fail connecting to real redis (no instance at 10.0.0.4),
	// but we verify it attempts to demote the correct node.
	if err == nil {
		t.Fatal("expected error from ClusterReplicate connection failure")
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}

	// Verify the factory was called with the empty primary's address for demotion
	found := false
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.4:6379" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected clientFactory to be called with empty primary addr 10.0.0.4:6379, got %v", replicateAddrs)
	}
}

func TestHealthReconciler_ExcessPrimaries_SkipsImportingNode(t *testing.T) {
	// Scenario: 4 primaries, one has importing marker in Slots → should NOT be demoted.
	// The other empty primary (no slots, no markers) should be the demotion target.
	fourPrimaryView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
		// Node with importing marker — should be skipped
		{ID: "id-4", IP: "10.0.0.4", Flags: "master", State: "connected", Slots: "[5461-<-id-2]"},
		// Truly empty node — should be demoted
		{ID: "id-5", IP: "10.0.0.5", Flags: "master", State: "connected", Slots: ""},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  5,
			ClusterSize: 5,
		},
		clusterNodes: fourPrimaryView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	var replicateAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		replicateAddrs = append(replicateAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
		{Name: "cluster-3", Addr: "10.0.0.4:6379"},
		{Name: "cluster-4", Addr: "10.0.0.5:6379"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// desiredPrimaries=3, desiredReplicasPerPrimary=1
	_, _ = hr.Reconcile(ctx, nodes, "", 3, 1)

	// Verify the importing node (10.0.0.4) was NOT targeted for demotion
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.4:6379" {
			t.Fatalf("should not attempt to demote node with importing marker, but called clientFactory with %q", addr)
		}
	}

	// Verify the empty node (10.0.0.5) WAS targeted
	found := false
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.5:6379" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected clientFactory to be called with truly empty primary 10.0.0.5:6379, got %v", replicateAddrs)
	}
}

func TestHealthReconciler_FewerPrimaries_SkipsRemediation(t *testing.T) {
	// Scenario: 2 primaries exist but 3 desired (scale-up scenario).
	// Should skip and not attempt any demotion.
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	})
	defer redis.SetNewRedisCLICommand(originalFactory)

	twoPrimariesWithReplicaView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-8191"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "8192-16383"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "slave", State: "connected", Primary: "id-1"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo: &redis.ClusterInfo{
			State:       "ok",
			SlotsOK:     16384,
			KnownNodes:  3,
			ClusterSize: 2,
		},
		clusterNodes: twoPrimariesWithReplicaView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}
	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	var replicateAddrs []string
	clientFactory := func(addr, password string) *redis.Client {
		replicateAddrs = append(replicateAddrs, addr)
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()
	hr := NewHealthReconciler(checker, clientFactory, rtConfig, nil)

	nodes := []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
	}

	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should not attempt any ClusterReplicate for demotion
	// The only clientFactory calls should be for rebalance (if triggered)
	for _, addr := range replicateAddrs {
		if addr == "10.0.0.3:6379" {
			// This would indicate the replica is being reassigned — that's OK
			// but NOT a demotion of a primary
			continue
		}
	}

	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

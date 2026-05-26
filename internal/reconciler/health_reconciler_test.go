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

// fakeHealthClusterClient implements health.ClusterClient for testing.
type fakeHealthClusterClient struct {
	clusterInfo  *redis.ClusterInfo
	clusterNodes []redis.ClusterNode
	clusterCheck *redis.ClusterCheckResult
}

func (f *fakeHealthClusterClient) GetClusterInfo(context.Context) (*redis.ClusterInfo, error) {
	return f.clusterInfo, nil
}

func (f *fakeHealthClusterClient) GetClusterNodes(context.Context) ([]redis.ClusterNode, error) {
	return append([]redis.ClusterNode(nil), f.clusterNodes...), nil
}

func (f *fakeHealthClusterClient) ClusterCheck(context.Context) (*redis.ClusterCheckResult, error) {
	if f.clusterCheck == nil {
		return &redis.ClusterCheckResult{CommandCodeOutput: 0}, nil
	}
	return f.clusterCheck, nil
}

func (f *fakeHealthClusterClient) Close() error { return nil }

func healthyNodes() []health.Node {
	return []health.Node{
		{Name: "cluster-0", Addr: "10.0.0.1:6379"},
		{Name: "cluster-1", Addr: "10.0.0.2:6379"},
		{Name: "cluster-2", Addr: "10.0.0.3:6379"},
	}
}

func healthyClusterView() []redis.ClusterNode {
	return []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "10922-16383"},
	}
}

func healthyInfo() *redis.ClusterInfo {
	return &redis.ClusterInfo{
		State:       "ok",
		SlotsOK:     16384,
		SlotsFail:   0,
		KnownNodes:  3,
		ClusterSize: 3,
	}
}

func newTestHealthReconciler(clusterNodes []redis.ClusterNode, clusterInfo *redis.ClusterInfo) *HealthReconciler {
	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  clusterInfo,
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}

	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Redis client factory (returns real client, but unused since remediation won't trigger)
	clientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}

	rtConfig := config.NewRuntimeConfig()

	return NewHealthReconciler(checker, clientFactory, rtConfig, nil)
}

func TestHealthReconciler_Reconcile_HealthyCluster_NoRemediation(t *testing.T) {
	hr := newTestHealthReconciler(healthyClusterView(), healthyInfo())
	nodes := healthyNodes()

	schedule, err := hr.Reconcile(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected reconcileAfterInterval, got %v", schedule)
	}
}

func TestHealthReconciler_Reconcile_EmptyNodes_Error(t *testing.T) {
	hr := newTestHealthReconciler(healthyClusterView(), healthyInfo())

	_, err := hr.Reconcile(context.Background(), nil, "", 3, 0)
	if err == nil {
		t.Fatal("expected error for nil nodes")
	}
}

func TestHealthReconciler_Reconcile_EmptyNodeList_Error(t *testing.T) {
	hr := newTestHealthReconciler(healthyClusterView(), healthyInfo())

	_, err := hr.Reconcile(context.Background(), []health.Node{}, "", 3, 0)
	if err == nil {
		t.Fatal("expected error for empty node list")
	}
}

func TestHealthReconciler_Reconcile_UnbalancedCluster_TriggersRemediation(t *testing.T) {
	// One primary has far too many slots (unbalanced)
	unbalancedView := []redis.ClusterNode{
		{ID: "id-1", IP: "10.0.0.1", Flags: "master", State: "connected", Slots: "0-100"},
		{ID: "id-2", IP: "10.0.0.2", Flags: "master", State: "connected", Slots: "101-8200"},
		{ID: "id-3", IP: "10.0.0.3", Flags: "master", State: "connected", Slots: "8201-16383"},
	}

	fakeClient := &fakeHealthClusterClient{
		clusterInfo:  healthyInfo(),
		clusterNodes: unbalancedView,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
	}

	checkerFactory := func(addr, password string) health.ClusterClient {
		return fakeClient
	}

	checker := health.NewChecker(nil, checkerFactory, 2*time.Second)

	// Client factory - since rebalance will actually try to run redis-cli,
	// we swap newRedisCLICommand to no-op
	originalFactory := redis.ExportNewRedisCLICommand()
	redis.SetNewRedisCLICommand(func(ctx context.Context, args []string, env map[string]string) *redis.RedisCLICommand {
		return redis.NewCLICommandExported(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
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
	// Unbalanced → remediation happened → returns wait interval
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}
}

func TestExtractIP(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"10.0.0.1:6379", "10.0.0.1"},
		{"10.0.0.1", "10.0.0.1"},
		{":6379", ":6379"},
		{"", ""},
	}
	for _, tc := range tests {
		got := extractIP(tc.input)
		if got != tc.expected {
			t.Errorf("extractIP(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestTruncateOutput(t *testing.T) {
	short := "hello"
	if truncateOutput(short, 10) != "hello" {
		t.Fatal("short string should not be truncated")
	}

	long := "abcdefghijklmnop"
	result := truncateOutput(long, 5)
	if result != "abcde...(truncated)" {
		t.Fatalf("expected truncated result, got %q", result)
	}
}

func TestMin(t *testing.T) {
	if min(3, 5) != 3 {
		t.Fatal("min(3,5) should be 3")
	}
	if min(5, 3) != 3 {
		t.Fatal("min(5,3) should be 3")
	}
	if min(4, 4) != 4 {
		t.Fatal("min(4,4) should be 4")
	}
}

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

type fakeClusterClient struct {
	clusterInfo  *redis.ClusterInfo
	clusterNodes []redis.ClusterNode
	clusterCheck *redis.ClusterCheckResult

	clusterInfoErr  error
	clusterNodesErr error
	clusterCheckErr error
}

func (f *fakeClusterClient) GetClusterInfo(context.Context) (*redis.ClusterInfo, error) {
	if f.clusterInfoErr != nil {
		return nil, f.clusterInfoErr
	}
	return f.clusterInfo, nil
}

func (f *fakeClusterClient) GetClusterNodes(context.Context) ([]redis.ClusterNode, error) {
	if f.clusterNodesErr != nil {
		return nil, f.clusterNodesErr
	}
	return append([]redis.ClusterNode(nil), f.clusterNodes...), nil
}

func (f *fakeClusterClient) ClusterCheck(context.Context) (*redis.ClusterCheckResult, error) {
	if f.clusterCheckErr != nil {
		return nil, f.clusterCheckErr
	}
	if f.clusterCheck == nil {
		return &redis.ClusterCheckResult{CommandCodeOutput: -1}, nil
	}
	copyResult := *f.clusterCheck
	copyResult.Errors = append([]string(nil), f.clusterCheck.Errors...)
	copyResult.Warnings = append([]string(nil), f.clusterCheck.Warnings...)
	return &copyResult, nil
}

func (f *fakeClusterClient) Close() error { return nil }

func TestChecker_CheckHealthyClusterWithSeedFallback(t *testing.T) {
	nodes := testNodes()
	views := healthyClusterNodes()
	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		nodes[0].Addr: {clusterInfoErr: errors.New("boom"), clusterNodes: views, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		nodes[1].Addr: {
			clusterInfo:  healthyClusterInfo(),
			clusterNodes: views,
			clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
		},
		nodes[2].Addr: {
			clusterInfo:  healthyClusterInfo(),
			clusterNodes: views,
			clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
		},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.MembershipOK || !report.SlotsCoveredOK || !report.BalancedOK || !report.ClusterCheckOK || !report.Healthy {
		t.Fatalf("expected fully healthy report, got %+v", report)
	}
	if report.ClusterCheckCommandOutputCode != 0 {
		t.Fatalf("expected cluster check exit code 0, got %d", report.ClusterCheckCommandOutputCode)
	}
}

func TestChecker_CheckFailsMembershipWhenNodeUnreachable(t *testing.T) {
	nodes := testNodes()
	views := healthyClusterNodes()
	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		nodes[0].Addr: {
			clusterInfo:  healthyClusterInfo(),
			clusterNodes: views,
			clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
		},
		nodes[1].Addr: {
			clusterInfo:  healthyClusterInfo(),
			clusterNodes: views,
			clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0},
		},
		nodes[2].Addr: {clusterNodesErr: errors.New("unreachable")},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.MembershipOK {
		t.Fatalf("expected membership failure, got %+v", report)
	}
	if report.SlotsCoveredOK != true || report.BalancedOK != true || report.ClusterCheckOK != true {
		t.Fatalf("expected structural checks other than membership to remain true, got %+v", report)
	}
	if report.Healthy {
		t.Fatal("expected overall healthy=false")
	}
}

func TestChecker_CheckFailsSlotCoverageOnGap(t *testing.T) {
	nodes := testNodes()
	gapView := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-100"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "102-200"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "201-16383"},
	}
	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		nodes[0].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: gapView, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		nodes[1].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: gapView, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		nodes[2].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: gapView, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.SlotsCoveredOK {
		t.Fatalf("expected slot coverage failure, got %+v", report)
	}
}

func TestChecker_CheckFailsBalanceWhenPrimaryHasTooManySlots(t *testing.T) {
	nodes := testNodes()
	unbalancedView := []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-100"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "101-8200"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "8201-16383"},
	}
	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		nodes[0].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: unbalancedView, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		nodes[1].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: unbalancedView, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
		nodes[2].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: unbalancedView, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 0}},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.BalancedOK {
		t.Fatalf("expected balance failure, got %+v", report)
	}
}

func TestChecker_CheckFailsHealthyWhenClusterCheckFails(t *testing.T) {
	nodes := testNodes()
	views := healthyClusterNodes()
	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		nodes[0].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: views, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 1, Errors: []string{"problem"}}},
		nodes[1].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: views, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 1, Errors: []string{"problem"}}},
		nodes[2].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: views, clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: 1, Errors: []string{"problem"}}},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.MembershipOK || !report.SlotsCoveredOK || !report.BalancedOK {
		t.Fatalf("expected structural checks to pass, got %+v", report)
	}
	if report.ClusterCheckOK {
		t.Fatalf("expected cluster check to fail, got %+v", report)
	}
	if report.Healthy {
		t.Fatal("expected overall healthy=false")
	}
	if report.ClusterCheckCommandOutputCode != 1 {
		t.Fatalf("expected cluster check exit code 1, got %d", report.ClusterCheckCommandOutputCode)
	}
}

func TestChecker_CheckReturnsStructuralReportWhenClusterCheckErrors(t *testing.T) {
	nodes := testNodes()
	views := healthyClusterNodes()
	checker := NewChecker(nil, fakeFactory(map[string]*fakeClusterClient{
		nodes[0].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: views, clusterCheckErr: errors.New("redis-cli unavailable")},
		nodes[1].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: views, clusterCheckErr: errors.New("redis-cli unavailable")},
		nodes[2].Addr: {clusterInfo: healthyClusterInfo(), clusterNodes: views, clusterCheckErr: errors.New("redis-cli unavailable")},
	}), 2*time.Second)

	report, err := checker.Check(context.Background(), nodes, "", 3, 0)
	if err == nil {
		t.Fatal("expected partial error from cluster check execution failure")
	}
	if !report.MembershipOK || !report.SlotsCoveredOK || !report.BalancedOK {
		t.Fatalf("expected structural checks to remain available, got %+v", report)
	}
	if report.ClusterCheckOK {
		t.Fatalf("expected cluster check to be false, got %+v", report)
	}
	if report.ClusterCheckCommandOutputCode != -1 {
		t.Fatalf("expected unknown command output code -1, got %d", report.ClusterCheckCommandOutputCode)
	}
	if report.Healthy {
		t.Fatal("expected overall healthy=false")
	}
}

func fakeFactory(clients map[string]*fakeClusterClient) ClientFactory {
	return func(addr, password string) ClusterClient {
		client, ok := clients[addr]
		if !ok {
			return &fakeClusterClient{clusterInfoErr: errors.New("missing fake client")}
		}
		return client
	}
}

func testNodes() []Node {
	return []Node{
		{Name: "cluster-0", Addr: "node-0:6379"},
		{Name: "cluster-1", Addr: "node-1:6379"},
		{Name: "cluster-2", Addr: "node-2:6379"},
	}
}

func healthyClusterInfo() *redis.ClusterInfo {
	return &redis.ClusterInfo{
		State:       "ok",
		SlotsOK:     16384,
		SlotsFail:   0,
		KnownNodes:  3,
		ClusterSize: 3,
	}
}

func healthyClusterNodes() []redis.ClusterNode {
	return []redis.ClusterNode{
		{ID: "id-1", Flags: "master", State: "connected", Slots: "0-5460"},
		{ID: "id-2", Flags: "master", State: "connected", Slots: "5461-10921"},
		{ID: "id-3", Flags: "master", State: "connected", Slots: "10922-16383"},
	}
}

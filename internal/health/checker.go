// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/inditextech/redkeyrobin/internal/redis"
)

const (
	clusterTotalSlots       = 16384
	unbalancedThresholdPct  = 2
	defaultClusterCheckCode = -1
)

var membershipFailFlags = map[string]struct{}{
	"fail":      {},
	"handshake": {},
	"noaddr":    {},
	"pfail":     {},
}

// Node identifies a cluster node that should participate in the health check.
type Node struct {
	Name string
	Addr string
}

// ClusterClient is the cluster-level Redis interface required by the checker.
type ClusterClient interface {
	GetClusterInfo(ctx context.Context) (*redis.ClusterInfo, error)
	GetClusterNodes(ctx context.Context) ([]redis.ClusterNode, error)
	ClusterCheck(ctx context.Context) (*redis.ClusterCheckResult, error)
	Close() error
}

// ClientFactory builds cluster clients for a node address.
type ClientFactory func(addr, password string) ClusterClient

// Report contains the reusable cluster health evaluation.
type Report struct {
	ClusterInfo  *redis.ClusterInfo
	ClusterNodes []redis.ClusterNode

	MembershipOK   bool
	SlotsCoveredOK bool
	BalancedOK     bool
	ClusterCheckOK bool
	Healthy        bool

	ClusterCheckErrors            []string
	ClusterCheckWarnings          []string
	ClusterCheckCommandOutputCode int
}

// Checker computes cluster health using go-redis for structural checks and redis-cli for cluster check.
type Checker struct {
	logger       *slog.Logger
	newClient    ClientFactory
	probeTimeout time.Duration
}

// NewChecker creates a reusable cluster health checker.
func NewChecker(logger *slog.Logger, newClient ClientFactory, probeTimeout time.Duration) *Checker {
	if logger == nil {
		logger = slog.Default()
	}
	if newClient == nil {
		panic("health checker requires a client factory")
	}
	if probeTimeout <= 0 {
		probeTimeout = 5 * time.Second
	}
	return &Checker{
		logger:       logger,
		newClient:    newClient,
		probeTimeout: probeTimeout,
	}
}

// Check computes the current cluster health report.
func (c *Checker) Check(ctx context.Context, nodes []Node, password string) (*Report, error) {
	report := &Report{ClusterCheckCommandOutputCode: defaultClusterCheckCode}
	if len(nodes) == 0 {
		return report, fmt.Errorf("no nodes provided for cluster health check")
	}

	seed, err := c.selectSeed(ctx, nodes, password)
	if err != nil {
		return report, err
	}

	report.ClusterInfo = seed.clusterInfo
	report.ClusterNodes = seed.clusterNodes
	report.ClusterCheckCommandOutputCode = seed.clusterCheck.CommandCodeOutput
	report.ClusterCheckErrors = append(report.ClusterCheckErrors, seed.clusterCheck.Errors...)
	report.ClusterCheckWarnings = append(report.ClusterCheckWarnings, seed.clusterCheck.Warnings...)
	report.ClusterCheckOK = seed.clusterCheck.CommandCodeOutput == 0

	report.MembershipOK = c.checkMembership(ctx, nodes, password, seed.clusterNodes)
	report.SlotsCoveredOK = slotsCovered(seed.clusterNodes)
	report.BalancedOK = balanced(seed.clusterNodes)
	report.Healthy = report.MembershipOK && report.SlotsCoveredOK && report.BalancedOK && report.ClusterCheckOK

	return report, seed.partialErr
}

type seedProbe struct {
	clusterInfo  *redis.ClusterInfo
	clusterNodes []redis.ClusterNode
	clusterCheck *redis.ClusterCheckResult
	partialErr   error
}

func (c *Checker) selectSeed(ctx context.Context, nodes []Node, password string) (*seedProbe, error) {
	var structuralSeed *seedProbe
	var lastErr error

	for _, node := range nodes {
		probe, err := c.probeNode(ctx, node.Addr, password)
		if err != nil {
			if probe != nil && structuralSeed == nil {
				structuralSeed = probe
				structuralSeed.partialErr = err
			}
			lastErr = err
			continue
		}

		if structuralSeed == nil {
			structuralSeed = probe
		}

		if probe.clusterCheck != nil {
			return probe, nil
		}
	}

	if structuralSeed != nil {
		return structuralSeed, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no reachable cluster node found")
	}
	return nil, lastErr
}

func (c *Checker) probeNode(ctx context.Context, addr, password string) (*seedProbe, error) {
	client := c.newClient(addr, password)
	defer func() {
		if err := client.Close(); err != nil {
			c.logger.Debug("Error closing Redis client during health check", "addr", addr, "error", err)
		}
	}()

	probeCtx, cancel := context.WithTimeout(ctx, c.probeTimeout)
	defer cancel()

	clusterInfo, err := client.GetClusterInfo(probeCtx)
	if err != nil {
		return nil, err
	}

	clusterNodes, err := client.GetClusterNodes(probeCtx)
	if err != nil {
		return nil, err
	}

	probe := &seedProbe{
		clusterInfo:  clusterInfo,
		clusterNodes: clusterNodes,
		clusterCheck: &redis.ClusterCheckResult{CommandCodeOutput: defaultClusterCheckCode},
	}

	clusterCheck, err := client.ClusterCheck(probeCtx)
	if err != nil {
		return probe, err
	}

	probe.clusterCheck = clusterCheck
	return probe, nil
}

func (c *Checker) checkMembership(ctx context.Context, nodes []Node, password string, seedNodes []redis.ClusterNode) bool {
	seedIDs, ok := visibleNodeIDs(seedNodes)
	if !ok || len(seedIDs) != len(nodes) {
		return false
	}

	for _, node := range nodes {
		client := c.newClient(node.Addr, password)
		probeCtx, cancel := context.WithTimeout(ctx, c.probeTimeout)
		clusterNodes, err := client.GetClusterNodes(probeCtx)
		cancel()
		if closeErr := client.Close(); closeErr != nil {
			c.logger.Debug("Error closing Redis client during membership check", "addr", node.Addr, "error", closeErr)
		}
		if err != nil {
			c.logger.Debug("Membership check failed to read CLUSTER NODES", "addr", node.Addr, "error", err)
			return false
		}

		visibleIDs, ok := visibleNodeIDs(clusterNodes)
		if !ok || len(visibleIDs) != len(nodes) {
			return false
		}
		if !maps.Equal(seedIDs, visibleIDs) {
			return false
		}
	}

	return true
}

func visibleNodeIDs(nodes []redis.ClusterNode) (map[string]struct{}, bool) {
	ids := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node.ID == "" {
			return nil, false
		}
		if node.State != "connected" {
			return nil, false
		}
		if hasFailingFlag(node.Flags) {
			return nil, false
		}
		ids[node.ID] = struct{}{}
	}
	return ids, true
}

func hasFailingFlag(flags string) bool {
	for _, flag := range splitFlags(flags) {
		if _, exists := membershipFailFlags[flag]; exists {
			return true
		}
	}
	return false
}

func splitFlags(flags string) []string {
	parts := strings.Split(flags, ",")
	filtered := parts[:0]
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}
	return filtered
}

func slotsCovered(nodes []redis.ClusterNode) bool {
	ranges := make([]redis.SlotRange, 0)
	for _, node := range nodes {
		if !isPrimary(node) {
			continue
		}
		parsed, err := redis.ParseSlotRanges(node.Slots)
		if err != nil {
			return false
		}
		ranges = append(ranges, parsed...)
	}
	if len(ranges) == 0 {
		return false
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Start == ranges[j].Start {
			return ranges[i].End < ranges[j].End
		}
		return ranges[i].Start < ranges[j].Start
	})

	if ranges[0].Start != 0 {
		return false
	}

	currentEnd := -1
	for _, rng := range ranges {
		if rng.Start <= currentEnd {
			return false
		}
		if rng.Start != currentEnd+1 {
			return false
		}
		currentEnd = rng.End
	}

	return currentEnd == clusterTotalSlots-1
}

func balanced(nodes []redis.ClusterNode) bool {
	primaryCounts := make([]int, 0)
	for _, node := range nodes {
		if !isPrimary(node) {
			continue
		}
		parsed, err := redis.ParseSlotRanges(node.Slots)
		if err != nil {
			return false
		}
		slotCount := redis.CountSlots(parsed)
		if slotCount == 0 {
			return false
		}
		primaryCounts = append(primaryCounts, slotCount)
	}

	if len(primaryCounts) == 0 {
		return false
	}

	slotsPerPrimary := int(math.Ceil(float64(clusterTotalSlots) / float64(len(primaryCounts))))
	maximumSlots := slotsPerPrimary + (slotsPerPrimary*unbalancedThresholdPct)/100
	minimumSlots := slotsPerPrimary - (slotsPerPrimary*unbalancedThresholdPct)/100

	for _, slotCount := range primaryCounts {
		if slotCount < minimumSlots || slotCount > maximumSlots {
			return false
		}
	}

	return true
}

func isPrimary(node redis.ClusterNode) bool {
	flags := splitFlags(node.Flags)
	return slicesContains(flags, "master")
}

func slicesContains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

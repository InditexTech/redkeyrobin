// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/health"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

// RedisClientFactory creates Redis clients for a given address and password.
type RedisClientFactory func(addr, password string) *redis.Client

// HealthReconciler performs health checks and remediation on a Ready cluster.
type HealthReconciler struct {
	checker       *health.Checker
	clientFactory RedisClientFactory
	runtimeConfig *config.RuntimeConfig
	logger        *slog.Logger
}

// NewHealthReconciler creates a new HealthReconciler.
func NewHealthReconciler(
	checker *health.Checker,
	clientFactory RedisClientFactory,
	runtimeConfig *config.RuntimeConfig,
	logger *slog.Logger,
) *HealthReconciler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HealthReconciler{
		checker:       checker,
		clientFactory: clientFactory,
		runtimeConfig: runtimeConfig,
		logger:        logger.With("component", "health-reconciler"),
	}
}

// Close releases resources held by the health reconciler, closing all cached
// health-check Redis clients. It should be called on shutdown.
func (hr *HealthReconciler) Close() {
	if hr.checker != nil {
		hr.checker.CloseAll()
	}
}

// Reconcile performs health checks and, if needed, remediation on the cluster.
// Returns the recommended schedule for the next reconciliation.
func (hr *HealthReconciler) Reconcile(ctx context.Context, nodes []health.Node, password string, desiredPrimaries, desiredReplicasPerPrimary int) (reconcileSchedule, error) {
	if len(nodes) == 0 {
		return reconcileAfterInterval, fmt.Errorf("no nodes provided for health reconciliation")
	}

	hr.logger.Info("Starting cluster health check", "nodes", len(nodes))

	report, err := hr.checker.Check(ctx, nodes, password, desiredPrimaries, desiredReplicasPerPrimary)
	if err != nil {
		hr.logger.Warn("Health check completed with errors", "error", err)
	}
	if report == nil {
		return reconcileAfterInterval, fmt.Errorf("health check returned nil report: %w", err)
	}

	hr.logReport(report)

	if report.Healthy {
		hr.logger.Info("Cluster is healthy")
		return reconcileAfterInterval, nil
	}

	// Sequential remediation pipeline
	hr.logger.Info("Cluster health issues detected, starting remediation")

	if !report.MembershipOK {
		if remErr := hr.remediateMembership(ctx, report, nodes, password); remErr != nil {
			hr.logger.Error("Membership remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
		// Membership changed — the cluster topology is in flux. Further remediation
		// (slots, balance) must use fresh cluster state, not the stale report captured
		// before the MEET. Defer to the next reconciliation cycle.
		hr.logger.Info("Membership remediated, deferring further checks to next cycle")
		return reconcileAfterWaitInterval, nil
	}

	if !report.SlotsCoveredOK {
		if remErr := hr.remediateSlotCoverage(ctx, report, nodes, password); remErr != nil {
			hr.logger.Error("Slot coverage remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
	}

	if !report.ReplicaSpreadOK {
		if remErr := hr.remediateReplicaSpread(ctx, report, nodes, password, desiredPrimaries, desiredReplicasPerPrimary); remErr != nil {
			hr.logger.Error("Replica spread remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
	}

	if !report.ClusterCheckOK {
		if remErr := hr.remediateClusterCheck(ctx, nodes, password); remErr != nil {
			hr.logger.Error("Cluster check remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
	}

	if !report.BalancedOK {
		if remErr := hr.remediateBalance(ctx, nodes, password); remErr != nil {
			hr.logger.Error("Balance remediation failed", "error", remErr)
			return reconcileAfterWaitInterval, remErr
		}
	}

	hr.logger.Info("Remediation complete, will re-check soon")
	return reconcileAfterWaitInterval, nil
}

// logReport logs the health report details.
func (hr *HealthReconciler) logReport(report *health.Report) {
	hr.logger.Info("Health check report",
		"membershipOK", report.MembershipOK,
		"slotsCoveredOK", report.SlotsCoveredOK,
		"replicaSpreadOK", report.ReplicaSpreadOK,
		"clusterCheckOK", report.ClusterCheckOK,
		"balancedOK", report.BalancedOK,
		"healthy", report.Healthy,
	)
	if len(report.ClusterCheckErrors) > 0 {
		hr.logger.Warn("Cluster check errors", "errors", report.ClusterCheckErrors)
	}
	if len(report.ClusterCheckWarnings) > 0 {
		hr.logger.Warn("Cluster check warnings", "warnings", report.ClusterCheckWarnings)
	}
}

// --- Remediation: Membership ---

// remediateMembership fixes cluster membership by:
// - FOGETting nodes visible in the cluster that don't correspond to real K8s pods
// - MEETing nodes that should be in the cluster but aren't visible
func (hr *HealthReconciler) remediateMembership(ctx context.Context, report *health.Report, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating cluster membership")

	// Build set of expected IPs from K8s pod list
	expectedIPs := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		ip := extractIP(node.Addr)
		expectedIPs[ip] = struct{}{}
	}

	// Use the first reachable node as the command executor
	seedAddr := nodes[0].Addr
	seedClient := hr.clientFactory(seedAddr, password)
	defer func() { _ = seedClient.Close() }()

	// Get current cluster view
	clusterNodes, err := seedClient.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("getting cluster nodes from seed %s: %w", seedAddr, err)
	}

	// Forget nodes that shouldn't be in the cluster
	for _, cn := range clusterNodes {
		if cn.IP == "" {
			hr.logger.Warn("Skipping node with empty IP in membership remediation", "nodeID", cn.ID, "addr", cn.Addr)
			continue
		}
		if _, expected := expectedIPs[cn.IP]; !expected {
			hr.logger.Info("Forgetting stale node from cluster",
				"nodeID", cn.ID, "nodeIP", cn.IP, "nodeFlags", cn.Flags)
			// Forget from all reachable nodes
			for _, node := range nodes {
				client := hr.clientFactory(node.Addr, password)
				if forgetErr := client.ClusterForget(ctx, cn.ID); forgetErr != nil {
					hr.logger.Warn("Failed to forget node from peer",
						"peer", node.Addr, "targetID", cn.ID, "error", forgetErr)
				}
				_ = client.Close()
			}
		}
	}

	// Meet nodes that are missing from cluster view
	clusterIPs := make(map[string]struct{}, len(clusterNodes))
	for _, cn := range clusterNodes {
		clusterIPs[cn.IP] = struct{}{}
	}

	for _, node := range nodes {
		ip := extractIP(node.Addr)
		if _, inCluster := clusterIPs[ip]; !inCluster {
			hr.logger.Info("Meeting missing node from all cluster members", "addr", node.Addr, "ip", ip)
			// Issue CLUSTER MEET from all existing cluster nodes, not just the seed.
			// After CLUSTER FORGET, each node maintains a 60-second blacklist for the
			// forgotten node's ID. CLUSTER MEET is silently dropped if the revealed ID
			// is blacklisted. By meeting from all nodes we ensure the node is re-added
			// as soon as any node's blacklist entry expires.
			for _, peer := range nodes {
				peerIP := extractIP(peer.Addr)
				if _, peerInCluster := clusterIPs[peerIP]; !peerInCluster {
					continue // skip nodes not yet in the cluster
				}
				peerClient := hr.clientFactory(peer.Addr, password)
				if meetErr := peerClient.ClusterMeet(ctx, ip, redis.DefaultPort); meetErr != nil {
					hr.logger.Warn("Failed to meet from peer", "peer", peer.Addr, "target", ip, "error", meetErr)
				}
				_ = peerClient.Close()
			}
		}
	}

	// Wait for gossip convergence
	meetWait := hr.runtimeConfig.ClusterMeetWait()
	hr.logger.Info("Waiting for gossip convergence", "duration", meetWait)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(meetWait):
	}

	hr.logger.Info("Membership remediation complete")
	return nil
}

// --- Remediation: Slot Coverage ---

// remediateSlotCoverage assigns missing slots to primaries.
func (hr *HealthReconciler) remediateSlotCoverage(ctx context.Context, report *health.Report, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating slot coverage")

	// Identify primaries and their current slots
	type primaryInfo struct {
		addr     string
		id       string
		numSlots int
	}

	var primaries []primaryInfo
	assignedSlots := make(map[int]struct{})

	for _, cn := range report.ClusterNodes {
		if !strings.Contains(cn.Flags, "master") {
			continue
		}
		if cn.IP == "" {
			hr.logger.Warn("Skipping primary with empty IP in slot coverage remediation", "nodeID", cn.ID, "addr", cn.Addr)
			continue
		}
		ranges, err := redis.ParseSlotRanges(cn.Slots)
		if err != nil {
			continue
		}
		count := redis.CountSlots(ranges)
		primaries = append(primaries, primaryInfo{addr: cn.IP + ":6379", id: cn.ID, numSlots: count})
		for _, r := range ranges {
			for s := r.Start; s <= r.End; s++ {
				assignedSlots[s] = struct{}{}
			}
		}
	}

	if len(primaries) == 0 {
		return fmt.Errorf("no primaries found in cluster nodes")
	}

	// Find missing slots
	var missingSlots []int
	for s := range clusterTotalSlots {
		if _, ok := assignedSlots[s]; !ok {
			missingSlots = append(missingSlots, s)
		}
	}

	if len(missingSlots) == 0 {
		hr.logger.Info("No missing slots found")
		return nil
	}

	sort.Ints(missingSlots)
	hr.logger.Info("Found missing slots", "count", len(missingSlots))

	// Distribute missing slots among primaries: fill up nodes that need more
	slotsPerPrimary := int(math.Ceil(float64(clusterTotalSlots) / float64(len(primaries))))

	// Sort primaries by current slot count (ascending) to fill emptier ones first
	sort.Slice(primaries, func(i, j int) bool {
		return primaries[i].numSlots < primaries[j].numSlots
	})

	slotIdx := 0
	for i := range primaries {
		if slotIdx >= len(missingSlots) {
			break
		}

		needed := slotsPerPrimary - primaries[i].numSlots
		if needed <= 0 {
			continue
		}

		// Last primary gets all remaining
		end := slotIdx + needed
		if i == len(primaries)-1 || end > len(missingSlots) {
			end = len(missingSlots)
		}

		slotsToAssign := missingSlots[slotIdx:end]
		if len(slotsToAssign) == 0 {
			continue
		}

		client := hr.clientFactory(primaries[i].addr, password)
		err := client.ClusterAddSlots(ctx, slotsToAssign...)
		_ = client.Close()
		if err != nil {
			if strings.Contains(err.Error(), "already busy") {
				// Config disagreement: the seed thinks slots are unassigned, but
				// the target node sees them as owned by someone. Resolve via SETSLOT.
				hr.logger.Warn("Slot already busy, resolving config disagreement",
					"target", primaries[i].addr, "slots", len(slotsToAssign), "error", err)
				if resolveErr := hr.resolveSlotDisagreement(ctx, nodes, password, missingSlots); resolveErr != nil {
					return fmt.Errorf("resolving slot disagreement: %w", resolveErr)
				}
				// All missing slots handled via SETSLOT, stop ADDSLOTS loop.
				break
			}
			return fmt.Errorf("assigning slots to %s: %w", primaries[i].addr, err)
		}

		hr.logger.Info("Assigned missing slots to primary",
			"primary", primaries[i].addr, "count", len(slotsToAssign))
		slotIdx = end
	}

	// Wait for convergence
	meetWait := hr.runtimeConfig.ClusterMeetWait()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(meetWait):
	}

	hr.logger.Info("Slot coverage remediation complete")
	return nil
}

// resolveSlotDisagreement handles the case where the seed node thinks slots are
// unassigned but other nodes still see them as owned. It queries all nodes to find
// the actual owner (by highest epoch/majority) and forces agreement via CLUSTER SETSLOT NODE.
func (hr *HealthReconciler) resolveSlotDisagreement(ctx context.Context, nodes []health.Node, password string, disputedSlots []int) error {
	const perNodeTimeout = 5 * time.Second

	// Build a lookup set for fast slot membership check.
	disputedSet := make(map[int]struct{}, len(disputedSlots))
	for _, s := range disputedSlots {
		disputedSet[s] = struct{}{}
	}

	// Query all nodes to find who owns each disputed slot.
	// ownerInfo: slot -> (nodeID with highest epoch/votes)
	type candidate struct {
		nodeID string
		epoch  int
		votes  int
	}
	slotOwners := make(map[int]*candidate)

	for _, node := range nodes {
		queryCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		clusterNodes, err := client.GetClusterNodes(queryCtx)
		_ = client.Close()
		cancel()
		if err != nil {
			continue
		}
		for _, cn := range clusterNodes {
			if !strings.Contains(cn.Flags, "master") {
				continue
			}
			ranges, err := redis.ParseSlotRanges(cn.Slots)
			if err != nil {
				continue
			}
			for _, rng := range ranges {
				for slot := rng.Start; slot <= rng.End; slot++ {
					if _, disputed := disputedSet[slot]; !disputed {
						continue
					}
					existing := slotOwners[slot]
					if existing == nil {
						slotOwners[slot] = &candidate{nodeID: cn.ID, epoch: cn.Epoch, votes: 1}
					} else if cn.ID == existing.nodeID {
						existing.votes++
						if cn.Epoch > existing.epoch {
							existing.epoch = cn.Epoch
						}
					} else if cn.Epoch > existing.epoch || (cn.Epoch == existing.epoch && 1 > existing.votes) {
						slotOwners[slot] = &candidate{nodeID: cn.ID, epoch: cn.Epoch, votes: 1}
					}
				}
			}
		}
	}

	if len(slotOwners) == 0 {
		return fmt.Errorf("no owner found for disputed slots across any node")
	}

	hr.logger.Info("Resolving slot disagreement via CLUSTER SETSLOT",
		"disputedSlots", len(disputedSlots), "ownersFound", len(slotOwners))

	// Force agreement on all nodes using CLUSTER SETSLOT <slot> NODE <owner>.
	var setSlotErrors int
	for _, node := range nodes {
		setCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		for slot, owner := range slotOwners {
			if err := client.ClusterSetSlotNode(setCtx, slot, owner.nodeID); err != nil {
				setSlotErrors++
			}
		}
		_ = client.Close()
		cancel()
	}

	// For slots that had no owner from any node (truly unassigned everywhere),
	// assign them to the first primary via ADDSLOTS.
	var trulyMissing []int
	for _, s := range disputedSlots {
		if _, found := slotOwners[s]; !found {
			trulyMissing = append(trulyMissing, s)
		}
	}
	if len(trulyMissing) > 0 {
		// Pick the first node to own these truly missing slots.
		client := hr.clientFactory(nodes[0].Addr, password)
		if err := client.ClusterAddSlots(ctx, trulyMissing...); err != nil {
			_ = client.Close()
			return fmt.Errorf("assigning truly missing slots to %s: %w", nodes[0].Addr, err)
		}
		_ = client.Close()
	}

	if setSlotErrors > 0 {
		hr.logger.Warn("Some CLUSTER SETSLOT commands failed during slot disagreement resolution",
			"errors", setSlotErrors)
	}

	hr.logger.Info("Slot disagreement resolution complete", "resolvedViaSetSlot", len(slotOwners), "trulyMissing", len(trulyMissing))
	return nil
}

// --- Remediation: Replica Spread ---

// replicaInfo describes a replica node in the cluster topology.
type replicaInfo struct {
	id        string
	addr      string
	primaryID string
}

// remediateReplicaSpread fixes the replica-to-primary mapping without changing node counts.
func (hr *HealthReconciler) remediateReplicaSpread(ctx context.Context, report *health.Report, nodes []health.Node, password string, desiredPrimaries, desiredReplicasPerPrimary int) error {
	hr.logger.Info("Remediating replica spread")

	// Identify primaries and replicas from cluster state
	primaryIDs := make(map[string]string) // id -> addr
	var replicas []replicaInfo

	for _, cn := range report.ClusterNodes {
		if cn.IP == "" {
			hr.logger.Warn("Skipping node with empty IP in replica spread remediation", "nodeID", cn.ID, "addr", cn.Addr)
			continue
		}
		if strings.Contains(cn.Flags, "master") {
			primaryIDs[cn.ID] = cn.IP + ":6379"
		} else if strings.Contains(cn.Flags, "slave") {
			replicas = append(replicas, replicaInfo{
				id:        cn.ID,
				addr:      cn.IP + ":6379",
				primaryID: cn.Primary,
			})
		}
	}

	// If primary count doesn't match desired, check for failover scenario
	if len(primaryIDs) != desiredPrimaries {
		if len(primaryIDs) > desiredPrimaries {
			// Failover scenario: a replica was promoted and the replacement pod joined as an empty primary.
			// Demote empty primaries (0 slots, not importing/migrating) to replicas of primaries that need them.
			return hr.remediateExcessPrimaries(ctx, report, primaryIDs, replicas, password, desiredPrimaries, desiredReplicasPerPrimary)
		}
		// Fewer primaries than desired (scale-up scenario): skip, not our concern here.
		hr.logger.Info("Primary count below desired, skipping replica spread remediation",
			"actual", len(primaryIDs), "desired", desiredPrimaries)
		return nil
	}

	// Count replicas per primary
	replicaCount := make(map[string]int)
	var orphaned []replicaInfo     // replicas pointing to unknown primaries
	var overAssigned []replicaInfo // excess replicas from primaries with too many

	for _, r := range replicas {
		if _, exists := primaryIDs[r.primaryID]; !exists {
			orphaned = append(orphaned, r)
		} else {
			replicaCount[r.primaryID]++
		}
	}

	// Find primaries with too many replicas (collect excess)
	for _, r := range replicas {
		if _, exists := primaryIDs[r.primaryID]; !exists {
			continue
		}
		if replicaCount[r.primaryID] > desiredReplicasPerPrimary {
			overAssigned = append(overAssigned, r)
			replicaCount[r.primaryID]--
		}
	}

	// Find primaries that need more replicas
	var needsReplicas []string // primary IDs needing replicas
	for id := range primaryIDs {
		deficit := desiredReplicasPerPrimary - replicaCount[id]
		for range deficit {
			needsReplicas = append(needsReplicas, id)
		}
	}

	// Available replicas to reassign: orphaned + overAssigned
	available := append(orphaned, overAssigned...)

	if len(available) < len(needsReplicas) {
		hr.logger.Warn("Not enough replicas to fully remediate spread",
			"available", len(available), "needed", len(needsReplicas))
	}

	// Reassign replicas
	reassignCount := min(len(available), len(needsReplicas))
	for i := range reassignCount {
		replica := available[i]
		targetPrimaryID := needsReplicas[i]

		hr.logger.Info("Reassigning replica to primary",
			"replica", replica.addr, "replicaID", replica.id,
			"fromPrimary", replica.primaryID, "toPrimary", targetPrimaryID)

		client := hr.clientFactory(replica.addr, password)
		if err := client.ClusterReplicate(ctx, targetPrimaryID); err != nil {
			_ = client.Close()
			return fmt.Errorf("replicating %s to primary %s: %w", replica.addr, targetPrimaryID, err)
		}
		_ = client.Close()
	}

	if reassignCount > 0 {
		hr.logger.Info("Replica spread remediation complete", "reassigned", reassignCount)
	} else {
		hr.logger.Info("No replica reassignment needed")
	}

	return nil
}

// remediateExcessPrimaries handles the failover scenario where more primaries exist
// than desired. This happens when a replica is promoted to primary (auto-failover)
// and the replacement pod joins as an empty master. The empty primaries are demoted
// to replicas of primaries that are missing replicas.
func (hr *HealthReconciler) remediateExcessPrimaries(
	ctx context.Context,
	report *health.Report,
	primaryIDs map[string]string,
	replicas []replicaInfo,
	password string,
	desiredPrimaries, desiredReplicasPerPrimary int,
) error {
	hr.logger.Info("Detected excess primaries after failover, demoting empty primaries to replicas",
		"actual", len(primaryIDs), "desired", desiredPrimaries)

	// Identify empty primaries: those with 0 assigned slots and no importing/migrating state.
	// These are newly joined replacement pods that have no cluster data.
	type emptyPrimary struct {
		id   string
		addr string
	}
	var emptyPrimaries []emptyPrimary

	for _, cn := range report.ClusterNodes {
		if cn.IP == "" || !strings.Contains(cn.Flags, "master") {
			continue
		}
		// Skip if the node has importing/migrating markers (contains '[')
		if strings.Contains(cn.Slots, "[") {
			continue
		}
		// Check if the node has 0 slots
		parsed, err := redis.ParseSlotRanges(cn.Slots)
		if err != nil {
			// Parse error but no '[' marker — treat as empty (unparseable empty string)
			emptyPrimaries = append(emptyPrimaries, emptyPrimary{id: cn.ID, addr: cn.IP + ":6379"})
			continue
		}
		if redis.CountSlots(parsed) == 0 {
			emptyPrimaries = append(emptyPrimaries, emptyPrimary{id: cn.ID, addr: cn.IP + ":6379"})
		}
	}

	if len(emptyPrimaries) == 0 {
		hr.logger.Info("No empty primaries found to demote, skipping")
		return nil
	}

	// Determine how many primaries we need to demote
	excess := len(primaryIDs) - desiredPrimaries
	if excess <= 0 {
		return nil
	}
	if excess > len(emptyPrimaries) {
		excess = len(emptyPrimaries)
	}

	// Find primaries that need replicas (the promoted replicas that are now primaries with slots).
	// Count current replicas per non-empty primary.
	replicaCount := make(map[string]int)
	emptyIDs := make(map[string]struct{}, len(emptyPrimaries))
	for _, ep := range emptyPrimaries {
		emptyIDs[ep.id] = struct{}{}
	}

	for _, r := range replicas {
		if _, isTarget := primaryIDs[r.primaryID]; isTarget {
			replicaCount[r.primaryID]++
		}
	}

	// Collect primaries (non-empty) that have fewer replicas than desired
	var needsReplicas []string
	for id := range primaryIDs {
		if _, isEmpty := emptyIDs[id]; isEmpty {
			continue
		}
		deficit := desiredReplicasPerPrimary - replicaCount[id]
		for range deficit {
			needsReplicas = append(needsReplicas, id)
		}
	}

	// Demote empty primaries to replicas
	demoted := 0
	for i := range excess {
		if i >= len(needsReplicas) {
			hr.logger.Warn("No more primaries needing replicas, stopping demotion",
				"demoted", demoted, "remaining", excess-demoted)
			break
		}

		ep := emptyPrimaries[i]
		targetPrimaryID := needsReplicas[i]

		hr.logger.Info("Demoting empty primary to replica after failover",
			"emptyPrimary", ep.addr, "emptyPrimaryID", ep.id,
			"targetPrimary", targetPrimaryID)

		client := hr.clientFactory(ep.addr, password)
		if err := client.ClusterReplicate(ctx, targetPrimaryID); err != nil {
			_ = client.Close()
			return fmt.Errorf("demoting %s to replica of %s: %w", ep.addr, targetPrimaryID, err)
		}
		_ = client.Close()
		demoted++
	}

	hr.logger.Info("Excess primary demotion complete", "demoted", demoted)
	return nil
}

// --- Remediation: Cluster Check (Fix) ---

// remediateClusterCheck runs redis-cli --cluster fix to repair cluster issues.
// It attempts the fix from each node in the cluster, since nodes may have different
// views of the configuration. If all attempts fail, it returns nil so the pipeline
// can continue to the balance step (which may resolve the underlying issue).
func (hr *HealthReconciler) remediateClusterCheck(ctx context.Context, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating cluster check issues with redis-cli --cluster fix")

	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for cluster fix")
	}

	timeout := hr.runtimeConfig.ClusterCommandTimeout()

	var lastErr error
	for _, node := range nodes {
		fixCtx, cancel := context.WithTimeout(ctx, timeout)
		client := hr.clientFactory(node.Addr, password)

		result, err := client.ClusterFix(fixCtx)
		_ = client.Close()
		cancel()

		if err == nil {
			hr.logger.Info("Cluster fix completed",
				"node", node.Addr,
				"exitCode", result.CommandCodeOutput,
				"output", truncateOutput(result.Output, 500))
			return nil
		}

		hr.logger.Warn("Cluster fix failed on node, trying next",
			"node", node.Addr, "error", err)
		lastErr = err
	}

	// All nodes failed — try to resolve config disagreement directly.
	hr.logger.Warn("Cluster fix failed on all nodes, attempting config disagreement resolution",
		"lastError", lastErr, "nodesAttempted", len(nodes))

	if resolveErr := hr.resolveConfigDisagreement(ctx, nodes, password); resolveErr != nil {
		hr.logger.Warn("Config disagreement resolution failed", "error", resolveErr)
		return fmt.Errorf("cluster fix failed on all %d nodes: %w", len(nodes), lastErr)
	}

	hr.logger.Info("Config disagreement resolved via CLUSTER SETSLOT")
	return nil
}

// resolveConfigDisagreement queries CLUSTER NODES from all nodes and forces
// agreement on disputed slots using CLUSTER SETSLOT <slot> NODE <owner>.
// This handles the case where nodes disagree about slot ownership after a
// reshard/migration, which redis-cli --cluster fix cannot resolve.
func (hr *HealthReconciler) resolveConfigDisagreement(ctx context.Context, nodes []health.Node, password string) error {
	if len(nodes) < 2 {
		return fmt.Errorf("need at least 2 nodes to resolve disagreement")
	}

	// Use a short per-node timeout for CLUSTER NODES queries (not the CLI timeout).
	const perNodeTimeout = 5 * time.Second

	// Step 1: Query CLUSTER NODES from all nodes to build per-node slot maps.
	type nodeView struct {
		addr         string
		clusterNodes []redis.ClusterNode
	}
	views := make([]nodeView, 0, len(nodes))
	for _, node := range nodes {
		queryCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		clusterNodes, err := client.GetClusterNodes(queryCtx)
		_ = client.Close()
		cancel()
		if err != nil {
			hr.logger.Warn("Failed to query CLUSTER NODES for disagreement resolution",
				"node", node.Addr, "error", err)
			continue
		}
		views = append(views, nodeView{addr: node.Addr, clusterNodes: clusterNodes})
	}

	if len(views) < 2 {
		return fmt.Errorf("could not query enough nodes for disagreement resolution")
	}

	// Step 2: Build slot ownership map from each node's perspective.
	// For each slot, collect the owner claimed by each view.
	// The "correct" owner is determined by highest epoch.
	const totalSlots = 16384
	type ownerCandidate struct {
		nodeID string
		epoch  int
		votes  int
	}

	// Build a map: slot -> ownerID -> (epoch, vote count)
	slotCandidates := make(map[int]map[string]*ownerCandidate)

	for _, view := range views {
		for _, cn := range view.clusterNodes {
			if !strings.Contains(cn.Flags, "master") {
				continue
			}
			ranges, err := redis.ParseSlotRanges(cn.Slots)
			if err != nil {
				continue
			}
			for _, rng := range ranges {
				for slot := rng.Start; slot <= rng.End; slot++ {
					if slotCandidates[slot] == nil {
						slotCandidates[slot] = make(map[string]*ownerCandidate)
					}
					if slotCandidates[slot][cn.ID] == nil {
						slotCandidates[slot][cn.ID] = &ownerCandidate{nodeID: cn.ID, epoch: cn.Epoch}
					}
					slotCandidates[slot][cn.ID].votes++
					if cn.Epoch > slotCandidates[slot][cn.ID].epoch {
						slotCandidates[slot][cn.ID].epoch = cn.Epoch
					}
				}
			}
		}
	}

	// Step 3: Find disputed slots (where not all views agree on the same owner).
	// For each disputed slot, pick the owner with the highest epoch (tie-break: most votes).
	type slotFix struct {
		slot    int
		ownerID string
	}
	var fixes []slotFix

	for slot := 0; slot < totalSlots; slot++ {
		candidates := slotCandidates[slot]
		if len(candidates) <= 1 {
			continue // all agree (or uncovered — handled elsewhere)
		}
		// Multiple candidates for this slot — pick best.
		var best *ownerCandidate
		for _, c := range candidates {
			if best == nil || c.epoch > best.epoch || (c.epoch == best.epoch && c.votes > best.votes) {
				best = c
			}
		}
		if best != nil {
			fixes = append(fixes, slotFix{slot: slot, ownerID: best.nodeID})
		}
	}

	// Also find slots that are uncovered (no candidate at all from any view).
	// This shouldn't normally happen if slotsCoveredOK=true, but handle it.

	if len(fixes) == 0 {
		return fmt.Errorf("no disputed slots found, cannot resolve disagreement")
	}

	hr.logger.Info("Found disputed slots, forcing agreement",
		"disputedSlots", len(fixes))

	// Step 4: Send CLUSTER SETSLOT <slot> NODE <owner> to ALL nodes.
	var setSlotErrors int
	for _, node := range nodes {
		setCtx, cancel := context.WithTimeout(ctx, perNodeTimeout)
		client := hr.clientFactory(node.Addr, password)
		for _, fix := range fixes {
			if err := client.ClusterSetSlotNode(setCtx, fix.slot, fix.ownerID); err != nil {
				setSlotErrors++
				// Don't fail on individual slot errors — continue best effort.
			}
		}
		_ = client.Close()
		cancel()
	}

	if setSlotErrors > 0 {
		hr.logger.Warn("Some CLUSTER SETSLOT commands failed",
			"errors", setSlotErrors, "totalCommands", len(fixes)*len(nodes))
	}

	hr.logger.Info("Config disagreement resolution complete",
		"fixedSlots", len(fixes), "errors", setSlotErrors)
	return nil
}

// --- Remediation: Balance ---

// remediateBalance runs redis-cli --cluster rebalance to distribute slots evenly.
func (hr *HealthReconciler) remediateBalance(ctx context.Context, nodes []health.Node, password string) error {
	hr.logger.Info("Remediating cluster balance with redis-cli --cluster rebalance")

	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for cluster rebalance")
	}

	timeout := hr.runtimeConfig.RebalanceTimeout()
	rebalanceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	seedAddr := nodes[0].Addr
	client := hr.clientFactory(seedAddr, password)
	defer func() { _ = client.Close() }()

	result, err := client.ClusterRebalance(rebalanceCtx)
	if err != nil {
		return fmt.Errorf("redis-cli --cluster rebalance on %s: %w", seedAddr, err)
	}

	hr.logger.Info("Cluster rebalance completed",
		"exitCode", result.CommandCodeOutput,
		"output", truncateOutput(result.Output, 500))

	return nil
}

// --- Helpers ---

func extractIP(addr string) string {
	if idx := strings.Index(addr, ":"); idx > 0 {
		return addr[:idx]
	}
	return addr
}

func truncateOutput(output string, maxLen int) string {
	if len(output) <= maxLen {
		return output
	}
	return output[:maxLen] + "...(truncated)"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

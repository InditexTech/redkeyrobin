// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/kubernetes"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

// defaultRebalanceMaxAttempts bounds how many times a rebalance is retried when it
// fails with a transient migration error (TRYAGAIN and friends).
const defaultRebalanceMaxAttempts = 5

// fastScalingEligible reports whether the cluster qualifies for fast scaling, which
// recreates the cluster from scratch (losing all data) instead of migrating slots.
// It is only safe for ephemeral clusters with no replicas where the operator has
// explicitly opted in via purgeKeysOnRebalance.
func fastScalingEligible(config *redisv1.RedkeyClusterConfig) bool {
	if !config.Spec.Ephemeral {
		return false
	}
	if config.Spec.ReplicasPerPrimary != 0 {
		return false
	}
	return config.Spec.PurgeKeysOnRebalance != nil && *config.Spec.PurgeKeysOnRebalance
}

// totalNodes returns the desired total node count for a topology.
func totalNodes(config *redisv1.RedkeyClusterConfig) int {
	return int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
}

// handleScalingUp grows the cluster: it adds the new primaries and replicas, rebalances
// the slots onto the new (empty) primaries, and finally attaches the replicas.
// See docs/operator-guide/scaling.md (Scale Up flow) for the detailed protocol.
func (cr *ClusterReconciler) handleScalingUp(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	if fastScalingEligible(config) {
		return cr.handleFastScaling(ctx, config)
	}

	targetTotal := totalNodes(config)
	primaryCount := int(config.Spec.Primaries)
	cr.logger.Info("Starting scale-up reconciliation", "targetTotal", targetTotal, "primaries", primaryCount)

	// Phase 1: grow the StatefulSet so every new primary and replica pod exists.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusWaitingForPods)
	if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal)); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scaling StatefulSet up: %w", err)
	}
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal))
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pod readiness during scale up: %w", err)
	}
	if !ready {
		cr.logger.Info("Waiting for new pods to be ready before scaling up", "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for scale up: %w", err)
	}
	defer closeNodes(nodes)
	if len(nodes) < targetTotal {
		cr.logger.Info("Not all nodes available for scale up, will retry", "available", len(nodes), "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	// Discover which nodes are already part of the cluster by querying an existing
	// member's CLUSTER NODES output. nodes[0] is always an existing primary (lowest
	// ordinal, created during the initial cluster formation).
	clusterMembers, err := nodes[0].Client().GetClusterNodes(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting cluster nodes for topology discovery: %w", err)
	}
	memberIDs := make(map[string]struct{}, len(clusterMembers))
	for _, cm := range clusterMembers {
		if cm.ID != "" && !strings.Contains(cm.Flags, "handshake") {
			memberIDs[cm.ID] = struct{}{}
		}
	}

	// Classify nodes into existing cluster members and truly new pods that have not
	// yet been introduced to the cluster. This avoids the broken ordinal-based split
	// which would try to convert existing replicas into primaries.
	//
	// Classification:
	// - existingPrimaries: cluster members that are masters WITH slots assigned
	// - existingReplicas: cluster members that are replicas
	// - pendingNodes: cluster members that are empty masters (met on a previous pass but
	//   not yet configured) — these need to be assigned a role
	// - newNodes: pods not yet in the cluster at all
	var existingPrimaries []*redis.Node
	var existingReplicas []*redis.Node
	var pendingNodes []*redis.Node
	var newNodes []*redis.Node
	for _, node := range nodes {
		if _, isMember := memberIDs[node.ID]; isMember {
			if err := node.RefreshInfo(ctx); err != nil {
				return reconcileAfterInterval, fmt.Errorf("refreshing existing node %s: %w", node.Name, err)
			}
			if node.IsReplica() {
				existingReplicas = append(existingReplicas, node)
			} else if node.Slots != "" {
				// Master with slots — a real existing primary.
				existingPrimaries = append(existingPrimaries, node)
			} else {
				// Master with no slots — met on a previous pass but not yet assigned a
				// role (could be a new primary waiting for rebalance, or a replica-to-be
				// that was met but not yet CLUSTER REPLICATE'd).
				pendingNodes = append(pendingNodes, node)
			}
		} else {
			newNodes = append(newNodes, node)
		}
	}

	// Nodes available for role assignment: pending (already met, prefer for primary role
	// since they're already in the cluster) + new (not yet met).
	availableNodes := append(append([]*redis.Node{}, pendingNodes...), newNodes...)

	// Determine how many new primaries we need.
	newPrimariesNeeded := primaryCount - len(existingPrimaries)
	if newPrimariesNeeded < 0 {
		newPrimariesNeeded = 0
	}
	if newPrimariesNeeded > len(availableNodes) {
		cr.logger.Info("Not enough available nodes to satisfy primary requirement, will retry",
			"newPrimariesNeeded", newPrimariesNeeded, "availableNodes", len(availableNodes))
		return reconcileAfterWaitInterval, nil
	}
	futurePrimaries := availableNodes[:newPrimariesNeeded]
	futureReplicas := availableNodes[newPrimariesNeeded:]

	// All primaries = existing + future primaries to add.
	allPrimaries := append(existingPrimaries, futurePrimaries...)

	cr.logger.Info("Scale-up topology discovery",
		"existingPrimaries", len(existingPrimaries),
		"existingReplicas", len(existingReplicas),
		"pendingNodes", len(pendingNodes),
		"futurePrimaries", len(futurePrimaries),
		"futureReplicas", len(futureReplicas))

	// Introduce only the future primary nodes that are not yet in the cluster.
	// Pending nodes (empty masters from a previous pass) are already met.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusInitializingNodes)
	unmeetPrimaries := filterUnmet(futurePrimaries, memberIDs)
	if len(unmeetPrimaries) > 0 {
		// Meet unmet primaries with all existing cluster members so gossip propagates.
		meetGroup := append(append([]*redis.Node{}, existingPrimaries...), unmeetPrimaries...)
		if err := cr.meetNodes(ctx, meetGroup); err != nil {
			cr.logger.Error("Failed to meet new primary nodes during scale up", "error", err)
			return reconcileAfterWaitInterval, nil
		}
		converged, err := cr.checkGossipConvergence(ctx, meetGroup)
		if err != nil {
			cr.logger.Warn("Failed to check gossip convergence during scale up", "error", err)
			return reconcileAfterWaitInterval, nil
		}
		if !converged {
			cr.logger.Info("Gossip not yet converged during scale up, will retry")
			return reconcileAfterWaitInterval, nil
		}
	}

	// Forget phantom nodes left behind by restarted ephemeral pods before doing any slot
	// work. A restarted ephemeral pod rejoins with a new ID while its old ID lingers in
	// the gossip table (still owning slots), which makes redis-cli rebalance across stale
	// masters and never converge.
	// Pass ALL nodes (existing + new) so existing replicas are not mistakenly pruned.
	if pruned, err := cr.pruneStaleNodes(ctx, nodes); err != nil {
		cr.logger.Warn("Failed to prune stale nodes during scale up", "error", err)
		return reconcileAfterWaitInterval, nil
	} else if pruned {
		// Reassign any slots orphaned by the forgotten nodes, then re-evaluate from a
		// clean topology on the next pass.
		if err := cr.runClusterFix(ctx, existingPrimaries[0]); err != nil {
			cr.logger.Warn("Failed to fix cluster after pruning stale nodes", "error", err)
		}
		return reconcileImmediately, nil
	}

	// Phase 2: rebalance slots onto the new empty primaries (TRYAGAIN-protected).
	// Only run if there are futurePrimaries that need slots. On re-entry (e.g. after
	// Phase 3 gossip convergence failed), all primaries already have slots and no
	// rebalance is needed — running it would distribute slots to pending replica nodes
	// that are visible as empty masters but should become replicas.
	if len(futurePrimaries) > 0 {
		cr.updateSubstatus(ctx, config, redisv1.SubstatusRebalancing)
		cr.logger.Info("Starting rebalance to distribute slots to new primaries", "newPrimaries", len(futurePrimaries))
		if err := cr.rebalanceWithRetry(ctx, existingPrimaries[0], nil); err != nil {
			cr.logger.Error("Rebalance failed during scale up", "error", err)
			return reconcileAfterWaitInterval, nil
		}

		// Wait until no slots remain in a migrating/importing state.
		stable, err := cr.slotsStable(ctx, existingPrimaries[0])
		if err != nil {
			cr.logger.Warn("Failed to check slot stability during scale up", "error", err)
			return reconcileAfterWaitInterval, nil
		}
		if !stable {
			cr.logger.Info("Slots still migrating during scale up, will retry")
			return reconcileAfterWaitInterval, nil
		}
	}

	// Phase 3: introduce future replica nodes and attach ALL replicas (existing + future)
	// to their correct primaries. Adding replicas after rebalance avoids replicating empty
	// or in-flux nodes, and prevents the rebalance from assigning slots to replicas.
	allReplicas := append(existingReplicas, futureReplicas...)
	if config.Spec.ReplicasPerPrimary > 0 && len(allReplicas) > 0 {
		cr.updateSubstatus(ctx, config, redisv1.SubstatusAttachingReplicas)
		unmeetReplicas := filterUnmet(futureReplicas, memberIDs)
		if len(unmeetReplicas) > 0 {
			// Meet unmet replicas with the existing cluster so they join with empty state.
			meetGroup := append(append([]*redis.Node{}, allPrimaries...), unmeetReplicas...)
			if err := cr.meetNodes(ctx, meetGroup); err != nil {
				cr.logger.Warn("Failed to meet new replica nodes during scale up", "error", err)
				return reconcileAfterWaitInterval, nil
			}
			allActive := append(append([]*redis.Node{}, allPrimaries...), allReplicas...)
			converged, err := cr.checkGossipConvergence(ctx, allActive)
			if err != nil {
				cr.logger.Warn("Failed to check gossip convergence for replicas", "error", err)
				return reconcileAfterWaitInterval, nil
			}
			if !converged {
				cr.logger.Info("Gossip not yet converged for replica nodes, will retry")
				return reconcileAfterWaitInterval, nil
			}
		}

		if err := cr.refreshRoles(ctx, nodes); err != nil {
			cr.logger.Warn("Failed to refresh roles before attaching replicas", "error", err)
			return reconcileAfterWaitInterval, nil
		}
		if err := cr.setReplicas(ctx, allPrimaries, allReplicas, config.Spec.ReplicasPerPrimary); err != nil {
			cr.logger.Warn("Failed to attach replicas during scale up, will retry", "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	// Phase 4: validate.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusVerifying)
	ok, err := cr.verifyCluster(ctx, nodes[0])
	if err != nil {
		cr.logger.Error("Failed to verify cluster after scale up", "error", err)
		return reconcileAfterWaitInterval, nil
	}
	if !ok {
		cr.logger.Info("Cluster not yet healthy after scale up, will retry")
		return reconcileAfterWaitInterval, nil
	}

	return cr.finishScaling(ctx, config, nodes, "scale up")
}

// handleScalingDown shrinks the cluster: it drains the highest-ordinal primaries, removes
// the surplus replicas and primaries, and finally shrinks the StatefulSet.
// See docs/operator-guide/scaling.md (Scale Down flow) for the detailed protocol.
func (cr *ClusterReconciler) handleScalingDown(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	targetTotal := totalNodes(config)
	targetPrimaries := int(config.Spec.Primaries)
	cr.logger.Info("Starting scale-down reconciliation", "targetTotal", targetTotal, "targetPrimaries", targetPrimaries)

	password := cr.getPassword(ctx, config)
	nodes, err := cr.initAllNodes(ctx, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for scale down: %w", err)
	}
	defer closeNodes(nodes)
	if err := cr.refreshRoles(ctx, nodes); err != nil {
		cr.logger.Warn("Failed to refresh roles during scale down", "error", err)
		return reconcileAfterWaitInterval, nil
	}

	// Fast scaling is only safe when the cluster currently has no replicas.
	// Transitioning from replicasPerPrimary>0 to 0 must use normal scaling so that
	// existing replicas are cleanly removed without destroying the cluster data.
	if fastScalingEligible(config) && !hasReplicaNodes(nodes) {
		closeNodes(nodes)
		return cr.handleFastScaling(ctx, config)
	}

	currentTotal := len(nodes)
	if currentTotal <= targetTotal {
		// Nodes already removed; make sure the StatefulSet matches and verify.
		cr.updateSubstatus(ctx, config, redisv1.SubstatusVerifying)
		if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal)); err != nil {
			return reconcileAfterInterval, fmt.Errorf("scaling StatefulSet down: %w", err)
		}
		if len(nodes) == 0 {
			return reconcileAfterWaitInterval, nil
		}
		ok, err := cr.verifyCluster(ctx, nodes[0])
		if err != nil || !ok {
			cr.logger.Info("Cluster not yet healthy after scale down, will retry")
			return reconcileAfterWaitInterval, nil
		}
		return cr.finishScaling(ctx, config, nodes[:targetTotal], "scale down")
	}

	// Classify nodes by current role — scale-down decisions must be topology-aware, not
	// ordinal-based, to handle clusters where failovers have shuffled roles.
	var currentPrimaries []*redis.Node
	var currentReplicas []*redis.Node
	for _, n := range nodes {
		if n.IsPrimary() {
			currentPrimaries = append(currentPrimaries, n)
		} else {
			currentReplicas = append(currentReplicas, n)
		}
	}

	// Determine which primaries to keep and which to drain.
	// Keep the first targetPrimaries primaries (by ordinal, since they were sorted).
	var keepPrimaries, drainPrimaries []*redis.Node
	if len(currentPrimaries) > targetPrimaries {
		keepPrimaries = currentPrimaries[:targetPrimaries]
		drainPrimaries = currentPrimaries[targetPrimaries:]
	} else {
		keepPrimaries = currentPrimaries
	}

	// Determine which replicas to keep: targetReplicas = targetPrimaries * replicasPerPrimary.
	targetReplicaCount := targetPrimaries * int(config.Spec.ReplicasPerPrimary)
	var keepReplicas, removeReplicas []*redis.Node
	if len(currentReplicas) > targetReplicaCount {
		keepReplicas = currentReplicas[:targetReplicaCount]
		removeReplicas = currentReplicas[targetReplicaCount:]
	} else {
		keepReplicas = currentReplicas
	}

	// Build the keep and remove sets.
	keep := append(append([]*redis.Node{}, keepPrimaries...), keepReplicas...)
	remove := append(append([]*redis.Node{}, removeReplicas...), drainPrimaries...)

	cr.logger.Info("Scale-down topology classification",
		"currentPrimaries", len(currentPrimaries),
		"currentReplicas", len(currentReplicas),
		"keepPrimaries", len(keepPrimaries),
		"keepReplicas", len(keepReplicas),
		"drainPrimaries", len(drainPrimaries),
		"removeReplicas", len(removeReplicas))

	// Phase 1: drain the primaries that are being removed (weight 0), keeping
	// the surviving primaries (weight 1). Slots flow from the drained nodes to the keepers.
	if len(drainPrimaries) > 0 {
		cr.updateSubstatus(ctx, config, redisv1.SubstatusDrainingPrimaries)
		cr.logger.Info("Draining primaries before removal", "drainCount", len(drainPrimaries))
		weights := make(map[string]int)
		drainHasSlots := false
		for _, n := range keepPrimaries {
			weights[n.ID] = 1
		}
		for _, n := range drainPrimaries {
			weights[n.ID] = 0
			if strings.TrimSpace(n.Slots) != "" {
				drainHasSlots = true
			}
		}

		if drainHasSlots {
			if err := cr.rebalanceWithRetry(ctx, keepPrimaries[0], weights); err != nil {
				cr.logger.Error("Rebalance failed during scale down", "error", err)
				return reconcileAfterWaitInterval, nil
			}
			stable, err := cr.slotsStable(ctx, keepPrimaries[0])
			if err != nil {
				cr.logger.Warn("Failed to check slot stability during scale down", "error", err)
				return reconcileAfterWaitInterval, nil
			}
			if !stable {
				cr.logger.Info("Slots still migrating during scale down, will retry")
				return reconcileAfterWaitInterval, nil
			}
			// Validate the drained primaries no longer hold slots.
			if err := cr.refreshRoles(ctx, drainPrimaries); err != nil {
				cr.logger.Warn("Failed to refresh roles after drain", "error", err)
				return reconcileAfterWaitInterval, nil
			}
			for _, n := range drainPrimaries {
				if strings.TrimSpace(n.Slots) != "" {
					cr.logger.Info("Drained primary still holds slots, will retry", "node", n.Name)
					return reconcileAfterWaitInterval, nil
				}
			}
		}
	}

	// Phase 2: remove the surplus nodes — replicas first to avoid spurious failovers,
	// then the now-empty primaries — propagating CLUSTER FORGET across the survivors.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusRemovingNodes)
	cr.logger.Info("Removing surplus nodes from cluster", "count", len(remove))
	if err := cr.removeNodes(ctx, keep, remove); err != nil {
		cr.logger.Error("Failed to remove nodes during scale down", "error", err)
		return reconcileAfterWaitInterval, nil
	}

	// Phase 3: shrink the StatefulSet now that the cluster no longer references the pods.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusShrinkingStatefulSet)
	cr.logger.Info("Shrinking StatefulSet after node removal", "target", targetTotal)
	if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal)); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scaling StatefulSet down: %w", err)
	}

	// Re-attach replicas if needed (former primaries demoted to replicas).
	if config.Spec.ReplicasPerPrimary > 0 && len(keepReplicas) > 0 {
		cr.updateSubstatus(ctx, config, redisv1.SubstatusAttachingReplicas)
		if err := cr.refreshRoles(ctx, keep); err != nil {
			cr.logger.Warn("Failed to refresh roles before re-attaching replicas", "error", err)
			return reconcileAfterWaitInterval, nil
		}
		if err := cr.setReplicas(ctx, keepPrimaries, keepReplicas, config.Spec.ReplicasPerPrimary); err != nil {
			cr.logger.Warn("Failed to re-attach replicas during scale down, will retry", "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	if len(keep) == 0 {
		return reconcileAfterWaitInterval, nil
	}
	cr.updateSubstatus(ctx, config, redisv1.SubstatusVerifying)
	ok, err := cr.verifyCluster(ctx, keep[0])
	if err != nil || !ok {
		cr.logger.Info("Cluster not yet healthy after scale down, will retry")
		return reconcileAfterWaitInterval, nil
	}

	return cr.finishScaling(ctx, config, keep, "scale down")
}

// handleFastScaling recreates the cluster from scratch at the new topology. It is only
// used for eligible clusters (ephemeral, no replicas, purgeKeysOnRebalance) and accepts
// full data loss in exchange for a much faster operation.
func (cr *ClusterReconciler) handleFastScaling(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	targetTotal := totalNodes(config)
	cr.logger.Info("Starting fast scaling reconciliation", "targetTotal", targetTotal)

	exists, err := kubernetes.StatefulSetExists(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking StatefulSet for fast scaling: %w", err)
	}

	if exists {
		current, err := kubernetes.GetStatefulSetReplicas(ctx, cr.client, cr.clusterName, cr.namespace)
		if err != nil {
			return reconcileAfterInterval, fmt.Errorf("reading StatefulSet replicas for fast scaling: %w", err)
		}
		// The StatefulSet still reflects the old topology: delete it so Kubernetes wipes
		// the pods, then recreate it on the next pass.
		if int(current) != targetTotal {
			cr.updateSubstatus(ctx, config, redisv1.SubstatusDeletingStatefulSet)
			cr.logger.Info("Fast scaling: deleting StatefulSet to recreate at new size", "from", current, "to", targetTotal)
			if err := kubernetes.DeleteStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
				return reconcileAfterInterval, fmt.Errorf("deleting StatefulSet for fast scaling: %w", err)
			}
			return reconcileAfterWaitInterval, nil
		}
	} else {
		// Recreate all cluster objects (StatefulSet is built with the new replica count).
		cr.updateSubstatus(ctx, config, redisv1.SubstatusRecreatingCluster)
		owner, err := cr.getOwner(ctx)
		if err != nil {
			return reconcileAfterInterval, fmt.Errorf("getting owner for fast scaling: %w", err)
		}
		password := cr.getPassword(ctx, config)
		if err := kubernetes.EnsureClusterObjects(ctx, cr.client, config, owner, password); err != nil {
			return reconcileAfterInterval, fmt.Errorf("recreating cluster objects for fast scaling: %w", err)
		}
		cr.logger.Info("Fast scaling: StatefulSet recreated, waiting for pods")
		return reconcileAfterWaitInterval, nil
	}

	// StatefulSet is at the new size: wait for pods, then form the cluster from scratch.
	cr.updateSubstatus(ctx, config, redisv1.SubstatusWaitingForPods)
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, int32(targetTotal))
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pod readiness during fast scaling: %w", err)
	}
	if !ready {
		cr.logger.Info("Fast scaling: waiting for recreated pods to be ready", "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for fast scaling: %w", err)
	}
	defer closeNodes(nodes)
	if len(nodes) < targetTotal {
		cr.logger.Info("Fast scaling: not all nodes available, will retry", "available", len(nodes), "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	cr.updateSubstatus(ctx, config, redisv1.SubstatusFormingCluster)
	cr.logger.Info("Fast scaling: forming new cluster from scratch", "nodes", len(nodes))
	if !cr.formCluster(ctx, config, nodes) {
		return reconcileAfterWaitInterval, nil
	}

	return cr.finishScaling(ctx, config, nodes, "fast scaling")
}

// --- Scaling helpers ---

// finishScaling transitions the cluster to Ready, marks the config as applied, and
// records the final node topology in the status.
func (cr *ClusterReconciler) finishScaling(ctx context.Context, config *redisv1.RedkeyClusterConfig, nodes []*redis.Node, op string) (reconcileSchedule, error) {
	config.Status.Substatus.Status = ""
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusReady); err != nil {
		return reconcileAfterInterval, err
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}
	if err := cr.updateNodeStatus(ctx, config, nodes); err != nil {
		cr.logger.Error("Failed to update node status after scaling", "error", err)
		// Non-critical, continue.
	}
	cr.logger.Info("Scaling complete, status set to Ready", "operation", op)
	return reconcileAfterInterval, nil
}

// hasReplicaNodes reports whether any node in the set has the replica role.
func hasReplicaNodes(nodes []*redis.Node) bool {
	for _, n := range nodes {
		if n.IsReplica() {
			return true
		}
	}
	return false
}

// initAllNodes initializes every pod that currently belongs to the cluster (regardless
// of the target topology) ordered by StatefulSet ordinal. Scale-down needs to talk to
// the nodes that are about to be removed, which are not part of the target topology.
func (cr *ClusterReconciler) initAllNodes(ctx context.Context, password string) ([]*redis.Node, error) {
	clusterCfg := cr.runtimeConfig.ClusterConfig()
	maxRetries := clusterCfg.ConnectionMaxRetries
	backoff := time.Duration(clusterCfg.ConnectionBackOffSeconds) * time.Second

	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return nil, fmt.Errorf("getting pod addresses: %w", err)
	}

	names := make([]string, 0, len(podAddrs))
	for name := range podAddrs {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return cr.ordinalOf(names[i]) < cr.ordinalOf(names[j])
	})

	var nodes []*redis.Node
	for _, name := range names {
		node := redis.NewNode(name, podAddrs[name], password)
		if err := node.Init(ctx, maxRetries, backoff); err != nil {
			cr.logger.Error("Failed to initialize node", "node", name, "error", err)
			closeNodes(nodes)
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// filterUnmet returns the subset of nodes whose IDs are NOT in the given memberIDs set.
func filterUnmet(nodes []*redis.Node, memberIDs map[string]struct{}) []*redis.Node {
	var unmet []*redis.Node
	for _, n := range nodes {
		if _, ok := memberIDs[n.ID]; !ok {
			unmet = append(unmet, n)
		}
	}
	return unmet
}

// ordinalOf extracts the StatefulSet ordinal from a pod name (e.g. "mycluster-3" -> 3).
// Unrecognized names sort last.
func (cr *ClusterReconciler) ordinalOf(name string) int {
	idx := strings.LastIndex(name, "-")
	if idx < 0 || idx == len(name)-1 {
		return 1 << 30
	}
	ordinal, err := strconv.Atoi(name[idx+1:])
	if err != nil {
		return 1 << 30
	}
	return ordinal
}

// refreshRoles refreshes the cached role/slot metadata for every node.
func (cr *ClusterReconciler) refreshRoles(ctx context.Context, nodes []*redis.Node) error {
	for _, n := range nodes {
		if err := n.RefreshInfo(ctx); err != nil {
			return fmt.Errorf("refreshing %s: %w", n.Name, err)
		}
	}
	return nil
}

// rebalanceWithRetry runs a weighted rebalance from the given seed node, retrying with
// backoff until it succeeds or the attempts are exhausted. A nil/empty weights map
// performs an empty-masters rebalance that distributes slots evenly across all primaries.
//
// The retry decision is driven by the cluster's live topology and the context state, not
// by parsing redis-cli error text (which is brittle and could change between Redis
// versions):
//   - Before every attempt it inspects CLUSTER NODES for open (importing/migrating)
//     slots left behind by a previous, possibly interrupted, rebalance and repairs them
//     with redis-cli --cluster fix. redis-cli refuses to rebalance a cluster with open
//     slots, so this repair is what lets the operation make progress.
//   - A failure is retried as long as the parent context is still alive. The per-attempt
//     rebalance timeout cancels only an inner context, so a slow reshard is naturally
//     retryable; cancellation of the parent context (e.g. shutdown) aborts immediately.
func (cr *ClusterReconciler) rebalanceWithRetry(ctx context.Context, seed *redis.Node, weights map[string]int) error {
	backoff := cr.runtimeConfig.ClusterMeetWait()
	if backoff <= 0 {
		backoff = time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= defaultRebalanceMaxAttempts; attempt++ {
		// Heal leftover open slots (detected structurally from CLUSTER NODES) before
		// rebalancing, then run the rebalance.
		if err := cr.recoverOpenSlots(ctx, seed); err != nil {
			cr.logger.Warn("Failed to recover open slots before rebalance, continuing", "error", err)
		}

		err := cr.runRebalanceOnce(ctx, seed, weights)
		if err == nil {
			return nil
		}
		lastErr = err

		// Abort immediately only if the parent context was cancelled (e.g. shutdown). A
		// per-attempt rebalance timeout cancels only the inner context, so it falls
		// through to the retry path where the cluster is repaired and the rebalance re-run.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == defaultRebalanceMaxAttempts {
			break
		}
		cr.logger.Info("Rebalance attempt failed, will repair open slots and retry", "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("rebalance from %s did not converge after %d attempt(s): %w", seed.Name, defaultRebalanceMaxAttempts, lastErr)
}

// runRebalanceOnce performs a single weighted rebalance bounded by the configured
// rebalance timeout.
func (cr *ClusterReconciler) runRebalanceOnce(ctx context.Context, seed *redis.Node, weights map[string]int) error {
	rebalanceCtx := ctx
	if timeout := cr.runtimeConfig.RebalanceTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		rebalanceCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	_, err := seed.Client().ClusterRebalanceWithWeights(rebalanceCtx, weights)
	return err
}

// recoverOpenSlots detects slots left in a migrating/importing state by a previously
// interrupted rebalance and runs redis-cli --cluster fix to close them. It is a no-op
// when the cluster has no in-flight slots.
func (cr *ClusterReconciler) recoverOpenSlots(ctx context.Context, seed *redis.Node) error {
	clusterNodes, err := seed.Client().GetClusterNodes(ctx)
	if err != nil {
		return err
	}
	if !redis.HasInFlightSlots(clusterNodes) {
		return nil
	}

	cr.logger.Info("Open slots detected from a previous rebalance, running cluster fix", "node", seed.Name)
	return cr.runClusterFix(ctx, seed)
}

// runClusterFix runs redis-cli --cluster fix from the seed node, bounded by the configured
// rebalance timeout. The fix both closes open (migrating/importing) slots and reassigns
// slots left uncovered, which is needed after forgetting a slot-owning stale node.
func (cr *ClusterReconciler) runClusterFix(ctx context.Context, seed *redis.Node) error {
	fixCtx := ctx
	if timeout := cr.runtimeConfig.RebalanceTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		fixCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if _, err := seed.Client().ClusterFix(fixCtx); err != nil {
		return fmt.Errorf("cluster fix: %w", err)
	}
	return nil
}

// pruneStaleNodes forgets any cluster node that is not part of the live, expected
// topology. Ephemeral pods that restart during a long operation (for example, when an
// interrupted rebalance kills them) rejoin with a brand new node ID while their previous
// ID lingers in the gossip table — often still owning slots. redis-cli then rebalances
// "across" those phantom masters and never converges. We detect them structurally: any ID
// present in CLUSTER NODES but absent from the set of live nodes we just initialized is
// stale and is forgotten from every surviving node. Nodes still in the handshake
// state are skipped, as they are transient gossip entries rather than true phantoms. It
// returns true if anything was pruned.
func (cr *ClusterReconciler) pruneStaleNodes(ctx context.Context, nodes []*redis.Node) (bool, error) {
	expected := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		expected[n.ID] = struct{}{}
	}

	clusterNodes, err := nodes[0].Client().GetClusterNodes(ctx)
	if err != nil {
		return false, fmt.Errorf("getting cluster nodes to prune stale entries: %w", err)
	}

	pruned := false
	for _, cn := range clusterNodes {
		if cn.ID == "" {
			continue
		}
		if _, ok := expected[cn.ID]; ok {
			continue
		}
		// Nodes in the handshake state are transient gossip churn (a MEET still in
		// progress), not phantom masters left behind by a restart. They resolve on their
		// own, and CLUSTER FORGET on them just returns "ERR Unknown node", so skip them.
		if strings.Contains(cn.Flags, "handshake") {
			continue
		}
		cr.logger.Info("Forgetting stale cluster node",
			"staleNodeID", cn.ID, "addr", cn.Addr, "flags", cn.Flags, "slots", cn.Slots)
		for _, n := range nodes {
			if n.ID == cn.ID {
				continue
			}
			if err := n.Client().ClusterForget(ctx, cn.ID); err != nil {
				// The node may already not know about the stale entry; log and continue.
				cr.logger.Warn("Failed to forget stale node, continuing",
					"from", n.Name, "staleNodeID", cn.ID, "error", err)
			}
		}
		pruned = true
	}
	return pruned, nil
}

// slotsStable reports whether no node currently has slots in a migrating or importing state.
func (cr *ClusterReconciler) slotsStable(ctx context.Context, seed *redis.Node) (bool, error) {
	clusterNodes, err := seed.Client().GetClusterNodes(ctx)
	if err != nil {
		return false, err
	}
	return !redis.HasInFlightSlots(clusterNodes), nil
}

// removeNodes removes the given nodes from the cluster. Replicas are removed before
// primaries to avoid spurious failovers, each removal is propagated to every surviving
// node via CLUSTER FORGET, and the removed node is shut down cleanly.
func (cr *ClusterReconciler) removeNodes(ctx context.Context, keep, remove []*redis.Node) error {
	ordered := make([]*redis.Node, 0, len(remove))
	for _, n := range remove {
		if n.IsReplica() {
			ordered = append(ordered, n)
		}
	}
	for _, n := range remove {
		if !n.IsReplica() {
			ordered = append(ordered, n)
		}
	}

	for _, n := range ordered {
		for _, k := range keep {
			if k.ID == n.ID {
				continue
			}
			if err := k.Client().ClusterForget(ctx, n.ID); err != nil {
				// A node may already not know about the forgotten node; log and continue.
				cr.logger.Warn("Failed to forget node, continuing", "from", k.Name, "node", n.Name, "error", err)
			}
		}
		if err := n.Client().ShutdownSave(ctx); err != nil {
			cr.logger.Warn("Failed to shut down node cleanly, continuing", "node", n.Name, "error", err)
		}
		cr.logger.Info("Removed node from cluster", "node", n.Name)
	}

	// Allow gossip to propagate the removals before continuing.
	if wait := cr.runtimeConfig.ClusterMeetWait(); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil
}

// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"strings"
	"time"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/kubernetes"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

// handleUpgrading is the entry point for the Upgrading status. It decides between
// Fast Upgrade (delete and recreate cluster) and Rolling N+1 (partition-by-partition
// slot migration) based on the cluster configuration.
//
// IMPORTANT: This upgrade mechanism assumes compatible images (same major version).
// For incompatible major version upgrades, the side-by-side strategy (Phase 13) must
// be used instead.
func (cr *ClusterReconciler) handleUpgrading(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	if fastUpgradeEligible(config) {
		return cr.handleFastUpgrade(ctx, config)
	}
	return cr.handleRollingUpgrade(ctx, config)
}

// fastUpgradeEligible reports whether the cluster qualifies for a fast upgrade that
// deletes all pods and rebuilds the cluster from scratch (data loss). It applies only
// when the cluster is ephemeral, has no replicas, AND purgeKeysOnRebalance is explicitly
// set to true. Clusters with replicas always use the rolling N+1 strategy to avoid
// service disruption.
func fastUpgradeEligible(config *redisv1.RedkeyClusterConfig) bool {
	return config.Spec.Ephemeral &&
		config.Spec.ReplicasPerPrimary == 0 &&
		config.Spec.PurgeKeysOnRebalance != nil && *config.Spec.PurgeKeysOnRebalance
}

// --- Fast Upgrade ---

// handleFastUpgrade performs a destructive upgrade by deleting all pods and rebuilding
// the cluster from scratch. This is the fastest path but causes total data loss.
//
// Substatus lifecycle:
//   - SubstatusFastUpgrading: update StatefulSet template + ConfigMap, delete all pods
//   - SubstatusEndingFastUpgrade: wait for pods to be Ready, then trigger cluster formation
func (cr *ClusterReconciler) handleFastUpgrade(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	switch config.Status.Substatus.Status {
	case redisv1.SubstatusFastUpgrading:
		return cr.handleFastUpgradeWaitReady(ctx, config)
	case redisv1.SubstatusEndingFastUpgrade:
		return cr.handleFastUpgradeFormCluster(ctx, config)
	default:
		return cr.handleFastUpgradeStart(ctx, config)
	}
}

func (cr *ClusterReconciler) handleFastUpgradeStart(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	cr.logger.Info("Starting fast upgrade (purgeKeysOnRebalance=true)",
		"config", config.Name,
		"image", config.Spec.Image)

	password := cr.getPassword(ctx, config)

	// Update ConfigMap with new redis.conf
	if err := kubernetes.UpdateConfigMap(ctx, cr.client, cr.clusterName, cr.namespace, config, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating ConfigMap for fast upgrade: %w", err)
	}

	// Update StatefulSet template (image, labels, annotations, config checksum)
	checksum := kubernetes.ComputeConfigChecksum(config.Spec.Image, config.Spec.Version, config.Spec.RedisConfig)
	if err := kubernetes.UpdateStatefulSetTemplate(ctx, cr.client, cr.clusterName, cr.namespace, config, checksum); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating StatefulSet template for fast upgrade: %w", err)
	}

	// Reconcile the PodDisruptionBudget so PDB-only changes are applied during upgrades.
	owner, err := cr.getOwner(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting owner for fast upgrade: %w", err)
	}
	if err := kubernetes.ReconcilePDB(ctx, cr.client, config, owner); err != nil {
		return reconcileAfterInterval, fmt.Errorf("reconciling PDB for fast upgrade: %w", err)
	}

	// Set partition to 0 so deleted pods are recreated with the NEW template.
	// UpdateStatefulSetTemplate sets partition=replicas to prevent automatic rolling,
	// but for fast upgrade we need all pods to get the new image immediately.
	if err := kubernetes.SetStatefulSetPartition(ctx, cr.client, cr.clusterName, cr.namespace, 0); err != nil {
		return reconcileAfterInterval, fmt.Errorf("setting partition to 0 for fast upgrade: %w", err)
	}

	// Delete all pods — StatefulSet will recreate them with the new template
	if err := kubernetes.DeleteAllPods(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
		return reconcileAfterInterval, fmt.Errorf("deleting pods for fast upgrade: %w", err)
	}

	cr.logger.Info("Fast upgrade: pods deleted, waiting for recreation with new image")
	cr.updateSubstatus(ctx, config, redisv1.SubstatusFastUpgrading)
	return reconcileAfterWaitInterval, nil
}

func (cr *ClusterReconciler) handleFastUpgradeWaitReady(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	totalNodes := int32(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, totalNodes)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pods during fast upgrade: %w", err)
	}
	if !ready {
		cr.logger.Info("Fast upgrade: waiting for pods to be ready", "expected", totalNodes)
		return reconcileAfterWaitInterval, nil
	}

	cr.logger.Info("Fast upgrade: all pods ready, proceeding to cluster formation")
	cr.updateSubstatus(ctx, config, redisv1.SubstatusEndingFastUpgrade)
	return reconcileImmediately, nil
}

func (cr *ClusterReconciler) handleFastUpgradeFormCluster(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	// Reuse the Configuring handler logic — it forms the cluster from scratch.
	// First, reset the status to Configuring so the existing handler picks it up.
	cr.logger.Info("Fast upgrade: forming cluster from scratch")

	password := cr.getPassword(ctx, config)
	totalNodes := int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))

	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for fast upgrade cluster formation: %w", err)
	}
	defer closeNodes(nodes)
	if len(nodes) < totalNodes {
		cr.logger.Info("Fast upgrade: not all nodes available for cluster formation", "available", len(nodes), "expected", totalNodes)
		return reconcileAfterWaitInterval, nil
	}

	primaryCount := int(config.Spec.Primaries)
	primaries := nodes[:primaryCount]

	// Check whether all nodes already share a single cluster view (gossip converged).
	converged, err := cr.checkGossipConvergence(ctx, nodes)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking gossip convergence during fast upgrade: %w", err)
	}

	if !converged {
		// Nodes have not formed a single cluster view yet. This is either a fresh start
		// (clean pods) or stale state left over on PVC-backed pods. If any node still
		// carries slots from a previous incarnation, hard-reset all nodes so meet starts
		// from a clean slate. We only reset while gossip is NOT converged: once the nodes
		// share a view and we have assigned slots ourselves, resetting on every retry
		// would change node IDs and loop forever.
		needsReset := false
		for _, node := range nodes {
			info, err := node.Client().GetClusterInfo(ctx)
			if err != nil {
				return reconcileAfterInterval, fmt.Errorf("getting cluster info for %s: %w", node.Name, err)
			}
			if info.SlotsOK > 0 || info.SlotsFail > 0 {
				needsReset = true
				break
			}
		}
		if needsReset {
			for _, node := range nodes {
				if err := node.Client().ClusterReset(ctx, true); err != nil {
					cr.logger.Warn("Failed to reset node during fast upgrade", "node", node.Name, "error", err)
				}
			}
			// Return to let nodes settle after reset before trying to meet
			return reconcileAfterWaitInterval, nil
		}

		// Meet all nodes and wait for gossip to converge on a subsequent cycle.
		if err := cr.meetNodes(ctx, nodes); err != nil {
			return reconcileAfterInterval, fmt.Errorf("meeting nodes during fast upgrade: %w", err)
		}
		cr.logger.Info("Fast upgrade: gossip not converged yet, retrying")
		return reconcileAfterWaitInterval, nil
	}

	// Assign slots to primaries idempotently — assignSlots skips any primary that already
	// owns slots, so it is safe to call on every retry until the cluster verifies as ok.
	if err := cr.assignSlots(ctx, primaries); err != nil {
		return reconcileAfterInterval, fmt.Errorf("assigning slots during fast upgrade: %w", err)
	}
	cr.logger.Info("Fast upgrade: slots assigned to primaries", "primaries", primaryCount)

	// Attach replicas if configured (idempotent — skips replicas already attached correctly).
	if config.Spec.ReplicasPerPrimary > 0 {
		replicas := nodes[primaryCount:]
		if err := cr.setReplicas(ctx, primaries, replicas, config.Spec.ReplicasPerPrimary); err != nil {
			return reconcileAfterInterval, fmt.Errorf("attaching replicas during fast upgrade: %w", err)
		}
		cr.logger.Info("Fast upgrade: replicas attached", "replicasPerPrimary", config.Spec.ReplicasPerPrimary)
	}

	// Verify the cluster has converged to a healthy state (cluster_state:ok with full
	// slot coverage) before declaring success. Immediately after CLUSTER ADDSLOTS the
	// cluster needs a brief moment to mark all slots as covered and transition to ok;
	// marking the config Applied prematurely surfaces a transient cluster_state:fail.
	clusterOK, err := cr.verifyCluster(ctx, nodes[0])
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("verifying cluster after fast upgrade: %w", err)
	}
	if !clusterOK {
		cr.logger.Info("Fast upgrade: cluster not yet converged, retrying")
		return reconcileAfterWaitInterval, nil
	}

	// Finish: set status to Ready and mark config as Applied
	cr.logger.Info("Fast upgrade completed successfully",
		"image", config.Spec.Image,
		"primaries", config.Spec.Primaries)

	config.Status.Substatus = redisv1.RedkeyClusterSubstatus{}
	config.Status.Status = redisv1.ClusterStatusReady
	if err := cr.client.Status().Update(ctx, config); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating status to Ready after fast upgrade: %w", err)
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}

	return reconcileAfterInterval, nil
}

// --- Rolling N+1 Upgrade ---

// handleRollingUpgrade orchestrates a zero-downtime upgrade by iterating partition-by-partition:
// scale up by 1 (or 1+replicas), migrate slots from victim → destination, recycle the victim
// pod (which gets the new image), and repeat until all nodes run the new image.
//
// Substatus lifecycle:
//   - SubstatusUpgradeScalingUp: scale StatefulSet +1(+replicas), meet extra nodes
//   - SubstatusUpgradeResharding: migrate slots from partition N to N+1 (pivot pattern)
//   - SubstatusUpgradeRollingUpdate: set partition, wait for pod recreation with new image
//   - SubstatusUpgradeEnding: migrate slots from extra node back to node 0
//   - SubstatusUpgradeScalingDown: scale down, verify, set Ready
func (cr *ClusterReconciler) handleRollingUpgrade(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	switch config.Status.Substatus.Status {
	case redisv1.SubstatusUpgradeScalingUp:
		return cr.handleUpgradeScalingUp(ctx, config)
	case redisv1.SubstatusUpgradeResharding:
		return cr.handleUpgradeResharding(ctx, config)
	case redisv1.SubstatusUpgradeRollingUpdate:
		return cr.handleUpgradeRollingUpdate(ctx, config)
	case redisv1.SubstatusUpgradeEnding:
		return cr.handleUpgradeEnding(ctx, config)
	case redisv1.SubstatusUpgradeScalingDown:
		return cr.handleUpgradeScalingDown(ctx, config)
	default:
		return cr.handleUpgradeStart(ctx, config)
	}
}

// originalPrimaries returns the number of primaries before the +1 scale up for upgrade.
func originalPrimaries(config *redisv1.RedkeyClusterConfig) int32 {
	return config.Spec.Primaries
}

// upgradeExtraNodes returns how many extra nodes are added during upgrade (1 primary + replicas).
func upgradeExtraNodes(config *redisv1.RedkeyClusterConfig) int32 {
	return 1 + config.Spec.ReplicasPerPrimary
}

// handleUpgradeStart initiates the Rolling N+1 upgrade by scaling up the StatefulSet.
func (cr *ClusterReconciler) handleUpgradeStart(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	cr.logger.Info("Starting rolling N+1 upgrade",
		"config", config.Name,
		"image", config.Spec.Image,
		"primaries", config.Spec.Primaries,
		"replicasPerPrimary", config.Spec.ReplicasPerPrimary)

	password := cr.getPassword(ctx, config)

	// Update ConfigMap first (all new pods will get the new redis.conf)
	if err := kubernetes.UpdateConfigMap(ctx, cr.client, cr.clusterName, cr.namespace, config, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating ConfigMap for rolling upgrade: %w", err)
	}

	// Scale up: add extra nodes (1 primary + replicasPerPrimary replicas)
	currentTotal := int32(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	extra := upgradeExtraNodes(config)
	targetTotal := currentTotal + extra

	cr.logger.Info("Rolling upgrade: scaling StatefulSet for extra nodes",
		"currentTotal", currentTotal, "targetTotal", targetTotal, "extraNodes", extra)

	// Update the StatefulSet template BEFORE scaling up so the new pods get the new image.
	// OnDelete strategy: existing pods are NOT restarted automatically — only manually
	// deleted pods will be recreated with the new template.
	checksum := kubernetes.ComputeConfigChecksum(config.Spec.Image, config.Spec.Version, config.Spec.RedisConfig)
	if err := kubernetes.UpdateStatefulSetTemplate(ctx, cr.client, cr.clusterName, cr.namespace, config, checksum); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating StatefulSet template for rolling upgrade: %w", err)
	}

	// Reconcile the PodDisruptionBudget so PDB-only changes are applied during upgrades.
	owner, err := cr.getOwner(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting owner for rolling upgrade: %w", err)
	}
	if err := kubernetes.ReconcilePDB(ctx, cr.client, config, owner); err != nil {
		return reconcileAfterInterval, fmt.Errorf("reconciling PDB for rolling upgrade: %w", err)
	}

	if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, targetTotal); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scaling up for rolling upgrade: %w", err)
	}

	cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeScalingUp)
	return reconcileAfterWaitInterval, nil
}

// handleUpgradeScalingUp waits for the extra pods to be ready, then meets them into the cluster.
func (cr *ClusterReconciler) handleUpgradeScalingUp(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	currentTotal := int32(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	extra := upgradeExtraNodes(config)
	targetTotal := currentTotal + extra

	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, targetTotal)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pods during upgrade scale up: %w", err)
	}
	if !ready {
		cr.logger.Info("Rolling upgrade: waiting for extra pods to be ready", "expected", targetTotal)
		return reconcileAfterWaitInterval, nil
	}

	cr.logger.Info("Rolling upgrade: extra pods ready, meeting into cluster")

	password := cr.getPassword(ctx, config)

	// Initialize all nodes (including the new ones)
	nodes, err := cr.initNodesWithCount(ctx, int(targetTotal), password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for upgrade: %w", err)
	}
	defer closeNodes(nodes)

	// Meet new nodes into the cluster
	if err := cr.meetNodes(ctx, nodes); err != nil {
		return reconcileAfterInterval, fmt.Errorf("meeting nodes during upgrade scale up: %w", err)
	}

	converged, err := cr.checkGossipConvergence(ctx, nodes)
	if err != nil || !converged {
		cr.logger.Info("Rolling upgrade: gossip not converged yet after meeting extra nodes")
		return reconcileAfterWaitInterval, nil
	}

	// If replicas: configure extra replica(s) to replicate the extra primary
	if config.Spec.ReplicasPerPrimary > 0 {
		extraPrimaryIdx := int(currentTotal) // first extra pod = extra primary
		extraPrimaryNode := nodes[extraPrimaryIdx]
		for r := range int(config.Spec.ReplicasPerPrimary) {
			extraReplicaIdx := extraPrimaryIdx + 1 + r
			extraReplicaNode := nodes[extraReplicaIdx]
			if err := extraReplicaNode.Client().ClusterReplicate(ctx, extraPrimaryNode.ID); err != nil {
				return reconcileAfterInterval, fmt.Errorf("replicating extra replica %d to extra primary: %w", extraReplicaIdx, err)
			}
			cr.logger.Info("Rolling upgrade: extra replica attached to extra primary",
				"extraReplicaOrdinal", extraReplicaIdx,
				"extraPrimaryOrdinal", extraPrimaryIdx)
		}
	}

	// Set starting partition = primaries - 1 (last original primary, working backwards to 0)
	startPartition := int(config.Spec.Primaries) - 1
	cr.logger.Info("Rolling upgrade: scale up complete, starting resharding",
		"startPartition", startPartition)

	config.Status.Substatus.UpgradingPartition = startPartition
	cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeResharding)
	return reconcileImmediately, nil
}

// handleUpgradeResharding migrates all slots from the node at current partition to the
// destination node. The destination is partition+1: for the first iteration (partition=N-1)
// that equals N (the extra node); for subsequent iterations it's the previously recycled node
// which already runs the new image.
func (cr *ClusterReconciler) handleUpgradeResharding(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	partition := config.Status.Substatus.UpgradingPartition

	// Determine destination ordinal:
	// - First iteration (partition == primaries-1): destination is the extra primary,
	//   which sits after all original pods (primaries + replicas).
	// - Subsequent iterations: destination is partition+1 (previously recycled primary).
	var destOrdinal int
	if partition == int(config.Spec.Primaries)-1 {
		destOrdinal = int(config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary)
	} else {
		destOrdinal = partition + 1
	}

	cr.logger.Info("Rolling upgrade: resharding slots from partition to destination",
		"partition", partition,
		"destOrdinal", destOrdinal)

	password := cr.getPassword(ctx, config)

	// Forget any dead (failed) nodes left over from the previous rolling update cycle.
	// Without this, redis-cli --cluster reshard/fix tries to contact unreachable nodes
	// (old pod IPs before recycling) and hangs or fails.
	seedAddr, err := cr.getPodAddr(ctx, int32(destOrdinal))
	if err == nil {
		seedClient := redis.NewClient(seedAddr, password)
		if err := cr.forgetFailedNodes(ctx, seedClient, password, config); err != nil {
			cr.logger.Warn("Rolling upgrade: failed to forget dead nodes before reshard, continuing",
				"partition", partition, "error", err)
		}
		seedClient.Close()
	}

	// Before each reshard, ensure the pivot has its replica for HA.
	// Redis auto-migration can steal the pivot's replica when other nodes become orphaned.
	if config.Spec.ReplicasPerPrimary > 0 {
		if err := cr.ensurePivotReplica(ctx, config, password); err != nil {
			cr.logger.Warn("Rolling upgrade: failed to ensure pivot replica before reshard, will retry",
				"partition", partition, "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	// Get node info for source (victim) and destination
	sourceAddr, err := cr.getPodAddr(ctx, int32(partition))
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting source pod address: %w", err)
	}
	destAddr, err := cr.getPodAddr(ctx, int32(destOrdinal))
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting destination pod address: %w", err)
	}

	sourceClient := redis.NewClient(sourceAddr, password)
	defer sourceClient.Close()
	destClient := redis.NewClient(destAddr, password)
	defer destClient.Close()

	sourceID, err := sourceClient.ClusterMyID(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting source node ID at partition %d: %w", partition, err)
	}
	destID, err := destClient.ClusterMyID(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting destination node ID: %w", err)
	}

	// Count slots on source
	sourceSlots, err := sourceClient.CountSlotsForNode(ctx, sourceID)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("counting source slots: %w", err)
	}

	if sourceSlots == 0 {
		cr.logger.Info("Rolling upgrade: source already has 0 slots, proceeding to rolling update",
			"partition", partition)
		cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeRollingUpdate)
		return reconcileImmediately, nil
	}

	cr.logger.Info("Rolling upgrade: migrating slots",
		"partition", partition,
		"sourceID", sourceID,
		"destID", destID,
		"slots", sourceSlots)

	// Run cluster fix to resolve any open/stuck slots from a previous partial reshard.
	// This is more comprehensive than manually stabilizing individual slots.
	if _, err := sourceClient.ClusterFix(ctx); err != nil {
		cr.logger.Warn("Rolling upgrade: cluster fix before reshard failed, continuing",
			"partition", partition, "error", err)
	}

	// Execute reshard: move ALL slots from source to destination
	result, err := sourceClient.ClusterReshard(ctx, sourceID, destID, sourceSlots)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("resharding from partition %d to destination: %w", partition, err)
	}

	cr.logger.Info("Rolling upgrade: reshard completed",
		"partition", partition,
		"output", truncateOutput(result.Output, 200))

	// Verify source has 0 slots now
	remainingSlots, err := sourceClient.CountSlotsForNode(ctx, sourceID)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("verifying source empty after reshard: %w", err)
	}
	if remainingSlots > 0 {
		cr.logger.Warn("Rolling upgrade: source still has slots after reshard, retrying",
			"partition", partition, "remaining", remainingSlots)
		return reconcileAfterWaitInterval, nil
	}

	// If replicas exist, detach replicas of the source before recycling
	if config.Spec.ReplicasPerPrimary > 0 {
		if err := cr.forgetReplicasOfNode(ctx, sourceID, password, config); err != nil {
			cr.logger.Warn("Rolling upgrade: failed to forget replicas of source, will retry",
				"partition", partition, "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	cr.logger.Info("Rolling upgrade: slots migrated successfully, proceeding to rolling update",
		"partition", partition)
	cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeRollingUpdate)
	return reconcileImmediately, nil
}

// handleUpgradeRollingUpdate recycles ONLY the drained primary and its replica(s).
// Uses OnDelete strategy with manual pod deletion instead of the partition mechanism,
// ensuring that replicas of active (non-drained) primaries are never restarted.
// This preserves HA throughout the entire upgrade process as described in section 6.2:
// "Solo se reciclan los pares que ya están vacíos."
func (cr *ClusterReconciler) handleUpgradeRollingUpdate(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	partition := config.Status.Substatus.UpgradingPartition
	podName := fmt.Sprintf("%s-%d", cr.clusterName, partition)

	expectedImage := config.Spec.Image

	// Decide whether the pod still needs recycling by inspecting its current image.
	//
	// CRITICAL: the pod must be deleted at most ONCE per partition. This handler returns
	// reconcileAfterWaitInterval while waiting for the pod to be recreated and ready, so it
	// re-enters on every retry. Deleting unconditionally on each entry would repeatedly
	// destroy the freshly-recreated pod, preventing it from ever stabilizing — and if the
	// pod had already been met and received migrated slots, the re-deletion orphans those
	// slots under a now-dead node ID, permanently deadlocking the upgrade (redis-cli reshard
	// refuses to run while an unreachable master still owns slots).
	actualImage, imgErr := kubernetes.GetPodImage(ctx, cr.client, cr.clusterName, cr.namespace, int32(partition))
	if imgErr != nil {
		// Pod is most likely mid-recreation (not yet present). Wait for it to come back
		// rather than treating this as a fatal error or re-issuing a delete.
		cr.logger.Info("Rolling upgrade: pod not available yet, waiting for recreation",
			"partition", partition, "error", imgErr)
		return reconcileAfterWaitInterval, nil
	}

	if actualImage != expectedImage {
		// Pod still runs the OLD image: delete it once so the OnDelete strategy recreates
		// it with the current template (new image).
		cr.logger.Info("Rolling upgrade: deleting drained primary pod for recycling",
			"partition", partition, "pod", podName)
		if err := kubernetes.DeletePod(ctx, cr.client, podName, cr.namespace); err != nil {
			return reconcileAfterInterval, fmt.Errorf("deleting pod %s: %w", podName, err)
		}
		cr.logger.Info("Rolling upgrade: waiting for pod to be recreated with new image",
			"partition", partition)
		return reconcileAfterWaitInterval, nil
	}

	// Pod already runs the new image. Ensure it is Ready before meeting it back.
	ready, err := kubernetes.IsPodOrdinalReady(ctx, cr.client, cr.clusterName, cr.namespace, int32(partition))
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pod %d readiness: %w", partition, err)
	}
	if !ready {
		cr.logger.Info("Rolling upgrade: waiting for recycled pod to be ready",
			"partition", partition)
		return reconcileAfterWaitInterval, nil
	}

	// Pod is ready with new image. Meet the recycled node back into the cluster.
	password := cr.getPassword(ctx, config)
	recycledAddr, err := cr.getPodAddr(ctx, int32(partition))
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting recycled pod address: %w", err)
	}
	recycledNode := redis.NewNode(fmt.Sprintf("%s-%d", cr.clusterName, partition), recycledAddr, password)
	defer recycledNode.Close()

	clusterCfg := cr.runtimeConfig.ClusterConfig()
	if err := recycledNode.Init(ctx, clusterCfg.ConnectionMaxRetries, time.Duration(clusterCfg.ConnectionBackOffSeconds)*time.Second); err != nil {
		cr.logger.Info("Rolling upgrade: recycled pod not yet initialized, retrying",
			"partition", partition, "error", err)
		return reconcileAfterWaitInterval, nil
	}

	// Get another node to help meet — use the extra primary which is always up
	seedOrdinal := int32(config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary)
	seedAddr, err := cr.getPodAddr(ctx, seedOrdinal)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting seed pod address: %w", err)
	}
	seedClient := redis.NewClient(seedAddr, password)
	defer seedClient.Close()

	// Meet the recycled node
	if err := seedClient.ClusterMeet(ctx, recycledNode.IP, redis.DefaultPort); err != nil {
		return reconcileAfterInterval, fmt.Errorf("meeting recycled node %d: %w", partition, err)
	}

	cr.logger.Info("Rolling upgrade: pod recycled and met into cluster",
		"partition", partition,
		"newNodeID", recycledNode.ID)

	// Forget any dead (failed) nodes left over from previous recycles.
	// Without this, redis-cli reshard would try to send SETSLOT to unreachable nodes,
	// making each slot migration extremely slow.
	if err := cr.forgetFailedNodes(ctx, seedClient, password, config); err != nil {
		cr.logger.Warn("Rolling upgrade: failed to forget dead nodes, continuing",
			"partition", partition, "error", err)
	}

	// If replicas: recycle ONLY the replica pods of this specific drained primary.
	// Other replicas (for primaries that still hold slots) are left untouched.
	if config.Spec.ReplicasPerPrimary > 0 {
		if err := cr.recycleReplicasForPrimary(ctx, config, partition, password); err != nil {
			cr.logger.Warn("Rolling upgrade: failed to recycle replicas, will retry",
				"partition", partition, "error", err)
			return reconcileAfterWaitInterval, nil
		}

		// Ensure the pivot (extra primary) still has its replica — it may have been
		// disrupted by Redis auto-migration or gossip delays during pod recreation.
		if err := cr.ensurePivotReplica(ctx, config, password); err != nil {
			cr.logger.Warn("Rolling upgrade: failed to ensure pivot replica, will retry",
				"partition", partition, "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	// Advance to next partition or proceed to ending
	if partition > 0 {
		nextPartition := partition - 1
		cr.logger.Info("Rolling upgrade: advancing to next partition",
			"currentPartition", partition,
			"nextPartition", nextPartition)
		config.Status.Substatus.UpgradingPartition = nextPartition
		cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeResharding)
		return reconcileAfterWaitInterval, nil
	}

	// partition == 0: all original nodes have been upgraded, move to ending
	cr.logger.Info("Rolling upgrade: all partitions upgraded, proceeding to ending phase")
	cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeEnding)
	return reconcileAfterWaitInterval, nil
}

// handleUpgradeEnding migrates slots from the extra node (which holds the first batch
// of slots from the very first iteration) back to node 0 (which was just recycled and
// is empty). Then forgets the extra node and proceeds to scale down.
func (cr *ClusterReconciler) handleUpgradeEnding(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	// The extra primary sits right after all original pods (primaries + replicas)
	extraOrdinal := int32(config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary)

	password := cr.getPassword(ctx, config)

	// Get extra node info
	extraAddr, err := cr.getPodAddr(ctx, extraOrdinal)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting extra pod address: %w", err)
	}
	extraClient := redis.NewClient(extraAddr, password)
	defer extraClient.Close()

	extraID, err := extraClient.ClusterMyID(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting extra node ID: %w", err)
	}

	// Count slots on extra node
	extraSlots, err := extraClient.CountSlotsForNode(ctx, extraID)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("counting extra node slots: %w", err)
	}

	if extraSlots == 0 {
		// All slots already moved — forget the extra node and proceed
		cr.logger.Info("Rolling upgrade ending: extra has 0 slots, forgetting from cluster", "extraID", extraID)
		allMembers := int(config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary)
		if err := cr.forgetNodeFromAll(ctx, extraID, password, allMembers); err != nil {
			return reconcileAfterInterval, fmt.Errorf("forgetting extra node: %w", err)
		}

		// If replicas: also forget extra replicas
		if config.Spec.ReplicasPerPrimary > 0 {
			if err := cr.forgetExtraReplicas(ctx, config, password); err != nil {
				cr.logger.Warn("Rolling upgrade ending: failed to forget extra replicas", "error", err)
				return reconcileAfterWaitInterval, nil
			}
		}

		cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeScalingDown)
		return reconcileImmediately, nil
	}

	// Move all slots from extra to node 0 (which was just recycled and is empty).
	// With the pivot algorithm, the extra only holds ~1/N of total slots.
	destAddr, err := cr.getPodAddr(ctx, 0)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting pod 0 address: %w", err)
	}
	destClient := redis.NewClient(destAddr, password)
	destID, err := destClient.ClusterMyID(ctx)
	destClient.Close()
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting node 0 ID: %w", err)
	}

	cr.logger.Info("Rolling upgrade ending: moving slots from extra to node 0",
		"extraID", extraID, "destID", destID, "slots", extraSlots)

	// Run cluster fix to resolve any open/stuck slots before reshard
	if _, err := extraClient.ClusterFix(ctx); err != nil {
		cr.logger.Warn("Rolling upgrade ending: cluster fix failed, continuing", "error", err)
	}

	result, err := extraClient.ClusterReshard(ctx, extraID, destID, extraSlots)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("resharding from extra to node 0: %w", err)
	}
	cr.logger.Info("Rolling upgrade ending: reshard to node 0 completed",
		"output", truncateOutput(result.Output, 200))

	// Verify extra has 0 slots now
	remaining, err := extraClient.CountSlotsForNode(ctx, extraID)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("verifying extra empty: %w", err)
	}
	if remaining > 0 {
		cr.logger.Warn("Rolling upgrade ending: extra still has slots, retrying", "remaining", remaining)
		return reconcileAfterWaitInterval, nil
	}

	// Forget extra from cluster
	cr.logger.Info("Rolling upgrade ending: forgetting extra node", "extraID", extraID)
	allMembers := int(config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary)
	if err := cr.forgetNodeFromAll(ctx, extraID, password, allMembers); err != nil {
		return reconcileAfterInterval, fmt.Errorf("forgetting extra node: %w", err)
	}

	// If replicas: also forget extra replicas
	if config.Spec.ReplicasPerPrimary > 0 {
		if err := cr.forgetExtraReplicas(ctx, config, password); err != nil {
			cr.logger.Warn("Rolling upgrade ending: failed to forget extra replicas", "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	cr.updateSubstatus(ctx, config, redisv1.SubstatusUpgradeScalingDown)
	return reconcileImmediately, nil
}

// handleUpgradeScalingDown scales the StatefulSet back to the original size, restores
// the RollingUpdate strategy, runs a health check, and transitions to Ready.
func (cr *ClusterReconciler) handleUpgradeScalingDown(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	originalTotal := int32(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))

	cr.logger.Info("Rolling upgrade: scaling down to original size", "targetTotal", originalTotal)

	// Restore RollingUpdate strategy with partition=0 (all pods now have the new image,
	// so no automatic recreation will occur). This returns the StatefulSet to its normal
	// operating mode after the OnDelete-based upgrade.
	if err := kubernetes.SetStatefulSetPartition(ctx, cr.client, cr.clusterName, cr.namespace, 0); err != nil {
		return reconcileAfterInterval, fmt.Errorf("restoring RollingUpdate strategy: %w", err)
	}

	// Scale down
	if err := kubernetes.ScaleStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace, originalTotal); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scaling down after upgrade: %w", err)
	}

	// Wait for correct number of ready pods
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, originalTotal)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking readiness after upgrade scale down: %w", err)
	}
	if !ready {
		cr.logger.Info("Rolling upgrade: waiting for pods after scale down", "expected", originalTotal)
		return reconcileAfterWaitInterval, nil
	}

	// Run cluster check to verify health
	password := cr.getPassword(ctx, config)
	node0Addr, err := cr.getPodAddr(ctx, 0)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting node 0 for cluster check: %w", err)
	}
	node0Client := redis.NewClient(node0Addr, password)
	defer node0Client.Close()

	checkResult, err := node0Client.ClusterCheck(ctx)
	if err != nil {
		cr.logger.Warn("Rolling upgrade: cluster check failed, will retry", "error", err)
		return reconcileAfterWaitInterval, nil
	}
	if len(checkResult.Errors) > 0 {
		cr.logger.Warn("Rolling upgrade: cluster check reported errors, will retry",
			"errors", checkResult.Errors)
		return reconcileAfterWaitInterval, nil
	}

	// Ensure each primary has exactly its expected replica(s) assigned.
	// Redis auto-migration (cluster-allow-replica-migration=yes, the default) can
	// redistribute replicas during the upgrade, leaving some primaries unprotected.
	if config.Spec.ReplicasPerPrimary > 0 {
		if err := cr.rebalanceReplicas(ctx, config, password); err != nil {
			cr.logger.Warn("Rolling upgrade: replica rebalance failed, will retry", "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	// Upgrade complete!
	cr.logger.Info("Rolling N+1 upgrade completed successfully",
		"image", config.Spec.Image,
		"primaries", config.Spec.Primaries)

	config.Status.Substatus = redisv1.RedkeyClusterSubstatus{}
	config.Status.Status = redisv1.ClusterStatusReady
	if err := cr.client.Status().Update(ctx, config); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating status to Ready after rolling upgrade: %w", err)
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}

	return reconcileAfterInterval, nil
}

// --- Upgrade helper methods ---

// initNodesWithCount initializes a specific number of Redis nodes (for upgrade, which
// may have more nodes than the spec's expected topology due to the extra node).
func (cr *ClusterReconciler) initNodesWithCount(ctx context.Context, count int, password string) ([]*redis.Node, error) {
	clusterCfg := cr.runtimeConfig.ClusterConfig()
	maxRetries := clusterCfg.ConnectionMaxRetries
	backoff := time.Duration(clusterCfg.ConnectionBackOffSeconds) * time.Second

	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return nil, fmt.Errorf("getting pod addresses: %w", err)
	}

	var nodes []*redis.Node
	for i := range count {
		name := fmt.Sprintf("%s-%d", cr.clusterName, i)
		addr, ok := podAddrs[name]
		if !ok {
			closeNodes(nodes)
			return nil, fmt.Errorf("pod %s not found or has no IP", name)
		}
		node := redis.NewNode(name, addr, password)
		if err := node.Init(ctx, maxRetries, backoff); err != nil {
			cr.logger.Error("Failed to initialize node during upgrade", "node", name, "error", err)
			closeNodes(nodes)
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// getPodAddr returns the address (IP:port) of a pod at the given ordinal.
func (cr *ClusterReconciler) getPodAddr(ctx context.Context, ordinal int32) (string, error) {
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return "", fmt.Errorf("getting pod addresses: %w", err)
	}
	name := fmt.Sprintf("%s-%d", cr.clusterName, ordinal)
	addr, ok := podAddrs[name]
	if !ok {
		return "", fmt.Errorf("pod %s not found or has no IP", name)
	}
	return addr, nil
}

// forgetNodeFromAll makes every cluster member (up to nodeCount) forget the specified node.
func (cr *ClusterReconciler) forgetNodeFromAll(ctx context.Context, nodeID, password string, nodeCount int) error {
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return fmt.Errorf("getting pod addresses for forget: %w", err)
	}

	for i := range nodeCount {
		name := fmt.Sprintf("%s-%d", cr.clusterName, i)
		addr, ok := podAddrs[name]
		if !ok {
			continue
		}
		c := redis.NewClient(addr, password)
		myID, _ := c.ClusterMyID(ctx)
		if myID == nodeID {
			c.Close()
			continue
		}
		if err := c.ClusterForget(ctx, nodeID); err != nil {
			// Ignore "Unknown node" errors — node may already be forgotten by this member.
			if !strings.Contains(err.Error(), "Unknown node") {
				c.Close()
				return fmt.Errorf("node %s forgetting %s: %w", name, nodeID, err)
			}
		}
		c.Close()
	}
	return nil
}

// forgetReplicasOfNode forgets all replicas of a given primary node ID from the cluster.
// This removes the replica nodes from cluster topology so they can be safely recycled.
func (cr *ClusterReconciler) forgetReplicasOfNode(ctx context.Context, primaryID, password string, config *redisv1.RedkeyClusterConfig) error {
	// Get cluster topology from any node
	node0Addr, err := cr.getPodAddr(ctx, 0)
	if err != nil {
		return err
	}
	c := redis.NewClient(node0Addr, password)
	defer c.Close()

	nodes, err := c.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("getting cluster nodes to find replicas: %w", err)
	}

	totalPods := int(config.Spec.Primaries+config.Spec.Primaries*config.Spec.ReplicasPerPrimary) + int(upgradeExtraNodes(config))

	for _, node := range nodes {
		if node.Primary == primaryID && strings.Contains(node.Flags, "slave") {
			cr.logger.Info("Rolling upgrade: forgetting replica of source node",
				"replicaID", node.ID, "primaryID", primaryID)
			if err := cr.forgetNodeFromAll(ctx, node.ID, password, totalPods); err != nil {
				if !strings.Contains(err.Error(), "Unknown node") {
					return fmt.Errorf("forgetting replica %s of primary %s: %w", node.ID, primaryID, err)
				}
			}
		}
	}
	return nil
}

// recycleReplicasForPrimary deletes and re-attaches ONLY the replica pods of a specific
// primary that was just drained and recycled. Unlike the old partition-based approach,
// this never touches replicas of other primaries that still hold slots, preserving HA.
func (cr *ClusterReconciler) recycleReplicasForPrimary(ctx context.Context, config *redisv1.RedkeyClusterConfig, primaryOrdinal int, password string) error {
	primaries := int(config.Spec.Primaries)
	replicasPerPrimary := int(config.Spec.ReplicasPerPrimary)

	// Get the primary's current ID (it was just recycled and met back)
	primaryAddr, err := cr.getPodAddr(ctx, int32(primaryOrdinal))
	if err != nil {
		return fmt.Errorf("getting primary %d address: %w", primaryOrdinal, err)
	}
	primaryClient := redis.NewClient(primaryAddr, password)
	primaryID, err := primaryClient.ClusterMyID(ctx)
	if err != nil {
		primaryClient.Close()
		return fmt.Errorf("getting primary %d ID: %w", primaryOrdinal, err)
	}

	expectedImage := config.Spec.Image

	for r := range replicasPerPrimary {
		replicaOrdinal := int32(primaries + primaryOrdinal*replicasPerPrimary + r)
		podName := fmt.Sprintf("%s-%d", cr.clusterName, replicaOrdinal)

		// Delete the replica pod at most ONCE: only when it still runs the OLD image.
		// This handler is re-entered on every retry, so deleting unconditionally would
		// repeatedly destroy the freshly-recreated replica and prevent it from stabilizing.
		actualImage, imgErr := kubernetes.GetPodImage(ctx, cr.client, cr.clusterName, cr.namespace, replicaOrdinal)
		if imgErr != nil {
			// Replica is most likely mid-recreation (not yet present). Signal a retry.
			primaryClient.Close()
			return fmt.Errorf("replica pod %d not available yet: %w", replicaOrdinal, imgErr)
		}
		if actualImage != expectedImage {
			cr.logger.Info("Rolling upgrade: deleting replica pod for recycling",
				"replicaOrdinal", replicaOrdinal,
				"primaryOrdinal", primaryOrdinal)

			// Delete the replica pod — it will be recreated with the new image
			// (OnDelete strategy: new pods always use the current template)
			if err := kubernetes.DeletePod(ctx, cr.client, podName, cr.namespace); err != nil {
				primaryClient.Close()
				return fmt.Errorf("deleting replica pod %d: %w", replicaOrdinal, err)
			}
			primaryClient.Close()
			return fmt.Errorf("replica pod %d deleted, waiting for recreation with new image", replicaOrdinal)
		}

		// Wait for the replica pod to be ready
		ready, err := kubernetes.IsPodOrdinalReady(ctx, cr.client, cr.clusterName, cr.namespace, replicaOrdinal)
		if err != nil {
			primaryClient.Close()
			return fmt.Errorf("checking replica pod %d readiness: %w", replicaOrdinal, err)
		}
		if !ready {
			primaryClient.Close()
			return fmt.Errorf("replica pod %d not ready yet", replicaOrdinal)
		}

		// Meet and replicate
		replicaAddr, err := cr.getPodAddr(ctx, replicaOrdinal)
		if err != nil {
			primaryClient.Close()
			return fmt.Errorf("getting replica %d address: %w", replicaOrdinal, err)
		}
		replicaClient := redis.NewClient(replicaAddr, password)

		// Meet the replica into the cluster
		if err := primaryClient.ClusterMeet(ctx, strings.Split(replicaAddr, ":")[0], redis.DefaultPort); err != nil {
			replicaClient.Close()
			primaryClient.Close()
			return fmt.Errorf("meeting replica %d: %w", replicaOrdinal, err)
		}

		// Short wait for gossip propagation
		time.Sleep(2 * time.Second)

		// Set up replication
		if err := replicaClient.ClusterReplicate(ctx, primaryID); err != nil {
			replicaClient.Close()
			primaryClient.Close()
			return fmt.Errorf("replicating %d to primary %d: %w", replicaOrdinal, primaryOrdinal, err)
		}

		cr.logger.Info("Rolling upgrade: replica recycled and attached",
			"replicaOrdinal", replicaOrdinal,
			"primaryOrdinal", primaryOrdinal)
		replicaClient.Close()
	}
	primaryClient.Close()
	return nil
}

// ensurePivotReplica verifies that the extra primary (pivot) has its expected replica
// attached. If the replica was lost (due to Redis auto-migration, timing issues, or pod
// recreation), it re-meets and re-attaches it.
func (cr *ClusterReconciler) ensurePivotReplica(ctx context.Context, config *redisv1.RedkeyClusterConfig, password string) error {
	primaries := int(config.Spec.Primaries)
	replicasPerPrimary := int(config.Spec.ReplicasPerPrimary)

	// Extra primary is at ordinal: primaries + primaries*replicasPerPrimary
	extraPrimaryOrdinal := int32(primaries + primaries*replicasPerPrimary)
	extraPrimaryAddr, err := cr.getPodAddr(ctx, extraPrimaryOrdinal)
	if err != nil {
		return fmt.Errorf("getting extra primary address: %w", err)
	}
	extraPrimaryClient := redis.NewClient(extraPrimaryAddr, password)
	defer extraPrimaryClient.Close()

	extraPrimaryID, err := extraPrimaryClient.ClusterMyID(ctx)
	if err != nil {
		return fmt.Errorf("getting extra primary ID: %w", err)
	}

	// Check if the extra primary already has its replica(s) attached
	nodes, err := extraPrimaryClient.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("getting cluster nodes from pivot: %w", err)
	}

	existingReplicas := 0
	for _, node := range nodes {
		if node.Primary == extraPrimaryID && strings.Contains(node.Flags, "slave") &&
			!strings.Contains(node.Flags, "fail") {
			existingReplicas++
		}
	}

	if existingReplicas >= replicasPerPrimary {
		return nil // Pivot already has enough replicas
	}

	cr.logger.Info("Rolling upgrade: pivot missing replica(s), re-attaching",
		"existingReplicas", existingReplicas,
		"expected", replicasPerPrimary)

	// Re-attach each extra replica
	for r := range replicasPerPrimary {
		extraReplicaOrdinal := extraPrimaryOrdinal + 1 + int32(r)

		// Verify the extra replica pod is ready
		ready, err := kubernetes.IsPodOrdinalReady(ctx, cr.client, cr.clusterName, cr.namespace, extraReplicaOrdinal)
		if err != nil {
			return fmt.Errorf("checking extra replica pod %d readiness: %w", extraReplicaOrdinal, err)
		}
		if !ready {
			return fmt.Errorf("extra replica pod %d not ready", extraReplicaOrdinal)
		}

		extraReplicaAddr, err := cr.getPodAddr(ctx, extraReplicaOrdinal)
		if err != nil {
			return fmt.Errorf("getting extra replica %d address: %w", extraReplicaOrdinal, err)
		}

		// Meet the replica (idempotent if already known)
		if err := extraPrimaryClient.ClusterMeet(ctx, strings.Split(extraReplicaAddr, ":")[0], redis.DefaultPort); err != nil {
			return fmt.Errorf("meeting extra replica %d: %w", extraReplicaOrdinal, err)
		}

		time.Sleep(2 * time.Second)

		// Force replication to the pivot
		extraReplicaClient := redis.NewClient(extraReplicaAddr, password)
		defer extraReplicaClient.Close()

		if err := extraReplicaClient.ClusterReplicate(ctx, extraPrimaryID); err != nil {
			return fmt.Errorf("replicating extra replica %d to pivot: %w", extraReplicaOrdinal, err)
		}

		cr.logger.Info("Rolling upgrade: pivot replica re-attached",
			"extraReplicaOrdinal", extraReplicaOrdinal,
			"pivotOrdinal", extraPrimaryOrdinal)
	}
	return nil
}

// rebalanceReplicas ensures each original primary has exactly its expected replica(s).
// After the upgrade, Redis auto-migration may have redistributed replicas incorrectly.
// This function forces the correct assignment based on the known ordinal layout:
// primary P → replica at ordinal (primaries + P*replicasPerPrimary + R).
func (cr *ClusterReconciler) rebalanceReplicas(ctx context.Context, config *redisv1.RedkeyClusterConfig, password string) error {
	primaries := int(config.Spec.Primaries)
	replicasPerPrimary := int(config.Spec.ReplicasPerPrimary)

	for p := range primaries {
		primaryAddr, err := cr.getPodAddr(ctx, int32(p))
		if err != nil {
			return fmt.Errorf("getting primary %d address for rebalance: %w", p, err)
		}
		primaryClient := redis.NewClient(primaryAddr, password)

		primaryID, err := primaryClient.ClusterMyID(ctx)
		if err != nil {
			primaryClient.Close()
			return fmt.Errorf("getting primary %d ID for rebalance: %w", p, err)
		}

		for r := range replicasPerPrimary {
			replicaOrdinal := int32(primaries + p*replicasPerPrimary + r)
			replicaAddr, err := cr.getPodAddr(ctx, replicaOrdinal)
			if err != nil {
				primaryClient.Close()
				return fmt.Errorf("getting replica %d address for rebalance: %w", replicaOrdinal, err)
			}
			replicaClient := redis.NewClient(replicaAddr, password)

			// Meet (idempotent) and replicate
			if err := primaryClient.ClusterMeet(ctx, strings.Split(replicaAddr, ":")[0], redis.DefaultPort); err != nil {
				replicaClient.Close()
				primaryClient.Close()
				return fmt.Errorf("meeting replica %d during rebalance: %w", replicaOrdinal, err)
			}

			time.Sleep(2 * time.Second)

			if err := replicaClient.ClusterReplicate(ctx, primaryID); err != nil {
				replicaClient.Close()
				primaryClient.Close()
				return fmt.Errorf("replicating %d to primary %d during rebalance: %w", replicaOrdinal, p, err)
			}

			cr.logger.Info("Rolling upgrade: replica rebalanced",
				"replicaOrdinal", replicaOrdinal,
				"primaryOrdinal", p)
			replicaClient.Close()
		}
		primaryClient.Close()
	}
	return nil
}

// forgetExtraReplicas forgets extra replica nodes that were part of the upgrade scale-up.
func (cr *ClusterReconciler) forgetExtraReplicas(ctx context.Context, config *redisv1.RedkeyClusterConfig, password string) error {
	extraPrimaryOrdinal := int32(config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary)
	allMembers := int(config.Spec.Primaries + config.Spec.Primaries*config.Spec.ReplicasPerPrimary)

	// Get the extra primary's ID
	extraAddr, err := cr.getPodAddr(ctx, extraPrimaryOrdinal)
	if err != nil {
		return err
	}
	extraClient := redis.NewClient(extraAddr, password)
	defer extraClient.Close()

	extraID, err := extraClient.ClusterMyID(ctx)
	if err != nil {
		return err
	}

	// Find and forget replicas of the extra primary
	node0Addr, err := cr.getPodAddr(ctx, 0)
	if err != nil {
		return err
	}
	node0Client := redis.NewClient(node0Addr, password)
	defer node0Client.Close()

	clusterNodes, err := node0Client.GetClusterNodes(ctx)
	if err != nil {
		return err
	}

	for _, node := range clusterNodes {
		if node.Primary == extraID && strings.Contains(node.Flags, "slave") {
			cr.logger.Info("Rolling upgrade ending: forgetting extra replica", "replicaID", node.ID)
			if err := cr.forgetNodeFromAll(ctx, node.ID, password, allMembers); err != nil {
				return fmt.Errorf("forgetting extra replica %s: %w", node.ID, err)
			}
		}
	}
	return nil
}

// forgetFailedNodes finds any nodes in FAIL state (dead nodes from previous recycles)
// and removes them from all cluster members. This is critical for performance: without
// this cleanup, redis-cli --cluster reshard attempts to send SETSLOT to unreachable
// nodes on every slot migration, making each reshard extremely slow.
func (cr *ClusterReconciler) forgetFailedNodes(ctx context.Context, seedClient *redis.Client, password string, config *redisv1.RedkeyClusterConfig) error {
	nodes, err := seedClient.GetClusterNodes(ctx)
	if err != nil {
		return fmt.Errorf("getting cluster nodes to find failed entries: %w", err)
	}

	// Total alive pods: primaries + replicas + extra(s)
	totalPods := int(config.Spec.Primaries+config.Spec.Primaries*config.Spec.ReplicasPerPrimary) + int(upgradeExtraNodes(config))

	for _, node := range nodes {
		if !isConfirmedFail(node.Flags) {
			continue
		}
		// Skip "myself" (should never be "myself,fail" but just in case)
		if strings.Contains(node.Flags, "myself") {
			continue
		}
		// NEVER forget a node that still owns slots — doing so would leave those
		// slots unassigned and corrupt the cluster topology.
		if redis.CountSlotsFromRanges(node.Slots) > 0 {
			cr.logger.Info("Rolling upgrade: skipping forget of failed node that still owns slots",
				"nodeID", node.ID, "flags", node.Flags, "addr", node.Addr, "slots", node.Slots)
			continue
		}
		cr.logger.Info("Rolling upgrade: forgetting dead node",
			"deadNodeID", node.ID, "flags", node.Flags, "addr", node.Addr)
		if err := cr.forgetNodeFromAll(ctx, node.ID, password, totalPods); err != nil {
			// Non-fatal: log and continue with other dead nodes
			cr.logger.Warn("Rolling upgrade: could not forget dead node from all members",
				"deadNodeID", node.ID, "error", err)
		}
	}
	return nil
}

// isConfirmedFail checks whether the node flags indicate a confirmed failure ("fail")
// as opposed to a suspected failure ("fail?" / pfail). Only confirmed failures should
// be forgotten — pfail is transient and may resolve via gossip.
func isConfirmedFail(flags string) bool {
	for _, flag := range strings.Split(flags, ",") {
		if flag == "fail" {
			return true
		}
	}
	return false
}

// stabilizeOpenSlots detects migrating/importing slots on the source and destination
// nodes and clears them with CLUSTER SETSLOT <slot> STABLE. This recovers from a
// previous partial reshard attempt without running a full cluster fix.
func (cr *ClusterReconciler) stabilizeOpenSlots(ctx context.Context, sourceClient, destClient *redis.Client) error {
	stabilized := 0
	for _, client := range []*redis.Client{sourceClient, destClient} {
		nodes, err := client.GetClusterNodes(ctx)
		if err != nil {
			return fmt.Errorf("getting cluster nodes for stabilization: %w", err)
		}
		myID, err := client.ClusterMyID(ctx)
		if err != nil {
			return fmt.Errorf("getting node ID for stabilization: %w", err)
		}
		for _, node := range nodes {
			if node.ID != myID {
				continue
			}
			openSlots := redis.ParseOpenSlots(node.Slots)
			for _, slot := range openSlots {
				if err := client.ClusterSetSlotStable(ctx, slot); err != nil {
					cr.logger.Warn("Failed to stabilize slot", "slot", slot, "error", err)
				} else {
					stabilized++
				}
			}
		}
	}
	if stabilized > 0 {
		cr.logger.Info("Stabilized open slots before reshard", "count", stabilized)
	}
	return nil
}

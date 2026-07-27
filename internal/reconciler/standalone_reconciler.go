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

// reconcileStandalone drives the lifecycle of a standalone (single-node,
// non-clustered) Redis deployment. Unlike the cluster path, it never issues
// CLUSTER commands: cluster formation, slot assignment, replication and
// cluster-level health checks do not apply. The single node is provisioned as a
// 1-replica StatefulSet with cluster-enabled disabled, and readiness is verified
// with a simple connection (PING) check.
func (cr *ClusterReconciler) reconcileStandalone(ctx context.Context, targetConfig, previousConfig *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	switch targetConfig.Status.Status {
	case "":
		if previousConfig == nil {
			// New standalone cluster: handleNew ensures the K8s objects (or marks
			// Applied when primaries==0) and transitions to Initializing. It performs
			// no CLUSTER operations, so it is safe to reuse here.
			return cr.handleNew(ctx, targetConfig)
		}
		return cr.handleStandaloneConfigChange(ctx, targetConfig, previousConfig)
	case redisv1.ClusterStatusInitializing:
		return cr.handleStandaloneInitializing(ctx, targetConfig)
	case redisv1.ClusterStatusConfiguring:
		// Standalone has no cluster-formation step; finish provisioning as if
		// initializing. This branch only triggers if a config was left mid-flight.
		return cr.handleStandaloneInitializing(ctx, targetConfig)
	case redisv1.ClusterStatusReady:
		return cr.handleStandaloneReady(ctx, targetConfig)
	case redisv1.ClusterStatusScalingToZero:
		// Tearing down the single node is identical to the cluster case.
		return cr.handleScalingToZero(ctx, targetConfig)
	case redisv1.ClusterStatusUpgrading:
		return cr.handleStandaloneUpgrading(ctx, targetConfig)
	case redisv1.ClusterStatusMaintenance:
		cr.logger.Info("Standalone cluster in maintenance mode, skipping reconciliation")
		return reconcileAfterInterval, nil
	default:
		cr.logger.Info("Unhandled standalone status, skipping", "status", targetConfig.Status.Status)
		return reconcileAfterInterval, nil
	}
}

// handleStandaloneConfigChange processes a new configuration for an existing
// standalone cluster. Auth changes are hot-applied via CONFIG SET; topology
// changes toggle between zero and one node; any other change re-applies the
// Redis/Kubernetes configuration via the Upgrading status.
func (cr *ClusterReconciler) handleStandaloneConfigChange(ctx context.Context, targetConfig, previousConfig *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	report := DetectChanges(previousConfig.Spec, targetConfig.Spec)

	cr.logger.Info("Detected standalone configuration changes",
		"config", targetConfig.Name,
		"hasRobinChanges", report.HasRobinChanges,
		"hasTopologyChanges", report.HasTopologyChanges,
		"hasKubernetesChanges", report.HasKubernetesChanges,
		"hasRedisConfigChanges", report.HasRedisConfigChanges,
		"hasAuthChanges", report.HasAuthChanges,
	)

	// Apply auth changes first (CONFIG SET on the running node) so subsequent
	// operations connect with the right credentials.
	if report.HasAuthChanges {
		cr.logger.Info("Auth change detected, applying via CONFIG SET", "config", targetConfig.Name)
		if err := cr.applyAuthToAllNodes(ctx, targetConfig, previousConfig); err != nil {
			return reconcileAfterInterval, fmt.Errorf("applying standalone auth config: %w", err)
		}
	}

	// Topology changes: 0 <-> 1 node.
	if report.HasTopologyChanges {
		if targetConfig.Spec.Primaries == 0 {
			cr.logger.Info("Standalone scaling to zero", "config", targetConfig.Name)
			if err := cr.updateClusterStatus(ctx, targetConfig, redisv1.ClusterStatusScalingToZero); err != nil {
				return reconcileAfterInterval, err
			}
			return reconcileImmediately, nil
		}
		// Scaling up from zero: (re)create the single node's K8s objects.
		cr.logger.Info("Standalone scaling up from zero, ensuring objects", "config", targetConfig.Name)
		return cr.ensureStandaloneObjects(ctx, targetConfig)
	}

	// Non-topology Redis/Kubernetes config changes: re-apply via Upgrading.
	if report.HasKubernetesChanges || report.HasRedisConfigChanges {
		if err := cr.updateClusterStatus(ctx, targetConfig, redisv1.ClusterStatusUpgrading); err != nil {
			return reconcileAfterInterval, err
		}
		return reconcileImmediately, nil
	}

	// Only auth and/or Robin changes (already applied / hot-reloaded). Mark Applied
	// and reflect the previous operational status.
	cr.logger.Info("No standalone cluster operation required, marking config as Applied", "config", targetConfig.Name)
	if targetConfig.Status.Status != previousConfig.Status.Status {
		targetConfig.Status.Status = previousConfig.Status.Status
		if err := cr.client.Status().Update(ctx, targetConfig); err != nil {
			return reconcileAfterInterval, fmt.Errorf("updating standalone status from previous: %w", err)
		}
	}
	if err := cr.setConfigPhaseApplied(ctx, targetConfig); err != nil {
		return reconcileAfterInterval, err
	}
	return reconcileAfterInterval, nil
}

// ensureStandaloneObjects (re)creates the single-node Kubernetes objects and
// transitions the cluster to Initializing.
func (cr *ClusterReconciler) ensureStandaloneObjects(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	owner, err := cr.getOwner(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting owner Redkey: %w", err)
	}

	password, err := kubernetes.GetRedisPassword(ctx, cr.client, config.Spec.Auth.SecretName, cr.namespace)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting Redis password: %w", err)
	}
	cr.runtimeConfig.SetAuthSecret(config.Spec.Auth.SecretName)

	if err := kubernetes.EnsureClusterObjects(ctx, cr.client, config, owner, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("ensuring standalone objects: %w", err)
	}

	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusInitializing); err != nil {
		return reconcileAfterInterval, err
	}
	cr.logger.Info("Standalone objects ensured, status set to Initializing")
	return reconcileImmediately, nil
}

// handleStandaloneInitializing waits for the single Redis pod to be ready and
// reachable, then transitions to Ready.
func (cr *ClusterReconciler) handleStandaloneInitializing(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, 1)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking standalone pod readiness: %w", err)
	}
	if !ready {
		cr.logger.Info("Waiting for standalone pod to be ready")
		return reconcileAfterWaitInterval, nil
	}

	password := cr.getPassword(ctx, config)
	addr, ok := cr.standaloneNodeReachable(ctx, password)
	if !ok {
		cr.logger.Info("Standalone node not reachable yet, will retry")
		return reconcileAfterWaitInterval, nil
	}

	if err := cr.updateStandaloneNodeStatus(ctx, config, addr); err != nil {
		cr.logger.Error("Failed to update standalone node status", "error", err)
		// Non-critical, continue.
	}

	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusReady); err != nil {
		return reconcileAfterInterval, err
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}
	cr.logger.Info("Standalone node ready, status set to Ready")
	return reconcileAfterInterval, nil
}

// handleStandaloneReady performs a lightweight health check on the running
// standalone node: it reconciles any out-of-band password rotation and verifies
// the node still responds to PING.
func (cr *ClusterReconciler) handleStandaloneReady(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	password := cr.getPassword(ctx, config)

	// Detect and apply an out-of-band password rotation (Secret edited in place).
	if err := cr.reconcileAuthRotation(ctx, config, password); err != nil {
		cr.logger.Warn("Standalone auth rotation reconciliation failed", "error", err)
	}

	if _, ok := cr.standaloneNodeReachable(ctx, password); !ok {
		cr.logger.Info("Standalone node not reachable, will retry")
		return reconcileAfterWaitInterval, nil
	}
	return reconcileAfterInterval, nil
}

// handleStandaloneUpgrading re-applies the Redis/Kubernetes configuration to the
// single node. Unlike the cluster path there is neither sharding nor HA to
// preserve, so the upgrade is a straightforward single-pod recycle:
//
//  1. The redis.conf ConfigMap and the StatefulSet pod template (image, resources,
//     labels, annotations and config checksum) are updated to the new desired state.
//     UpdateStatefulSetTemplate sets the OnDelete update strategy, so the running pod
//     is NOT recreated automatically.
//  2. The single pod is recycled exactly once by comparing its native
//     controller-revision-hash against the StatefulSet UpdateRevision: while it still
//     runs the outdated template the pod is deleted (and recreated by the StatefulSet
//     with the new template); once it runs the new revision it is left alone.
//  3. When the recycled pod is Ready and reachable (PING) the cluster returns to Ready.
//
// The revision guard makes this handler idempotent across the repeated reconcile
// cycles that occur while waiting for the pod to be recreated.
func (cr *ClusterReconciler) handleStandaloneUpgrading(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	owner, err := cr.getOwner(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting owner Redkey: %w", err)
	}

	password := cr.getPassword(ctx, config)

	// 1. Apply the new redis.conf to the ConfigMap.
	if err := kubernetes.UpdateConfigMap(ctx, cr.client, cr.clusterName, cr.namespace, config, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating ConfigMap during standalone upgrade: %w", err)
	}

	// 2. Update the StatefulSet pod template so the new image/resources/config take
	// effect when the pod is recreated. OnDelete strategy: nothing restarts yet.
	checksum := kubernetes.ComputeConfigChecksum(config.Spec.Image, config.Spec.Version, config.Spec.RedisConfig)
	if err := kubernetes.UpdateStatefulSetTemplate(ctx, cr.client, cr.clusterName, cr.namespace, config, owner, checksum); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating StatefulSet template during standalone upgrade: %w", err)
	}

	// Reconcile Service and PDB so override/PDB-only changes are applied during upgrades.
	if err := kubernetes.ReconcileService(ctx, cr.client, config, owner); err != nil {
		return reconcileAfterInterval, fmt.Errorf("reconciling Service during standalone upgrade: %w", err)
	}
	if err := kubernetes.ReconcilePDB(ctx, cr.client, config, owner); err != nil {
		return reconcileAfterInterval, fmt.Errorf("reconciling PDB during standalone upgrade: %w", err)
	}

	// 3. Recycle the single pod once, guarded by the StatefulSet revision.
	recycled, err := cr.recycleStandalonePod(ctx)
	if err != nil {
		return reconcileAfterInterval, err
	}
	if !recycled {
		return reconcileAfterWaitInterval, nil
	}

	// Pod runs the new template — wait for it to be Ready and reachable.
	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, 1)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking standalone pod readiness during upgrade: %w", err)
	}
	if !ready {
		cr.logger.Info("Waiting for standalone pod to be ready during upgrade")
		return reconcileAfterWaitInterval, nil
	}

	addr, ok := cr.standaloneNodeReachable(ctx, password)
	if !ok {
		cr.logger.Info("Standalone node not reachable during upgrade, will retry")
		return reconcileAfterWaitInterval, nil
	}

	if err := cr.updateStandaloneNodeStatus(ctx, config, addr); err != nil {
		cr.logger.Error("Failed to update standalone node status during upgrade", "error", err)
	}

	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusReady); err != nil {
		return reconcileAfterInterval, err
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}
	cr.logger.Info("Standalone upgrade complete, status set to Ready")
	return reconcileAfterInterval, nil
}

// recycleStandalonePod ensures the single standalone pod runs the StatefulSet's current
// (update) pod template. It first waits until the StatefulSet controller has observed the
// latest template change (Status.ObservedGeneration caught up with metadata.Generation),
// so the compared UpdateRevision is fresh rather than the pre-update value. It then compares
// the pod's native controller-revision-hash against the StatefulSet UpdateRevision and, while
// they differ, deletes the pod exactly once so the OnDelete StatefulSet recreates it with the
// new template. It returns true only once the pod runs the desired revision; false means the
// caller should wait and retry.
func (cr *ClusterReconciler) recycleStandalonePod(ctx context.Context) (bool, error) {
	desiredRevision, observed, err := kubernetes.GetStatefulSetUpdateRevisionObserved(
		ctx, cr.client, cr.clusterName, cr.namespace,
	)
	if err != nil {
		return false, fmt.Errorf("getting StatefulSet update revision during standalone upgrade: %w", err)
	}
	if !observed {
		// The StatefulSet controller has not yet observed the latest pod-template change,
		// so Status.UpdateRevision still points at the previous template. Wait rather than
		// risk a false "already up to date" decision that would skip recycling the pod.
		cr.logger.Info("Standalone upgrade: waiting for StatefulSet to observe the new pod template")
		return false, nil
	}
	if desiredRevision == "" {
		// The StatefulSet controller has not computed the update revision yet. Wait
		// rather than risk a false "already up to date" decision.
		cr.logger.Info("Standalone upgrade: StatefulSet update revision not yet available, waiting")
		return false, nil
	}

	actualRevision, err := kubernetes.GetPodControllerRevisionHash(ctx, cr.client, cr.clusterName, cr.namespace, 0)
	if err != nil {
		// Pod is most likely mid-recreation (not yet present). Wait for it to come back.
		cr.logger.Info("Standalone upgrade: pod not available yet, waiting for recreation", "error", err)
		return false, nil
	}

	if actualRevision != desiredRevision {
		podName := fmt.Sprintf("%s-0", cr.clusterName)
		cr.logger.Info("Standalone upgrade: deleting pod for recycling",
			"pod", podName, "actualRevision", actualRevision, "desiredRevision", desiredRevision)
		if err := kubernetes.DeletePod(ctx, cr.client, podName, cr.namespace); err != nil {
			return false, fmt.Errorf("deleting standalone pod %s for recycle: %w", podName, err)
		}
		cr.logger.Info("Standalone upgrade: waiting for pod to be recreated with new template")
		return false, nil
	}

	return true, nil
}

// standaloneNodeReachable returns the single node's address and whether it
// responds to a connection (PING) check, using the configured retry/backoff.
func (cr *ClusterReconciler) standaloneNodeReachable(ctx context.Context, password string) (string, bool) {
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		cr.logger.Error("Failed to get standalone pod address", "error", err)
		return "", false
	}
	addr, ok := podAddrs[fmt.Sprintf("%s-0", cr.clusterName)]
	if !ok || addr == "" {
		return "", false
	}

	clusterCfg := cr.runtimeConfig.ClusterConfig()
	backoff := time.Duration(clusterCfg.ConnectionBackOffSeconds) * time.Second
	client := redis.NewClient(addr, password)
	defer func() { _ = client.Close() }()
	if err := client.CheckConnection(ctx, clusterCfg.ConnectionMaxRetries, backoff); err != nil {
		cr.logger.Warn("Standalone node connection check failed", "addr", addr, "error", err)
		return "", false
	}
	return addr, true
}

// updateStandaloneNodeStatus records the single node in the config status as a
// primary, without querying CLUSTER metadata.
func (cr *ClusterReconciler) updateStandaloneNodeStatus(ctx context.Context, config *redisv1.RedkeyConfig, addr string) error {
	name := fmt.Sprintf("%s-0", cr.clusterName)
	ip := addr
	if idx := strings.Index(addr, ":"); idx > 0 {
		ip = addr[:idx]
	}
	config.Status.Nodes = map[string]*redisv1.RedisNode{
		name: {Role: "primary", IP: ip},
	}
	return cr.client.Status().Update(ctx, config)
}

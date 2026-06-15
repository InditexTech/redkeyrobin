// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/health"
	"github.com/inditextech/redkeyrobin/internal/kubernetes"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

const (
	// clusterTotalSlots is the total number of hash slots in a Redis cluster.
	clusterTotalSlots = 16384
)

// ClusterReconciler handles the Redis cluster lifecycle state machine.
type ClusterReconciler struct {
	client           client.Client
	clusterName      string
	namespace        string
	runtimeConfig    *config.RuntimeConfig
	logger           *slog.Logger
	healthReconciler *HealthReconciler
}

// NewClusterReconciler creates a new ClusterReconciler.
func NewClusterReconciler(c client.Client, clusterName, namespace string, runtimeConfig *config.RuntimeConfig) *ClusterReconciler {
	logger := slog.Default().With("component", "cluster-reconciler", "cluster", clusterName)

	// Create health checker with redis client factory
	healthClientFactory := func(addr, password string) health.ClusterClient {
		return redis.NewClient(addr, password)
	}
	checker := health.NewChecker(
		logger.With("subcomponent", "health-checker"),
		healthClientFactory,
		5*time.Second,
	)

	// Create health reconciler
	redisClientFactory := func(addr, password string) *redis.Client {
		return redis.NewClient(addr, password)
	}
	healthReconciler := NewHealthReconciler(checker, redisClientFactory, runtimeConfig, logger)

	return &ClusterReconciler{
		client:           c,
		clusterName:      clusterName,
		namespace:        namespace,
		runtimeConfig:    runtimeConfig,
		logger:           logger,
		healthReconciler: healthReconciler,
	}
}

// Close releases resources held by the cluster reconciler, closing all cached
// Redis clients used for health checks. It should be called on shutdown.
func (cr *ClusterReconciler) Close() {
	if cr.healthReconciler != nil {
		cr.healthReconciler.Close()
	}
}

// ReconcileCluster processes the cluster creation/configuration state machine.
// It returns whether the outer reconciliation loop should run immediately again
// or wait for the configured interval.
func (cr *ClusterReconciler) ReconcileCluster(ctx context.Context, targetConfig *redisv1.RedkeyClusterConfig, previousConfig *redisv1.RedkeyClusterConfig) (schedule reconcileSchedule, err error) {
	// Standalone (single-node, non-clustered) deployments follow a simplified
	// lifecycle that never issues CLUSTER commands. Route them to a dedicated
	// reconcile path.
	if targetConfig.Spec.IsStandalone() {
		return cr.reconcileStandalone(ctx, targetConfig, previousConfig)
	}

	switch targetConfig.Status.Status {
	case "":
		if previousConfig == nil {
			// New cluster: ensure K8s objects and set status to Initializing.
			return cr.handleNew(ctx, targetConfig)
		}
		// Existing cluster with a new config: detect changes and determine status transition.
		return cr.handleConfigChange(ctx, targetConfig, previousConfig)
	case redisv1.ClusterStatusInitializing:
		// Waiting for pods to be ready, then init nodes.
		return cr.handleInitializing(ctx, targetConfig)
	case redisv1.ClusterStatusConfiguring:
		// Cluster formation: meet, assign slots, set replicas.
		return cr.handleConfiguring(ctx, targetConfig)
	case redisv1.ClusterStatusReady:
		// Cluster is ready, perform health check.
		return cr.handleReady(ctx, targetConfig, previousConfig)
	case redisv1.ClusterStatusScalingUp:
		// Scaling up: add primaries/replicas and rebalance slots onto the new nodes.
		return cr.handleScalingUp(ctx, targetConfig)
	case redisv1.ClusterStatusScalingDown:
		// Scaling down: drain and remove the highest-ordinal nodes, then shrink the StatefulSet.
		return cr.handleScalingDown(ctx, targetConfig)
	case redisv1.ClusterStatusScalingToZero:
		// Scaling to zero: delete all cluster objects.
		return cr.handleScalingToZero(ctx, targetConfig)
	case redisv1.ClusterStatusUpgrading:
		return cr.handleUpgrading(ctx, targetConfig)
	case redisv1.ClusterStatusMaintenance:
		// In maintenance mode, Robin should not perform any operations on the cluster.
		cr.logger.Info("Cluster in maintenance mode, skipping reconciliation")
		return reconcileAfterInterval, nil
	default:
		cr.logger.Info("Unhandled cluster status, skipping", "status", targetConfig.Status.Status)
		return reconcileAfterInterval, nil
	}
}

// handleConfigChange processes a new configuration for an existing cluster.
// It detects what changed and determines the appropriate status transition.
// If auth changes are detected, they are applied via CONFIG SET to all running
// nodes BEFORE any cluster operation, ensuring all nodes share the same auth
// configuration throughout the upgrade/scaling process.
func (cr *ClusterReconciler) handleConfigChange(ctx context.Context, targetConfig *redisv1.RedkeyClusterConfig, previousConfig *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	report := DetectChanges(previousConfig.Spec, targetConfig.Spec)

	cr.logger.Info("Detected configuration changes",
		"config", targetConfig.Name,
		"hasRobinChanges", report.HasRobinChanges,
		"hasTopologyChanges", report.HasTopologyChanges,
		"hasKubernetesChanges", report.HasKubernetesChanges,
		"hasRedisConfigChanges", report.HasRedisConfigChanges,
		"hasAuthChanges", report.HasAuthChanges,
		"hasPurgeKeysOnRebalanceChange", report.HasPurgeKeysOnRebalanceChange,
		"topologyScaleDirection", report.TopologyScaleDirection,
		"primariesDelta", report.PrimariesDelta,
		"replicasDelta", report.ReplicasDelta,
	)

	// If auth changed, apply it via CONFIG SET to all running nodes first.
	// This ensures all nodes share the same auth before any operation
	// (rolling upgrade, scaling, etc.) that might create or recycle pods.
	if report.HasAuthChanges {
		cr.logger.Info("Auth change detected, applying via CONFIG SET to all nodes",
			"config", targetConfig.Name,
			"secretName", targetConfig.Spec.Auth.SecretName)
		if err := cr.applyAuthToAllNodes(ctx, targetConfig, previousConfig); err != nil {
			return reconcileAfterInterval, fmt.Errorf("applying auth config: %w", err)
		}
	}

	// Determine the status transition.
	newStatus := DetermineStatusTransition(report)
	if newStatus == "" {
		// No cluster operation required (auth-only was already applied via CONFIG SET above).
		cr.logger.Info("No cluster operation required, marking config as Applied",
			"config", targetConfig.Name)
		if err := cr.setConfigPhaseApplied(ctx, targetConfig); err != nil {
			return reconcileAfterInterval, err
		}
		// Ensure the cluster status reflects the previous (Ready) state.
		if targetConfig.Status.Status != previousConfig.Status.Status {
			targetConfig.Status.Status = previousConfig.Status.Status
			if err := cr.client.Status().Update(ctx, targetConfig); err != nil {
				return reconcileAfterInterval, fmt.Errorf("updating cluster status from previous: %w", err)
			}
		}
		return reconcileAfterInterval, nil
	}

	// Transition the cluster to the new status (auth was already applied above).
	cr.logger.Info("Transitioning cluster status for config change",
		"config", targetConfig.Name,
		"newStatus", newStatus,
	)
	if err := cr.updateClusterStatus(ctx, targetConfig, newStatus); err != nil {
		return reconcileAfterInterval, err
	}

	return reconcileImmediately, nil
}

// handleNew ensures Kubernetes objects exist and transitions to Initializing.
func (cr *ClusterReconciler) handleNew(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	// New cluster with 0 primaries: nothing to create, mark Applied immediately.
	if config.Spec.Primaries == 0 {
		cr.logger.Info("New cluster with 0 primaries, marking config as Applied", "config", config.Name)
		config.Status.Status = redisv1.ClusterStatusReady
		config.Status.Nodes = map[string]*redisv1.RedisNode{}
		if err := cr.client.Status().Update(ctx, config); err != nil {
			return reconcileAfterInterval, fmt.Errorf("updating status for zero-primary cluster: %w", err)
		}
		if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
			return reconcileAfterInterval, err
		}
		return reconcileAfterInterval, nil
	}

	cr.logger.Info("New cluster detected, creating Kubernetes objects")

	owner, err := cr.getOwner(ctx)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting owner RedkeyCluster: %w", err)
	}

	password, err := kubernetes.GetRedisPassword(ctx, cr.client, config.Spec.Auth.SecretName, cr.namespace)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting Redis password: %w", err)
	}
	cr.runtimeConfig.SetAuthSecret(config.Spec.Auth.SecretName)

	if err := kubernetes.EnsureClusterObjects(ctx, cr.client, config, owner, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("ensuring cluster objects: %w", err)
	}

	// Transition to Initializing
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusInitializing); err != nil {
		return reconcileAfterInterval, err
	}

	cr.logger.Info("Kubernetes objects created, status set to Initializing")
	return reconcileImmediately, nil
}

// handleInitializing waits for all pods to be ready, then inits nodes.
func (cr *ClusterReconciler) handleInitializing(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	expectedReplicas := config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary)

	ready, err := kubernetes.AllPodsReady(ctx, cr.client, cr.clusterName, cr.namespace, expectedReplicas)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("checking pod readiness: %w", err)
	}

	if !ready {
		cr.logger.Info("Waiting for all pods to be ready",
			"expected", expectedReplicas)
		return reconcileAfterWaitInterval, nil
	}

	cr.logger.Info("All pods ready, initializing nodes")

	// Init nodes: connect, get IDs
	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes: %w", err)
	}
	defer closeNodes(nodes)

	// Verify all nodes responded
	if len(nodes) < int(expectedReplicas) {
		cr.logger.Info("Not all nodes could be initialized, will retry",
			"initialized", len(nodes), "expected", expectedReplicas)
		return reconcileAfterWaitInterval, nil
	}

	// Transition to Configuring
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusConfiguring); err != nil {
		return reconcileAfterInterval, err
	}

	cr.logger.Info("All nodes initialized, status set to Configuring")
	return reconcileImmediately, nil
}

// handleConfiguring performs cluster formation: meet, assign slots, set replicas.
func (cr *ClusterReconciler) handleConfiguring(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	password := cr.getPassword(ctx, config)
	nodes, err := cr.initNodes(ctx, config, password)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("initializing nodes for configuration: %w", err)
	}
	defer closeNodes(nodes)

	expectedTotal := int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	if len(nodes) < expectedTotal {
		cr.logger.Info("Not all nodes available for configuration, will retry",
			"available", len(nodes), "expected", expectedTotal)
		return reconcileAfterWaitInterval, nil
	}

	// Form the cluster from scratch (meet, assign slots, replicas, verify).
	if !cr.formCluster(ctx, config, nodes) {
		return reconcileAfterWaitInterval, nil
	}

	// Success: transition to Ready and set ConfigPhase to Applied
	if err := cr.updateClusterStatus(ctx, config, redisv1.ClusterStatusReady); err != nil {
		return reconcileAfterInterval, err
	}
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, err
	}

	// Update node topology in status
	if err := cr.updateNodeStatus(ctx, config, nodes); err != nil {
		cr.logger.Error("Failed to update node status", "error", err)
		// Non-critical, continue
	}

	cr.logger.Info("Cluster formation complete, status set to Ready")
	return reconcileAfterInterval, nil
}

// formCluster forms a Redis cluster from a freshly initialized set of nodes by meeting
// the nodes, assigning slots to primaries, and attaching replicas. It returns true once
// the cluster has fully converged; false indicates a transient state and the caller
// should retry on the next reconciliation pass.
//
// The nodes slice must be ordered by StatefulSet ordinal: nodes[0:Primaries] become
// primaries and the remaining nodes become replicas. It is shared by the initial
// configuration flow and the fast-scaling path, which both build a cluster from scratch.
func (cr *ClusterReconciler) formCluster(ctx context.Context, config *redisv1.RedkeyClusterConfig, nodes []*redis.Node) bool {
	// Step 1: Meet all nodes
	if err := cr.meetNodes(ctx, nodes); err != nil {
		cr.logger.Error("Failed to meet nodes", "error", err)
		return false
	}

	// Step 2: Assign slots to primaries
	primaries := nodes[:config.Spec.Primaries]
	if err := cr.assignSlots(ctx, primaries); err != nil {
		cr.logger.Error("Failed to assign slots", "error", err)
		return false
	}

	// Step 3: Verify gossip convergence before replication, then attach replicas.
	if config.Spec.ReplicasPerPrimary > 0 {
		converged, err := cr.checkGossipConvergence(ctx, nodes)
		if err != nil {
			cr.logger.Warn("Failed to check gossip convergence", "error", err)
			return false
		}
		if !converged {
			cr.logger.Info("Gossip not yet converged, will retry before setting replicas")
			return false
		}

		replicas := nodes[config.Spec.Primaries:]
		if err := cr.setReplicas(ctx, primaries, replicas, config.Spec.ReplicasPerPrimary); err != nil {
			cr.logger.Warn("Failed to set replicas, will retry", "error", err)
			return false
		}
	}

	// Step 4: Verify cluster state
	clusterOK, err := cr.verifyCluster(ctx, nodes[0])
	if err != nil {
		cr.logger.Error("Failed to verify cluster", "error", err)
		return false
	}
	if !clusterOK {
		cr.logger.Info("Cluster not yet converged, will retry")
		return false
	}

	return true
}

// handleReady performs a health check on the ready cluster.
func (cr *ClusterReconciler) handleReady(ctx context.Context, config *redisv1.RedkeyClusterConfig, previousConfig *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	// Get fresh pod addresses from K8s
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return reconcileAfterInterval, fmt.Errorf("getting pod addresses for health check: %w", err)
	}

	// Build health node list from K8s pod state
	totalNodes := int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	nodes := make([]health.Node, 0, totalNodes)
	for i := range totalNodes {
		name := fmt.Sprintf("%s-%d", cr.clusterName, i)
		addr, ok := podAddrs[name]
		if !ok {
			cr.logger.Warn("Pod not found for health check, skipping", "pod", name)
			continue
		}
		nodes = append(nodes, health.Node{Name: name, Addr: addr})
	}

	if len(nodes) == 0 {
		return reconcileAfterInterval, fmt.Errorf("no pods found for cluster health check")
	}

	// Get Redis password
	password, err := kubernetes.GetRedisPassword(ctx, cr.client, config.Spec.Auth.SecretName, cr.namespace)
	if err != nil {
		cr.logger.Warn("Could not retrieve Redis password for health check", "error", err)
		// Continue without password — it may not be configured
	}

	// Detect an out-of-band password rotation (the auth Secret was edited in
	// place, with no new RedkeyClusterConfig) and apply the new password to all
	// nodes via CONFIG SET before health-checking, so the running nodes accept
	// the rotated credentials instead of failing with WRONGPASS.
	if err := cr.reconcileAuthRotation(ctx, config, password); err != nil {
		return reconcileAfterInterval, fmt.Errorf("reconciling auth rotation: %w", err)
	}

	// Run health reconciliation
	schedule, err := cr.healthReconciler.Reconcile(ctx, nodes, password,
		int(config.Spec.Primaries), int(config.Spec.ReplicasPerPrimary))
	if err != nil {
		return schedule, err
	}

	// If the config is still InProgress and the cluster is healthy, check for remaining changes.
	// This handles the post-scaling scenario: after topology is adjusted, non-topology changes
	// (K8s objects, Redis config) may still need to be applied.
	if config.Status.ConfigPhase == redisv1.ConfigPhaseInProgress && schedule == reconcileAfterInterval {
		if previousConfig != nil {
			report := DetectChanges(previousConfig.Spec, config.Spec)
			// Only consider non-topology changes (topology is already applied at this point).
			if report.HasKubernetesChanges || report.HasRedisConfigChanges {
				newStatus := DetermineStatusTransition(ChangeReport{
					HasKubernetesChanges:  report.HasKubernetesChanges,
					HasRedisConfigChanges: report.HasRedisConfigChanges,
				})
				if newStatus != "" {
					cr.logger.Info("Post-scaling: remaining non-topology changes detected, transitioning",
						"config", config.Name,
						"newStatus", newStatus,
						"hasKubernetesChanges", report.HasKubernetesChanges,
						"hasRedisConfigChanges", report.HasRedisConfigChanges,
					)
					if err := cr.updateClusterStatus(ctx, config, newStatus); err != nil {
						return reconcileAfterInterval, err
					}
					return reconcileImmediately, nil
				}
			}
		}

		// No remaining cluster changes, mark as Applied.
		if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
			return reconcileAfterInterval, err
		}
		cr.logger.Info("Config change verified healthy, marked as Applied", "name", config.Name)
	}

	return schedule, nil
}

// --- Node operations ---

func (cr *ClusterReconciler) initNodes(ctx context.Context, config *redisv1.RedkeyClusterConfig, password string) ([]*redis.Node, error) {
	totalNodes := int(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	clusterCfg := cr.runtimeConfig.ClusterConfig()
	maxRetries := clusterCfg.ConnectionMaxRetries
	backoff := time.Duration(clusterCfg.ConnectionBackOffSeconds) * time.Second

	// Use pod IPs for addressing — works both in-cluster and out-of-cluster (for debugging purposes).
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return nil, fmt.Errorf("getting pod addresses: %w", err)
	}

	var nodes []*redis.Node
	for i := range totalNodes {
		name := fmt.Sprintf("%s-%d", cr.clusterName, i)
		addr, ok := podAddrs[name]
		if !ok {
			closeNodes(nodes)
			return nil, fmt.Errorf("pod %s not found or has no IP", name)
		}
		node := redis.NewNode(name, addr, password)

		if err := node.Init(ctx, maxRetries, backoff); err != nil {
			cr.logger.Error("Failed to initialize node", "node", name, "addr", addr, "error", err)
			closeNodes(nodes)
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (cr *ClusterReconciler) meetNodes(ctx context.Context, nodes []*redis.Node) error {
	// Each node meets every other node.
	for i, node := range nodes {
		for j, other := range nodes {
			if i == j {
				continue
			}
			if err := node.Client().ClusterMeet(ctx, other.IP, redis.DefaultPort); err != nil {
				return fmt.Errorf("node %s meeting %s: %w", node.Name, other.Name, err)
			}
		}
	}
	cr.logger.Info("All nodes introduced to each other", "count", len(nodes))
	return nil
}

// checkGossipConvergence verifies that all nodes see every other node in their
// CLUSTER NODES output. This ensures gossip has fully propagated after CLUSTER MEET
// before attempting operations like CLUSTER REPLICATE that require node ID visibility.
func (cr *ClusterReconciler) checkGossipConvergence(ctx context.Context, nodes []*redis.Node) (bool, error) {
	expectedIDs := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		expectedIDs[n.ID] = struct{}{}
	}

	for _, node := range nodes {
		clusterNodes, err := node.Client().GetClusterNodes(ctx)
		if err != nil {
			return false, fmt.Errorf("getting cluster nodes from %s: %w", node.Name, err)
		}

		visibleIDs := make(map[string]struct{}, len(clusterNodes))
		for _, cn := range clusterNodes {
			visibleIDs[cn.ID] = struct{}{}
		}

		for id := range expectedIDs {
			if _, ok := visibleIDs[id]; !ok {
				cr.logger.Info("Node does not yet see all peers",
					"node", node.Name, "missingNodeID", id)
				return false, nil
			}
		}
	}

	cr.logger.Info("Gossip fully converged, all nodes visible to each other")
	return true, nil
}

func (cr *ClusterReconciler) assignSlots(ctx context.Context, primaries []*redis.Node) error {
	numPrimaries := len(primaries)
	slotsPerNode := clusterTotalSlots / numPrimaries
	remainder := clusterTotalSlots % numPrimaries

	slotStart := 0
	for i, primary := range primaries {
		count := slotsPerNode
		if i < remainder {
			count++
		}
		slots := make([]int, count)
		for s := range count {
			slots[s] = slotStart + s
		}
		slotStart += count

		// Check if the node already has slots assigned
		if err := primary.RefreshInfo(ctx); err != nil {
			return fmt.Errorf("refreshing %s info: %w", primary.Name, err)
		}
		if primary.Slots != "" {
			cr.logger.Info("Node already has slots, skipping assignment",
				"node", primary.Name, "slots", primary.Slots)
			continue
		}

		if err := primary.Client().ClusterAddSlots(ctx, slots...); err != nil {
			return fmt.Errorf("assigning slots to %s: %w", primary.Name, err)
		}
		cr.logger.Info("Assigned slots to node",
			"node", primary.Name, "start", slots[0], "end", slots[len(slots)-1])
	}
	return nil
}

func (cr *ClusterReconciler) setReplicas(ctx context.Context, primaries, replicas []*redis.Node, replicasPerPrimary int32) error {
	for i, replica := range replicas {
		primaryIdx := i / int(replicasPerPrimary)
		if primaryIdx >= len(primaries) {
			primaryIdx = len(primaries) - 1
		}
		primary := primaries[primaryIdx]

		// Check if already replicating
		if err := replica.RefreshInfo(ctx); err != nil {
			return fmt.Errorf("refreshing %s info: %w", replica.Name, err)
		}
		if replica.IsReplica() && replica.PrimaryID == primary.ID {
			cr.logger.Info("Node already replicating correct primary, skipping",
				"replica", replica.Name, "primary", primary.Name)
			continue
		}

		if err := replica.Client().ClusterReplicate(ctx, primary.ID); err != nil {
			return fmt.Errorf("setting %s as replica of %s: %w", replica.Name, primary.Name, err)
		}
		cr.logger.Info("Set replica",
			"replica", replica.Name, "primary", primary.Name)
	}
	return nil
}

func (cr *ClusterReconciler) verifyCluster(ctx context.Context, seedNode *redis.Node) (bool, error) {
	info, err := seedNode.Client().GetClusterInfo(ctx)
	if err != nil {
		return false, err
	}

	if info.State != "ok" {
		cr.logger.Info("Cluster state not ok", "state", info.State)
		return false, nil
	}
	if info.SlotsOK != clusterTotalSlots {
		cr.logger.Info("Not all slots covered",
			"slotsOK", info.SlotsOK, "expected", clusterTotalSlots)
		return false, nil
	}
	return true, nil
}

// --- Status updates ---

func (cr *ClusterReconciler) updateClusterStatus(ctx context.Context, config *redisv1.RedkeyClusterConfig, status string) error {
	config.Status.Status = status
	if err := cr.client.Status().Update(ctx, config); err != nil {
		return fmt.Errorf("updating cluster status to %s: %w", status, err)
	}
	return nil
}

// updateSubstatus sets the informational substatus field to indicate the current phase
// within a scaling operation. This is purely observational — it does not affect control flow.
func (cr *ClusterReconciler) updateSubstatus(ctx context.Context, config *redisv1.RedkeyClusterConfig, substatus string) {
	if config.Status.Substatus.Status == substatus {
		return
	}
	config.Status.Substatus.Status = substatus
	if err := cr.client.Status().Update(ctx, config); err != nil {
		cr.logger.Warn("Failed to update substatus (non-critical)", "substatus", substatus, "error", err)
	}
}

func (cr *ClusterReconciler) setConfigPhaseApplied(ctx context.Context, config *redisv1.RedkeyClusterConfig) error {
	config.Status.ConfigPhase = redisv1.ConfigPhaseApplied
	if err := cr.client.Status().Update(ctx, config); err != nil {
		return fmt.Errorf("setting ConfigPhase to Applied: %w", err)
	}
	return nil
}

func (cr *ClusterReconciler) updateNodeStatus(ctx context.Context, config *redisv1.RedkeyClusterConfig, nodes []*redis.Node) error {
	nodeStatus := make(map[string]*redisv1.RedisNode)
	for _, node := range nodes {
		role := "primary"
		if node.IsReplica() {
			role = "replica"
		}
		nodeStatus[node.Name] = &redisv1.RedisNode{
			Role: role,
			IP:   node.IP,
		}
	}
	config.Status.Nodes = nodeStatus
	return cr.client.Status().Update(ctx, config)
}

// --- Helpers ---

func (cr *ClusterReconciler) getOwner(ctx context.Context) (*redisv1.RedkeyCluster, error) {
	cluster := &redisv1.RedkeyCluster{}
	if err := cr.client.Get(ctx, types.NamespacedName{Name: cr.clusterName, Namespace: cr.namespace}, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (cr *ClusterReconciler) getPassword(ctx context.Context, config *redisv1.RedkeyClusterConfig) string {
	password, err := kubernetes.GetRedisPassword(ctx, cr.client, config.Spec.Auth.SecretName, cr.namespace)
	if err != nil {
		cr.logger.Error("Failed to read auth secret, proceeding without password", "error", err)
		return ""
	}
	return password
}

// applyAuthToAllNodes applies the target configuration's auth settings to ALL
// running Redis nodes via CONFIG SET requirepass / CONFIG SET masterauth,
// then updates the ConfigMap so future pod restarts pick up the correct auth.
//
// This must be called BEFORE any cluster operation (rolling upgrade, scaling)
// that could create or recycle pods, ensuring all nodes share the same auth
// throughout the process and avoiding mixed-auth communication failures.
func (cr *ClusterReconciler) applyAuthToAllNodes(ctx context.Context, config, previousConfig *redisv1.RedkeyClusterConfig) error {
	newPassword := cr.getPassword(ctx, config)

	// Use the previous config's credentials to connect to existing nodes.
	// If the previous config had no auth (empty secret), this returns ""
	// which means "connect without password" — correct for that case.
	oldPassword := cr.getPassword(ctx, previousConfig)

	if err := cr.applyAuthCredentials(ctx, config, oldPassword, newPassword); err != nil {
		return err
	}

	cr.logger.Info("Auth configuration applied to all nodes and ConfigMap updated",
		"secretName", config.Spec.Auth.SecretName)
	return nil
}

// reconcileAuthRotation detects and applies an out-of-band password rotation.
//
// A rotation happens when the auth Secret referenced by the cluster is edited in
// place: the SecretName does not change, so the operator generates no new
// RedkeyClusterConfig and handleConfigChange/applyAuthToAllNodes never runs. The
// running Redis nodes therefore keep the old requirepass while the Secret (and
// every component that reads it) already holds the new password, which surfaces
// as WRONGPASS errors during health checks.
//
// The cluster's ConfigMap (redis.conf requirepass) is the source of truth for the
// password the running nodes actually use: it is what they were started with and
// only Robin changes it. Comparing it against the Secret detects a rotation
// robustly — without any in-memory state, so it survives Robin restarts and has
// no start-up race. When they differ, Robin connects to every node with the
// ConfigMap password and applies the Secret's password via CONFIG SET, then
// updates the ConfigMap so the two converge.
//
// currentPassword is the password just read from the Secret.
func (cr *ClusterReconciler) reconcileAuthRotation(ctx context.Context, config *redisv1.RedkeyClusterConfig, currentPassword string) error {
	nodePassword, found, err := kubernetes.GetConfigMapPassword(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return fmt.Errorf("reading current node password from ConfigMap: %w", err)
	}

	// No ConfigMap yet (cluster not fully provisioned) or already in sync: nothing to do.
	if !found || nodePassword == currentPassword {
		return nil
	}

	cr.logger.Info("Password rotation detected (auth Secret differs from ConfigMap), applying to all nodes",
		"secretName", config.Spec.Auth.SecretName)

	if err := cr.applyAuthCredentials(ctx, config, nodePassword, currentPassword); err != nil {
		return fmt.Errorf("applying rotated auth: %w", err)
	}

	cr.logger.Info("Password rotation applied to all nodes",
		"secretName", config.Spec.Auth.SecretName)
	return nil
}

// applyAuthCredentials applies newPassword to ALL running Redis nodes via
// CONFIG SET requirepass / CONFIG SET masterauth, connecting to each node with
// oldPassword, then updates the ConfigMap so future pod restarts pick up the
// correct auth and refreshes the runtime auth secret so other components
// (metrics, health) use it.
func (cr *ClusterReconciler) applyAuthCredentials(ctx context.Context, config *redisv1.RedkeyClusterConfig, oldPassword, newPassword string) error {
	podAddrs, err := kubernetes.GetPodAddresses(ctx, cr.client, cr.clusterName, cr.namespace)
	if err != nil {
		return fmt.Errorf("getting pod addresses for auth config: %w", err)
	}

	for name, addr := range podAddrs {
		client := redis.NewClient(addr, oldPassword)

		if err := client.ConfigSet(ctx, "requirepass", newPassword); err != nil {
			_ = client.Close()
			return fmt.Errorf("CONFIG SET requirepass on %s: %w", name, err)
		}
		if err := client.ConfigSet(ctx, "masterauth", newPassword); err != nil {
			_ = client.Close()
			return fmt.Errorf("CONFIG SET masterauth on %s: %w", name, err)
		}
		_ = client.Close()

		cr.logger.Info("Applied auth configuration via CONFIG SET",
			"node", name, "hasAuth", newPassword != "")
	}

	// Update the ConfigMap so recycled/new pods start with the correct auth.
	// This also keeps the ConfigMap in sync with the Secret, which is what
	// reconcileAuthRotation compares against to detect future rotations.
	if err := kubernetes.UpdateConfigMap(ctx, cr.client, cr.clusterName, cr.namespace, config, newPassword); err != nil {
		return fmt.Errorf("updating ConfigMap with new auth: %w", err)
	}

	// Update the runtime auth secret so other components (metrics, health)
	// pick up the change immediately.
	cr.runtimeConfig.SetAuthSecret(config.Spec.Auth.SecretName)

	return nil
}

func closeNodes(nodes []*redis.Node) {
	for _, n := range nodes {
		_ = n.Close()
	}
}

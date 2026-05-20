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
	"github.com/inditextech/redkeyrobin/internal/kubernetes"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

const (
	// clusterTotalSlots is the total number of hash slots in a Redis cluster.
	clusterTotalSlots = 16384
)

// ClusterReconciler handles the Redis cluster lifecycle state machine.
type ClusterReconciler struct {
	client        client.Client
	clusterName   string
	namespace     string
	runtimeConfig *config.RuntimeConfig
	logger        *slog.Logger
}

// NewClusterReconciler creates a new ClusterReconciler.
func NewClusterReconciler(c client.Client, clusterName, namespace string, runtimeConfig *config.RuntimeConfig) *ClusterReconciler {
	return &ClusterReconciler{
		client:        c,
		clusterName:   clusterName,
		namespace:     namespace,
		runtimeConfig: runtimeConfig,
		logger:        slog.Default().With("component", "cluster-reconciler", "cluster", clusterName),
	}
}

// ReconcileCluster processes the cluster creation/configuration state machine.
// It returns whether the outer reconciliation loop should run immediately again
// or wait for the configured interval.
func (cr *ClusterReconciler) ReconcileCluster(ctx context.Context, config *redisv1.RedkeyClusterConfig) (schedule reconcileSchedule, err error) {
	switch config.Status.Status {
	case "":
		// New cluster: ensure K8s objects and set status to Initializing.
		return cr.handleNew(ctx, config)
	case redisv1.ClusterStatusInitializing:
		// Waiting for pods to be ready, then init nodes.
		return cr.handleInitializing(ctx, config)
	case redisv1.ClusterStatusConfiguring:
		// Cluster formation: meet, assign slots, set replicas.
		return cr.handleConfiguring(ctx, config)
	case redisv1.ClusterStatusReady:
		// Cluster is ready, perform health check.
		return cr.handleReady(ctx, config)
	default:
		cr.logger.Info("Unhandled cluster status, skipping", "status", config.Status.Status)
		return reconcileAfterInterval, nil
	}
}

// handleNew ensures Kubernetes objects exist and transitions to Initializing.
func (cr *ClusterReconciler) handleNew(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
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

	// Step 1: Meet all nodes
	if err := cr.meetNodes(ctx, nodes); err != nil {
		cr.logger.Error("Failed to meet nodes", "error", err)
		return reconcileAfterWaitInterval, nil
	}

	// Step 2: Assign slots to primaries
	primaries := nodes[:config.Spec.Primaries]
	if err := cr.assignSlots(ctx, primaries); err != nil {
		cr.logger.Error("Failed to assign slots", "error", err)
		return reconcileAfterWaitInterval, nil
	}

	// Step 3: Set replicas
	if config.Spec.ReplicasPerPrimary > 0 {
		replicas := nodes[config.Spec.Primaries:]
		if err := cr.setReplicas(ctx, primaries, replicas, config.Spec.ReplicasPerPrimary); err != nil {
			cr.logger.Error("Failed to set replicas", "error", err)
			return reconcileAfterWaitInterval, nil
		}
	}

	// Step 4: Verify cluster state
	clusterOK, err := cr.verifyCluster(ctx, nodes[0])
	if err != nil {
		cr.logger.Error("Failed to verify cluster", "error", err)
		return reconcileAfterWaitInterval, nil
	}
	if !clusterOK {
		cr.logger.Info("Cluster not yet converged, will retry")
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

// handleReady performs a health check on the ready cluster.
func (cr *ClusterReconciler) handleReady(ctx context.Context, config *redisv1.RedkeyClusterConfig) (reconcileSchedule, error) {
	// Health checks will be performed here in the future using the health.Checker.
	// For now, this is a no-op placeholder.
	return reconcileAfterInterval, nil
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

func closeNodes(nodes []*redis.Node) {
	for _, n := range nodes {
		_ = n.Close()
	}
}

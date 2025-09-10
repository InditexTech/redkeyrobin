// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

const (
	Initializing                  = "Initializing"
	Configuring                   = "Configuring"
	Ready                         = "Ready"
	Error                         = "Error"
	Upgrading                     = "Upgrading"
	UpgradingError                = "UpgradingError"
	ScalingDown                   = "ScalingDown"
	ScalingDownError              = "ScalingDownError"
	ScalingUp                     = "ScalingUp"
	ScalingUpError                = "ScalingUpError"
	Maintenance                   = "Maintenance"
	Unknown                       = "Unknown"
	Resharding                    = "Resharding"
	ReshardingError               = "ReshardingError"
	Rebalancing                   = "Rebalancing"
	RebalancingError              = "RebalancingError"
	Fixing                        = "Fixing"
	FixingError                   = "FixingError"
	CheckingIntegrity             = "CheckingIntegrity"
	CheckingIntegrityError        = "CheckingIntegrityError"
	Resetting                     = "Resetting"
	ResettingError                = "ResettingError"
	RedKeyClusterTotalSlots       = 16384
	RedisNodesUnbalancedThreshold = 2
)

type clusterGetter interface {
	// GetNodes returns the nodes of the cluster.
	GetNodes() []*redis.RedisNode
	// GetNode returns the node with the provided name.
	GetNode(name string) *redis.RedisNode
	// GetRedKeyClusterStatus returns the status of the RedKey cluster.
	GetRedKeyClusterStatus() string
	// GetReplicas returns the number of replicas of the cluster.
	GetReplicas() int
	// GetReplicasPerMaster returns the number of replicas per master of the cluster.
	GetReplicasPerMaster() int
	// GetStatus returns the status of the cluster.
	GetStatus() string
	// GetReconcilerInterval returns the interval of the cluster reconciler.
	GetReconcilerInterval() int
	// GetName returns the name of the cluster.
	GetName() string
	// GetAddress returns the address of the cluster.
	GetAddress() string
	// GetNamespace returns the namespace of the cluster.
	GetNamespace() string
	// GetMetadata returns the metadata of the cluster.
	GetMetadata() map[string]string
	// GetClusterMaxRetries returns the maximum number of retries for a RedKey Cluster check connection operation
	GetClusterMaxRetries() int
	// GetClusterBackOff returns the backoff duration for a RedKey Cluster check connection operation
	GetClusterBackOff() time.Duration
	// GetClusterMeetWaitTime returns the cluster meet wait time for a RedKey cluster
	GetClusterMeetWaitTime() time.Duration
	// GetNodeResetWaitTime returns the node reset wait time for a RedKey cluster
	GetNodeResetWaitTime() time.Duration
	// GetMetricsRedisInfoKeys returns the Redis info keys to be collected
	GetMetricsRedisInfoKeys() []string
	// GetMetricsInterval returns the interval for collecting Redis metrics
	GetMetricsInterval() int
}

type clusterSetter interface {
	// SetRedKeyClusterStatus sets the status of the RedKey cluster.
	SetRedKeyClusterStatus(status string) error
	// SetReplicas sets the number of replicas of the cluster.
	SetReplicas(replicas int, replicasPerMaster *int) error
	// SetStatus sets the status of the cluster.
	SetStatus(status string) error
}

type clusterAsker interface {
	// IsStandalone returns true if the cluster is standalone.
	IsStandalone() bool
	// IsBalanced returns true if the cluster is balanced.
	IsBalanced() bool
	// IsScaled returns true if the cluster is scaled.
	IsScaled() bool
	// IsUpgraded returns true if the cluster is upgraded.
	IsUpgraded() bool
	// IsEphemeral returns true if the cluster is ephemeral.
	IsEphemeral() bool
	// CanBeUpgraded returns true if the cluster can be upgraded.
	CanBeUpgraded() bool
}

type clusterPrivate interface {
	// ensureNodesAreUp ensures that all nodes are up.
	ensureNodesAreUp(ctx context.Context) error
	// convertNodesToMaster converts the provided nodes to master.
	convertNodesToMaster(ctx context.Context, nodes []*redis.RedisNode) error
	// getAndCheckRedisClient returns a Redis client and checks the connection.
	getAndCheckRedisClient(close bool) (redis.RedisClientInterface, error)
	// refreshNodes refreshes the nodes of the cluster.
	refreshNodes() error
	// checkNodes checks the nodes of the cluster.
	checkNodes() error
	// removeOutdatedNodes removes outdated nodes from the cluster.
	removeOutdatedNodes(ctx context.Context) error
	// meetNodesIfNeeded meets nodes if needed.
	meetNodesIfNeeded(ctx context.Context) error
	// ensureClusterRatio ensures the cluster ratio.
	ensureClusterRatio(ctx context.Context) error
	// assignMissingSlotsIfNeeded assigns missing slots if needed.
	assignMissingSlotsIfNeeded(ctx context.Context) error
	// fixClusterIfNeeded fixes the cluster if needed.
	fixClusterIfNeeded(ctx context.Context) error
	// balanceClusterIfNeeded balances the cluster if needed.
	balanceClusterIfNeeded(weights map[string]int) error
	// forgetNode forgets a node from the cluster.
	forgetNode(ctx context.Context, node redis.RedisNode) error
	// removeNodesIfNeeded removes nodes if needed.
	removeNodesIfNeeded(ctx context.Context) error
	// addNewNodesIfNeeded adds new nodes if needed.
	addNewNodesIfNeeded() error
}

// Cluster represents a cluster, either standalone or Redis.
type Cluster interface {
	clusterGetter
	clusterSetter
	clusterAsker
	clusterPrivate
	// Init initializes the cluster.
	Init() error
	// Check checks the cluster.
	Check() (*redis.ClusterCheckResult, error)
	// Fix fixes the cluster.
	Fix(async bool, force bool) error
	// Rebalance rebalances the cluster.
	Rebalance(async bool, weights map[string]int, force bool) error
	// MoveSlots returns the slots of the cluster.
	MoveSlots(from, to *redis.RedisNode, slots int) error
	// CheckIntegrity checks the integrity of the cluster.
	CheckIntegrity(async, force bool) error
	// ScaleUp scales up the cluster.
	ScaleUp(force bool) error
	// ScaleDown scales down the cluster.
	ScaleDown(force bool) error
	// Upgrade upgrades the cluster.
	Upgrade(force bool) error
	// ResetNode resets a node of the cluster.
	ResetNode(node *redis.RedisNode) error
}

// NewCluster creates a new cluster. It returns a standalone or RedKey Cluster based on the cluster type.
func NewCluster(ctx context.Context, conf *config.Configuration, channel chan struct{}) Cluster {
	if conf.Redis.Standalone {
		return NewRedKeyStandalone(ctx, conf)
	} else {
		return NewRedKeyCluster(ctx, conf, channel)
	}
}

// clusterBase represents the base of a cluster.
type clusterBase struct {
	ctx    context.Context
	logger *slog.Logger
	conf   *config.Configuration
	status string
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- GETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// GetRedKeyClusterStatus returns the status of the RedKey Cluster from Operator perspective
func (rc *clusterBase) GetRedKeyClusterStatus() string {
	return rc.conf.Redis.Cluster.Status
}

// GetStatus returns the status of the RedKey Cluster from Robin perspective
func (rc *clusterBase) GetStatus() string {
	return rc.status
}

// GetReplicas returns the number of replicas in the RedKey cluster
func (rc *clusterBase) GetReplicas() int {
	return rc.conf.Redis.Cluster.Replicas
}

// GetReplicasPerMaster returns the number of replicas per master in the RedKey cluster
func (rc *clusterBase) GetReplicasPerMaster() int {
	return rc.conf.Redis.Cluster.ReplicasPerMaster
}

// GetDesiredReplicas returns the number of nodes needed to reach the desired number of replicas
func (rc *clusterBase) GetDesiredReplicas() int {
	return rc.GetReplicas() + (rc.GetReplicas() * rc.GetReplicasPerMaster())
}

// GetName returns the name of the RedKey cluster
func (rc *clusterBase) GetName() string {
	return rc.conf.Redis.Cluster.Name
}

// GetNamespace returns the namespace of the RedKey cluster
func (rc *clusterBase) GetNamespace() string {
	return rc.conf.Redis.Cluster.Namespace
}

// GetAddress returns the address of the RedKey cluster
func (rc *clusterBase) GetAddress() string {
	return rc.conf.Redis.Cluster.Name
}

// GetReconcilerInterval returns the interval of the RedKey Cluster reconciler
func (rc *clusterBase) GetReconcilerInterval() int {
	return rc.conf.Redis.Reconciler.IntervalSeconds
}

// GetReconcilerOperationCleanupInterval returns the interval for cleaning up old operations
func (rc *clusterBase) GetReconcilerOperationCleanupInterval() int {
	return rc.conf.Redis.Reconciler.OperationCleanupIntervalSeconds
}

// GetClusterMaxRetries returns the maximum number of retries for a RedKey Cluster check connection operation
func (rc *clusterBase) GetClusterMaxRetries() int {
	return rc.conf.Redis.Cluster.MaxRetries
}

// GetClusterBackOff returns the backoff duration for a RedKey Cluster check connection operation
func (rc *clusterBase) GetClusterBackOff() time.Duration {
	return rc.conf.Redis.Cluster.BackOff
}

// GetClusterHealingTime returns the healing time for a RedKey cluster
func (rc *clusterBase) GetClusterHealingTime() int {
	return rc.conf.Redis.Cluster.HealingTimeSeconds
}

// GetClusterHealthProbePeriod returns the health probe period for a RedKey cluster
func (rc *clusterBase) GetClusterHealthProbePeriod() int {
	return rc.conf.Redis.Cluster.HealthProbePeriodSeconds
}

// GetClusterMeetWaitTime returns the cluster meet wait time for a RedKey cluster
func (rc *clusterBase) GetClusterMeetWaitTime() time.Duration {
	return time.Duration(rc.conf.Redis.Cluster.ClusterMeetWaitTimeSeconds) * time.Second
}

// GetNodeResetWaitTime returns the node reset wait time for a RedKey cluster
func (rc *clusterBase) GetNodeResetWaitTime() time.Duration {
	return time.Duration(rc.conf.Redis.Cluster.NodeResetWaitTimeSeconds) * time.Second
}

// GetMetricsRedisInfoKeys returns the Redis info keys to be collected
func (rc *clusterBase) GetMetricsRedisInfoKeys() []string {
	return rc.conf.Redis.Metrics.RedisInfoKeys
}

// GetMetricsInterval returns the interval for collecting Redis metrics
func (rc *clusterBase) GetMetricsInterval() int {
	return rc.conf.Redis.Metrics.IntervalSeconds
}

// GetMetadata returns the metadata of the RedKey cluster
func (rc *clusterBase) GetMetadata() map[string]string {
	return rc.conf.Metadata
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- SETTERS ---------------------------------------------
// ----------------------------------------------------------------------------------------------------

// SetStatus sets the status of the RedKey Cluster from Robin perspective
func (rc *clusterBase) SetStatus(status string) error {
	rc.status = status
	return nil
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- ASKERS ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// IsEphemeral returns true if the RedKey Cluster is ephemeral
func (rc *clusterBase) IsEphemeral() bool {
	return rc.conf.Redis.Cluster.Ephemeral
}

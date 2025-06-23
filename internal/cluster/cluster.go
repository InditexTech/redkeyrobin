// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/redis"
)

const (
	Initializing                  = "Initializing"
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
	RedisClusterTotalSlots        = 16384
	RedisNodesUnbalancedThreshold = 2
)

type clusterGetter interface {
	// GetNodes returns the nodes of the cluster.
	GetNodes() []*redis.RedisNode
	// GetNode returns the node with the provided name.
	GetNode(name string) *redis.RedisNode
	// GetRedisClusterStatus returns the status of the Redis cluster.
	GetRedisClusterStatus() string
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
	// GetClusterMaxRetries returns the maximum number of retries for a Redis cluster check connection operation
	GetClusterMaxRetries() int
	// GetClusterBackOff returns the backoff duration for a Redis cluster check connection operation
	GetClusterBackOff() time.Duration
	// GetMetricsRedisInfoKeys returns the Redis info keys to be collected
	GetMetricsRedisInfoKeys() []string
	// GetMetricsInterval returns the interval for collecting Redis metrics
	GetMetricsInterval() int
}

type clusterSetter interface {
	// SetRedisClusterStatus sets the status of the Redis cluster.
	SetRedisClusterStatus(status string) error
	// SetReplicas sets the number of replicas of the cluster.
	SetReplicas(replicas int, replicasPerMaster *int) error
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
	// CanBeUpgraded returns true if the cluster can be upgraded.
	CanBeUpgraded() bool
}

// Cluster represents a cluster, either standalone or Redis.
type Cluster interface {
	clusterGetter
	clusterSetter
	clusterAsker
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

// NewCluster creates a new cluster. It returns a standalone or Redis cluster based on the cluster type.
func NewCluster(ctx context.Context, conf *config.Configuration, channel chan struct{}) Cluster {
	if conf.Redis.Standalone {
		return NewRedisStandalone(ctx, conf)
	} else {
		return NewRedisCluster(ctx, conf, channel)
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

// GetRedisClusterStatus returns the status of the Redis cluster from Operator perspective
func (rc *clusterBase) GetRedisClusterStatus() string {
	return rc.conf.Redis.Cluster.Status
}

// GetStatus returns the status of the Redis cluster from Robin perspective
func (rc *clusterBase) GetStatus() string {
	return rc.status
}

// GetReplicas returns the number of replicas in the Redis cluster
func (rc *clusterBase) GetReplicas() int {
	return rc.conf.Redis.Cluster.Replicas
}

// GetReplicasPerMaster returns the number of replicas per master in the Redis cluster
func (rc *clusterBase) GetReplicasPerMaster() int {
	return rc.conf.Redis.Cluster.ReplicasPerMaster
}

// GetDesiredReplicas returns the number of nodes needed to reach the desired number of replicas
func (rc *clusterBase) GetDesiredReplicas() int {
	return rc.GetReplicas() + (rc.GetReplicas() * rc.GetReplicasPerMaster())
}

// GetName returns the name of the Redis cluster
func (rc *clusterBase) GetName() string {
	return rc.conf.Redis.Cluster.Name
}

// GetNamespace returns the namespace of the Redis cluster
func (rc *clusterBase) GetNamespace() string {
	return rc.conf.Redis.Cluster.Namespace
}

// GetAddress returns the address of the Redis cluster
func (rc *clusterBase) GetAddress() string {
	return rc.conf.Redis.Cluster.Name
}

// GetReconcilerInterval returns the interval of the Redis cluster reconciler
func (rc *clusterBase) GetReconcilerInterval() int {
	return rc.conf.Redis.Reconciler.IntervalSeconds
}

// GetReconcilerOperationCleanupInterval returns the interval for cleaning up old operations
func (rc *clusterBase) GetReconcilerOperationCleanupInterval() int {
	return rc.conf.Redis.Reconciler.OperationCleanupIntervalSeconds
}

// GetClusterMaxRetries returns the maximum number of retries for a Redis cluster check connection operation
func (rc *clusterBase) GetClusterMaxRetries() int {
	return rc.conf.Redis.Cluster.MaxRetries
}

// GetClusterBackOff returns the backoff duration for a Redis cluster check connection operation
func (rc *clusterBase) GetClusterBackOff() time.Duration {
	return rc.conf.Redis.Cluster.BackOff
}

// GetClusterHealingTime returns the healing time for a Redis cluster
func (rc *clusterBase) GetClusterHealingTime() int {
	return rc.conf.Redis.Cluster.HealingTimeSeconds
}

// GetClusterHealthProbePeriod returns the health probe period for a Redis cluster
func (rc *clusterBase) GetClusterHealthProbePeriod() int {
	return rc.conf.Redis.Cluster.HealthProbePeriodSeconds
}

// GetMetricsRedisInfoKeys returns the Redis info keys to be collected
func (rc *clusterBase) GetMetricsRedisInfoKeys() []string {
	return rc.conf.Redis.Metrics.RedisInfoKeys
}

// GetMetricsInterval returns the interval for collecting Redis metrics
func (rc *clusterBase) GetMetricsInterval() int {
	return rc.conf.Redis.Metrics.IntervalSeconds
}

// GetMetadata returns the metadata of the Redis cluster
func (rc *clusterBase) GetMetadata() map[string]string {
	return rc.conf.Metadata
}

// ----------------------------------------------------------------------------------------------------
// ---------------------------------------------- ASKERS ----------------------------------------------
// ----------------------------------------------------------------------------------------------------

// IsEphemeral returns true if the Redis cluster is ephemeral
func (rc *clusterBase) IsEphemeral() bool {
	return rc.conf.Redis.Cluster.Ephemeral
}

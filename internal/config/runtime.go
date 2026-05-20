// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"sync"
	"time"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

const (
	// DefaultReconcilerIntervalSeconds is the default reconciliation loop interval.
	DefaultReconcilerIntervalSeconds = 30
	// DefaultReconcilerIntervalOnErrorSeconds is the default reconciliation loop interval after errors.
	DefaultReconcilerIntervalOnErrorSeconds = 10
	// DefaultReconcilerIntervalOnWaitSeconds is the default reconciliation loop interval while waiting.
	DefaultReconcilerIntervalOnWaitSeconds = 10
	// DefaultMetricsIntervalSeconds is the default metrics collection interval.
	DefaultMetricsIntervalSeconds = 60
	// DefaultConnectionMaxRetries is the default number of connection retries.
	DefaultConnectionMaxRetries = 10
	// DefaultConnectionBackOffSeconds is the default backoff between connection retries.
	DefaultConnectionBackOffSeconds = 10
)

// DefaultRedisInfoKeys is the default list of Redis INFO keys to collect.
var DefaultRedisInfoKeys = []string{
	"keyspace_hits",
	"evicted_keys",
	"connected_clients",
	"total_commands_processed",
	"keyspace_misses",
	"expired_keys",
	"redis_version",
	"used_memory_rss",
	"maxmemory",
	"used_cpu_sys",
	"used_cpu_sys_children",
	"used_cpu_user",
	"used_cpu_user_children",
	"total_net_input_bytes",
	"total_net_output_bytes",
	"aof_base_size",
	"aof_current_size",
	"mem_aof_buffer",
}

// ClusterConfig holds the cluster connection configuration.
type ClusterConfig struct {
	ConnectionMaxRetries     int
	ConnectionBackOffSeconds int
}

// Topology holds the cluster topology parameters needed for node discovery.
type Topology struct {
	Primaries          int32
	ReplicasPerPrimary int32
}

// RuntimeConfig holds the active Robin operational configuration.
// It is safe for concurrent access by the reconciler (writer) and the metrics
// collector (reader).
type RuntimeConfig struct {
	mu                               sync.RWMutex
	bootstrapReconcilerInterval      time.Duration
	bootstrapReconcilerIntervalError time.Duration
	bootstrapReconcilerIntervalWait  time.Duration
	reconcilerInterval               time.Duration
	reconcilerIntervalOnError        time.Duration
	reconcilerIntervalOnWait         time.Duration
	metricsInterval                  time.Duration
	redisInfoKeys                    []string
	metricsLabels                    map[string]string
	connectionMaxRetries             int
	connectionBackOffSeconds         int
	topology                         Topology
	authSecret                       string
}

// NewRuntimeConfig creates a RuntimeConfig with default values.
func NewRuntimeConfig() *RuntimeConfig {
	return NewRuntimeConfigWithReconcilerIntervals(
		time.Duration(DefaultReconcilerIntervalSeconds)*time.Second,
		time.Duration(DefaultReconcilerIntervalOnErrorSeconds)*time.Second,
		time.Duration(DefaultReconcilerIntervalOnWaitSeconds)*time.Second,
	)
}

// NewRuntimeConfigWithReconcilerIntervals creates a RuntimeConfig using the
// provided reconciliation intervals as bootstrap defaults.
func NewRuntimeConfigWithReconcilerIntervals(interval, intervalOnError, intervalOnWait time.Duration) *RuntimeConfig {
	return &RuntimeConfig{
		bootstrapReconcilerInterval:      interval,
		bootstrapReconcilerIntervalError: intervalOnError,
		bootstrapReconcilerIntervalWait:  intervalOnWait,
		reconcilerInterval:               interval,
		reconcilerIntervalOnError:        intervalOnError,
		reconcilerIntervalOnWait:         intervalOnWait,
		metricsInterval:                  time.Duration(DefaultMetricsIntervalSeconds) * time.Second,
		redisInfoKeys:                    append([]string{}, DefaultRedisInfoKeys...),
		connectionMaxRetries:             DefaultConnectionMaxRetries,
		connectionBackOffSeconds:         DefaultConnectionBackOffSeconds,
	}
}

// SetFromRobinConfig updates the runtime configuration from a RobinConfig obtained
// from a RedkeyClusterConfig resource. Reconciler intervals fall back to their
// bootstrap defaults when omitted from the CR.
func (rc *RuntimeConfig) SetFromRobinConfig(cfg *redisv1.RobinConfig) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.reconcilerInterval = rc.bootstrapReconcilerInterval
	rc.reconcilerIntervalOnError = rc.bootstrapReconcilerIntervalError
	rc.reconcilerIntervalOnWait = rc.bootstrapReconcilerIntervalWait

	if cfg == nil {
		return
	}

	if cfg.Reconciler != nil {
		if cfg.Reconciler.IntervalSeconds != nil {
			rc.reconcilerInterval = time.Duration(*cfg.Reconciler.IntervalSeconds) * time.Second
		}
		if cfg.Reconciler.IntervalOnErrorSeconds != nil {
			rc.reconcilerIntervalOnError = time.Duration(*cfg.Reconciler.IntervalOnErrorSeconds) * time.Second
		}
		if cfg.Reconciler.IntervalOnWaitSeconds != nil {
			rc.reconcilerIntervalOnWait = time.Duration(*cfg.Reconciler.IntervalOnWaitSeconds) * time.Second
		}
	}
	if cfg.Metrics != nil {
		if cfg.Metrics.CollectionIntervalSeconds != nil {
			rc.metricsInterval = time.Duration(*cfg.Metrics.CollectionIntervalSeconds) * time.Second
		}
		if cfg.Metrics.RedisInfoKeys != nil {
			rc.redisInfoKeys = append([]string{}, cfg.Metrics.RedisInfoKeys...)
		}
		if cfg.Metrics.MetricsLabels != nil {
			labels := make(map[string]string, len(cfg.Metrics.MetricsLabels))
			for k, v := range cfg.Metrics.MetricsLabels {
				labels[k] = v
			}
			rc.metricsLabels = labels
		}
	}
	if cfg.Cluster != nil {
		if cfg.Cluster.ConnectionMaxRetries != nil {
			rc.connectionMaxRetries = *cfg.Cluster.ConnectionMaxRetries
		}
		if cfg.Cluster.ConnectionBackOffSeconds != nil {
			rc.connectionBackOffSeconds = *cfg.Cluster.ConnectionBackOffSeconds
		}
	}
}

// ReconcilerInterval returns the current reconciliation interval.
func (rc *RuntimeConfig) ReconcilerInterval() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.reconcilerInterval
}

// ReconcilerIntervalOnError returns the current reconciliation interval after errors.
func (rc *RuntimeConfig) ReconcilerIntervalOnError() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.reconcilerIntervalOnError
}

// ReconcilerIntervalOnWait returns the current reconciliation interval while waiting.
func (rc *RuntimeConfig) ReconcilerIntervalOnWait() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.reconcilerIntervalOnWait
}

// MetricsInterval returns the current metrics collection interval.
func (rc *RuntimeConfig) MetricsInterval() time.Duration {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.metricsInterval
}

// RedisInfoKeys returns a copy of the current Redis INFO keys to collect.
func (rc *RuntimeConfig) RedisInfoKeys() []string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return append([]string{}, rc.redisInfoKeys...)
}

// ClusterConfig returns the current cluster connection configuration.
func (rc *RuntimeConfig) ClusterConfig() ClusterConfig {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return ClusterConfig{
		ConnectionMaxRetries:     rc.connectionMaxRetries,
		ConnectionBackOffSeconds: rc.connectionBackOffSeconds,
	}
}

// SetTopology updates the stored topology used for node discovery.
func (rc *RuntimeConfig) SetTopology(primaries, replicasPerPrimary int32) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.topology = Topology{
		Primaries:          primaries,
		ReplicasPerPrimary: replicasPerPrimary,
	}
}

// AppliedTopology returns the currently stored topology.
func (rc *RuntimeConfig) AppliedTopology() Topology {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.topology
}

// SetAuthSecret updates the auth secret name.
func (rc *RuntimeConfig) SetAuthSecret(name string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.authSecret = name
}

// AuthSecret returns the current auth secret name. Empty means no auth.
func (rc *RuntimeConfig) AuthSecret() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.authSecret
}

// MetricsLabels returns a copy of the current metadata labels for metrics.
func (rc *RuntimeConfig) MetricsLabels() map[string]string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	if rc.metricsLabels == nil {
		return nil
	}
	labels := make(map[string]string, len(rc.metricsLabels))
	for k, v := range rc.metricsLabels {
		labels[k] = v
	}
	return labels
}

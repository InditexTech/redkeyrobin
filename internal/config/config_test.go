// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigurationString(t *testing.T) {
	cfg := &Configuration{
		Metadata: map[string]string{
			"key1": "value1",
		},
		Redis: RedisConfig{
			Reconciler: RedisReconcilerConfig{
				IntervalSeconds: 10,
			},
			Cluster: RedKeyClusterConfig{
				HealthProbePeriodSeconds: 20,
				HealingTimeSeconds:       30,
			},
			Metrics: RedisMetricsConfig{
				IntervalSeconds: 40,
				RedisInfoKeys:   []string{"key1", "key2"},
			},
		},
	}

	expected := `Configuration properties:
Metadata: {key1: value1, }
ReconcilerIntervalSeconds: 10
ClusterHealthProbePeriodSeconds: 20
ClusterHealingTimeSeconds: 30
RedisInfoKeys: [key1 key2]
RedisMetricsIntervalSeconds: 40`

	assert.Equal(t, expected, cfg.String())
}

func TestConfigurationValidate(t *testing.T) {
	cfg := &Configuration{}
	missing := cfg.validate()

	assert.Len(t, missing, 9)
	assert.Contains(t, missing, "metadata")
	assert.Contains(t, missing, "redis.reconciler.interval_seconds")
	assert.Contains(t, missing, "redis.cluster.namespace")
	assert.Contains(t, missing, "redis.cluster.name")
	assert.Contains(t, missing, "redis.cluster.replicas")
	assert.Contains(t, missing, "redis.cluster.status")
	assert.Contains(t, missing, "redis.cluster.health_probe_interval_seconds")
	assert.Contains(t, missing, "redis.cluster.healing_time_seconds")
	assert.Contains(t, missing, "redis.metrics.interval_seconds")
}

func TestDefaultValuesInitialization(t *testing.T) {
	cfg := &Configuration{}
	cfg.validate()

	assert.Equal(t, 60, cfg.Redis.Reconciler.OperationCleanupIntervalSeconds)
	assert.Equal(t, 3, cfg.Redis.Reconciler.StabilizeSlotsReconciliationThreshold)
	assert.Equal(t, []string{"used_memory", "connected_clients", "total_commands_processed", "instantaneous_ops_per_sec"}, cfg.Redis.Metrics.RedisInfoKeys)
	assert.Equal(t, 5, cfg.Redis.Cluster.ClusterMeetWaitTimeSeconds)
}

// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"sync"
	"testing"
	"time"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

func intPtr(v int) *int { return &v }

func TestNewRuntimeConfig_Defaults(t *testing.T) {
	rc := NewRuntimeConfig()

	if rc.ReconcilerInterval() != 30*time.Second {
		t.Fatalf("expected reconciler interval 30s, got %v", rc.ReconcilerInterval())
	}
	if rc.MetricsInterval() != 60*time.Second {
		t.Fatalf("expected metrics interval 60s, got %v", rc.MetricsInterval())
	}
	keys := rc.RedisInfoKeys()
	if len(keys) != len(DefaultRedisInfoKeys) {
		t.Fatalf("expected %d redis info keys, got %d", len(DefaultRedisInfoKeys), len(keys))
	}
	cc := rc.ClusterConfig()
	if cc.ConnectionMaxRetries != 10 {
		t.Fatalf("expected connectionMaxRetries 10, got %d", cc.ConnectionMaxRetries)
	}
	if cc.ConnectionBackOffSeconds != 10 {
		t.Fatalf("expected connectionBackOffSeconds 10, got %d", cc.ConnectionBackOffSeconds)
	}
}

func TestSetFromRobinConfig_Nil(t *testing.T) {
	rc := NewRuntimeConfig()
	rc.SetFromRobinConfig(nil)
	// Should not panic, values should remain defaults.
	if rc.ReconcilerInterval() != 30*time.Second {
		t.Fatalf("expected default interval after nil config")
	}
}

func TestSetFromRobinConfig_AllFields(t *testing.T) {
	rc := NewRuntimeConfig()
	cfg := &redisv1.RobinConfig{
		Reconciler: &redisv1.RobinConfigReconciler{
			IntervalSeconds: intPtr(5),
		},
		Cluster: &redisv1.RobinConfigCluster{
			ConnectionMaxRetries:     intPtr(3),
			ConnectionBackOffSeconds: intPtr(2),
		},
		Metrics: &redisv1.RobinConfigMetrics{
			CollectionIntervalSeconds: intPtr(15),
			RedisInfoKeys:             []string{"used_memory", "connected_clients"},
		},
	}

	rc.SetFromRobinConfig(cfg)

	if rc.ReconcilerInterval() != 5*time.Second {
		t.Fatalf("expected reconciler interval 5s, got %v", rc.ReconcilerInterval())
	}
	if rc.MetricsInterval() != 15*time.Second {
		t.Fatalf("expected metrics interval 15s, got %v", rc.MetricsInterval())
	}
	keys := rc.RedisInfoKeys()
	if len(keys) != 2 || keys[0] != "used_memory" || keys[1] != "connected_clients" {
		t.Fatalf("expected [used_memory connected_clients], got %v", keys)
	}
	cc := rc.ClusterConfig()
	if cc.ConnectionMaxRetries != 3 {
		t.Fatalf("expected connectionMaxRetries 3, got %d", cc.ConnectionMaxRetries)
	}
	if cc.ConnectionBackOffSeconds != 2 {
		t.Fatalf("expected connectionBackOffSeconds 2, got %d", cc.ConnectionBackOffSeconds)
	}
}

func TestSetFromRobinConfig_PartialFields(t *testing.T) {
	rc := NewRuntimeConfig()
	// Only set reconciler interval, leave the rest untouched.
	cfg := &redisv1.RobinConfig{
		Reconciler: &redisv1.RobinConfigReconciler{
			IntervalSeconds: intPtr(10),
		},
	}

	rc.SetFromRobinConfig(cfg)

	if rc.ReconcilerInterval() != 10*time.Second {
		t.Fatalf("expected reconciler interval 10s, got %v", rc.ReconcilerInterval())
	}
	// Metrics should retain default.
	if rc.MetricsInterval() != 60*time.Second {
		t.Fatalf("expected default metrics interval, got %v", rc.MetricsInterval())
	}
}

func TestSetTopology(t *testing.T) {
	rc := NewRuntimeConfig()
	rc.SetTopology(3, 1)

	topo := rc.AppliedTopology()
	if topo.Primaries != 3 || topo.ReplicasPerPrimary != 1 {
		t.Fatalf("expected topology 3/1, got %d/%d", topo.Primaries, topo.ReplicasPerPrimary)
	}
}

func TestRedisInfoKeys_ReturnsCopy(t *testing.T) {
	rc := NewRuntimeConfig()
	keys := rc.RedisInfoKeys()
	keys[0] = "modified"

	// Original should not be modified.
	original := rc.RedisInfoKeys()
	if original[0] == "modified" {
		t.Fatal("RedisInfoKeys returned a reference instead of a copy")
	}
}

func TestSetAuthSecret(t *testing.T) {
	rc := NewRuntimeConfig()

	// Default should be empty.
	if rc.AuthSecret() != "" {
		t.Fatalf("expected empty auth secret, got %q", rc.AuthSecret())
	}

	// Set a secret name.
	rc.SetAuthSecret("my-redis-secret")
	if rc.AuthSecret() != "my-redis-secret" {
		t.Fatalf("expected 'my-redis-secret', got %q", rc.AuthSecret())
	}

	// Change to a different secret.
	rc.SetAuthSecret("new-secret")
	if rc.AuthSecret() != "new-secret" {
		t.Fatalf("expected 'new-secret', got %q", rc.AuthSecret())
	}

	// Clear the secret (disable auth).
	rc.SetAuthSecret("")
	if rc.AuthSecret() != "" {
		t.Fatalf("expected empty after clear, got %q", rc.AuthSecret())
	}
}

func TestSetFromRobinConfig_DoesNotAffectAuthSecret(t *testing.T) {
	rc := NewRuntimeConfig()
	rc.SetAuthSecret("keep-this")

	// SetFromRobinConfig should not touch authSecret.
	cfg := &redisv1.RobinConfig{
		Reconciler: &redisv1.RobinConfigReconciler{
			IntervalSeconds: intPtr(5),
		},
	}
	rc.SetFromRobinConfig(cfg)

	if rc.AuthSecret() != "keep-this" {
		t.Fatalf("expected auth secret unchanged, got %q", rc.AuthSecret())
	}
}

func TestConcurrentAccess(t *testing.T) {
	rc := NewRuntimeConfig()
	var wg sync.WaitGroup

	// Writer goroutine for RobinConfig.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			cfg := &redisv1.RobinConfig{
				Reconciler: &redisv1.RobinConfigReconciler{
					IntervalSeconds: intPtr(i + 1),
				},
			}
			rc.SetFromRobinConfig(cfg)
		}
	}()

	// Writer goroutine for auth secret.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			if i%2 == 0 {
				rc.SetAuthSecret("secret-a")
			} else {
				rc.SetAuthSecret("secret-b")
			}
		}
	}()

	// Writer goroutine for topology.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			rc.SetTopology(int32(i%5+1), int32(i%3))
		}
	}()

	// Reader goroutines.
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = rc.ReconcilerInterval()
				_ = rc.MetricsInterval()
				_ = rc.RedisInfoKeys()
				_ = rc.ClusterConfig()
				_ = rc.AppliedTopology()
				_ = rc.AuthSecret()
				_ = rc.MetricsLabels()
			}
		}()
	}

	wg.Wait()
}

func TestMetricsLabels_Default(t *testing.T) {
	rc := NewRuntimeConfig()
	labels := rc.MetricsLabels()
	if labels != nil {
		t.Fatalf("expected nil default metricsLabels, got %v", labels)
	}
}

func TestMetricsLabels_SetFromRobinConfig(t *testing.T) {
	rc := NewRuntimeConfig()
	cfg := &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			MetricsLabels: map[string]string{
				"tenant":      "global",
				"environment": "production",
				"domain":      "commerce",
			},
		},
	}

	rc.SetFromRobinConfig(cfg)

	labels := rc.MetricsLabels()
	if len(labels) != 3 {
		t.Fatalf("expected 3 labels, got %d", len(labels))
	}
	if labels["tenant"] != "global" {
		t.Fatalf("expected tenant=global, got %s", labels["tenant"])
	}
	if labels["environment"] != "production" {
		t.Fatalf("expected environment=production, got %s", labels["environment"])
	}
	if labels["domain"] != "commerce" {
		t.Fatalf("expected domain=commerce, got %s", labels["domain"])
	}
}

func TestMetricsLabels_ReturnsCopy(t *testing.T) {
	rc := NewRuntimeConfig()
	cfg := &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			MetricsLabels: map[string]string{"key": "value"},
		},
	}
	rc.SetFromRobinConfig(cfg)

	labels := rc.MetricsLabels()
	labels["key"] = "modified"

	// Original should not be modified.
	original := rc.MetricsLabels()
	if original["key"] == "modified" {
		t.Fatal("MetricsLabels returned a reference instead of a copy")
	}
}

func TestMetricsLabels_NilDoesNotOverwrite(t *testing.T) {
	rc := NewRuntimeConfig()

	// Set initial labels.
	cfg1 := &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			MetricsLabels: map[string]string{"tenant": "global"},
		},
	}
	rc.SetFromRobinConfig(cfg1)

	// Apply config without MetricsLabels (nil) — should not clear.
	cfg2 := &redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			CollectionIntervalSeconds: intPtr(30),
		},
	}
	rc.SetFromRobinConfig(cfg2)

	labels := rc.MetricsLabels()
	if labels["tenant"] != "global" {
		t.Fatalf("expected labels preserved when nil, got %v", labels)
	}
}

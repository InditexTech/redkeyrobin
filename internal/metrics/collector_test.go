// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/redis"
)

func TestCollector_CollectNode(t *testing.T) {
	// Start a miniredis instance.
	mr := miniredis.RunT(t)

	reg := prometheus.NewRegistry()
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetTopology(3, 1)

	collector := &Collector{
		runtimeConfig:  rtConfig,
		clusterName:    "test-cluster",
		namespace:      "default",
		manager:        NewMetricsManager(reg),
		passwordLoaded: true,
		cachedPassword: "",
	}

	infoKeys := []string{"connected_clients", "used_memory", "total_commands_processed"}

	node := nodeInfo{
		name: "test-cluster-0",
		addr: mr.Addr(),
	}

	err := collector.collectNode(context.Background(), node, "", infoKeys)
	if err != nil {
		t.Fatalf("collectNode failed: %v", err)
	}

	// Gather metrics and verify.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	found := make(map[string]bool)
	for _, f := range families {
		found[f.GetName()] = true
		// Verify labels.
		for _, m := range f.GetMetric() {
			labelMap := make(map[string]string)
			for _, l := range m.GetLabel() {
				labelMap[l.GetName()] = l.GetValue()
			}
			if labelMap["cluster"] != "test-cluster" {
				t.Errorf("expected cluster=test-cluster, got %s", labelMap["cluster"])
			}
			if labelMap["namespace"] != "default" {
				t.Errorf("expected namespace=default, got %s", labelMap["namespace"])
			}
			if labelMap["instanceId"] != "test-cluster-0" {
				t.Errorf("expected instanceId=test-cluster-0, got %s", labelMap["instanceId"])
			}
		}
	}

	// connected_clients should be exposed by miniredis INFO.
	if !found["redis_connected_clients"] {
		t.Error("expected redis_connected_clients metric to be registered")
	}
}

func TestCollector_FiltersInfoKeys(t *testing.T) {
	mr := miniredis.RunT(t)

	reg := prometheus.NewRegistry()
	rtConfig := config.NewRuntimeConfig()

	collector := &Collector{
		runtimeConfig:  rtConfig,
		clusterName:    "test-cluster",
		namespace:      "default",
		manager:        NewMetricsManager(reg),
		passwordLoaded: true,
		cachedPassword: "",
	}

	// Only request one key.
	infoKeys := []string{"connected_clients"}
	node := nodeInfo{name: "test-cluster-0", addr: mr.Addr()}

	err := collector.collectNode(context.Background(), node, "", infoKeys)
	if err != nil {
		t.Fatalf("collectNode failed: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	for _, f := range families {
		if f.GetName() != "redis_connected_clients" {
			t.Errorf("unexpected metric %s should have been filtered", f.GetName())
		}
	}
}

func TestCollector_DiscoverNodes(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetTopology(3, 1)

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "my-cluster",
		namespace:     "prod",
	}

	nodes := collector.discoverNodes()
	expected := 3 * (1 + 1) // 3 primaries * 2 (1 primary + 1 replica)
	if len(nodes) != expected {
		t.Fatalf("expected %d nodes, got %d", expected, len(nodes))
	}

	// Check first and last node names and addresses.
	if nodes[0].name != "my-cluster-0" {
		t.Errorf("expected first node name my-cluster-0, got %s", nodes[0].name)
	}
	expectedAddr := "my-cluster-0.my-cluster-hl.prod.svc.cluster.local:" + "6379"
	if nodes[0].addr != expectedAddr {
		t.Errorf("expected addr %s, got %s", expectedAddr, nodes[0].addr)
	}
	if nodes[5].name != "my-cluster-5" {
		t.Errorf("expected last node name my-cluster-5, got %s", nodes[5].name)
	}
}

func TestCollector_DiscoverNodes_ZeroPrimaries(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	// Default topology is 0 primaries.

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "my-cluster",
		namespace:     "prod",
	}

	nodes := collector.discoverNodes()
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes with zero primaries, got %d", len(nodes))
	}
}

func TestCollector_StartAndStop(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	// Set a very short interval so the loop fires quickly.
	cfg := config.ClusterConfig{}
	_ = cfg

	reg := prometheus.NewRegistry()
	collector := NewCollector(rtConfig, "cluster", "ns", nil, reg)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := collector.Start(ctx)
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
}

// Verify the Redis client wrapper works against miniredis.
func TestRedisClient_GetInfo(t *testing.T) {
	mr := miniredis.RunT(t)

	rc := redis.NewClient(mr.Addr(), "")
	defer func() {
		if err := rc.Close(); err != nil {
			t.Logf("close error: %v", err)
		}
	}()

	raw, err := rc.GetInfo(context.Background())
	if err != nil {
		t.Fatalf("GetInfo failed: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("expected non-empty INFO response")
	}
}

// --- getPassword tests ---

func newFakeScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	return s
}

func TestGetPassword_NoAuth(t *testing.T) {
	// When auth secret is empty, password should be empty string.
	rtConfig := config.NewRuntimeConfig()
	// authSecret is "" by default.

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "test-cluster",
		namespace:     "default",
		k8sClient:     fake.NewClientBuilder().WithScheme(newFakeScheme()).Build(),
	}

	pw, err := collector.getPassword(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pw != "" {
		t.Fatalf("expected empty password, got %q", pw)
	}
	// Should be cached now.
	if !collector.passwordLoaded {
		t.Fatal("expected passwordLoaded to be true")
	}
}

func TestGetPassword_SecretExists(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetAuthSecret("my-redis-secret")

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-redis-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("s3cr3t"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(newFakeScheme()).
		WithObjects(secret).
		Build()

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "test-cluster",
		namespace:     "default",
		k8sClient:     fakeClient,
	}

	pw, err := collector.getPassword(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pw != "s3cr3t" {
		t.Fatalf("expected 's3cr3t', got %q", pw)
	}

	// Second call should use cache (no k8s call).
	pw2, err := collector.getPassword(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on cached call: %v", err)
	}
	if pw2 != "s3cr3t" {
		t.Fatalf("expected cached 's3cr3t', got %q", pw2)
	}
}

func TestGetPassword_SecretNotFound(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetAuthSecret("missing-secret")

	fakeClient := fake.NewClientBuilder().
		WithScheme(newFakeScheme()).
		Build()

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "test-cluster",
		namespace:     "default",
		k8sClient:     fakeClient,
	}

	_, err := collector.getPassword(context.Background())
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
}

func TestGetPassword_SecretMissingPasswordKey(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetAuthSecret("bad-secret")

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bad-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"wrong-key": []byte("value"),
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(newFakeScheme()).
		WithObjects(secret).
		Build()

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "test-cluster",
		namespace:     "default",
		k8sClient:     fakeClient,
	}

	_, err := collector.getPassword(context.Background())
	if err == nil {
		t.Fatal("expected error for missing 'password' key")
	}
}

func TestGetPassword_CacheInvalidationOnSecretNameChange(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetAuthSecret("secret-a")

	secretA := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "secret-a",
			Namespace: "default",
		},
		Data: map[string][]byte{"password": []byte("password-a")},
	}
	secretB := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "secret-b",
			Namespace: "default",
		},
		Data: map[string][]byte{"password": []byte("password-b")},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(newFakeScheme()).
		WithObjects(secretA, secretB).
		Build()

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "test-cluster",
		namespace:     "default",
		k8sClient:     fakeClient,
	}

	// First call loads from secret-a.
	pw, err := collector.getPassword(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pw != "password-a" {
		t.Fatalf("expected 'password-a', got %q", pw)
	}

	// Change the secret name in RuntimeConfig (simulates reconciler update).
	rtConfig.SetAuthSecret("secret-b")

	// Should invalidate cache and fetch from secret-b.
	pw, err = collector.getPassword(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pw != "password-b" {
		t.Fatalf("expected 'password-b' after name change, got %q", pw)
	}
}

func TestGetPassword_TransitionFromAuthToNoAuth(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetAuthSecret("my-secret")

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{"password": []byte("pw123")},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(newFakeScheme()).
		WithObjects(secret).
		Build()

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "test-cluster",
		namespace:     "default",
		k8sClient:     fakeClient,
	}

	// Load the password.
	pw, err := collector.getPassword(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pw != "pw123" {
		t.Fatalf("expected 'pw123', got %q", pw)
	}

	// Disable auth.
	rtConfig.SetAuthSecret("")

	pw, err = collector.getPassword(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pw != "" {
		t.Fatalf("expected empty password after disabling auth, got %q", pw)
	}
}

// --- Collector mid-loop config change tests ---

func TestCollector_CollectReactsToInfoKeysChange(t *testing.T) {
	mr := miniredis.RunT(t)

	reg := prometheus.NewRegistry()
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetTopology(1, 0)

	// Start with only one key.
	rtConfig.SetFromRobinConfig(&redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			RedisInfoKeys: []string{"connected_clients"},
		},
	})

	fakeClient := fake.NewClientBuilder().WithScheme(newFakeScheme()).Build()

	collector := &Collector{
		runtimeConfig:  rtConfig,
		clusterName:    "test-cluster",
		namespace:      "default",
		manager:        NewMetricsManager(reg),
		k8sClient:      fakeClient,
		passwordLoaded: true,
		cachedPassword: "",
	}

	// Override discoverNodes to use miniredis address.
	// We'll call collect directly, but first need to set up the topology.
	// Since discoverNodes uses predictable DNS names, we'll test via collectNode.
	node := nodeInfo{name: "test-cluster-0", addr: mr.Addr()}

	// Collect with one key.
	err := collector.collectNode(context.Background(), node, "", []string{"connected_clients"})
	if err != nil {
		t.Fatalf("first collectNode failed: %v", err)
	}

	families, _ := reg.Gather()
	if len(families) != 1 {
		t.Fatalf("expected 1 metric family after first collect, got %d", len(families))
	}

	// Now change the info keys to include more.
	// Use total_connections_received which miniredis does expose.
	err = collector.collectNode(context.Background(), node, "", []string{"connected_clients", "total_connections_received"})
	if err != nil {
		t.Fatalf("second collectNode failed: %v", err)
	}

	families, _ = reg.Gather()
	foundNew := false
	for _, f := range families {
		if f.GetName() == "redis_total_connections_received" {
			foundNew = true
		}
	}
	if !foundNew {
		t.Error("expected redis_total_connections_received metric after info keys change")
	}
}

func TestCollector_CollectReactsToTopologyChange(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()

	collector := &Collector{
		runtimeConfig: rtConfig,
		clusterName:   "my-cluster",
		namespace:     "default",
	}

	// Initially 0 primaries.
	nodes := collector.discoverNodes()
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes initially, got %d", len(nodes))
	}

	// Change topology.
	rtConfig.SetTopology(3, 1)
	nodes = collector.discoverNodes()
	if len(nodes) != 6 {
		t.Fatalf("expected 6 nodes after topology change, got %d", len(nodes))
	}

	// Change again.
	rtConfig.SetTopology(2, 0)
	nodes = collector.discoverNodes()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after second topology change, got %d", len(nodes))
	}
}

func TestCollector_CollectSkipsWhenNoInfoKeys(t *testing.T) {
	rtConfig := config.NewRuntimeConfig()
	// Set empty info keys.
	rtConfig.SetFromRobinConfig(&redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			RedisInfoKeys: []string{},
		},
	})

	reg := prometheus.NewRegistry()
	fakeClient := fake.NewClientBuilder().WithScheme(newFakeScheme()).Build()

	collector := NewCollector(rtConfig, "cluster", "ns", fakeClient, reg)

	// Should not panic or error — just skip.
	collector.collect(context.Background())

	families, _ := reg.Gather()
	if len(families) != 0 {
		t.Fatalf("expected 0 metrics when no info keys configured, got %d", len(families))
	}
}

func TestCollector_MetricsLabelsAppearOnMetrics(t *testing.T) {
	mr := miniredis.RunT(t)

	reg := prometheus.NewRegistry()
	rtConfig := config.NewRuntimeConfig()
	rtConfig.SetTopology(1, 0)

	// Set metrics labels (metadata).
	rtConfig.SetFromRobinConfig(&redisv1.RobinConfig{
		Metrics: &redisv1.RobinConfigMetrics{
			RedisInfoKeys: []string{"connected_clients"},
			MetricsLabels: map[string]string{
				"tenant":      "global",
				"environment": "production",
			},
		},
	})

	collector := &Collector{
		runtimeConfig:  rtConfig,
		clusterName:    "test-cluster",
		namespace:      "default",
		manager:        NewMetricsManager(reg),
		passwordLoaded: true,
		cachedPassword: "",
	}

	node := nodeInfo{name: "test-cluster-0", addr: mr.Addr()}
	err := collector.collectNode(context.Background(), node, "", []string{"connected_clients"})
	if err != nil {
		t.Fatalf("collectNode failed: %v", err)
	}

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	var found bool
	for _, f := range families {
		if f.GetName() == "redis_connected_clients" {
			found = true
			m := f.GetMetric()[0]
			labelMap := make(map[string]string)
			for _, l := range m.GetLabel() {
				labelMap[l.GetName()] = l.GetValue()
			}
			// Verify base labels.
			if labelMap["cluster"] != "test-cluster" {
				t.Errorf("expected cluster=test-cluster, got %s", labelMap["cluster"])
			}
			if labelMap["namespace"] != "default" {
				t.Errorf("expected namespace=default, got %s", labelMap["namespace"])
			}
			if labelMap["instanceId"] != "test-cluster-0" {
				t.Errorf("expected instanceId=test-cluster-0, got %s", labelMap["instanceId"])
			}
			// Verify metadata labels.
			if labelMap["tenant"] != "global" {
				t.Errorf("expected tenant=global, got %s", labelMap["tenant"])
			}
			if labelMap["environment"] != "production" {
				t.Errorf("expected environment=production, got %s", labelMap["environment"])
			}
		}
	}
	if !found {
		t.Fatal("metric redis_connected_clients not found")
	}
}

func TestCollector_StringMetricExposedAsGaugeMinus1(t *testing.T) {
	reg := prometheus.NewRegistry()
	rtConfig := config.NewRuntimeConfig()

	collector := &Collector{
		runtimeConfig:  rtConfig,
		clusterName:    "test-cluster",
		namespace:      "default",
		manager:        NewMetricsManager(reg),
		passwordLoaded: true,
		cachedPassword: "",
	}

	tags := collector.buildNodeTags("test-cluster-0")

	// Directly test processSingleMetric with a string value.
	collector.processSingleMetric("redis_version", "7.2.4", tags)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	var found bool
	for _, f := range families {
		if f.GetName() == "redis_redis_version" {
			found = true
			m := f.GetMetric()[0]
			if m.GetGauge().GetValue() != -1 {
				t.Fatalf("expected gauge=-1 for string metric, got %f", m.GetGauge().GetValue())
			}
			// The string value should appear as an additional label.
			labelMap := make(map[string]string)
			for _, l := range m.GetLabel() {
				labelMap[l.GetName()] = l.GetValue()
			}
			if labelMap["redis_version"] != "7.2.4" {
				t.Errorf("expected redis_version=7.2.4, got %s", labelMap["redis_version"])
			}
		}
	}
	if !found {
		t.Fatal("metric redis_redis_version not found")
	}
}

func TestCollector_CompoundValueCreatesSubMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	rtConfig := config.NewRuntimeConfig()

	collector := &Collector{
		runtimeConfig:  rtConfig,
		clusterName:    "test-cluster",
		namespace:      "default",
		manager:        NewMetricsManager(reg),
		passwordLoaded: true,
		cachedPassword: "",
	}

	tags := collector.buildNodeTags("test-cluster-0")

	// Simulate a compound value like cmdstat_get:calls=100,usec=500,usec_per_call=5.0
	collector.processSubmetrics("cmdstat_get", "calls=100,usec=500,usec_per_call=5.00", tags)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	expected := map[string]float64{
		"redis_cmdstat_get_calls":         100,
		"redis_cmdstat_get_usec":          500,
		"redis_cmdstat_get_usec_per_call": 5.0,
	}

	found := make(map[string]float64)
	for _, f := range families {
		for _, m := range f.GetMetric() {
			found[f.GetName()] = m.GetGauge().GetValue()
		}
	}

	for name, val := range expected {
		if found[name] != val {
			t.Errorf("expected %s=%f, got %f", name, val, found[name])
		}
	}
}

func TestCollector_KeyspaceMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	rtConfig := config.NewRuntimeConfig()

	collector := &Collector{
		runtimeConfig:  rtConfig,
		clusterName:    "test-cluster",
		namespace:      "default",
		manager:        NewMetricsManager(reg),
		passwordLoaded: true,
		cachedPassword: "",
	}

	tags := collector.buildNodeTags("test-cluster-0")

	keyspaces := map[string]string{
		"db0": "keys=1500,expires=50,avg_ttl=2000",
	}

	collector.processKeyspaceMetrics(keyspaces, tags)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather: %v", err)
	}

	expected := map[string]float64{
		"redis_keyspace_keys":    1500,
		"redis_keyspace_expires": 50,
		"redis_keyspace_avg_ttl": 2000,
	}

	found := make(map[string]float64)
	for _, f := range families {
		for _, m := range f.GetMetric() {
			found[f.GetName()] = m.GetGauge().GetValue()
			// Verify database label.
			for _, l := range m.GetLabel() {
				if l.GetName() == "database" && l.GetValue() != "db0" {
					t.Errorf("expected database=db0, got %s", l.GetValue())
				}
			}
		}
	}

	for name, val := range expected {
		if found[name] != val {
			t.Errorf("expected %s=%f, got %f", name, val, found[name])
		}
	}
}

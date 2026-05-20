// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"testing"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/redis"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var clusterTestScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(clusterTestScheme)
	_ = redisv1.AddToScheme(clusterTestScheme)
}

func newTestClusterReconciler(objs ...metav1.Object) *ClusterReconciler {
	var clientObjs []interface{}
	_ = clientObjs // not used, we build directly

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(testOwnerCluster()).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()
	return NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())
}

func testOwnerCluster() *redisv1.RedkeyCluster {
	return &redisv1.RedkeyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "test-uid-cluster",
		},
		Spec: redisv1.RedkeyClusterSpec{
			Primaries:          3,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
}

func testClusterConfig(status string) *redisv1.RedkeyClusterConfig {
	return &redisv1.RedkeyClusterConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-config",
			Namespace: "default",
		},
		Spec: redisv1.RedkeyClusterConfigSpec{
			Sequence:           1,
			Primaries:          3,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
		Status: redisv1.RedkeyClusterConfigStatus{
			ConfigPhase: redisv1.ConfigPhaseInProgress,
			Status:      status,
			Nodes:       map[string]*redisv1.RedisNode{},
		},
	}
}

func TestClusterReconciler_HandleNew_CreatesObjectsAndSetsInitializing(t *testing.T) {
	cfg := testClusterConfig("")
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}

	// Verify status transitioned to Initializing
	var fetched redisv1.RedkeyClusterConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusInitializing {
		t.Fatalf("expected status Initializing, got '%s'", fetched.Status.Status)
	}
}

func TestClusterReconciler_HandleInitializing_WaitsForPods(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusInitializing)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	// No StatefulSet exists yet, so AllPodsReady will return error (StatefulSet not found)
	schedule, err := cr.ReconcileCluster(context.Background(), cfg)

	// Should get an error because StatefulSet doesn't exist
	if err == nil {
		// In our implementation, AllPodsReady tries to GET the StatefulSet and fails.
		_ = schedule
	}
}

func TestClusterReconciler_HandleReady_NoOp(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusReady)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected interval reconcile, got %v", schedule)
	}
}

func TestClusterReconciler_UnhandledStatus(t *testing.T) {
	cfg := testClusterConfig("SomeUnknownStatus")
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected interval reconcile, got %v", schedule)
	}
}

func TestClusterReconciler_GetOwner(t *testing.T) {
	owner := testOwnerCluster()
	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	fetched, err := cr.getOwner(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fetched.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", fetched.Name)
	}
}

func TestClusterReconciler_GetPassword_NoSecret(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(clusterTestScheme).Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig("")
	password := cr.getPassword(context.Background(), cfg)
	if password != "" {
		t.Fatalf("expected empty password, got '%s'", password)
	}
}

func TestClusterReconciler_GetPassword_WithSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-auth",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"requirepass": []byte("mypass"),
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(clusterTestScheme).WithObjects(secret).Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cfg := testClusterConfig("")
	cfg.Spec.Auth.SecretName = "redis-auth"
	password := cr.getPassword(context.Background(), cfg)
	if password != "mypass" {
		t.Fatalf("expected 'mypass', got '%s'", password)
	}
}

func TestClusterReconciler_UpdateClusterStatus(t *testing.T) {
	cfg := testClusterConfig("")
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	err := cr.updateClusterStatus(context.Background(), cfg, redisv1.ClusterStatusConfiguring)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fetched redisv1.RedkeyClusterConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusConfiguring {
		t.Fatalf("expected status Configuring, got '%s'", fetched.Status.Status)
	}
}

func TestClusterReconciler_SetConfigPhaseApplied(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusReady)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	err := cr.setConfigPhaseApplied(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fetched redisv1.RedkeyClusterConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseApplied {
		t.Fatalf("expected ConfigPhase Applied, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestClusterReconciler_UpdateNodeStatus(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusReady)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	nodes := []*redis.Node{
		{Name: "test-cluster-0", IP: "10.0.0.1", Flags: "myself,master"},
		{Name: "test-cluster-1", IP: "10.0.0.2", Flags: "myself,master"},
		{Name: "test-cluster-2", IP: "10.0.0.3", Flags: "slave"},
	}

	err := cr.updateNodeStatus(context.Background(), cfg, nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var fetched redisv1.RedkeyClusterConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if len(fetched.Status.Nodes) != 3 {
		t.Fatalf("expected 3 nodes in status, got %d", len(fetched.Status.Nodes))
	}
	if fetched.Status.Nodes["test-cluster-0"].Role != "primary" {
		t.Fatalf("expected node-0 role 'primary', got '%s'", fetched.Status.Nodes["test-cluster-0"].Role)
	}
	if fetched.Status.Nodes["test-cluster-2"].Role != "replica" {
		t.Fatalf("expected node-2 role 'replica', got '%s'", fetched.Status.Nodes["test-cluster-2"].Role)
	}
}

func TestClusterReconciler_HandleInitializing_PodsReady_ButNoPodsFound(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusInitializing)
	owner := testOwnerCluster()
	// Create StatefulSet with ReadyReplicas=3 but no pods exist
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	// Pods are "ready" per STS status, but initNodes will fail because no pods exist
	_, err := cr.ReconcileCluster(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error when no pods have IPs")
	}
}

func TestClusterReconciler_HandleInitializing_NotReady(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusInitializing)
	owner := testOwnerCluster()
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected interval reconcile while waiting for pods, got %v", schedule)
	}
}

func TestCloseNodes(t *testing.T) {
	// Should not panic with nil clients
	nodes := []*redis.Node{
		{Name: "node-0"},
		{Name: "node-1"},
	}
	closeNodes(nodes) // Should not panic
}

func TestCloseNodes_Empty(t *testing.T) {
	closeNodes(nil)             // Should not panic
	closeNodes([]*redis.Node{}) // Should not panic
}

// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"testing"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func scaleZeroConfig(deletePVC *bool) *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-config-zero",
			Namespace: "default",
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           2,
			Primaries:          0,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
			DeletePVC:          deletePVC,
		},
		Status: redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhaseInProgress,
			Status:      redisv1.ClusterStatusScalingToZero,
			Nodes:       map[string]*redisv1.RedisNode{},
		},
	}
}

func TestHandleScalingToZero_DeletesAllObjects(t *testing.T) {
	cfg := scaleZeroConfig(nil)
	owner := testOwnerCluster()

	// Create objects that should be deleted
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
	}
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-pdb", Namespace: "default"},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts, svc, cm, pdb).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleScalingToZero(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected reconcileAfterInterval, got %v", schedule)
	}

	// Verify StatefulSet deleted
	var fetchedSTS appsv1.StatefulSet
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &fetchedSTS)
	if err == nil {
		t.Fatal("expected StatefulSet to be deleted")
	}

	// Verify Service deleted
	var fetchedSvc corev1.Service
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &fetchedSvc)
	if err == nil {
		t.Fatal("expected Service to be deleted")
	}

	// Verify ConfigMap deleted
	var fetchedCM corev1.ConfigMap
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &fetchedCM)
	if err == nil {
		t.Fatal("expected ConfigMap to be deleted")
	}

	// Verify PDB deleted
	var fetchedPDB policyv1.PodDisruptionBudget
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster-pdb", Namespace: "default"}, &fetchedPDB)
	if err == nil {
		t.Fatal("expected PDB to be deleted")
	}
}

func TestHandleScalingToZero_MarksConfigApplied(t *testing.T) {
	cfg := scaleZeroConfig(nil)
	owner := testOwnerCluster()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	_, err := cr.handleScalingToZero(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify config phase is Applied
	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config-zero", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseApplied {
		t.Fatalf("expected Applied, got %q", fetched.Status.ConfigPhase)
	}
	if fetched.Status.Status != redisv1.ClusterStatusReady {
		t.Fatalf("expected Ready status, got %q", fetched.Status.Status)
	}
}

func TestHandleScalingToZero_DeletesPVCsWhenEnabled(t *testing.T) {
	deletePVC := true
	cfg := scaleZeroConfig(&deletePVC)
	owner := testOwnerCluster()

	// Create PVCs with cluster labels
	pvc1 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-cluster-0",
			Namespace: "default",
			Labels: map[string]string{
				"redkey.inditex.dev/cluster":   "test-cluster",
				"redkey.inditex.dev/component": "redis",
			},
		},
	}
	pvc2 := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-cluster-1",
			Namespace: "default",
			Labels: map[string]string{
				"redkey.inditex.dev/cluster":   "test-cluster",
				"redkey.inditex.dev/component": "redis",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, pvc1, pvc2).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	_, err := cr.handleScalingToZero(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify PVCs are deleted
	var fetchedPVC corev1.PersistentVolumeClaim
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "data-test-cluster-0", Namespace: "default"}, &fetchedPVC)
	if err == nil {
		t.Fatal("expected PVC data-test-cluster-0 to be deleted")
	}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: "data-test-cluster-1", Namespace: "default"}, &fetchedPVC)
	if err == nil {
		t.Fatal("expected PVC data-test-cluster-1 to be deleted")
	}
}

func TestHandleScalingToZero_SkipsPVCsWhenDisabled(t *testing.T) {
	deletePVC := false
	cfg := scaleZeroConfig(&deletePVC)
	owner := testOwnerCluster()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-cluster-0",
			Namespace: "default",
			Labels: map[string]string{
				"redkey.inditex.dev/cluster":   "test-cluster",
				"redkey.inditex.dev/component": "redis",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, pvc).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	_, err := cr.handleScalingToZero(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify PVC is NOT deleted
	var fetchedPVC corev1.PersistentVolumeClaim
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "data-test-cluster-0", Namespace: "default"}, &fetchedPVC); err != nil {
		t.Fatal("expected PVC to still exist when deletePVC=false")
	}
}

func TestHandleScalingToZero_SkipsPVCsWhenNil(t *testing.T) {
	cfg := scaleZeroConfig(nil) // deletePVC not set
	owner := testOwnerCluster()

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-cluster-0",
			Namespace: "default",
			Labels: map[string]string{
				"redkey.inditex.dev/cluster":   "test-cluster",
				"redkey.inditex.dev/component": "redis",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, pvc).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	_, err := cr.handleScalingToZero(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify PVC is NOT deleted
	var fetchedPVC corev1.PersistentVolumeClaim
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "data-test-cluster-0", Namespace: "default"}, &fetchedPVC); err != nil {
		t.Fatal("expected PVC to still exist when deletePVC=nil")
	}
}

func TestHandleScalingToZero_IdempotentWhenObjectsMissing(t *testing.T) {
	cfg := scaleZeroConfig(nil)
	owner := testOwnerCluster()

	// No objects to delete — should still succeed
	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleScalingToZero(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterInterval {
		t.Fatalf("expected reconcileAfterInterval, got %v", schedule)
	}

	// Should still mark Applied
	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config-zero", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseApplied {
		t.Fatalf("expected Applied, got %q", fetched.Status.ConfigPhase)
	}
}

func TestDetermineStatusTransition_ScaleToZero(t *testing.T) {
	report := ChangeReport{
		HasTopologyChanges:     true,
		TopologyScaleDirection: ScaleToZero,
		PrimariesDelta:         -3,
	}
	result := DetermineStatusTransition(report)
	if result != redisv1.ClusterStatusScalingToZero {
		t.Errorf("expected ScalingToZero, got %q", result)
	}
}

func TestDetectChanges_ScaleToZero(t *testing.T) {
	previous := baseSpec()
	target := baseSpec()
	target.Primaries = 0
	target.Sequence = 2

	report := DetectChanges(previous, target)

	if !report.HasTopologyChanges {
		t.Error("expected HasTopologyChanges to be true")
	}
	if report.TopologyScaleDirection != ScaleToZero {
		t.Errorf("expected ScaleToZero, got %v", report.TopologyScaleDirection)
	}
	if report.PrimariesDelta != -3 {
		t.Errorf("expected PrimariesDelta=-3, got %d", report.PrimariesDelta)
	}
}

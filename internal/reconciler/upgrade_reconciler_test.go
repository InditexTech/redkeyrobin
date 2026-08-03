// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"
	"testing"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
	"github.com/inditextech/redkey-robin/internal/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// --- Pure function tests ---

func TestFastUpgradeEligible(t *testing.T) {
	cases := []struct {
		name      string
		ephemeral bool
		replicas  int32
		purge     *bool
		want      bool
	}{
		{"ephemeral + no replicas + purge true", true, 0, boolPtr(true), true},
		{"ephemeral + no replicas + purge false", true, 0, boolPtr(false), false},
		{"ephemeral + no replicas + purge nil", true, 0, nil, false},
		{"ephemeral + replicas + purge true", true, 1, boolPtr(true), false},
		{"ephemeral + replicas + purge false", true, 1, boolPtr(false), false},
		{"persistent + no replicas + purge true", false, 0, boolPtr(true), false},
		{"persistent + replicas + purge true", false, 1, boolPtr(true), false},
		{"persistent + no replicas + purge nil", false, 0, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
			cfg.Spec.Ephemeral = tc.ephemeral
			cfg.Spec.ReplicasPerPrimary = tc.replicas
			cfg.Spec.PurgeKeysOnRebalance = tc.purge
			if got := fastUpgradeEligible(cfg); got != tc.want {
				t.Fatalf("fastUpgradeEligible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestOriginalPrimaries(t *testing.T) {
	cfg := testClusterConfig("")
	cfg.Spec.Primaries = 5
	if got := originalPrimaries(cfg); got != 5 {
		t.Fatalf("originalPrimaries = %d, want 5", got)
	}
}

func TestUpgradeExtraNodes(t *testing.T) {
	cases := []struct {
		name     string
		replicas int32
		want     int32
	}{
		{"no replicas", 0, 1},
		{"1 replica", 1, 2},
		{"2 replicas", 2, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testClusterConfig("")
			cfg.Spec.ReplicasPerPrimary = tc.replicas
			if got := upgradeExtraNodes(cfg); got != tc.want {
				t.Fatalf("upgradeExtraNodes = %d, want %d", got, tc.want)
			}
		})
	}
}

// --- Substatus routing tests ---

func TestHandleUpgrading_SelectsFastUpgrade(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(true)
	cfg.Spec.Image = "redis:8"
	owner := testOwnerCluster()

	// Create StatefulSet for the template update
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{},
					Annotations: map[string]string{},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "redis",
						Image: "redis:7",
					}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}

	// Create ConfigMap that UpdateConfigMap will patch
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Data: map[string]string{"redis.conf": ""},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts, cm).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleUpgrading(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Fast upgrade starts by updating template+configmap and deleting pods — should schedule wait
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}

	// Verify substatus changed to SubstatusFastUpgrading
	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Substatus.Status != redisv1.SubstatusFastUpgrading {
		t.Fatalf("expected substatus %s, got %s", redisv1.SubstatusFastUpgrading, fetched.Status.Substatus.Status)
	}
}

func TestHandleUpgrading_SelectsRollingUpgrade(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(false)
	cfg.Spec.Image = "redis:8"
	owner := testOwnerCluster()

	// Create StatefulSet
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{},
					Annotations: map[string]string{},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "redis",
						Image: "redis:7",
					}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Data: map[string]string{"redis.conf": ""},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts, cm).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleUpgrading(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Rolling upgrade starts by updating ConfigMap + StatefulSet + scaling up
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval, got %v", schedule)
	}

	// Verify substatus changed to SubstatusUpgradeScalingUp
	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Substatus.Status != redisv1.SubstatusUpgradeScalingUp {
		t.Fatalf("expected substatus %s, got %s", redisv1.SubstatusUpgradeScalingUp, fetched.Status.Substatus.Status)
	}
}

func TestHandleFastUpgrade_WaitReady_NotReady(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(true)
	cfg.Spec.Primaries = 3
	cfg.Spec.ReplicasPerPrimary = 0
	cfg.Status.Substatus.Status = redisv1.SubstatusFastUpgrading
	owner := testOwnerCluster()

	// StatefulSet with 0 ready replicas
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 0},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleFastUpgradeWaitReady(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval while pods not ready, got %v", schedule)
	}
}

func TestHandleFastUpgrade_WaitReady_AllReady(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(true)
	cfg.Spec.Primaries = 3
	cfg.Spec.ReplicasPerPrimary = 0
	cfg.Status.Substatus.Status = redisv1.SubstatusFastUpgrading
	owner := testOwnerCluster()

	// StatefulSet with 3 ready replicas
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleFastUpgradeWaitReady(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should proceed immediately (transition to SubstatusEndingFastUpgrade)
	if schedule != reconcileImmediately {
		t.Fatalf("expected reconcileImmediately when all pods ready, got %v", schedule)
	}

	// Verify substatus
	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Substatus.Status != redisv1.SubstatusEndingFastUpgrade {
		t.Fatalf("expected substatus %s, got %s", redisv1.SubstatusEndingFastUpgrade, fetched.Status.Substatus.Status)
	}
}

func TestHandleUpgradeScalingUp_WaitsForPods(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(false)
	cfg.Spec.Primaries = 3
	cfg.Spec.ReplicasPerPrimary = 0
	cfg.Status.Substatus.Status = redisv1.SubstatusUpgradeScalingUp
	owner := testOwnerCluster()

	// StatefulSet scaled to 4 but only 3 ready
	replicas := int32(4)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleUpgradeScalingUp(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval while waiting for extra pods, got %v", schedule)
	}
}

func TestHandleUpgradeScalingDown_WaitsForPods(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.PurgeKeysOnRebalance = boolPtr(false)
	cfg.Spec.Primaries = 3
	cfg.Spec.ReplicasPerPrimary = 0
	cfg.Status.Substatus.Status = redisv1.SubstatusUpgradeScalingDown
	owner := testOwnerCluster()

	// StatefulSet with only 2 of 3 pods ready (still scaling down)
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec:   appsv1.StatefulSetSpec{Replicas: &replicas},
		Status: appsv1.StatefulSetStatus{ReadyReplicas: 2},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleUpgradeScalingDown(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Not all pods ready yet, should schedule wait
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval while pods not ready, got %v", schedule)
	}
}

// --- Rolling upgrade substatus routing ---

func TestRollingUpgradeSubstatusRouting(t *testing.T) {
	cases := []struct {
		name      string
		substatus string
	}{
		{"scaling up", redisv1.SubstatusUpgradeScalingUp},
		{"resharding", redisv1.SubstatusUpgradeResharding},
		{"rolling update", redisv1.SubstatusUpgradeRollingUpdate},
		{"ending", redisv1.SubstatusUpgradeEnding},
		{"scaling down", redisv1.SubstatusUpgradeScalingDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
			cfg.Spec.PurgeKeysOnRebalance = boolPtr(false)
			cfg.Spec.Primaries = 3
			cfg.Spec.ReplicasPerPrimary = 0
			cfg.Status.Substatus.Status = tc.substatus
			cfg.Status.Substatus.UpgradingPartition = 2
			owner := testOwnerCluster()

			replicas := int32(4)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
				},
				Spec:   appsv1.StatefulSetSpec{Replicas: &replicas},
				Status: appsv1.StatefulSetStatus{ReadyReplicas: 3},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(clusterTestScheme).
				WithObjects(owner, cfg, sts).
				WithStatusSubresource(&redisv1.RedkeyConfig{}).
				Build()

			cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

			// Should not panic; expected to return error or wait (no real Redis nodes)
			_, _ = cr.handleRollingUpgrade(context.Background(), cfg)
		})
	}
}

// upgradePartitionPod builds a pod at the given ordinal running the given image and
// carrying the given controller-revision-hash label, marked Ready.
func upgradePartitionPod(ordinal int, image, revision string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("test-cluster-%d", ordinal),
			Namespace: "default",
			Labels: map[string]string{
				"redkey.inditex.dev/cluster":   "test-cluster",
				"redkey.inditex.dev/component": "redis",
				"controller-revision-hash":     revision,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "redis", Image: image}},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

// TestHandleUpgradeRollingUpdate_DeletesPodWithOldImageOnce verifies that a pod whose
// controller-revision-hash differs from the StatefulSet's UpdateRevision (i.e. it is
// still running the OLD spec) is deleted exactly once so the OnDelete strategy can
// recreate it.
func TestHandleUpgradeRollingUpdate_DeletesPodWithOldImageOnce(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.Primaries = 3
	cfg.Spec.ReplicasPerPrimary = 0
	cfg.Spec.Image = "redis:8"
	cfg.Status.Substatus.Status = redisv1.SubstatusUpgradeRollingUpdate
	cfg.Status.Substatus.UpgradingPartition = 2
	owner := testOwnerCluster()

	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-new"},
	}

	// Pod 2 still carries the OLD revision — it must be deleted to be recycled.
	oldPod := upgradePartitionPod(2, "redis:7", "test-cluster-old")

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts, oldPod).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.handleUpgradeRollingUpdate(context.Background(), cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval after deleting old pod, got %v", schedule)
	}

	// The old pod must have been deleted.
	var fetched corev1.Pod
	err = fakeClient.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster-2", Namespace: "default"}, &fetched)
	if err == nil {
		t.Fatalf("expected pod test-cluster-2 to be deleted, but it still exists")
	}
}

// TestHandleUpgradeRollingUpdate_DoesNotDeletePodWithNewImage is a regression test for the
// upgrade deadlock bug: a pod already running the NEW image must NOT be deleted again on
// retry. Re-deleting an already-recycled (and possibly slot-owning) pod orphans its slots
// under a dead node ID and permanently blocks the reshard.
func TestHandleUpgradeRollingUpdate_DoesNotDeletePodWithNewImage(t *testing.T) {
	cfg := testClusterConfig(redisv1.ClusterStatusUpgrading)
	cfg.Spec.Primaries = 3
	cfg.Spec.ReplicasPerPrimary = 0
	cfg.Spec.Image = "redis:8"
	cfg.Status.Substatus.Status = redisv1.SubstatusUpgradeRollingUpdate
	cfg.Status.Substatus.UpgradingPartition = 2
	owner := testOwnerCluster()

	// Pod 2 already carries the NEW revision — it must be preserved across retries.
	newPod := upgradePartitionPod(2, "redis:8", "test-cluster-new")

	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-new"},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg, sts, newPod).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	// No real Redis backing the pod, so the handler will fail/wait when meeting the node —
	// but it must NOT delete the pod, which is the behavior under test.
	_, _ = cr.handleUpgradeRollingUpdate(context.Background(), cfg)

	var fetched corev1.Pod
	if err := fakeClient.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster-2", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("pod running the new image must not be deleted, but Get failed: %v", err)
	}
	if fetched.DeletionTimestamp != nil {
		t.Fatalf("pod running the new image must not be marked for deletion")
	}
}

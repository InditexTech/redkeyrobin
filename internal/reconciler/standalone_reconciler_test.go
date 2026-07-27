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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testStandaloneOwner() *redisv1.Redkey {
	return &redisv1.Redkey{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "test-uid-standalone",
		},
		Spec: redisv1.RedkeySpec{
			Mode:               redisv1.ModeStandalone,
			Primaries:          1,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
}

func testStandaloneConfig(status string, primaries int32) *redisv1.RedkeyConfig {
	return &redisv1.RedkeyConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-config",
			Namespace: "default",
		},
		Spec: redisv1.RedkeyConfigSpec{
			Sequence:           1,
			Mode:               redisv1.ModeStandalone,
			Primaries:          primaries,
			ReplicasPerPrimary: 0,
			Ephemeral:          true,
			Image:              "redis:7",
		},
		Status: redisv1.RedkeyConfigStatus{
			ConfigPhase: redisv1.ConfigPhaseInProgress,
			Status:      status,
			Nodes:       map[string]*redisv1.RedisNode{},
		},
	}
}

func TestStandalone_New_CreatesObjectsAndInitializing(t *testing.T) {
	cfg := testStandaloneConfig("", 1)
	owner := testStandaloneOwner()

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusInitializing {
		t.Fatalf("expected status Initializing, got '%s'", fetched.Status.Status)
	}

	// Verify a single-replica StatefulSet was created.
	var sts appsv1.StatefulSet
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &sts); err != nil {
		t.Fatalf("expected StatefulSet to be created: %v", err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 1 {
		t.Fatalf("expected 1 replica, got %v", sts.Spec.Replicas)
	}
}

func TestStandalone_New_PrimariesZero_MarksApplied(t *testing.T) {
	cfg := testStandaloneConfig("", 0)
	owner := testStandaloneOwner()
	owner.Spec.Primaries = 0

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(owner, cfg).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	if _, err := cr.ReconcileCluster(context.Background(), cfg, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No StatefulSet should be created for a zero-primary standalone cluster.
	var sts appsv1.StatefulSet
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &sts); err == nil {
		t.Fatal("expected no StatefulSet for zero-primary standalone cluster")
	}
}

func TestStandalone_ConfigChange_ScaleToZero(t *testing.T) {
	prev := testStandaloneConfig(redisv1.ClusterStatusReady, 1)
	target := testStandaloneConfig("", 0)
	target.Spec.Sequence = 2

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(testStandaloneOwner(), target).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), target, prev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusScalingToZero {
		t.Fatalf("expected status ScalingToZero, got '%s'", fetched.Status.Status)
	}
}

func TestStandalone_ConfigChange_ScaleUpFromZero(t *testing.T) {
	prev := testStandaloneConfig(redisv1.ClusterStatusReady, 0)
	target := testStandaloneConfig("", 1)
	target.Spec.Sequence = 2

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(testStandaloneOwner(), target).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), target, prev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusInitializing {
		t.Fatalf("expected status Initializing, got '%s'", fetched.Status.Status)
	}

	var sts appsv1.StatefulSet
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &sts); err != nil {
		t.Fatalf("expected StatefulSet to be created on scale-up: %v", err)
	}
}

func TestStandalone_ConfigChange_RedisConfig_Upgrading(t *testing.T) {
	prev := testStandaloneConfig(redisv1.ClusterStatusReady, 1)
	target := testStandaloneConfig("", 1)
	target.Spec.Sequence = 2
	target.Spec.RedisConfig = "maxmemory 128mb"

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(testStandaloneOwner(), target).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), target, prev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileImmediately {
		t.Fatalf("expected immediate reconcile, got %v", schedule)
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusUpgrading {
		t.Fatalf("expected status Upgrading, got '%s'", fetched.Status.Status)
	}
}

func TestStandalone_Initializing_WaitsForPod(t *testing.T) {
	cfg := testStandaloneConfig(redisv1.ClusterStatusInitializing, 1)

	// StatefulSet exists but no ready replicas yet.
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 0},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(testStandaloneOwner(), cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	schedule, err := cr.ReconcileCluster(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval while pod not ready, got %v", schedule)
	}
}

func TestStandalone_ScalingToZero_DeletesObjects(t *testing.T) {
	cfg := testStandaloneConfig(redisv1.ClusterStatusScalingToZero, 0)

	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(testStandaloneOwner(), cfg, sts).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	if _, err := cr.ReconcileCluster(context.Background(), cfg, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var deleted appsv1.StatefulSet
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &deleted); err == nil {
		t.Fatal("expected StatefulSet to be deleted on scale-to-zero")
	}

	var fetched redisv1.RedkeyConfig
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusReady {
		t.Fatalf("expected status Ready after scale-to-zero, got '%s'", fetched.Status.Status)
	}
}

// standalonePod builds the single standalone pod (ordinal 0) carrying the given
// controller-revision-hash label, marked Ready.
func standalonePod(revision string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-0",
			Namespace: "default",
			Labels: map[string]string{
				"redkey.inditex.dev/cluster":   "test-cluster",
				"redkey.inditex.dev/component": "redis",
				"controller-revision-hash":     revision,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "redis", Image: "redis:7"}},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func newStandaloneReconciler(objs ...client.Object) *ClusterReconciler {
	fakeClient := fake.NewClientBuilder().
		WithScheme(clusterTestScheme).
		WithObjects(objs...).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()
	return NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())
}

// TestStandalone_recycleStandalonePod_WaitsWhenRevisionEmpty verifies that the recycle
// helper waits (does not declare the pod up to date) while the StatefulSet has not yet
// published an UpdateRevision, avoiding a false "already upgraded" decision.
func TestStandalone_recycleStandalonePod_WaitsWhenRevisionEmpty(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: ""},
	}
	cr := newStandaloneReconciler(testStandaloneOwner(), sts, standalonePod("anything"))

	recycled, err := cr.recycleStandalonePod(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recycled {
		t.Fatal("expected recycle to wait while UpdateRevision is empty")
	}
}

// TestStandalone_recycleStandalonePod_WaitsWhenGenerationNotObserved verifies that the
// recycle helper waits while the StatefulSet controller has not yet observed the latest
// pod-template change (Status.ObservedGeneration < metadata.Generation). In that window
// Status.UpdateRevision still points at the previous template, so comparing it against the
// pod's revision would wrongly report "already up to date" and skip recycling the pod —
// the exact race that left a standalone config change unapplied. The outdated pod must be
// preserved (not deleted) until the controller catches up.
func TestStandalone_recycleStandalonePod_WaitsWhenGenerationNotObserved(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", Generation: 2},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		// Stale revision still matches the pod below; only the unobserved generation
		// must keep the helper from declaring the pod up to date.
		Status: appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-old", ObservedGeneration: 1},
	}
	oldPod := standalonePod("test-cluster-old")
	cr := newStandaloneReconciler(testStandaloneOwner(), sts, oldPod)

	recycled, err := cr.recycleStandalonePod(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recycled {
		t.Fatal("expected recycle to wait while the StatefulSet generation is not yet observed")
	}

	var pod corev1.Pod
	key := types.NamespacedName{Name: "test-cluster-0", Namespace: "default"}
	if err := cr.client.Get(context.Background(), key, &pod); err != nil {
		t.Fatalf("expected the standalone pod to be preserved while waiting, got error: %v", err)
	}
}

// TestStandalone_recycleStandalonePod_WaitsWhenPodMissing verifies that the helper waits
// (rather than erroring) while the pod is mid-recreation and not yet present.
func TestStandalone_recycleStandalonePod_WaitsWhenPodMissing(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-new"},
	}
	cr := newStandaloneReconciler(testStandaloneOwner(), sts)

	recycled, err := cr.recycleStandalonePod(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recycled {
		t.Fatal("expected recycle to wait while the pod is not present")
	}
}

// TestStandalone_recycleStandalonePod_DeletesPodWhenRevisionDiffers verifies that a pod
// still running the outdated template is deleted exactly once so the OnDelete StatefulSet
// recreates it with the new template.
func TestStandalone_recycleStandalonePod_DeletesPodWhenRevisionDiffers(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-new"},
	}
	oldPod := standalonePod("test-cluster-old")
	cr := newStandaloneReconciler(testStandaloneOwner(), sts, oldPod)

	recycled, err := cr.recycleStandalonePod(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recycled {
		t.Fatal("expected recycle to report not-yet-done after deleting the outdated pod")
	}

	var pod corev1.Pod
	key := types.NamespacedName{Name: "test-cluster-0", Namespace: "default"}
	if err := cr.client.Get(context.Background(), key, &pod); err == nil {
		t.Fatal("expected the outdated standalone pod to be deleted")
	}
}

// TestStandalone_recycleStandalonePod_CompletesWhenPodOnNewRevision verifies that a pod
// already running the desired template is left untouched and the recycle is reported done.
func TestStandalone_recycleStandalonePod_CompletesWhenPodOnNewRevision(t *testing.T) {
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &replicas},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-new"},
	}
	newPod := standalonePod("test-cluster-new")
	cr := newStandaloneReconciler(testStandaloneOwner(), sts, newPod)

	recycled, err := cr.recycleStandalonePod(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !recycled {
		t.Fatal("expected recycle to report done when the pod already runs the new revision")
	}

	var pod corev1.Pod
	key := types.NamespacedName{Name: "test-cluster-0", Namespace: "default"}
	if err := cr.client.Get(context.Background(), key, &pod); err != nil {
		t.Fatalf("expected the standalone pod to be preserved, got error: %v", err)
	}
}

// TestStandalone_Upgrading_UpdatesStatefulSetTemplate verifies that an upgrade applies the
// new image to the existing StatefulSet pod template (i.e. it actually mutates the running
// StatefulSet instead of being a no-op) and waits for the pod to be recycled.
func TestStandalone_Upgrading_UpdatesStatefulSetTemplate(t *testing.T) {
	cfg := testStandaloneConfig(redisv1.ClusterStatusUpgrading, 1)
	cfg.Spec.Image = "redis:8"

	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "redis", Image: "redis:7"}},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-new"},
	}
	// The redis.conf ConfigMap already exists; the upgrade updates it in place.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Data:       map[string]string{"redis.conf": "# old"},
	}
	// Pod still runs the old revision, so the upgrade must recycle it and wait.
	oldPod := standalonePod("test-cluster-old")
	cr := newStandaloneReconciler(testStandaloneOwner(), cfg, sts, cm, oldPod)
	schedule, err := cr.ReconcileCluster(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schedule != reconcileAfterWaitInterval {
		t.Fatalf("expected reconcileAfterWaitInterval while recycling pod, got %v", schedule)
	}

	var updated appsv1.StatefulSet
	if err := cr.client.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &updated); err != nil {
		t.Fatalf("failed to get StatefulSet: %v", err)
	}
	if len(updated.Spec.Template.Spec.Containers) == 0 ||
		updated.Spec.Template.Spec.Containers[0].Image != "redis:8" {
		t.Fatalf("expected StatefulSet template image to be updated to redis:8, got %+v",
			updated.Spec.Template.Spec.Containers)
	}

	// The outdated pod must have been deleted to force recreation with the new template.
	var pod corev1.Pod
	key := types.NamespacedName{Name: "test-cluster-0", Namespace: "default"}
	if err := cr.client.Get(context.Background(), key, &pod); err == nil {
		t.Fatal("expected the outdated standalone pod to be deleted during upgrade")
	}

	// The config must remain in the Upgrading state until the pod is ready and reachable.
	var fetched redisv1.RedkeyConfig
	if err := cr.client.Get(context.Background(),
		types.NamespacedName{Name: "test-config", Namespace: "default"}, &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.Status != redisv1.ClusterStatusUpgrading {
		t.Fatalf("expected status to remain Upgrading, got '%s'", fetched.Status.Status)
	}
}

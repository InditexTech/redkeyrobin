// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"testing"
	"time"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var testScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = redisv1.AddToScheme(testScheme)
}

func newTestReconciler(objs ...client.Object) *Reconciler {
	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(objs...).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()
	return NewReconciler(fakeClient, "test-cluster", "default", 5*time.Second, 2*time.Second)
}

func makeConfigWithLabels(name string, seq int, phase string, primaries, replicas int32) *redisv1.RedkeyClusterConfig {
	return &redisv1.RedkeyClusterConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{ClusterLabel: "test-cluster"},
		},
		Spec: redisv1.RedkeyClusterConfigSpec{
			Sequence:           seq,
			Primaries:          primaries,
			ReplicasPerPrimary: replicas,
			Ephemeral:          true,
			Image:              "redis:7",
			Version:            "7.0",
		},
		Status: redisv1.RedkeyClusterConfigStatus{
			ConfigPhase: phase,
			Nodes:       map[string]*redisv1.RedisNode{},
		},
	}
}

// --- NewReconciler tests ---

func TestNewReconciler(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()
	r := NewReconciler(fakeClient, "my-cluster", "ns-1", 10*time.Second, 3*time.Second)

	if r.clusterName != "my-cluster" {
		t.Fatalf("expected clusterName 'my-cluster', got '%s'", r.clusterName)
	}
	if r.namespace != "ns-1" {
		t.Fatalf("expected namespace 'ns-1', got '%s'", r.namespace)
	}
	if r.interval != 10*time.Second {
		t.Fatalf("expected interval 10s, got %v", r.interval)
	}
	if r.intervalOnError != 3*time.Second {
		t.Fatalf("expected intervalOnError 3s, got %v", r.intervalOnError)
	}
	if r.client == nil {
		t.Fatal("expected non-nil client")
	}
	if r.logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

// --- listConfigs tests ---

func TestListConfigs_Empty(t *testing.T) {
	r := newTestReconciler()
	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 0 {
		t.Fatalf("expected 0 configs, got %d", len(configs))
	}
}

func TestListConfigs_FiltersNamespace(t *testing.T) {
	// Config in the right namespace and cluster
	cfg1 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)
	// Config in a different namespace (should not appear)
	cfg2 := makeConfigWithLabels("cfg-other-ns", 2, redisv1.ConfigPhasePending, 3, 1)
	cfg2.Namespace = "other-namespace"

	r := newTestReconciler(cfg1, cfg2)
	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(configs))
	}
	if configs[0].Name != "cfg-1" {
		t.Fatalf("expected cfg-1, got %s", configs[0].Name)
	}
}

func TestListConfigs_FiltersClusterLabel(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-other-cluster", 2, redisv1.ConfigPhasePending, 3, 1)
	cfg2.Labels[ClusterLabel] = "other-cluster"

	r := newTestReconciler(cfg1, cfg2)
	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("expected 1 config, got %d", len(configs))
	}
	if configs[0].Name != "cfg-1" {
		t.Fatalf("expected cfg-1, got %s", configs[0].Name)
	}
}

func TestListConfigs_Sorted(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1)
	cfg2 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)
	cfg3 := makeConfigWithLabels("cfg-2", 2, redisv1.ConfigPhasePending, 5, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3)

	configs, err := r.listConfigs(context.Background(), true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 3 {
		t.Fatalf("expected 3 configs, got %d", len(configs))
	}
	if configs[0].Spec.Sequence != 1 || configs[1].Spec.Sequence != 2 || configs[2].Spec.Sequence != 3 {
		t.Fatalf("expected sorted by sequence [1,2,3], got [%d,%d,%d]",
			configs[0].Spec.Sequence, configs[1].Spec.Sequence, configs[2].Spec.Sequence)
	}
}

func TestListConfigs_Unsorted(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-3", 3, redisv1.ConfigPhasePending, 7, 1)
	cfg2 := makeConfigWithLabels("cfg-1", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg1, cfg2)

	configs, err := r.listConfigs(context.Background(), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(configs))
	}
	// Not asserting order — just that both are returned.
}

// --- initConfigPhase tests ---

func TestInitConfigPhase(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-init", 1, "", 3, 1)

	r := newTestReconciler(cfg)

	// Re-fetch the object so we have the right resource version
	var fetched redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if err := r.initConfigPhase(context.Background(), &fetched); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the status was updated
	var updated redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &updated); err != nil {
		t.Fatalf("failed to get updated config: %v", err)
	}
	if updated.Status.ConfigPhase != redisv1.ConfigPhasePending {
		t.Fatalf("expected ConfigPhase Pending, got '%s'", updated.Status.ConfigPhase)
	}
}

// --- setConfigPhaseInProgress tests ---

func TestSetConfigPhaseInProgress(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-ip", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg)

	var fetched redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}

	if err := r.setConfigPhaseInProgress(context.Background(), &fetched); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var updated redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &updated); err != nil {
		t.Fatalf("failed to get updated config: %v", err)
	}
	if updated.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected ConfigPhase InProgress, got '%s'", updated.Status.ConfigPhase)
	}
}

// --- selectConfig tests (full method with real fake client) ---

func TestSelectConfig_NoConfigs(t *testing.T) {
	r := newTestReconciler()

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected != nil {
		t.Fatalf("expected nil, got %s", selected.Name)
	}
	if prev != nil {
		t.Fatalf("expected nil prev, got %s", prev.Name)
	}
}

func TestSelectConfig_InitialisesEmptyPhase(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-nophase", 1, "", 3, 1)

	r := newTestReconciler(cfg)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == nil {
		t.Fatal("expected non-nil selected config")
	}
	if prev != nil {
		t.Fatalf("expected nil prev for single config, got %s", prev.Name)
	}

	// Verify the phase was initialised to Pending
	var fetched redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhasePending {
		t.Fatalf("expected ConfigPhase Pending after init, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestSelectConfig_ReturnsFirstPending(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-pending", 2, redisv1.ConfigPhasePending, 5, 1)

	r := newTestReconciler(cfg1, cfg2)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == nil || selected.Name != "cfg-pending" {
		t.Fatalf("expected cfg-pending, got %v", selected)
	}
	if prev == nil || prev.Name != "cfg-applied" {
		t.Fatalf("expected prev cfg-applied, got %v", prev)
	}
}

func TestSelectConfig_WithSuperseding(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-skip", 2, redisv1.ConfigPhasePending, 5, 1)
	cfg2.Spec.SkipIfSuperseded = true
	cfg3 := makeConfigWithLabels("cfg-target", 3, redisv1.ConfigPhasePending, 7, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3)

	prev, selected, err := r.selectConfig(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selected == nil || selected.Name != "cfg-target" {
		t.Fatalf("expected cfg-target (superseding), got %v", selected)
	}
	if prev == nil || prev.Name != "cfg-applied" {
		t.Fatalf("expected prev cfg-applied, got %v", prev)
	}

	// Verify cfg-skip is now Superseded in the cluster
	var fetched redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg2), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseSuperseded {
		t.Fatalf("expected cfg-skip to be Superseded, got '%s'", fetched.Status.ConfigPhase)
	}
}

// --- reconcile tests ---

func TestReconcile_NoConfigs(t *testing.T) {
	r := newTestReconciler()

	hasPending, onError := r.reconcile(context.Background())
	if hasPending {
		t.Fatal("expected no pending configs")
	}
	if onError {
		t.Fatal("expected no error")
	}
}

func TestReconcile_PendingConfig_TransitionsToInProgress(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-pending", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg)

	hasPending, onError := r.reconcile(context.Background())
	if !hasPending {
		t.Fatal("expected pending=true (there are configs being processed)")
	}
	if onError {
		t.Fatal("expected no error")
	}

	// Verify it transitioned to InProgress
	var fetched redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected InProgress, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestReconcile_InProgressConfig_Resumes(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-ip", 1, redisv1.ConfigPhaseInProgress, 3, 1)

	r := newTestReconciler(cfg)

	hasPending, onError := r.reconcile(context.Background())
	if !hasPending {
		t.Fatal("expected pending=true")
	}
	if onError {
		t.Fatal("expected no error")
	}

	// Should still be InProgress (it resumes, doesn't re-transition)
	var fetched redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected InProgress, got '%s'", fetched.Status.ConfigPhase)
	}
}

func TestReconcile_AllApplied(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)

	r := newTestReconciler(cfg)

	hasPending, onError := r.reconcile(context.Background())
	// When all configs are applied, SelectConfig returns the last one (which is Applied),
	// so it will try to re-process it (transition to InProgress).
	// This is expected behavior for the "re-apply last config" case.
	if onError {
		t.Fatal("expected no error")
	}
	_ = hasPending // The exact value depends on the code path; we just ensure no error.
}

// --- Start tests ---

func TestStart_CancelledContextStops(t *testing.T) {
	r := newTestReconciler()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := r.Start(ctx)
	if err != nil {
		t.Fatalf("expected nil error from cancelled Start, got %v", err)
	}
}

func TestStart_RunsReconciliationThenStops(t *testing.T) {
	cfg := makeConfigWithLabels("cfg-start", 1, redisv1.ConfigPhasePending, 3, 1)

	r := newTestReconciler(cfg)
	r.interval = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err := r.Start(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// After running, the config should have been transitioned to InProgress
	var fetched redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg), &fetched); err != nil {
		t.Fatalf("failed to get config: %v", err)
	}
	if fetched.Status.ConfigPhase != redisv1.ConfigPhaseInProgress {
		t.Fatalf("expected InProgress after Start, got '%s'", fetched.Status.ConfigPhase)
	}
}

// --- applySupersedingStatus tests ---

func TestApplySupersedingStatus_Success(t *testing.T) {
	cfg1 := makeConfigWithLabels("cfg-applied", 1, redisv1.ConfigPhaseApplied, 3, 1)
	cfg2 := makeConfigWithLabels("cfg-skip", 2, redisv1.ConfigPhasePending, 5, 1)
	cfg3 := makeConfigWithLabels("cfg-target", 3, redisv1.ConfigPhasePending, 7, 1)

	r := newTestReconciler(cfg1, cfg2, cfg3)

	// Fetch cfg2 to have the right resource version
	var fetchedCfg2 redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg2), &fetchedCfg2); err != nil {
		t.Fatalf("failed to get cfg2: %v", err)
	}

	sr := SupersedingResult{
		Selected:   cfg3,
		Superseded: []*redisv1.RedkeyClusterConfig{&fetchedCfg2},
	}

	result := r.applySupersedingStatus(context.Background(), cfg2, sr)
	if result.Name != "cfg-target" {
		t.Fatalf("expected cfg-target, got %s", result.Name)
	}

	// Verify cfg-skip was marked as Superseded
	var updated redisv1.RedkeyClusterConfig
	if err := r.client.Get(context.Background(), client.ObjectKeyFromObject(cfg2), &updated); err != nil {
		t.Fatalf("failed to get updated cfg2: %v", err)
	}
	if updated.Status.ConfigPhase != redisv1.ConfigPhaseSuperseded {
		t.Fatalf("expected Superseded, got '%s'", updated.Status.ConfigPhase)
	}
}

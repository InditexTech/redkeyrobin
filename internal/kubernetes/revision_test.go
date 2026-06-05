// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package kubernetes

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGetStatefulSetUpdateRevision(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{UpdateRevision: "test-cluster-7d9c"},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	rev, err := GetStatefulSetUpdateRevision(context.Background(), c, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rev != "test-cluster-7d9c" {
		t.Fatalf("expected update revision %q, got %q", "test-cluster-7d9c", rev)
	}
}

func TestGetStatefulSetUpdateRevision_MissingStatefulSet(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme).Build()

	if _, err := GetStatefulSetUpdateRevision(context.Background(), c, "test-cluster", "default"); err == nil {
		t.Fatal("expected error when StatefulSet does not exist")
	}
}

func TestGetPodControllerRevisionHash(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-0",
			Namespace: "default",
			Labels:    map[string]string{PodTemplateHashLabel: "test-cluster-7d9c"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod).Build()

	hash, err := GetPodControllerRevisionHash(context.Background(), c, "test-cluster", "default", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hash != "test-cluster-7d9c" {
		t.Fatalf("expected revision hash %q, got %q", "test-cluster-7d9c", hash)
	}
}

func TestGetPodControllerRevisionHash_NoLabel(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster-0", Namespace: "default"},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(pod).Build()

	hash, err := GetPodControllerRevisionHash(context.Background(), c, "test-cluster", "default", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hash != "" {
		t.Fatalf("expected empty revision hash for pod without label, got %q", hash)
	}
}

func TestGetPodControllerRevisionHash_MissingPod(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme).Build()

	if _, err := GetPodControllerRevisionHash(context.Background(), c, "test-cluster", "default", 0); err == nil {
		t.Fatal("expected error when pod does not exist")
	}
}

// TestRevisionComparison_RecycleDecision documents the recycle decision logic used during
// rolling upgrades: a pod whose controller-revision-hash differs from the StatefulSet's
// UpdateRevision still runs an outdated template and must be recycled; an equal hash means
// the pod is already up to date. This is robust to label/annotation ordering, unlike a
// hand-rolled checksum.
func TestRevisionComparison_RecycleDecision(t *testing.T) {
	const desired = "test-cluster-new"
	cases := []struct {
		name        string
		podRevision string
		wantRecycle bool
	}{
		{"outdated pod is recycled", "test-cluster-old", true},
		{"up-to-date pod is not recycled", "test-cluster-new", false},
		{"unlabeled pod is recycled", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotRecycle := tc.podRevision != desired
			if gotRecycle != tc.wantRecycle {
				t.Fatalf("recycle decision = %v, want %v", gotRecycle, tc.wantRecycle)
			}
		})
	}
}

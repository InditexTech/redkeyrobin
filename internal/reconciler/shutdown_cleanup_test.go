// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"testing"

	"github.com/inditextech/redkeyrobin/internal/config"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestClusterReconciler_Close_Safe verifies that the shutdown cleanup chain
// (ClusterReconciler.Close -> HealthReconciler.Close -> Checker.CloseAll) is wired and
// safe to invoke, including when no clients have been cached and when called repeatedly.
// The actual closing of cached clients is covered by health.TestChecker_CloseAll.
func TestClusterReconciler_Close_Safe(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(clusterTestScheme).Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	// Must not panic with no cached clients and must be idempotent.
	cr.Close()
	cr.Close()
}

// TestHealthReconciler_Close_Safe verifies HealthReconciler.Close delegates to the
// checker without panicking.
func TestHealthReconciler_Close_Safe(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(clusterTestScheme).Build()
	cr := NewClusterReconciler(fakeClient, "test-cluster", "default", config.NewRuntimeConfig())

	cr.healthReconciler.Close()
}

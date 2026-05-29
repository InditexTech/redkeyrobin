// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

// createAppliedConfig creates a config in Applied phase with Ready status,
// simulating a fully applied configuration for an existing cluster.
func createAppliedConfig(name string, seq int, primaries, replicas int32) *redisv1.RedkeyClusterConfig {
	cfg := &redisv1.RedkeyClusterConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			Labels:    map[string]string{clusterLabel: clusterName},
		},
		Spec: redisv1.RedkeyClusterConfigSpec{
			Sequence:           seq,
			Primaries:          primaries,
			ReplicasPerPrimary: replicas,
			Ephemeral:          true,
			Image:              "redis:7",
			Version:            "7.0",
			RedisConfig:        "maxmemory 100mb",
			RobinConfig: &redisv1.RobinConfig{
				Reconciler: &redisv1.RobinConfigReconciler{
					IntervalSeconds: intPtrHelper(30),
				},
				Metrics: &redisv1.RobinConfigMetrics{
					CollectionIntervalSeconds: intPtrHelper(60),
				},
			},
		},
	}
	Expect(k8sClient.Create(ctx, cfg)).To(Succeed())

	cfg.Status = redisv1.RedkeyClusterConfigStatus{
		ConfigPhase: redisv1.ConfigPhaseApplied,
		Status:      redisv1.ClusterStatusReady,
		Nodes:       map[string]*redisv1.RedisNode{},
	}
	Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())
	return cfg
}

func intPtrHelper(v int) *int { return &v }

var _ = Describe("Config Changes Detection (integration)", func() {
	AfterEach(func() {
		cleanup()
	})

	Describe("Change detection and status transitions", func() {
		It("marks config as Applied immediately when only Robin config changes", func() {
			// First config: Applied with Ready status
			createAppliedConfig("changes-robin-1", 1, 3, 0)

			// Second config: only Robin config differs
			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "changes-robin-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RedisConfig:        "maxmemory 100mb",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtrHelper(60), // Changed from 30 to 60
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtrHelper(120), // Changed from 60 to 120
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			cfg2.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg2)).To(Succeed())

			// Start reconciler
			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Config should be marked as Applied without changing cluster status.
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseApplied))
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusReady))
			}, timeout, interval).Should(Succeed())
		})

		It("transitions to ScalingUp when primaries increase", func() {
			createAppliedConfig("changes-scaleup-1", 1, 3, 0)

			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "changes-scaleup-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,
					Primaries:          6, // Scale up from 3 to 6
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RedisConfig:        "maxmemory 100mb",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtrHelper(30),
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtrHelper(60),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			cfg2.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg2)).To(Succeed())

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusScalingUp))
			}, timeout, interval).Should(Succeed())
		})

		It("transitions to ScalingDown when primaries decrease", func() {
			createAppliedConfig("changes-scaledown-1", 1, 6, 0)

			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "changes-scaledown-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,
					Primaries:          3, // Scale down from 6 to 3
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RedisConfig:        "maxmemory 100mb",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtrHelper(30),
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtrHelper(60),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			cfg2.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg2)).To(Succeed())

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusScalingDown))
			}, timeout, interval).Should(Succeed())
		})

		It("transitions to ScalingUp when replicas increase (primaries same)", func() {
			createAppliedConfig("changes-replicas-up-1", 1, 3, 0)

			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "changes-replicas-up-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,
					Primaries:          3,
					ReplicasPerPrimary: 2, // Add 2 replicas per primary
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RedisConfig:        "maxmemory 100mb",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtrHelper(30),
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtrHelper(60),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			cfg2.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg2)).To(Succeed())

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusScalingUp))
			}, timeout, interval).Should(Succeed())
		})

		It("transitions to Upgrading when only Kubernetes/Redis config changes", func() {
			createAppliedConfig("changes-upgrade-1", 1, 3, 0)

			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "changes-upgrade-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:9-bookworm", // Image change
					Version:            "9.0",              // Version change
					RedisConfig:        "maxmemory 200mb",  // Redis config change
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtrHelper(30),
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtrHelper(60),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			cfg2.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg2)).To(Succeed())

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusUpgrading))
			}, timeout, interval).Should(Succeed())
		})

		It("scaling takes priority over Kubernetes/Redis changes", func() {
			createAppliedConfig("changes-combined-1", 1, 3, 0)

			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "changes-combined-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,
					Primaries:          6, // Scale up
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:9-bookworm", // Also image change
					Version:            "9.0",
					RedisConfig:        "maxmemory 200mb", // Also redis config change
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtrHelper(60), // Also Robin change
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtrHelper(60),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			cfg2.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg2)).To(Succeed())

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			// Scaling takes priority over other changes
			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseInProgress))
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusScalingUp))
			}, timeout, interval).Should(Succeed())

			// Status should remain ScalingUp (operation not implemented yet)
			Consistently(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusScalingUp))
			}, 200*time.Millisecond, 50*time.Millisecond).Should(Succeed())
		})

		It("no-change config (only control fields differ) is marked as Applied", func() {
			createAppliedConfig("changes-noop-1", 1, 3, 0)

			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "changes-noop-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: clusterName},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,    // Different sequence (control field)
					SkipIfSuperseded:   true, // Different skipIfSuperseded (control field)
					Primaries:          3,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RedisConfig:        "maxmemory 100mb",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtrHelper(30),
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtrHelper(60),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			cfg2.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhasePending,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg2)).To(Succeed())

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			Eventually(func(g Gomega) {
				var fetched redisv1.RedkeyClusterConfig
				g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &fetched)).To(Succeed())
				g.Expect(fetched.Status.ConfigPhase).To(Equal(redisv1.ConfigPhaseApplied))
				g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusReady))
			}, timeout, interval).Should(Succeed())
		})
	})
})

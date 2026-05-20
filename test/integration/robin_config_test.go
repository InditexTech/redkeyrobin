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

func intPtr(v int) *int { return &v }

var _ = Describe("Robin Config Application", func() {
	const robinCluster = "robin-cfg-cluster"

	Context("when a RedkeyClusterConfig has robinConfig", func() {
		It("should update the runtime config reconciler interval from an Applied config", func() {
			// Create a config with RobinConfig specifying intervalSeconds=5.
			cfg := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-applied",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           1,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds:        intPtr(5),
							IntervalOnErrorSeconds: intPtr(3),
							IntervalOnWaitSeconds:  intPtr(7),
						},
						Metrics: &redisv1.RobinConfigMetrics{
							CollectionIntervalSeconds: intPtr(20),
							RedisInfoKeys:             []string{"used_memory", "connected_clients"},
						},
						Cluster: &redisv1.RobinConfigCluster{
							ConnectionMaxRetries:     intPtr(3),
							ConnectionBackOffSeconds: intPtr(2),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg)).To(Succeed())
			// Set status separately via status subresource.
			cfg.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhaseApplied,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())

			// Create RuntimeConfig and reconciler.
			rtConfig := newTestRuntimeConfig()
			rec := newIntegrationReconciler(robinCluster, rtConfig)

			// Run reconciliation loop.
			loopCancel, errCh := startReconcilerLoop(rec)
			defer stopReconcilerLoop(loopCancel, errCh)

			// Wait until the runtime config is updated.
			Eventually(func() time.Duration {
				return rtConfig.ReconcilerInterval()
			}, timeout, interval).Should(Equal(5 * time.Second))
			Eventually(func() time.Duration {
				return rtConfig.ReconcilerIntervalOnError()
			}, timeout, interval).Should(Equal(3 * time.Second))
			Eventually(func() time.Duration {
				return rtConfig.ReconcilerIntervalOnWait()
			}, timeout, interval).Should(Equal(7 * time.Second))

			// Verify metrics config was also applied.
			Expect(rtConfig.MetricsInterval()).To(Equal(20 * time.Second))
			Expect(rtConfig.RedisInfoKeys()).To(Equal([]string{"used_memory", "connected_clients"}))

			// Verify cluster config was stored.
			cc := rtConfig.ClusterConfig()
			Expect(cc.ConnectionMaxRetries).To(Equal(3))
			Expect(cc.ConnectionBackOffSeconds).To(Equal(2))

			// Verify topology was stored.
			topo := rtConfig.AppliedTopology()
			Expect(topo.Primaries).To(Equal(int32(3)))
			Expect(topo.ReplicasPerPrimary).To(Equal(int32(1)))

			// Cleanup.
			Expect(k8sClient.Delete(ctx, cfg)).To(Succeed())
		})

		It("should hot-reload reconciler error and wait intervals when a new config changes them", func() {
			cfg1 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-reconciler-a",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           8,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds:        intPtr(5),
							IntervalOnErrorSeconds: intPtr(2),
							IntervalOnWaitSeconds:  intPtr(4),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg1)).To(Succeed())
			cfg1.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhaseApplied,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg1)).To(Succeed())

			rtConfig := newTestRuntimeConfig()
			rec := newIntegrationReconciler(robinCluster, rtConfig)

			loopCancel, errCh := startReconcilerLoop(rec)
			defer stopReconcilerLoop(loopCancel, errCh)

			Eventually(func() time.Duration {
				return rtConfig.ReconcilerIntervalOnError()
			}, timeout, interval).Should(Equal(2 * time.Second))
			Eventually(func() time.Duration {
				return rtConfig.ReconcilerIntervalOnWait()
			}, timeout, interval).Should(Equal(4 * time.Second))

			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-reconciler-b",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           9,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds:        intPtr(5),
							IntervalOnErrorSeconds: intPtr(6),
							IntervalOnWaitSeconds:  intPtr(8),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			Eventually(func() error {
				var latest redisv1.RedkeyClusterConfig
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &latest); err != nil {
					return err
				}
				latest.Status = redisv1.RedkeyClusterConfigStatus{
					ConfigPhase: redisv1.ConfigPhaseApplied,
					Nodes:       map[string]*redisv1.RedisNode{},
				}
				return k8sClient.Status().Update(ctx, &latest)
			}, timeout, interval).Should(Succeed())

			Eventually(func() time.Duration {
				return rtConfig.ReconcilerIntervalOnError()
			}, timeout, interval).Should(Equal(6 * time.Second))
			Eventually(func() time.Duration {
				return rtConfig.ReconcilerIntervalOnWait()
			}, timeout, interval).Should(Equal(8 * time.Second))

			Expect(k8sClient.Delete(ctx, cfg1)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cfg2)).To(Succeed())
		})

		It("should update the runtime config from a Pending config when no previous exists", func() {
			// Create a Pending config with RobinConfig (no status set = reconciler will init to Pending).
			cfg := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-pending",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           2,
					Primaries:          6,
					ReplicasPerPrimary: 0,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtr(15),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg)).To(Succeed())

			// Create RuntimeConfig and reconciler.
			rtConfig := newTestRuntimeConfig()
			rec := newIntegrationReconciler(robinCluster, rtConfig)

			loopCancel, errCh := startReconcilerLoop(rec)
			defer stopReconcilerLoop(loopCancel, errCh)

			// Wait until the runtime config is updated.
			Eventually(func() time.Duration {
				return rtConfig.ReconcilerInterval()
			}, timeout, interval).Should(Equal(15 * time.Second))

			// Verify topology from Pending config.
			Eventually(func() int32 {
				return rtConfig.AppliedTopology().Primaries
			}, timeout, interval).Should(Equal(int32(6)))

			// Cleanup.
			Expect(k8sClient.Delete(ctx, cfg)).To(Succeed())
		})

		It("should propagate auth secret from RedkeyClusterConfig to RuntimeConfig", func() {
			cfg := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-auth",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           3,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					Auth:               redisv1.RedisAuth{SecretName: "my-redis-auth"},
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtr(10),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg)).To(Succeed())
			cfg.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhaseApplied,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg)).To(Succeed())

			rtConfig := newTestRuntimeConfig()
			rec := newIntegrationReconciler(robinCluster, rtConfig)

			loopCancel, errCh := startReconcilerLoop(rec)
			defer stopReconcilerLoop(loopCancel, errCh)

			// Wait until the auth secret is propagated.
			Eventually(func() string {
				return rtConfig.AuthSecret()
			}, timeout, interval).Should(Equal("my-redis-auth"))

			// Cleanup.
			Expect(k8sClient.Delete(ctx, cfg)).To(Succeed())
		})

		It("should update auth secret when a new config changes it", func() {
			// First config with secret-a.
			cfg1 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-auth-a",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           4,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					Auth:               redisv1.RedisAuth{SecretName: "secret-a"},
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtr(10),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg1)).To(Succeed())
			cfg1.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhaseApplied,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg1)).To(Succeed())

			rtConfig := newTestRuntimeConfig()
			rec := newIntegrationReconciler(robinCluster, rtConfig)

			loopCancel, errCh := startReconcilerLoop(rec)
			defer stopReconcilerLoop(loopCancel, errCh)

			// Wait for initial auth to propagate.
			Eventually(func() string {
				return rtConfig.AuthSecret()
			}, timeout, interval).Should(Equal("secret-a"))

			// Create a new config with a different secret.
			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-auth-b",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           5,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					Auth:               redisv1.RedisAuth{SecretName: "secret-b"},
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtr(10),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			Eventually(func() error {
				var latest redisv1.RedkeyClusterConfig
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &latest); err != nil {
					return err
				}
				latest.Status = redisv1.RedkeyClusterConfigStatus{
					ConfigPhase: redisv1.ConfigPhaseApplied,
					Nodes:       map[string]*redisv1.RedisNode{},
				}
				return k8sClient.Status().Update(ctx, &latest)
			}, timeout, interval).Should(Succeed())

			// Wait for auth to update to secret-b.
			Eventually(func() string {
				return rtConfig.AuthSecret()
			}, timeout, interval).Should(Equal("secret-b"))

			// Cleanup.
			Expect(k8sClient.Delete(ctx, cfg1)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cfg2)).To(Succeed())
		})

		It("should clear auth secret when new config has no auth", func() {
			// Config with auth.
			cfg1 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-auth-clear-1",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           6,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					Auth:               redisv1.RedisAuth{SecretName: "secret-to-clear"},
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtr(10),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg1)).To(Succeed())
			cfg1.Status = redisv1.RedkeyClusterConfigStatus{
				ConfigPhase: redisv1.ConfigPhaseApplied,
				Nodes:       map[string]*redisv1.RedisNode{},
			}
			Expect(k8sClient.Status().Update(ctx, cfg1)).To(Succeed())

			rtConfig := newTestRuntimeConfig()
			rec := newIntegrationReconciler(robinCluster, rtConfig)

			loopCancel, errCh := startReconcilerLoop(rec)
			defer stopReconcilerLoop(loopCancel, errCh)

			Eventually(func() string {
				return rtConfig.AuthSecret()
			}, timeout, interval).Should(Equal("secret-to-clear"))

			// New config without auth.
			cfg2 := &redisv1.RedkeyClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "robin-cfg-auth-clear-2",
					Namespace: testNamespace,
					Labels:    map[string]string{clusterLabel: robinCluster},
				},
				Spec: redisv1.RedkeyClusterConfigSpec{
					Sequence:           7,
					Primaries:          3,
					ReplicasPerPrimary: 1,
					Ephemeral:          true,
					Image:              "redis:7",
					Version:            "7.0",
					// Auth is zero-value (no secret).
					RobinConfig: &redisv1.RobinConfig{
						Reconciler: &redisv1.RobinConfigReconciler{
							IntervalSeconds: intPtr(10),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cfg2)).To(Succeed())
			// Use Eventually for status update since the reconciler may race to set the phase.
			Eventually(func() error {
				var latest redisv1.RedkeyClusterConfig
				if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg2), &latest); err != nil {
					return err
				}
				latest.Status = redisv1.RedkeyClusterConfigStatus{
					ConfigPhase: redisv1.ConfigPhaseApplied,
					Nodes:       map[string]*redisv1.RedisNode{},
				}
				return k8sClient.Status().Update(ctx, &latest)
			}, timeout, interval).Should(Succeed())

			// Auth secret should be cleared.
			Eventually(func() string {
				return rtConfig.AuthSecret()
			}, timeout, interval).Should(Equal(""))

			// Cleanup.
			Expect(k8sClient.Delete(ctx, cfg1)).To(Succeed())
			Expect(k8sClient.Delete(ctx, cfg2)).To(Succeed())
		})
	})
})

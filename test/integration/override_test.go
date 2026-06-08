// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

var _ = Describe("Object Overrides (integration)", func() {
	AfterEach(func() {
		cleanup()
		_ = k8sClient.Delete(ctx, &redisv1.RedkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
		})
		var stsList appsv1.StatefulSetList
		_ = k8sClient.List(ctx, &stsList, client.InNamespace(testNamespace))
		for i := range stsList.Items {
			_ = k8sClient.Delete(ctx, &stsList.Items[i])
		}
		var svcList corev1.ServiceList
		_ = k8sClient.List(ctx, &svcList, client.InNamespace(testNamespace))
		for i := range svcList.Items {
			if svcList.Items[i].Name == "kubernetes" {
				continue
			}
			_ = k8sClient.Delete(ctx, &svcList.Items[i])
		}
	})

	createOwner := func() {
		owner := &redisv1.RedkeyCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: testNamespace},
			Spec: redisv1.RedkeyClusterSpec{
				Primaries:          3,
				ReplicasPerPrimary: 0,
				Ephemeral:          true,
				Image:              "redis:7",
			},
		}
		Expect(k8sClient.Create(ctx, owner)).To(Succeed())
	}

	waitForInitializing := func(cfg *redisv1.RedkeyClusterConfig) {
		Eventually(func(g Gomega) {
			var fetched redisv1.RedkeyClusterConfig
			g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(cfg), &fetched)).To(Succeed())
			g.Expect(fetched.Status.Status).To(Equal(redisv1.ClusterStatusInitializing))
		}, timeout, interval).Should(Succeed())
	}

	Describe("StatefulSet override on cluster creation", func() {
		It("applies pod template scheduling fields while preserving identity", func() {
			createOwner()

			cfg := newConfig("override-sts-1", 1, "", 3, 0)
			cfg.Spec.Ephemeral = true
			cfg.Spec.Override = &redisv1.RedkeyClusterOverrideSpec{
				StatefulSet: &redisv1.PartialStatefulSet{
					Metadata: metav1.ObjectMeta{
						Annotations: map[string]string{"backup.io/enabled": "true"},
					},
					Spec: &redisv1.PartialStatefulSetSpec{
						Template: &redisv1.PartialPodTemplateSpec{
							Spec: redisv1.PartialPodSpec{
								NodeSelector: map[string]string{"disktype": "ssd"},
								Tolerations: []corev1.Toleration{
									{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "redis", Effect: corev1.TaintEffectNoSchedule},
								},
							},
						},
					},
				},
			}
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			waitForInitializing(cfg)

			var sts appsv1.StatefulSet
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
				g.Expect(sts.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("disktype", "ssd"))
			}, timeout, interval).Should(Succeed())

			Expect(sts.Spec.Template.Spec.Tolerations).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.Tolerations[0].Key).To(Equal("dedicated"))
			Expect(sts.Annotations).To(HaveKeyWithValue("backup.io/enabled", "true"))

			// Identity preserved.
			Expect(*sts.Spec.Replicas).To(Equal(int32(3)))
			Expect(sts.Spec.ServiceName).To(Equal(clusterName))
			Expect(sts.Spec.Template.Spec.Containers[0].Name).To(Equal("redis"))
		})
	})

	Describe("Service override on cluster creation", func() {
		It("applies extra ports and annotations while staying headless", func() {
			createOwner()

			cfg := newConfig("override-svc-1", 1, "", 3, 0)
			cfg.Spec.Ephemeral = true
			cfg.Spec.Override = &redisv1.RedkeyClusterOverrideSpec{
				Service: &redisv1.PartialService{
					Metadata: metav1.ObjectMeta{
						Annotations: map[string]string{"team": "infra"},
					},
					Spec: &redisv1.PartialServiceSpec{
						Ports: []corev1.ServicePort{
							{Name: "metrics", Port: 9121, TargetPort: intstr.FromInt(9121)},
						},
					},
				},
			}
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			waitForInitializing(cfg)

			var svc corev1.Service
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &svc)).To(Succeed())
				g.Expect(svc.Annotations).To(HaveKeyWithValue("team", "infra"))
			}, timeout, interval).Should(Succeed())

			portNames := map[string]bool{}
			for _, p := range svc.Spec.Ports {
				portNames[p.Name] = true
			}
			Expect(portNames).To(HaveKey("metrics"))
			Expect(portNames).To(HaveKey("client"))
			Expect(portNames).To(HaveKey("gossip"))

			// Identity preserved.
			Expect(svc.Spec.ClusterIP).To(Equal("None"))
		})
	})

	Describe("No override (backward compatibility)", func() {
		It("creates default objects with no extra scheduling fields", func() {
			createOwner()

			cfg := newConfig("override-none-1", 1, "", 3, 0)
			cfg.Spec.Ephemeral = true
			createAndSetPhase(cfg, redisv1.ConfigPhasePending)

			rec := newIntegrationReconciler(clusterName, newTestRuntimeConfig())
			loopCancel, errCh := startReconcilerLoop(rec)
			DeferCleanup(stopReconcilerLoop, loopCancel, errCh)

			waitForInitializing(cfg)

			var sts appsv1.StatefulSet
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: testNamespace}, &sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.NodeSelector).To(BeNil())
			Expect(sts.Spec.Template.Spec.Tolerations).To(BeEmpty())
		})
	})
})

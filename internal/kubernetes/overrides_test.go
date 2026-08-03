// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package kubernetes

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	redisv1 "github.com/inditextech/redkey-operator/api/v1beta1"
)

// --- applyStatefulSetOverride ---

func TestApplyStatefulSetOverride_Nil(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	result, err := applyStatefulSetOverride(base, nil, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != base {
		t.Fatal("expected nil override to return the base object unchanged")
	}
}

func TestApplyStatefulSetOverride_PodTemplateFields(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	override := &redisv1.PartialStatefulSet{
		Spec: &redisv1.PartialStatefulSetSpec{
			Template: &redisv1.PartialPodTemplateSpec{
				Spec: redisv1.PartialPodSpec{
					NodeSelector: map[string]string{"disktype": "ssd"},
					Tolerations: []corev1.Toleration{
						{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "redis", Effect: corev1.TaintEffectNoSchedule},
					},
					PriorityClassName: "high-priority",
				},
			},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	podSpec := result.Spec.Template.Spec
	if podSpec.NodeSelector["disktype"] != "ssd" {
		t.Errorf("expected nodeSelector disktype=ssd, got %v", podSpec.NodeSelector)
	}
	if len(podSpec.Tolerations) != 1 || podSpec.Tolerations[0].Key != "dedicated" {
		t.Errorf("expected dedicated toleration, got %v", podSpec.Tolerations)
	}
	if podSpec.PriorityClassName != "high-priority" {
		t.Errorf("expected priorityClassName high-priority, got %q", podSpec.PriorityClassName)
	}
	// The redis container must still be present at index 0.
	if len(podSpec.Containers) == 0 || podSpec.Containers[0].Name != "redis" {
		t.Fatalf("expected redis container at index 0, got %+v", podSpec.Containers)
	}
}

func TestApplyStatefulSetOverride_ExtraVolumePreservesConfig(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	override := &redisv1.PartialStatefulSet{
		Spec: &redisv1.PartialStatefulSetSpec{
			Template: &redisv1.PartialPodTemplateSpec{
				Spec: redisv1.PartialPodSpec{
					Volumes: []corev1.Volume{
						{Name: "extra", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !hasVolume(result, "extra") {
		t.Error("expected extra volume to be added")
	}
	if !hasVolume(result, "config") {
		t.Error("expected config volume to be preserved")
	}
}

func TestApplyStatefulSetOverride_RedisContainerResources(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	override := &redisv1.PartialStatefulSet{
		Spec: &redisv1.PartialStatefulSetSpec{
			Template: &redisv1.PartialPodTemplateSpec{
				Spec: redisv1.PartialPodSpec{
					Containers: []corev1.Container{
						{
							Name: "redis",
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
							},
						},
					},
				},
			},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c := result.Spec.Template.Spec.Containers[0]
	if c.Name != "redis" {
		t.Fatalf("expected redis container at index 0, got %q", c.Name)
	}
	mem := c.Resources.Limits[corev1.ResourceMemory]
	if mem.String() != "2Gi" {
		t.Errorf("expected memory limit 2Gi, got %s", mem.String())
	}
	// The redis image and command from the base must be preserved.
	if c.Image == "" {
		t.Error("expected redis image to be preserved")
	}
	if len(c.Command) == 0 {
		t.Error("expected redis command to be preserved")
	}
}

func TestApplyStatefulSetOverride_SidecarKeepsRedisFirst(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	override := &redisv1.PartialStatefulSet{
		Spec: &redisv1.PartialStatefulSetSpec{
			Template: &redisv1.PartialPodTemplateSpec{
				Spec: redisv1.PartialPodSpec{
					Containers: []corev1.Container{
						{Name: "exporter", Image: "exporter:1"},
					},
				},
			},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	containers := result.Spec.Template.Spec.Containers
	if containers[0].Name != "redis" {
		t.Errorf("expected redis container to remain at index 0, got %q", containers[0].Name)
	}
	if !hasContainer(result, "exporter") {
		t.Error("expected exporter sidecar container to be added")
	}
}

func TestApplyStatefulSetOverride_MetadataMerge(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	override := &redisv1.PartialStatefulSet{
		Metadata: metav1.ObjectMeta{
			Annotations: map[string]string{"backup.io/enabled": "true"},
			Labels:      map[string]string{"tier": "cache"},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Annotations["backup.io/enabled"] != "true" {
		t.Errorf("expected override annotation, got %v", result.Annotations)
	}
	if result.Labels["tier"] != "cache" {
		t.Errorf("expected override label, got %v", result.Labels)
	}
	// Base cluster label must be preserved.
	if result.Labels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected base cluster label to be preserved, got %v", result.Labels)
	}
}

func TestApplyStatefulSetOverride_GuardRails(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	baseReplicas := *base.Spec.Replicas

	bogusReplicas := int32(99)
	override := &redisv1.PartialStatefulSet{
		Spec: &redisv1.PartialStatefulSetSpec{
			Replicas:    &bogusReplicas,
			ServiceName: "hijacked",
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if *result.Spec.Replicas != baseReplicas {
		t.Errorf("expected replicas to stay %d, got %d", baseReplicas, *result.Spec.Replicas)
	}
	if result.Spec.ServiceName != "test-cluster" {
		t.Errorf("expected serviceName to stay 'test-cluster', got %q", result.Spec.ServiceName)
	}
	if result.Spec.Selector.MatchLabels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected selector to stay on cluster label, got %v", result.Spec.Selector.MatchLabels)
	}
	if _, hijacked := result.Spec.Selector.MatchLabels["foo"]; hijacked {
		t.Error("selector must not be overridable")
	}
}

func TestApplyStatefulSetOverride_RemovingRedisContainerRestoresIt(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	// Override provides only a sidecar, no redis container.
	override := &redisv1.PartialStatefulSet{
		Spec: &redisv1.PartialStatefulSetSpec{
			Template: &redisv1.PartialPodTemplateSpec{
				Spec: redisv1.PartialPodSpec{
					Containers: []corev1.Container{{Name: "sidecar", Image: "side:1"}},
				},
			},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !hasContainer(result, "redis") {
		t.Fatal("expected redis container to be restored")
	}
	if result.Spec.Template.Spec.Containers[0].Name != "redis" {
		t.Errorf("expected redis container at index 0, got %q", result.Spec.Template.Spec.Containers[0].Name)
	}
}

func TestApplyStatefulSetOverride_UpdateStrategy(t *testing.T) {
	base := buildStatefulSet("test-cluster", "default", testConfig(), testOwner())
	override := &redisv1.PartialStatefulSet{
		Spec: &redisv1.PartialStatefulSetSpec{
			UpdateStrategy: &appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
		},
	}

	result, err := applyStatefulSetOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		t.Errorf("expected OnDelete update strategy, got %q", result.Spec.UpdateStrategy.Type)
	}
}

// --- applyServiceOverride ---

func TestApplyServiceOverride_Nil(t *testing.T) {
	base := buildService("test-cluster", "default", testConfig())
	result, err := applyServiceOverride(base, nil, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != base {
		t.Fatal("expected nil override to return the base object unchanged")
	}
}

func TestApplyServiceOverride_MetadataAndType(t *testing.T) {
	base := buildService("test-cluster", "default", testConfig())
	override := &redisv1.PartialService{
		Metadata: metav1.ObjectMeta{
			Annotations: map[string]string{"service.beta.kubernetes.io/aws-load-balancer-internal": "true"},
		},
		Spec: &redisv1.PartialServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
		},
	}

	result, err := applyServiceOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Annotations["service.beta.kubernetes.io/aws-load-balancer-internal"] != "true" {
		t.Errorf("expected override annotation, got %v", result.Annotations)
	}
	if result.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("expected service type ClusterIP, got %q", result.Spec.Type)
	}
}

func TestApplyServiceOverride_ExtraPortPreservesDefaults(t *testing.T) {
	base := buildService("test-cluster", "default", testConfig())
	override := &redisv1.PartialService{
		Spec: &redisv1.PartialServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "metrics", Port: 9121, TargetPort: intstr.FromInt(9121)},
			},
		},
	}

	result, err := applyServiceOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !hasServicePort(result, "metrics") {
		t.Error("expected metrics port to be added")
	}
	if !hasServicePort(result, "client") {
		t.Error("expected client port to be preserved")
	}
	if !hasServicePort(result, "gossip") {
		t.Error("expected gossip port to be preserved")
	}
}

func TestApplyServiceOverride_GuardRails(t *testing.T) {
	base := buildService("test-cluster", "default", testConfig())
	override := &redisv1.PartialService{
		Spec: &redisv1.PartialServiceSpec{
			ClusterIP: "10.0.0.1",
			Selector:  map[string]string{"foo": "bar"},
		},
	}

	result, err := applyServiceOverride(base, override, "test-cluster", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Spec.ClusterIP != "None" {
		t.Errorf("expected ClusterIP to stay 'None', got %q", result.Spec.ClusterIP)
	}
	if result.Spec.Selector[ClusterLabel] != "test-cluster" {
		t.Errorf("expected selector to stay on cluster label, got %v", result.Spec.Selector)
	}
	if _, hijacked := result.Spec.Selector["foo"]; hijacked {
		t.Error("selector must not be overridable")
	}
}

// --- Reconcile/ensure integration with a fake client ---

func TestEnsureClusterObjects_AppliesStatefulSetOverride(t *testing.T) {
	owner := testOwner()
	config := testConfig()
	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		StatefulSet: &redisv1.PartialStatefulSet{
			Spec: &redisv1.PartialStatefulSetSpec{
				Template: &redisv1.PartialPodTemplateSpec{
					Spec: redisv1.PartialPodSpec{
						NodeSelector: map[string]string{"disktype": "ssd"},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	if err := EnsureClusterObjects(context.Background(), fakeClient, config, owner, ""); err != nil {
		t.Fatalf("EnsureClusterObjects failed: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, sts); err != nil {
		t.Fatalf("StatefulSet not created: %v", err)
	}
	if sts.Spec.Template.Spec.NodeSelector["disktype"] != "ssd" {
		t.Errorf("expected override nodeSelector to be applied, got %v", sts.Spec.Template.Spec.NodeSelector)
	}
	// Identity preserved.
	if *sts.Spec.Replicas != 6 {
		t.Errorf("expected 6 replicas, got %d", *sts.Spec.Replicas)
	}
}

func TestEnsureClusterObjects_AppliesServiceOverride(t *testing.T) {
	owner := testOwner()
	config := testConfig()
	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		Service: &redisv1.PartialService{
			Metadata: metav1.ObjectMeta{Annotations: map[string]string{"team": "infra"}},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	if err := EnsureClusterObjects(context.Background(), fakeClient, config, owner, ""); err != nil {
		t.Fatalf("EnsureClusterObjects failed: %v", err)
	}

	svc := &corev1.Service{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, svc); err != nil {
		t.Fatalf("Service not created: %v", err)
	}
	if svc.Annotations["team"] != "infra" {
		t.Errorf("expected override annotation on Service, got %v", svc.Annotations)
	}
	if svc.Spec.ClusterIP != "None" {
		t.Errorf("expected headless service, got %q", svc.Spec.ClusterIP)
	}
}

func TestEnsureClusterObjects_NoOverrideBackwardCompatible(t *testing.T) {
	owner := testOwner()
	config := testConfig()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()

	if err := EnsureClusterObjects(context.Background(), fakeClient, config, owner, ""); err != nil {
		t.Fatalf("EnsureClusterObjects failed: %v", err)
	}

	sts := &appsv1.StatefulSet{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, sts); err != nil {
		t.Fatalf("StatefulSet not created: %v", err)
	}
	// Without override there must be no extra scheduling constraints.
	if sts.Spec.Template.Spec.NodeSelector != nil {
		t.Errorf("expected nil nodeSelector without override, got %v", sts.Spec.Template.Spec.NodeSelector)
	}
	if len(sts.Spec.Template.Spec.Tolerations) != 0 {
		t.Errorf("expected no tolerations without override, got %v", sts.Spec.Template.Spec.Tolerations)
	}
}

func TestReconcileService_UpdatesOnDrift(t *testing.T) {
	owner := testOwner()
	config := testConfig()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		Build()

	// Create the base service first (no override).
	if err := ReconcileService(context.Background(), fakeClient, config, owner); err != nil {
		t.Fatalf("initial ReconcileService failed: %v", err)
	}

	// Now add an override and reconcile again — the existing Service must be updated.
	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		Service: &redisv1.PartialService{
			Spec: &redisv1.PartialServiceSpec{
				Ports: []corev1.ServicePort{{Name: "metrics", Port: 9121, TargetPort: intstr.FromInt(9121)}},
			},
		},
	}
	if err := ReconcileService(context.Background(), fakeClient, config, owner); err != nil {
		t.Fatalf("second ReconcileService failed: %v", err)
	}

	svc := &corev1.Service{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, svc); err != nil {
		t.Fatalf("Service not found: %v", err)
	}
	if !hasServicePort(svc, "metrics") {
		t.Error("expected metrics port after override update")
	}
	if !hasServicePort(svc, "client") || !hasServicePort(svc, "gossip") {
		t.Error("expected default ports to be preserved after update")
	}
}

func TestReconcileService_RemovingOverrideReverts(t *testing.T) {
	owner := testOwner()
	config := testConfig()
	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		Service: &redisv1.PartialService{
			Spec: &redisv1.PartialServiceSpec{
				Ports: []corev1.ServicePort{{Name: "metrics", Port: 9121, TargetPort: intstr.FromInt(9121)}},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		Build()

	if err := ReconcileService(context.Background(), fakeClient, config, owner); err != nil {
		t.Fatalf("ReconcileService with override failed: %v", err)
	}

	// Remove the override and reconcile again.
	config.Spec.Override = nil
	if err := ReconcileService(context.Background(), fakeClient, config, owner); err != nil {
		t.Fatalf("ReconcileService without override failed: %v", err)
	}

	svc := &corev1.Service{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, svc); err != nil {
		t.Fatalf("Service not found: %v", err)
	}
	if hasServicePort(svc, "metrics") {
		t.Error("expected metrics port to be removed after override removal")
	}
}

// --- UpdateStatefulSetTemplate: full override application on an existing object ---

// createClusterStatefulSet creates the cluster objects and returns the fake client
// with an existing StatefulSet ready to be updated.
func createClusterStatefulSet(t *testing.T, config *redisv1.RedkeyConfig, owner *redisv1.Redkey) client.Client {
	t.Helper()
	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		WithStatusSubresource(&redisv1.RedkeyConfig{}).
		Build()
	if err := EnsureClusterObjects(context.Background(), fakeClient, config, owner, ""); err != nil {
		t.Fatalf("EnsureClusterObjects failed: %v", err)
	}
	return fakeClient
}

func getStatefulSet(t *testing.T, c client.Client) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, sts); err != nil {
		t.Fatalf("StatefulSet not found: %v", err)
	}
	return sts
}

// TestUpdateStatefulSetTemplate_SyncsTopLevelMetadata verifies that adding a
// StatefulSet override metadata (labels/annotations) to an EXISTING cluster is
// reconciled onto the StatefulSet's own metadata, not just the pod template.
func TestUpdateStatefulSetTemplate_SyncsTopLevelMetadata(t *testing.T) {
	owner := testOwner()
	config := testConfig()
	fakeClient := createClusterStatefulSet(t, config, owner)

	// Add an override after creation and run the update path used by upgrades.
	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		StatefulSet: &redisv1.PartialStatefulSet{
			Metadata: metav1.ObjectMeta{
				Labels:      map[string]string{"inditex.dev/test": "test"},
				Annotations: map[string]string{"traffic.inditex.dev/weight": "10"},
			},
		},
	}
	if err := UpdateStatefulSetTemplate(context.Background(), fakeClient, "test-cluster", "default", config, owner, "checksum-1"); err != nil {
		t.Fatalf("UpdateStatefulSetTemplate failed: %v", err)
	}

	sts := getStatefulSet(t, fakeClient)
	if sts.Labels["inditex.dev/test"] != "test" {
		t.Errorf("expected override label on StatefulSet metadata, got %v", sts.Labels)
	}
	if sts.Annotations["traffic.inditex.dev/weight"] != "10" {
		t.Errorf("expected override annotation on StatefulSet metadata, got %v", sts.Annotations)
	}
	// The cluster identity label must be preserved.
	if sts.Labels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected cluster label preserved, got %v", sts.Labels)
	}
}

// TestUpdateStatefulSetTemplate_MergesPodTemplateMetadata verifies the labels /
// annotations precedence on the pod template: a pod-template override BLOCK-REPLACES
// spec.labels / spec.annotations (the spec entries are discarded), while the internal
// cluster selector labels and the config checksum always win.
func TestUpdateStatefulSetTemplate_MergesPodTemplateMetadata(t *testing.T) {
	owner := testOwner()
	config := testConfig()
	specLabels := map[string]string{"team": "a-team"}
	specAnnotations := map[string]string{"custom-annotation": "custom-value"}
	config.Spec.Labels = &specLabels
	config.Spec.Annotations = &specAnnotations
	fakeClient := createClusterStatefulSet(t, config, owner)

	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		StatefulSet: &redisv1.PartialStatefulSet{
			Spec: &redisv1.PartialStatefulSetSpec{
				Template: &redisv1.PartialPodTemplateSpec{
					Metadata: metav1.ObjectMeta{
						Labels:      map[string]string{"inditex.dev/test": "test"},
						Annotations: map[string]string{"sidecar.io/inject": "true"},
					},
				},
			},
		},
	}
	if err := UpdateStatefulSetTemplate(context.Background(), fakeClient, "test-cluster", "default", config, owner, "checksum-1"); err != nil {
		t.Fatalf("UpdateStatefulSetTemplate failed: %v", err)
	}

	sts := getStatefulSet(t, fakeClient)
	podLabels := sts.Spec.Template.Labels
	// Override label is present.
	if podLabels["inditex.dev/test"] != "test" {
		t.Errorf("expected override pod label, got %v", podLabels)
	}
	// Internal cluster selector label is always preserved.
	if podLabels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected cluster selector label preserved on pods, got %v", podLabels)
	}
	// Block replacement: spec.labels are discarded once the override defines labels.
	if _, ok := podLabels["team"]; ok {
		t.Errorf("expected spec.labels to be discarded by override, got %v", podLabels)
	}

	podAnnotations := sts.Spec.Template.Annotations
	if podAnnotations["sidecar.io/inject"] != "true" {
		t.Errorf("expected override pod annotation, got %v", podAnnotations)
	}
	// Block replacement: spec.annotations are discarded once the override defines annotations.
	if _, ok := podAnnotations["custom-annotation"]; ok {
		t.Errorf("expected spec.annotations to be discarded by override, got %v", podAnnotations)
	}
	// The config checksum must always be stamped.
	if podAnnotations[ConfigChecksumAnnotation] != "checksum-1" {
		t.Errorf("expected config checksum annotation, got %v", podAnnotations)
	}
}

// TestUpdateStatefulSetTemplate_AppliesPodSpecOverride verifies that a full pod
// spec override (sidecars, tolerations, topology spread, container env) is applied
// to the existing StatefulSet while the cluster identity fields are preserved.
func TestUpdateStatefulSetTemplate_AppliesPodSpecOverride(t *testing.T) {
	owner := testOwner()
	config := testConfig()
	fakeClient := createClusterStatefulSet(t, config, owner)

	existing := getStatefulSet(t, fakeClient)
	wantReplicas := *existing.Spec.Replicas
	wantServiceName := existing.Spec.ServiceName

	grace := int64(10)
	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		StatefulSet: &redisv1.PartialStatefulSet{
			Spec: &redisv1.PartialStatefulSetSpec{
				Template: &redisv1.PartialPodTemplateSpec{
					Spec: redisv1.PartialPodSpec{
						TerminationGracePeriodSeconds: &grace,
						Tolerations: []corev1.Toleration{
							{Key: "test", Operator: corev1.TolerationOpEqual, Value: "test", Effect: corev1.TaintEffectNoSchedule},
						},
						TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
							{MaxSkew: 1, TopologyKey: "kubernetes.io/hostname", WhenUnsatisfiable: corev1.DoNotSchedule},
						},
						Containers: []corev1.Container{
							{Name: "nginx", Image: "nginx"},
							{Name: "redis", Env: []corev1.EnvVar{{Name: "test", Value: "test"}}},
						},
					},
				},
			},
		},
	}
	if err := UpdateStatefulSetTemplate(context.Background(), fakeClient, "test-cluster", "default", config, owner, "checksum-1"); err != nil {
		t.Fatalf("UpdateStatefulSetTemplate failed: %v", err)
	}

	sts := getStatefulSet(t, fakeClient)
	podSpec := sts.Spec.Template.Spec

	if podSpec.TerminationGracePeriodSeconds == nil || *podSpec.TerminationGracePeriodSeconds != 10 {
		t.Errorf("expected terminationGracePeriodSeconds=10, got %v", podSpec.TerminationGracePeriodSeconds)
	}
	if len(podSpec.Tolerations) != 1 || podSpec.Tolerations[0].Key != "test" {
		t.Errorf("expected test toleration, got %v", podSpec.Tolerations)
	}
	if len(podSpec.TopologySpreadConstraints) != 1 {
		t.Errorf("expected one topology spread constraint, got %v", podSpec.TopologySpreadConstraints)
	}
	if !hasContainer(sts, "nginx") {
		t.Error("expected nginx sidecar to be added")
	}
	// The redis container must remain at index 0 with the override env applied.
	if podSpec.Containers[0].Name != "redis" {
		t.Fatalf("expected redis container at index 0, got %q", podSpec.Containers[0].Name)
	}
	if len(podSpec.Containers[0].Env) == 0 || podSpec.Containers[0].Env[0].Name != "test" {
		t.Errorf("expected env override on redis container, got %v", podSpec.Containers[0].Env)
	}

	// Identity preserved.
	if *sts.Spec.Replicas != wantReplicas {
		t.Errorf("expected replicas %d preserved, got %d", wantReplicas, *sts.Spec.Replicas)
	}
	if sts.Spec.ServiceName != wantServiceName {
		t.Errorf("expected serviceName %q preserved, got %q", wantServiceName, sts.Spec.ServiceName)
	}
	if sts.Spec.Selector.MatchLabels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected selector preserved, got %v", sts.Spec.Selector.MatchLabels)
	}
	// Update strategy must be OnDelete for controlled upgrade recycling.
	if sts.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		t.Errorf("expected OnDelete update strategy, got %q", sts.Spec.UpdateStrategy.Type)
	}
}

// TestUpdateStatefulSetTemplate_RemovingOverrideReverts verifies that removing a
// previously-set override reverts the StatefulSet to its base desired state.
func TestUpdateStatefulSetTemplate_RemovingOverrideReverts(t *testing.T) {
	owner := testOwner()
	config := testConfig()
	config.Spec.Override = &redisv1.RedkeyOverrideSpec{
		StatefulSet: &redisv1.PartialStatefulSet{
			Metadata: metav1.ObjectMeta{Labels: map[string]string{"inditex.dev/test": "test"}},
			Spec: &redisv1.PartialStatefulSetSpec{
				Template: &redisv1.PartialPodTemplateSpec{
					Spec: redisv1.PartialPodSpec{
						Tolerations: []corev1.Toleration{{Key: "test", Operator: corev1.TolerationOpExists}},
					},
				},
			},
		},
	}
	fakeClient := createClusterStatefulSet(t, config, owner)

	// Remove the override and update.
	config.Spec.Override = nil
	if err := UpdateStatefulSetTemplate(context.Background(), fakeClient, "test-cluster", "default", config, owner, "checksum-2"); err != nil {
		t.Fatalf("UpdateStatefulSetTemplate failed: %v", err)
	}

	sts := getStatefulSet(t, fakeClient)
	if _, ok := sts.Labels["inditex.dev/test"]; ok {
		t.Errorf("expected override label removed from StatefulSet metadata, got %v", sts.Labels)
	}
	if len(sts.Spec.Template.Spec.Tolerations) != 0 {
		t.Errorf("expected tolerations removed after override removal, got %v", sts.Spec.Template.Spec.Tolerations)
	}
}

// --- helpers ---

func hasVolume(sts *appsv1.StatefulSet, name string) bool {
	for _, v := range sts.Spec.Template.Spec.Volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

func hasContainer(sts *appsv1.StatefulSet, name string) bool {
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == name {
			return true
		}
	}
	return false
}

func hasServicePort(svc *corev1.Service, name string) bool {
	for _, p := range svc.Spec.Ports {
		if p.Name == name {
			return true
		}
	}
	return false
}

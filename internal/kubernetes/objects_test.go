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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

var testScheme = runtime.NewScheme()

func init() {
	_ = clientgoscheme.AddToScheme(testScheme)
	_ = redisv1.AddToScheme(testScheme)
}

func testOwner() *redisv1.RedkeyCluster {
	return &redisv1.RedkeyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       "test-uid-1234",
		},
		Spec: redisv1.RedkeyClusterSpec{
			Primaries:          3,
			ReplicasPerPrimary: 1,
			Ephemeral:          true,
			Image:              "redis:7",
			AccessModes:        []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}
}

func testConfig() *redisv1.RedkeyClusterConfig {
	return &redisv1.RedkeyClusterConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-config-1",
			Namespace: "default",
		},
		Spec: redisv1.RedkeyClusterConfigSpec{
			Sequence:           1,
			Primaries:          3,
			ReplicasPerPrimary: 1,
			Ephemeral:          true,
			Image:              "redis:7",
		},
	}
}

func TestBuildConfigMap_Basic(t *testing.T) {
	config := testConfig()
	cm := buildConfigMap("test-cluster", "default", config, "")

	if cm.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", cm.Name)
	}
	if cm.Namespace != "default" {
		t.Fatalf("expected namespace 'default', got '%s'", cm.Namespace)
	}
	if _, ok := cm.Data["redis.conf"]; !ok {
		t.Fatal("expected redis.conf key in ConfigMap data")
	}
}

func TestBuildConfigMap_WithPassword(t *testing.T) {
	config := testConfig()
	cm := buildConfigMap("test-cluster", "default", config, "secret123")

	conf := cm.Data["redis.conf"]
	if conf == "" {
		t.Fatal("expected non-empty redis.conf")
	}
	// Should contain requirepass and masterauth
	if !contains(conf, "requirepass secret123") {
		t.Error("expected requirepass in redis.conf")
	}
	if !contains(conf, "masterauth secret123") {
		t.Error("expected masterauth in redis.conf")
	}
}

func TestBuildConfigMap_Ephemeral(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = true
	cm := buildConfigMap("test-cluster", "default", config, "")

	conf := cm.Data["redis.conf"]
	if !contains(conf, "appendonly no") {
		t.Error("expected 'appendonly no' for ephemeral cluster")
	}
	if !contains(conf, "save \"\"") {
		t.Error("expected 'save \"\"' for ephemeral cluster")
	}
}

func TestBuildConfigMap_NonEphemeral(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = false
	cm := buildConfigMap("test-cluster", "default", config, "")

	conf := cm.Data["redis.conf"]
	if !contains(conf, "appendonly yes") {
		t.Error("expected 'appendonly yes' for non-ephemeral cluster")
	}
}

func TestBuildService(t *testing.T) {
	svc := buildService("test-cluster", "default")

	if svc.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", svc.Name)
	}
	if svc.Spec.ClusterIP != "None" {
		t.Fatalf("expected headless service (ClusterIP=None), got '%s'", svc.Spec.ClusterIP)
	}
	if len(svc.Spec.Ports) != 2 {
		t.Fatalf("expected 2 ports, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].Port != RedisPort {
		t.Errorf("expected port %d, got %d", RedisPort, svc.Spec.Ports[0].Port)
	}
	if !svc.Spec.PublishNotReadyAddresses {
		t.Error("expected PublishNotReadyAddresses=true")
	}
}

func TestBuildStatefulSet_Ephemeral(t *testing.T) {
	config := testConfig()
	config.Spec.Primaries = 3
	config.Spec.ReplicasPerPrimary = 1
	config.Spec.Image = "redis:7.2"
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	expectedReplicas := int32(6) // 3 + 3*1
	if *sts.Spec.Replicas != expectedReplicas {
		t.Fatalf("expected %d replicas, got %d", expectedReplicas, *sts.Spec.Replicas)
	}
	if sts.Spec.Template.Spec.Containers[0].Image != "redis:7.2" {
		t.Fatalf("expected image 'redis:7.2', got '%s'", sts.Spec.Template.Spec.Containers[0].Image)
	}
	if sts.Spec.ServiceName != "test-cluster" {
		t.Fatalf("expected serviceName 'test-cluster', got '%s'", sts.Spec.ServiceName)
	}
	// Ephemeral: no VolumeClaimTemplates
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("expected no VolumeClaimTemplates for ephemeral, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy != nil {
		t.Fatal("expected no retention policy for ephemeral cluster")
	}
}

func TestBuildStatefulSet_WithStorage(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = false
	config.Spec.Storage = "10Gi"
	config.Spec.StorageClassName = "fast-ssd"
	deletePVC := true
	config.Spec.DeletePVC = &deletePVC

	owner := testOwner()
	owner.Spec.Ephemeral = false
	owner.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("expected 1 VolumeClaimTemplate, got %d", len(sts.Spec.VolumeClaimTemplates))
	}
	pvc := sts.Spec.VolumeClaimTemplates[0]
	if pvc.Name != "data" {
		t.Fatalf("expected PVC name 'data', got '%s'", pvc.Name)
	}
	expectedStorage := resource.MustParse("10Gi")
	actualStorage := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	if !actualStorage.Equal(expectedStorage) {
		t.Fatalf("expected storage '%s', got '%s'", expectedStorage.String(), actualStorage.String())
	}
	if *pvc.Spec.StorageClassName != "fast-ssd" {
		t.Fatalf("expected storageClassName 'fast-ssd', got '%s'", *pvc.Spec.StorageClassName)
	}
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != corev1.ReadWriteOnce {
		t.Fatalf("expected AccessModes [ReadWriteOnce], got %v", pvc.Spec.AccessModes)
	}

	// Check retention policy
	if sts.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		t.Fatal("expected retention policy")
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected WhenDeleted=Delete, got '%s'", sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	}
	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected WhenScaled=Retain, got '%s'", sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	}

	// Check data volume mount
	found := false
	for _, vm := range sts.Spec.Template.Spec.Containers[0].VolumeMounts {
		if vm.Name == "data" && vm.MountPath == "/data" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected data volume mount at /data")
	}
}

func TestBuildStatefulSet_RetainPVC(t *testing.T) {
	config := testConfig()
	config.Spec.Ephemeral = false
	config.Spec.Storage = "5Gi"
	deletePVC := false
	config.Spec.DeletePVC = &deletePVC

	owner := testOwner()
	owner.Spec.Ephemeral = false

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatalf("expected WhenDeleted=Retain, got '%s'", sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	}
}

func TestBuildStatefulSet_DefaultImage(t *testing.T) {
	config := testConfig()
	config.Spec.Image = ""
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if sts.Spec.Template.Spec.Containers[0].Image != defaultRedisImage {
		t.Fatalf("expected default image '%s', got '%s'", defaultRedisImage, sts.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestBuildStatefulSet_WithLabels(t *testing.T) {
	config := testConfig()
	customLabels := map[string]string{
		"app.kubernetes.io/team": "platform",
		"environment":            "test",
	}
	config.Spec.Labels = &customLabels
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	// Verify custom labels on StatefulSet ObjectMeta
	if sts.Labels["app.kubernetes.io/team"] != "platform" {
		t.Errorf("expected StatefulSet label 'app.kubernetes.io/team'='platform', got '%s'", sts.Labels["app.kubernetes.io/team"])
	}
	if sts.Labels["environment"] != "test" {
		t.Errorf("expected StatefulSet label 'environment'='test', got '%s'", sts.Labels["environment"])
	}

	// Verify custom labels on pod template
	podLabels := sts.Spec.Template.Labels
	if podLabels["app.kubernetes.io/team"] != "platform" {
		t.Errorf("expected pod template label 'app.kubernetes.io/team'='platform', got '%s'", podLabels["app.kubernetes.io/team"])
	}
	if podLabels["environment"] != "test" {
		t.Errorf("expected pod template label 'environment'='test', got '%s'", podLabels["environment"])
	}

	// Verify base labels are still present
	if podLabels[ClusterLabel] != "test-cluster" {
		t.Errorf("expected base ClusterLabel, got '%s'", podLabels[ClusterLabel])
	}
	if podLabels[ComponentLabel] != ComponentRedis {
		t.Errorf("expected base ComponentLabel, got '%s'", podLabels[ComponentLabel])
	}

	// Verify selector does NOT include custom labels (immutable after creation)
	selectorLabels := sts.Spec.Selector.MatchLabels
	if _, exists := selectorLabels["app.kubernetes.io/team"]; exists {
		t.Error("selector should not include custom labels")
	}
}

func TestBuildStatefulSet_WithAnnotations(t *testing.T) {
	config := testConfig()
	customAnnotations := map[string]string{
		"prometheus.io/scrape": "true",
		"prometheus.io/port":   "9121",
	}
	config.Spec.Annotations = &customAnnotations
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	// Verify custom annotations on StatefulSet ObjectMeta
	if sts.Annotations["prometheus.io/scrape"] != "true" {
		t.Errorf("expected StatefulSet annotation 'prometheus.io/scrape'='true', got '%s'", sts.Annotations["prometheus.io/scrape"])
	}
	if sts.Annotations["prometheus.io/port"] != "9121" {
		t.Errorf("expected StatefulSet annotation 'prometheus.io/port'='9121', got '%s'", sts.Annotations["prometheus.io/port"])
	}

	// Verify custom annotations on pod template
	podAnnotations := sts.Spec.Template.Annotations
	if podAnnotations["prometheus.io/scrape"] != "true" {
		t.Errorf("expected pod template annotation 'prometheus.io/scrape'='true', got '%s'", podAnnotations["prometheus.io/scrape"])
	}
	if podAnnotations["prometheus.io/port"] != "9121" {
		t.Errorf("expected pod template annotation 'prometheus.io/port'='9121', got '%s'", podAnnotations["prometheus.io/port"])
	}
}

func TestBuildStatefulSet_WithLabelsAndAnnotations(t *testing.T) {
	config := testConfig()
	labels := map[string]string{"team": "infra"}
	annotations := map[string]string{"note": "test"}
	config.Spec.Labels = &labels
	config.Spec.Annotations = &annotations
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	// Labels
	if sts.Spec.Template.Labels["team"] != "infra" {
		t.Errorf("expected pod template label 'team'='infra', got '%s'", sts.Spec.Template.Labels["team"])
	}
	// Annotations
	if sts.Spec.Template.Annotations["note"] != "test" {
		t.Errorf("expected pod template annotation 'note'='test', got '%s'", sts.Spec.Template.Annotations["note"])
	}
}

func TestBuildStatefulSet_NoAnnotations(t *testing.T) {
	config := testConfig()
	// No annotations set
	owner := testOwner()

	sts := buildStatefulSet("test-cluster", "default", config, owner)

	if sts.Annotations != nil {
		t.Errorf("expected nil annotations on StatefulSet, got %v", sts.Annotations)
	}
	if sts.Spec.Template.Annotations != nil {
		t.Errorf("expected nil annotations on pod template, got %v", sts.Spec.Template.Annotations)
	}
}

func TestEnsureClusterObjects_CreatesAll(t *testing.T) {
	owner := testOwner()
	config := testConfig()

	fakeClient := fake.NewClientBuilder().
		WithScheme(testScheme).
		WithObjects(owner).
		WithStatusSubresource(&redisv1.RedkeyClusterConfig{}).
		Build()

	err := EnsureClusterObjects(context.Background(), fakeClient, config, owner, "")
	if err != nil {
		t.Fatalf("EnsureClusterObjects failed: %v", err)
	}

	// Verify ConfigMap exists
	cm := &corev1.ConfigMap{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, cm); err != nil {
		t.Fatalf("ConfigMap not created: %v", err)
	}

	// Verify Service exists
	svc := &corev1.Service{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, svc); err != nil {
		t.Fatalf("Service not created: %v", err)
	}

	// Verify StatefulSet exists
	sts := &appsv1.StatefulSet{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, sts); err != nil {
		t.Fatalf("StatefulSet not created: %v", err)
	}
}

func TestGetRedisPassword_NoSecret(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

	password, err := GetRedisPassword(context.Background(), fakeClient, "", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if password != "" {
		t.Fatalf("expected empty password, got '%s'", password)
	}
}

func TestGetRedisPassword_WithSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-auth",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"requirepass": []byte("mysecret"),
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(secret).Build()

	password, err := GetRedisPassword(context.Background(), fakeClient, "redis-auth", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if password != "mysecret" {
		t.Fatalf("expected 'mysecret', got '%s'", password)
	}
}

func TestClusterLabels(t *testing.T) {
	labels := clusterLabels("my-cluster")
	if labels[ClusterLabel] != "my-cluster" {
		t.Fatalf("expected ClusterLabel='my-cluster', got '%s'", labels[ClusterLabel])
	}
	if labels[ComponentLabel] != ComponentRedis {
		t.Fatalf("expected ComponentLabel='%s', got '%s'", ComponentRedis, labels[ComponentLabel])
	}
}

func TestAllPodsReady_StatefulSetNotFound(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

	_, err := AllPodsReady(context.Background(), fakeClient, "missing", "default", 3)
	if err == nil {
		t.Fatal("expected error for missing StatefulSet")
	}
}

func TestAllPodsReady_NotEnoughReplicas(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 1},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	ready, err := AllPodsReady(context.Background(), fakeClient, "test-cluster", "default", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ready {
		t.Fatal("expected not ready with 1/3 replicas")
	}
}

func TestAllPodsReady_AllReady(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithObjects(sts).Build()

	ready, err := AllPodsReady(context.Background(), fakeClient, "test-cluster", "default", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ready {
		t.Fatal("expected ready with 3/3 replicas")
	}
}

func TestGetPodAddresses_NoPods(t *testing.T) {
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).Build()

	addrs, err := GetPodAddresses(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 0 {
		t.Fatalf("expected 0 addresses, got %d", len(addrs))
	}
}

func TestGetPodAddresses_WithPods(t *testing.T) {
	pods := []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cluster-0", Namespace: "default",
				Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
			},
			Status: corev1.PodStatus{PodIP: "10.0.0.1"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cluster-1", Namespace: "default",
				Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
			},
			Status: corev1.PodStatus{PodIP: "10.0.0.2"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-cluster-2", Namespace: "default",
				Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
			},
			Status: corev1.PodStatus{PodIP: ""}, // No IP yet
		},
	}
	objs := make([]runtime.Object, len(pods))
	for i := range pods {
		objs[i] = &pods[i]
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithRuntimeObjects(objs...).Build()

	addrs, err := GetPodAddresses(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only 2 pods have IPs
	if len(addrs) != 2 {
		t.Fatalf("expected 2 addresses, got %d", len(addrs))
	}
	if addrs["test-cluster-0"] != "10.0.0.1:6379" {
		t.Fatalf("expected '10.0.0.1:6379', got '%s'", addrs["test-cluster-0"])
	}
	if addrs["test-cluster-1"] != "10.0.0.2:6379" {
		t.Fatalf("expected '10.0.0.2:6379', got '%s'", addrs["test-cluster-1"])
	}
}

func TestGetPodAddresses_IgnoresOtherClusters(t *testing.T) {
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-cluster-0", Namespace: "default",
			Labels: map[string]string{ClusterLabel: "test-cluster", ComponentLabel: ComponentRedis},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.1"},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "other-cluster-0", Namespace: "default",
			Labels: map[string]string{ClusterLabel: "other-cluster", ComponentLabel: ComponentRedis},
		},
		Status: corev1.PodStatus{PodIP: "10.0.0.99"},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(testScheme).WithRuntimeObjects(pod1, pod2).Build()

	addrs, err := GetPodAddresses(context.Background(), fakeClient, "test-cluster", "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("expected 1 address, got %d", len(addrs))
	}
}

func TestBuildPDB(t *testing.T) {
	cfg := testConfig()
	cfg.Spec.Pdb.PdbSizeUnavailable = intstr.FromInt(1)

	pdb := buildPDB("test-cluster", "test-cluster", "default", cfg)

	if pdb.Name != "test-cluster" {
		t.Fatalf("expected name 'test-cluster', got '%s'", pdb.Name)
	}
	if pdb.Namespace != "default" {
		t.Fatalf("expected namespace 'default', got '%s'", pdb.Namespace)
	}
	if pdb.Spec.MaxUnavailable == nil {
		t.Fatal("expected MaxUnavailable to be set")
	}
	if pdb.Spec.MaxUnavailable.String() != "1" {
		t.Fatalf("expected MaxUnavailable='1', got '%s'", pdb.Spec.MaxUnavailable.String())
	}
}

func TestBuildPDB_MinAvailable(t *testing.T) {
	cfg := testConfig()
	cfg.Spec.Pdb.PdbSizeAvailable = intstr.FromInt(2)

	pdb := buildPDB("test-cluster", "test-cluster", "default", cfg)

	if pdb.Spec.MinAvailable == nil {
		t.Fatal("expected MinAvailable to be set")
	}
	if pdb.Spec.MinAvailable.String() != "2" {
		t.Fatalf("expected MinAvailable='2', got '%s'", pdb.Spec.MinAvailable.String())
	}
}

// --- Helpers ---

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

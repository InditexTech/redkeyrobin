// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package kubernetes

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

const (
	// RedisPort is the default Redis port.
	RedisPort = 6379
	// RedisGossipPort is the Redis cluster gossip port.
	RedisGossipPort = 16379
	// ClusterLabel is the label used to identify resources belonging to a cluster.
	ClusterLabel = "redkey.inditex.dev/cluster"
	// ComponentLabel is the label used to identify the component type.
	ComponentLabel = "redkey.inditex.dev/component"
	// ComponentRedis is the value for the component label for Redis nodes.
	ComponentRedis = "redis"

	// defaultRedisImage is used when no image is specified.
	defaultRedisImage = "redis:8"
)

// EnsureClusterObjects creates or updates all Kubernetes objects required for the Redis cluster.
// It uses the RedkeyCluster as the owner reference so objects are garbage-collected on deletion.
func EnsureClusterObjects(
	ctx context.Context,
	c client.Client,
	config *redisv1.RedkeyClusterConfig,
	owner *redisv1.RedkeyCluster,
	password string,
) error {
	clusterName := owner.Name
	namespace := owner.Namespace

	// 1. ConfigMap
	if err := ensureConfigMap(ctx, c, clusterName, namespace, config, owner, password); err != nil {
		return fmt.Errorf("ensuring ConfigMap: %w", err)
	}

	// 2. Service
	if err := ensureService(ctx, c, clusterName, namespace, config, owner); err != nil {
		return fmt.Errorf("ensuring Service: %w", err)
	}

	// 3. StatefulSet
	if err := ensureStatefulSet(ctx, c, clusterName, namespace, config, owner); err != nil {
		return fmt.Errorf("ensuring StatefulSet: %w", err)
	}

	// 4. PodDisruptionBudget (optional)
	if config.Spec.Pdb.Enabled && config.Spec.Primaries > 1 {
		if err := ensurePDB(ctx, c, clusterName, namespace, config, owner); err != nil {
			return fmt.Errorf("ensuring PDB: %w", err)
		}
	}

	return nil
}

// GetRedisPassword reads the auth secret and returns the password, or empty string if no secret is configured.
func GetRedisPassword(ctx context.Context, c client.Client, secretName, namespace string) (string, error) {
	if secretName == "" {
		return "", nil
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		return "", fmt.Errorf("getting auth secret %s: %w", secretName, err)
	}
	if val, ok := secret.Data["requirepass"]; ok {
		return string(val), nil
	}
	return "", nil
}

// AllPodsReady checks whether the StatefulSet has all pods in Ready state.
func AllPodsReady(ctx context.Context, c client.Client, clusterName, namespace string, expectedReplicas int32) (bool, error) {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return false, err
	}
	return sts.Status.ReadyReplicas >= expectedReplicas, nil
}

// GetStatefulSetReplicas returns the desired replica count currently configured on the
// cluster's StatefulSet. It returns 0 if the StatefulSet has no explicit replica count.
func GetStatefulSetReplicas(ctx context.Context, c client.Client, clusterName, namespace string) (int32, error) {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return 0, err
	}
	if sts.Spec.Replicas == nil {
		return 0, nil
	}
	return *sts.Spec.Replicas, nil
}

// ScaleStatefulSet sets the desired replica count on the cluster's StatefulSet.
// Robin owns the StatefulSet replica count throughout scaling operations, so this is
// the single point where the pod count is changed. It is a no-op when the StatefulSet
// already has the requested replica count.
func ScaleStatefulSet(ctx context.Context, c client.Client, clusterName, namespace string, replicas int32) error {
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts); err != nil {
		return fmt.Errorf("getting StatefulSet %s: %w", clusterName, err)
	}
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == replicas {
		return nil
	}
	sts.Spec.Replicas = &replicas
	if err := c.Update(ctx, sts); err != nil {
		return fmt.Errorf("scaling StatefulSet %s to %d replicas: %w", clusterName, replicas, err)
	}
	return nil
}

// StatefulSetExists reports whether the cluster's StatefulSet currently exists.
func StatefulSetExists(ctx context.Context, c client.Client, clusterName, namespace string) (bool, error) {
	sts := &appsv1.StatefulSet{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, sts)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// DeleteStatefulSet deletes the cluster's StatefulSet. It is used by fast scaling, which
// wipes and recreates the StatefulSet. Missing StatefulSets are treated as success.
func DeleteStatefulSet(ctx context.Context, c client.Client, clusterName, namespace string) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
	}
	if err := c.Delete(ctx, sts); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting StatefulSet %s: %w", clusterName, err)
	}
	return nil
}

// GetPodAddresses returns a map of pod name → IP:port for the cluster's StatefulSet pods.
// This uses pod IPs directly, which works both in-cluster and out-of-cluster (e.g. kind dev).
func GetPodAddresses(ctx context.Context, c client.Client, clusterName, namespace string) (map[string]string, error) {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(namespace),
		client.MatchingLabels(clusterLabels(clusterName)),
	); err != nil {
		return nil, fmt.Errorf("listing cluster pods: %w", err)
	}

	addrs := make(map[string]string, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.PodIP == "" {
			continue
		}
		addrs[pod.Name] = fmt.Sprintf("%s:%d", pod.Status.PodIP, RedisPort)
	}
	return addrs, nil
}

// --- ConfigMap ---

func ensureConfigMap(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyClusterConfig, owner *redisv1.RedkeyCluster, password string) error {
	desired := buildConfigMap(clusterName, namespace, config, password)
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &corev1.ConfigMap{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	// Update if data changed
	if existing.Data["redis.conf"] != desired.Data["redis.conf"] {
		existing.Data = desired.Data
		return c.Update(ctx, existing)
	}
	return nil
}

func buildConfigMap(clusterName, namespace string, config *redisv1.RedkeyClusterConfig, password string) *corev1.ConfigMap {
	redisConf := buildRedisConf(config.Spec.RedisConfig, password, config.Spec.Ephemeral, config.Spec.ReplicasPerPrimary)
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
			Labels:    clusterLabels(clusterName),
		},
		Data: map[string]string{"redis.conf": redisConf},
	}
}

func buildRedisConf(userConfig, password string, ephemeral bool, replicasPerPrimary int32) string {
	// Start with cluster-required defaults
	lines := []string{
		"cluster-enabled yes",
		"cluster-config-file nodes.conf",
		"cluster-node-timeout 5000",
	}

	if ephemeral {
		lines = append(lines, "appendonly no", "save \"\"")
	} else {
		lines = append(lines, "appendonly yes")
	}

	if replicasPerPrimary > 0 {
		lines = append(lines, "cluster-allow-reads-when-down yes")
	}

	// Add user config
	if userConfig != "" {
		lines = append(lines, userConfig)
	}

	// Add auth
	if password != "" {
		lines = append(lines, fmt.Sprintf("requirepass %s", password))
		lines = append(lines, fmt.Sprintf("masterauth %s", password))
	}

	return strings.Join(lines, "\n") + "\n"
}

// --- Service ---

func ensureService(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyClusterConfig, owner *redisv1.RedkeyCluster) error {
	desired := buildService(clusterName, namespace)
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &corev1.Service{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	return err
}

func buildService(clusterName, namespace string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
			Labels:    clusterLabels(clusterName),
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name:       "client",
					Protocol:   corev1.ProtocolTCP,
					Port:       RedisPort,
					TargetPort: intstr.FromInt(RedisPort),
				},
				{
					Name:       "gossip",
					Protocol:   corev1.ProtocolTCP,
					Port:       RedisGossipPort,
					TargetPort: intstr.FromInt(RedisGossipPort),
				},
			},
			Selector:                 clusterLabels(clusterName),
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
		},
	}
}

// --- StatefulSet ---

func ensureStatefulSet(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyClusterConfig, owner *redisv1.RedkeyCluster) error {
	desired := buildStatefulSet(clusterName, namespace, config, owner)
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &appsv1.StatefulSet{}
	err := c.Get(ctx, types.NamespacedName{Name: clusterName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	return err
}

func buildStatefulSet(clusterName, namespace string, config *redisv1.RedkeyClusterConfig, owner *redisv1.RedkeyCluster) *appsv1.StatefulSet {
	replicas := int32(config.Spec.Primaries + (config.Spec.Primaries * config.Spec.ReplicasPerPrimary))
	image := config.Spec.Image
	if image == "" {
		image = defaultRedisImage
	}

	labels := clusterLabels(clusterName)
	podManagement := appsv1.ParallelPodManagement

	// Build pod labels: base labels + custom labels from spec.
	podLabels := clusterLabels(clusterName)
	if config.Spec.Labels != nil {
		for k, v := range *config.Spec.Labels {
			podLabels[k] = v
		}
	}

	// Build pod annotations from spec.
	var podAnnotations map[string]string
	if config.Spec.Annotations != nil {
		podAnnotations = make(map[string]string, len(*config.Spec.Annotations))
		for k, v := range *config.Spec.Annotations {
			podAnnotations[k] = v
		}
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        clusterName,
			Namespace:   namespace,
			Labels:      podLabels,
			Annotations: podAnnotations,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:            &replicas,
			PodManagementPolicy: podManagement,
			ServiceName:         clusterName,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      podLabels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "redis",
							Image: image,
							Ports: []corev1.ContainerPort{
								{Name: "client", ContainerPort: RedisPort},
								{Name: "gossip", ContainerPort: RedisGossipPort},
							},
							Command:        []string{"redis-server", "/conf/redis.conf"},
							LivenessProbe:  createProbe(15, 5),
							ReadinessProbe: createProbe(10, 5),
							VolumeMounts: []corev1.VolumeMount{
								{Name: "config", MountPath: "/conf"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: clusterName},
									Items:                []corev1.KeyToPath{{Key: "redis.conf", Path: "redis.conf"}},
								},
							},
						},
					},
				},
			},
		},
	}

	// Set resources if provided
	if config.Spec.Resources != nil {
		sts.Spec.Template.Spec.Containers[0].Resources = *config.Spec.Resources
	}

	// Add storage for non-ephemeral clusters
	if !config.Spec.Ephemeral {
		addStorage(sts, config, owner)
	}

	return sts
}

func addStorage(sts *appsv1.StatefulSet, config *redisv1.RedkeyClusterConfig, owner *redisv1.RedkeyCluster) {
	accessModes := owner.Spec.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(config.Spec.Storage),
				},
			},
		},
	}

	if config.Spec.StorageClassName != "" {
		pvc.Spec.StorageClassName = &config.Spec.StorageClassName
	}

	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{pvc}

	// PVC retention policy: delete PVCs when StatefulSet is deleted if deletePVC is true
	deletePVC := config.Spec.DeletePVC != nil && *config.Spec.DeletePVC
	whenDeleted := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	if deletePVC {
		whenDeleted = appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	}
	sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: whenDeleted,
		WhenScaled:  appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
	}

	// Add data volume mount
	sts.Spec.Template.Spec.Containers[0].VolumeMounts = append(
		sts.Spec.Template.Spec.Containers[0].VolumeMounts,
		corev1.VolumeMount{Name: "data", MountPath: "/data"},
	)
}

// --- PodDisruptionBudget ---

func ensurePDB(ctx context.Context, c client.Client, clusterName, namespace string, config *redisv1.RedkeyClusterConfig, owner *redisv1.RedkeyCluster) error {
	pdbName := clusterName + "-pdb"
	desired := buildPDB(pdbName, clusterName, namespace, config)
	if err := controllerutil.SetOwnerReference(owner, desired, c.Scheme()); err != nil {
		return err
	}

	existing := &policyv1.PodDisruptionBudget{}
	err := c.Get(ctx, types.NamespacedName{Name: pdbName, Namespace: namespace}, existing)
	if errors.IsNotFound(err) {
		return c.Create(ctx, desired)
	}
	return err
}

func buildPDB(pdbName, clusterName, namespace string, config *redisv1.RedkeyClusterConfig) *policyv1.PodDisruptionBudget {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pdbName,
			Namespace: namespace,
			Labels:    clusterLabels(clusterName),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: clusterLabels(clusterName),
			},
		},
	}

	if config.Spec.Pdb.PdbSizeAvailable.String() != "0" && config.Spec.Pdb.PdbSizeAvailable.String() != "" {
		pdb.Spec.MinAvailable = &config.Spec.Pdb.PdbSizeAvailable
	} else if config.Spec.Pdb.PdbSizeUnavailable.String() != "0" && config.Spec.Pdb.PdbSizeUnavailable.String() != "" {
		pdb.Spec.MaxUnavailable = &config.Spec.Pdb.PdbSizeUnavailable
	}

	return pdb
}

// --- Helpers ---

func clusterLabels(clusterName string) map[string]string {
	return map[string]string{
		ClusterLabel:   clusterName,
		ComponentLabel: ComponentRedis,
	}
}

func createProbe(initial, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(RedisPort)},
		},
		InitialDelaySeconds: initial,
		PeriodSeconds:       period,
	}
}

// DeleteService deletes the cluster's headless Service. Missing Services are treated as success.
func DeleteService(ctx context.Context, c client.Client, clusterName, namespace string) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
	}
	if err := c.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting Service %s: %w", clusterName, err)
	}
	return nil
}

// DeleteConfigMap deletes the cluster's ConfigMap. Missing ConfigMaps are treated as success.
func DeleteConfigMap(ctx context.Context, c client.Client, clusterName, namespace string) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
	}
	if err := c.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting ConfigMap %s: %w", clusterName, err)
	}
	return nil
}

// DeletePDB deletes the cluster's PodDisruptionBudget. Missing PDBs are treated as success.
func DeletePDB(ctx context.Context, c client.Client, clusterName, namespace string) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-pdb", Namespace: namespace},
	}
	if err := c.Delete(ctx, pdb); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("deleting PDB %s-pdb: %w", clusterName, err)
	}
	return nil
}

// DeletePVCs deletes all PersistentVolumeClaims belonging to the cluster.
// It uses the cluster label selector to find matching PVCs.
// Missing PVCs are treated as success.
func DeletePVCs(ctx context.Context, c client.Client, clusterName, namespace string) error {
	pvcList := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcList,
		client.InNamespace(namespace),
		client.MatchingLabels(clusterLabels(clusterName)),
	); err != nil {
		return fmt.Errorf("listing PVCs for cluster %s: %w", clusterName, err)
	}

	for i := range pvcList.Items {
		if err := c.Delete(ctx, &pvcList.Items[i]); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("deleting PVC %s: %w", pvcList.Items[i].Name, err)
		}
	}
	return nil
}

// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package kubernetes

import (
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/strategicpatch"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
)

// requiredVolumeNames are the volumes Robin always manages and that an override
// must never remove from the StatefulSet pod template.
var requiredVolumeNames = map[string]struct{}{
	"config": {},
	"data":   {},
}

// requiredServicePortNames are the Service ports Robin always manages and that an
// override must never remove from the headless Service.
var requiredServicePortNames = map[string]struct{}{
	"client": {},
	"gossip": {},
}

// applyStatefulSetOverride merges the user-provided override into the base
// StatefulSet using a Kubernetes strategic merge patch and then restores the
// fields that define the cluster identity so they cannot be changed.
//
// Guard rails (always restored from the base after the merge):
//   - spec.replicas       — driven by the primaries/replicas formula
//   - spec.serviceName    — must match the headless Service
//   - spec.selector       — must keep matching the managed pods
//   - the "redis" container, kept at index 0
//   - the "config" and "data" volumes
//   - spec.volumeClaimTemplates, unless the override sets its own
//
// A nil override returns the base unchanged.
func applyStatefulSetOverride(base *appsv1.StatefulSet, override *redisv1.PartialStatefulSet, clusterName string, specLabels, specAnnotations map[string]string) (*appsv1.StatefulSet, error) {
	if override == nil {
		return base, nil
	}

	// Capture identity and required pieces from the base before merging.
	baseReplicas := base.Spec.Replicas
	baseServiceName := base.Spec.ServiceName
	baseSelector := base.Spec.Selector
	baseContainers := base.Spec.Template.Spec.Containers
	baseVolumes := base.Spec.Template.Spec.Volumes
	baseVCT := base.Spec.VolumeClaimTemplates

	var overrideSpec *redisv1.PartialStatefulSetSpec
	if override.Spec != nil {
		overrideSpec = override.Spec
	}

	patch := struct {
		Metadata metav1.ObjectMeta               `json:"metadata"`
		Spec     *redisv1.PartialStatefulSetSpec `json:"spec,omitempty"`
	}{
		Metadata: override.Metadata,
		Spec:     overrideSpec,
	}

	result := &appsv1.StatefulSet{}
	if err := strategicMerge(base, patch, appsv1.StatefulSet{}, result); err != nil {
		return nil, fmt.Errorf("applying StatefulSet override: %w", err)
	}

	// --- Guard rails: restore cluster-identity fields. ---
	result.Spec.Replicas = baseReplicas
	result.Spec.ServiceName = baseServiceName
	result.Spec.Selector = baseSelector

	restoreRedisContainer(result, baseContainers)
	restoreRequiredVolumes(result, baseVolumes)

	// Storage is owned by the operator; only honour override-provided templates.
	if overrideSpec == nil || len(overrideSpec.VolumeClaimTemplates) == 0 {
		result.Spec.VolumeClaimTemplates = baseVCT
	}

	// --- Labels / annotations precedence. ---
	// The strategic merge above blends override metadata into the base key by key,
	// which is not the desired semantics. Recompute the final metadata explicitly:
	// an override block fully replaces spec.labels / spec.annotations for the level
	// it targets (block replacement), and the internal cluster labels always win.
	clusterBase := clusterLabels(clusterName)
	result.Labels = mergeMeta(specLabels, override.Metadata.Labels, clusterBase)
	result.Annotations = mergeMeta(specAnnotations, override.Metadata.Annotations, nil)

	var tplLabels, tplAnnotations map[string]string
	if overrideSpec != nil && overrideSpec.Template != nil {
		tplLabels = overrideSpec.Template.Metadata.Labels
		tplAnnotations = overrideSpec.Template.Metadata.Annotations
	}
	result.Spec.Template.Labels = mergeMeta(specLabels, tplLabels, clusterBase)
	result.Spec.Template.Annotations = mergeMeta(specAnnotations, tplAnnotations, nil)

	return result, nil
}

// applyServiceOverride merges the user-provided override into the base headless
// Service using a strategic merge patch and restores the fields that define the
// Service identity.
//
// Guard rails (always restored from the base after the merge):
//   - spec.clusterIP  — must stay "None" (headless)
//   - spec.selector   — must keep matching the managed pods
//   - the "client" and "gossip" ports
//
// A nil override returns the base unchanged.
func applyServiceOverride(base *corev1.Service, override *redisv1.PartialService, clusterName string, specLabels, specAnnotations map[string]string) (*corev1.Service, error) {
	if override == nil {
		return base, nil
	}

	baseClusterIP := base.Spec.ClusterIP
	baseSelector := base.Spec.Selector
	basePorts := base.Spec.Ports

	var overrideSpec *redisv1.PartialServiceSpec
	if override.Spec != nil {
		overrideSpec = override.Spec
	}

	patch := struct {
		Metadata metav1.ObjectMeta           `json:"metadata"`
		Spec     *redisv1.PartialServiceSpec `json:"spec,omitempty"`
	}{
		Metadata: override.Metadata,
		Spec:     overrideSpec,
	}

	result := &corev1.Service{}
	if err := strategicMerge(base, patch, corev1.Service{}, result); err != nil {
		return nil, fmt.Errorf("applying Service override: %w", err)
	}

	// --- Guard rails: preserve headless identity and cluster selector. ---
	result.Spec.ClusterIP = baseClusterIP
	result.Spec.Selector = baseSelector
	restoreRequiredPorts(result, basePorts)

	// --- Labels / annotations precedence. ---
	// An override block fully replaces spec.labels / spec.annotations (block
	// replacement), and the internal cluster labels always win.
	result.Labels = mergeMeta(specLabels, override.Metadata.Labels, clusterLabels(clusterName))
	result.Annotations = mergeMeta(specAnnotations, override.Metadata.Annotations, nil)

	return result, nil
}

// strategicMerge applies patch onto base using a strategic merge patch computed
// against dataStruct, and decodes the result into out.
func strategicMerge(base, patch, dataStruct, out any) error {
	original, err := json.Marshal(base)
	if err != nil {
		return fmt.Errorf("marshaling base object: %w", err)
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshaling override patch: %w", err)
	}
	merged, err := strategicpatch.StrategicMergePatch(original, patchBytes, dataStruct)
	if err != nil {
		return fmt.Errorf("computing strategic merge patch: %w", err)
	}
	if err := json.Unmarshal(merged, out); err != nil {
		return fmt.Errorf("unmarshaling merged object: %w", err)
	}
	return nil
}

// restoreRedisContainer guarantees that the "redis" container is present and kept
// at index 0, re-inserting the base container if an override removed it.
func restoreRedisContainer(result *appsv1.StatefulSet, baseContainers []corev1.Container) {
	containers := result.Spec.Template.Spec.Containers
	redisIdx := -1
	for i := range containers {
		if containers[i].Name == "redis" {
			redisIdx = i
			break
		}
	}
	if redisIdx == -1 {
		if len(baseContainers) == 0 {
			return
		}
		result.Spec.Template.Spec.Containers = append([]corev1.Container{baseContainers[0]}, containers...)
		return
	}
	if redisIdx != 0 {
		containers[0], containers[redisIdx] = containers[redisIdx], containers[0]
	}
}

// restoreRequiredVolumes guarantees that the operator-managed volumes ("config"
// and "data") remain present after an override is applied.
func restoreRequiredVolumes(result *appsv1.StatefulSet, baseVolumes []corev1.Volume) {
	for i := range baseVolumes {
		v := baseVolumes[i]
		if _, required := requiredVolumeNames[v.Name]; !required {
			continue
		}
		found := false
		for j := range result.Spec.Template.Spec.Volumes {
			if result.Spec.Template.Spec.Volumes[j].Name == v.Name {
				found = true
				break
			}
		}
		if !found {
			result.Spec.Template.Spec.Volumes = append(result.Spec.Template.Spec.Volumes, v)
		}
	}
}

// restoreRequiredPorts guarantees that the operator-managed Service ports
// ("client" and "gossip") remain present after an override is applied.
func restoreRequiredPorts(result *corev1.Service, basePorts []corev1.ServicePort) {
	for i := range basePorts {
		p := basePorts[i]
		if _, required := requiredServicePortNames[p.Name]; !required {
			continue
		}
		found := false
		for j := range result.Spec.Ports {
			if result.Spec.Ports[j].Name == p.Name {
				found = true
				break
			}
		}
		if !found {
			result.Spec.Ports = append(result.Spec.Ports, p)
		}
	}
}

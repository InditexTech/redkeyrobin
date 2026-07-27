// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"context"
	"fmt"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	"github.com/inditextech/redkeyrobin/internal/kubernetes"
)

// handleScalingToZero deletes all cluster objects managed by Robin (StatefulSet, Service,
// ConfigMap, PDB, and optionally PVCs) then marks the config as Applied.
// All deletions are idempotent — missing resources are treated as success.
func (cr *ClusterReconciler) handleScalingToZero(ctx context.Context, config *redisv1.RedkeyConfig) (reconcileSchedule, error) {
	cr.logger.Info("Scaling to zero: deleting cluster objects", "config", config.Name)
	cr.updateSubstatus(ctx, config, redisv1.SubstatusDeletingResources)

	// Delete StatefulSet first (stops pods)
	if err := kubernetes.DeleteStatefulSet(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scale-to-zero: %w", err)
	}

	// Delete Service
	if err := kubernetes.DeleteService(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scale-to-zero: %w", err)
	}

	// Delete ConfigMap
	if err := kubernetes.DeleteConfigMap(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scale-to-zero: %w", err)
	}

	// Delete PDB
	if err := kubernetes.DeletePDB(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scale-to-zero: %w", err)
	}

	// Delete PVCs if deletePVC is enabled
	if config.Spec.DeletePVC != nil && *config.Spec.DeletePVC {
		cr.updateSubstatus(ctx, config, redisv1.SubstatusDeletingPVCs)
		cr.logger.Info("Deleting PVCs for scale-to-zero", "cluster", cr.clusterName)
		if err := kubernetes.DeletePVCs(ctx, cr.client, cr.clusterName, cr.namespace); err != nil {
			return reconcileAfterInterval, fmt.Errorf("scale-to-zero: %w", err)
		}
	}

	// Clear node status and substatus since there are no nodes
	config.Status.Nodes = map[string]*redisv1.RedisNode{}
	config.Status.Status = redisv1.ClusterStatusReady
	config.Status.Substatus.Status = ""
	if err := cr.client.Status().Update(ctx, config); err != nil {
		return reconcileAfterInterval, fmt.Errorf("updating status after scale-to-zero: %w", err)
	}

	// Mark config as Applied
	if err := cr.setConfigPhaseApplied(ctx, config); err != nil {
		return reconcileAfterInterval, fmt.Errorf("scale-to-zero: %w", err)
	}

	cr.logger.Info("Scale-to-zero complete, config marked as Applied", "config", config.Name)
	return reconcileAfterInterval, nil
}

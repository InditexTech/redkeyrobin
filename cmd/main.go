// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	redisv1 "github.com/inditextech/redkeyoperator/api/v1beta1"
	robinconfig "github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/metrics"
	"github.com/inditextech/redkeyrobin/internal/reconciler"
)

const (
	defaultReconcileInterval        = 30 * time.Second
	defaultReconcileIntervalOnError = 10 * time.Second
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(redisv1.AddToScheme(scheme))
}

func main() {
	var clusterName string
	var namespace string
	var metricsAddr string
	var reconcileInterval time.Duration
	var reconcileIntervalOnError = defaultReconcileIntervalOnError

	flag.StringVar(&clusterName, "cluster-name", "", "Name of the RedkeyCluster this Robin instance manages (required)")
	flag.StringVar(&namespace, "namespace", "", "Namespace of the RedkeyCluster (required)")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metrics endpoint binds to")
	flag.DurationVar(&reconcileInterval, "reconcile-interval", defaultReconcileInterval, "Polling interval for the reconciliation loop")
	flag.DurationVar(&reconcileIntervalOnError, "reconcile-interval-on-error", defaultReconcileIntervalOnError, "Polling interval for the reconciliation loop when an error occurs")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if clusterName == "" {
		logger.Error("--cluster-name is required")
		os.Exit(1)
	}
	if namespace == "" {
		logger.Error("--namespace is required")
		os.Exit(1)
	}

	logger.Info("Starting Redkey Robin",
		"cluster", clusterName,
		"namespace", namespace,
		"metricsAddr", metricsAddr,
		"reconcileInterval", reconcileInterval,
		"reconcileIntervalOnError", reconcileIntervalOnError,
	)

	// Build Kubernetes client
	config, err := ctrl.GetConfig()
	if err != nil {
		logger.Error("Unable to get Kubernetes config", "error", err)
		os.Exit(1)
	}

	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		logger.Error("Unable to create Kubernetes client", "error", err)
		os.Exit(1)
	}

	// Set up signal handler for graceful shutdown
	ctx, cancel := signal.NotifyContext(
		ctrl.SetupSignalHandler(),
		syscall.SIGINT, syscall.SIGTERM,
	)
	defer cancel()

	// Error channel for critical failures
	errChan := make(chan error, 3)

	// Create shared runtime configuration with defaults from CLI flags.
	runtimeConfig := robinconfig.NewRuntimeConfig()

	// Start the reconciliation loop
	rec := reconciler.NewReconciler(k8sClient, clusterName, namespace, reconcileInterval, reconcileIntervalOnError, runtimeConfig)
	go func() {
		if err := rec.Start(ctx); err != nil {
			logger.Error("Error in reconciler", "error", err)
			errChan <- err
		}
	}()

	// Start the metrics collector (Redis INFO polling)
	collector := metrics.NewCollector(runtimeConfig, clusterName, namespace, k8sClient, prometheus.DefaultRegisterer)
	go func() {
		if err := collector.Start(ctx); err != nil {
			logger.Error("Error in metrics collector", "error", err)
			errChan <- err
		}
	}()

	// Start the metrics HTTP server (Prometheus endpoint)
	metricsSrv := metrics.NewServer(metricsAddr)
	go func() {
		if err := metricsSrv.Start(ctx); err != nil {
			logger.Error("Error in metrics server", "error", err)
			errChan <- err
		}
	}()

	// Wait for context cancellation or critical error
	select {
	case <-ctx.Done():
		logger.Info("Shutting down Redkey Robin")
	case err := <-errChan:
		logger.Error("Critical error, shutting down", "error", err)
		os.Exit(1)
	}
}

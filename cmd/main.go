// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"github.com/inditextech/redkeyrobin/internal/cluster"
	"github.com/inditextech/redkeyrobin/internal/config"
	"github.com/inditextech/redkeyrobin/internal/httpserver"
	"github.com/inditextech/redkeyrobin/internal/metrics"
	"github.com/inditextech/redkeyrobin/internal/reconciler"
	"github.com/inditextech/redkeyrobin/internal/util"
)

func main() {
	// Parse CLI options
	opts := util.ParseOptions()

	// Initialize logger
	logger := util.InitLogger()

	// Load configuration
	conf, err := config.GetConfiguration(opts.ConfigMapPath)
	if err != nil {
		logger.Error("Unable to read configuration", "error", err)
		os.Exit(1)
	}
	ctx := util.SetupSignalHandler()

	// Communication channel for the cluster reconciler
	channel := make(chan struct{})
	defer close(channel)

	// Error channel for critical failures
	errChan := make(chan error, 1)

	// Create the cluster
	clusterInstance := cluster.NewCluster(ctx, conf, channel)

	// Initialize the HTTP server
	server := httpserver.NewServer(clusterInstance)
	if err := server.Init(opts); err != nil {
		logger.Error("Unable to initialize HTTP server", "error", err)
		os.Exit(1)
	}
	// Start the server in a goroutine with error handling
	go func() {
		if err := server.Start(ctx); err != nil {
			logger.Error("Unable to run HTTP server", "error", err)
			errChan <- err
		}
	}()

	// Initialize the cluster in a goroutine
	go func() {
		if err := clusterInstance.Init(); err != nil {
			logger.Error("Unable to initialize RedKey Cluster", "error", err)
			errChan <- err
			return
		}
	}()

	// Create and launch the cluster reconciler
	reconciler, err := reconciler.NewReconciler(clusterInstance, channel)
	if err != nil {
		logger.Error("Unable to create reconciler", "error", err)
		errChan <- err
		return
	}
	go reconciler.Start(ctx)

	// Create and launch a metrics poller if needed
	if !opts.DisableMetrics {
		metricsPoller, err := metrics.NewMetricsPoller(clusterInstance)
		if err != nil {
			logger.Error("Unable to create metrics poller", "error", err)
			errChan <- err
			return
		}
		go metricsPoller.Start(ctx)
	}

	// Wait for context cancellation or critical error
	select {
	case <-ctx.Done():
		logger.Info("Shutting down RedKey Robin")
	case err := <-errChan:
		logger.Error("Critical error, shutting down", "error", err)
		os.Exit(1)
	}
}

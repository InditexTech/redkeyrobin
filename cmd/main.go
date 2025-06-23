// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/httpserver"
	"github.com/inditextech/redisrobin/internal/metrics"
	"github.com/inditextech/redisrobin/internal/reconciler"
	"github.com/inditextech/redisrobin/internal/util"
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

	// Communication channel for the reconciler
	channel := make(chan struct{})
	defer close(channel)

	// Initialize the cluster
	cluster := cluster.NewCluster(ctx, conf, channel)
	if err := cluster.Init(); err != nil {
		logger.Error("Unable to initialize Redis Cluster", "error", err)
		os.Exit(1)
	}

	// Create and launch the cluster reconciler
	reconciler, err := reconciler.NewReconciler(cluster, channel)
	if err != nil {
		logger.Error("Unable to create Redis reconciler", "error", err)
		os.Exit(1)
	}
	go reconciler.Start(ctx)

	// Create and launch a metrics poller if needed
	if !opts.DisableMetrics {
		metricsPoller, err := metrics.NewMetricsPoller(cluster)
		if err != nil {
			logger.Error("Unable to create Redis metrics poller", "error", err)
			os.Exit(1)
		}
		go metricsPoller.Start(ctx)
	}

	// Initialize the HTTP server
	server := httpserver.NewServer(cluster)
	if err := server.Init(opts); err != nil {
		logger.Error("Unable to initialize HTTP server", "error", err)
		os.Exit(1)
	}

	// Start the server (blocking call until shutdown)
	if err := server.Start(ctx); err != nil {
		logger.Error("Unable to run HTTP server", "error", err)
		os.Exit(1)
	}
}

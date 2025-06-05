// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/httpserver"
	"github.com/inditextech/redisrobin/internal/metrics"
	"github.com/inditextech/redisrobin/internal/reconciler"
	"github.com/inditextech/redisrobin/internal/redis"
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

	// Communication channe for the reconciler
	channel := make(chan struct{})
	defer close(channel)

	// Initialize the Redis cluster.
	redisCluster := redis.NewRedisCluster(ctx, conf, channel)
	if err := redisCluster.Init(); err != nil {
		logger.Error("Unable to initialize Redis Cluster", "error", err)
		os.Exit(1)
	}

	// Build the HTTP server with the provided APIConfig.
	server := httpserver.NewServer(redisCluster)
	if err := server.Init(opts); err != nil {
		logger.Error("Unable to initialize HTTP server", "error", err)
		os.Exit(1)
	}

	if !opts.DisableMetrics {
		// Create and launch the RedisPollMetrics instance to gather metrics in the background.
		metricsManager := metrics.NewMetricsManager(conf.Metadata)
		redisPollMetrics, err := metrics.NewRedisPollMetrics(redisCluster, metricsManager)
		if err != nil {
			logger.Error("Unable to create Redis metrics poller", "error", err)
			os.Exit(1)
		}
		go redisPollMetrics.Start(ctx)
	}

	// Create and launch the redis cluster reconciler
	redisClusterReconciler, err := reconciler.NewRedisClusterReconciler(redisCluster, channel)
	if err != nil {
		logger.Error("Unable to create Redis reconciler", "error", err)
		os.Exit(1)
	}
	go redisClusterReconciler.Start(ctx)

	// Start the server (blocking call until shutdown).
	if err := server.Start(ctx); err != nil {
		logger.Error("Unable to run HTTP server", "error", err)
		os.Exit(1)
	}
}

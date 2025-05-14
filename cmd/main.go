// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/httpserver"
	"github.com/inditextech/redisrobin/internal/metrics"
	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/inditextech/redisrobin/internal/util"
)

const configmapFilePath = "/opt/conf/configmap/application-configmap.yml"

func main() {
	var metricsAddr string

	// Parse CLI flags (e.g., --metrics-bind-address :8080).
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.Parse()

	logger := util.InitLogger()

	// Load configuration
	conf, err := config.GetConfiguration()
	if err != nil {
		logger.Error(err, "Unable to read configuration")
		os.Exit(1)
	}
	apiConf, err := config.GetAPIConfiguration()
	if err != nil {
		logger.Error(err, "Unable to read API configuration")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()

	// Set up controller-runtime Manager options.
	ctrlOptions := ctrl.Options{
		Logger: util.GetLogger("controller-runtime"),
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
	}

	// Create the manager for metrics and additional HTTP endpoints.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrlOptions)
	if err != nil {
		logger.Error(err, "Unable to create controller manager")
		os.Exit(1)
	}

	// Initialize the Redis cluster.
	redisCluster := redis.NewRedisCluster(conf)
	if err := redisCluster.Init(); err != nil {
		logger.Error(err, "Unable to initialize Redis Cluster")
		os.Exit(1)
	}

	// Build the HTTP server with the provided APIConfig.
	server := httpserver.NewServer(redisCluster, *apiConf)
	if err := server.Init(mgr); err != nil {
		logger.Error(err, "Unable to initialize HTTP server")
		os.Exit(1)
	}

	// Create and launch the RedisPollMetrics instance to gather metrics in the background.
	metricsManager := metrics.NewMetricsManager(conf.Metadata)
	redisPollMetrics, err := metrics.NewRedisPollMetrics(redisCluster, metricsManager)
	if err != nil {
		logger.Error(err, "Unable to create Redis metrics poller")
		os.Exit(1)
	}
	go redisPollMetrics.Start(ctx)

	// Create and launch the redis cluster reconciler
	redisClusterReconciler, err := redis.NewRedisClusterReconciler(redisCluster)
	if err != nil {
		logger.Error(err, "Unable to create Redis reconciler")
		os.Exit(1)
	}
	go redisClusterReconciler.Start(ctx)

	// Add the health and readiness checks.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		logger.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		logger.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	// Start the manager (blocking call until shutdown).
	if err := mgr.Start(ctx); err != nil {
		logger.Error(err, "Unable to run manager")
		os.Exit(1)
	}
}

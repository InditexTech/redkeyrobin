// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"log"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/inditextech/redisrobin/internal/httpserver"
	"github.com/inditextech/redisrobin/internal/metrics"
	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/redis"
)

const configmapFilePath = "/opt/conf/configmap/application-configmap.yml"

func main() {
	var metricsAddr string

	// Parse CLI flags (e.g., --metrics-bind-address :8080).
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.Parse()

	// Set up controller-runtime Manager options.
	ctrlOptions := ctrl.Options{
		Logger: ctrl.Log.WithName("metrics-server"),
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
	}

	// Create the manager for metrics and additional HTTP endpoints.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrlOptions)
	if err != nil {
		log.Fatalf("unable to create manager: %v", err)
	}

	// Load Redis configuration (e.g., from environment or file).
	conf := config.GetConfiguration()
	redisCluster := redis.NewRedisCluster(conf)
	redisCluster.Init()

	// Create the metrics manager.
	metricsManager := metrics.NewMetricsManager(conf.Metadata)
	ctx := context.Background()

	// Create a new RedisPollMetrics instance to gather metrics in the background.
	redisPollMetrics, err := metrics.NewRedisPollMetrics(redisCluster, metricsManager)
	if err != nil {
		log.Fatalf("unable to create Redis metrics poller: %v", err)
	}
	go redisPollMetrics.Start(ctx)

	// Create the redis cluster reconciler
	redisClusterReconciler := redis.NewRedisClusterReconciler(redisCluster)
	go redisClusterReconciler.Start(ctx)

	// Build the HTTP server with the provided Config.
	server := httpserver.NewServer(redisCluster, conf.API)
	if err := server.Init(mgr); err != nil {
		log.Fatalf("unable to initialize HTTP server: %v", err)
	}

	// Start the manager (blocking call until shutdown).
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Fatalf("problem running manager: %v", err)
	}
}

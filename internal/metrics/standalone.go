// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"

	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/util"
)

// RedisStandaloneMetricsPoller is a standalone metrics poller.
type RedisStandaloneMetricsPoller struct {
	basePoller
}

// NewRedisStandaloneMetricsPoller creates a new standalone metrics poller.
func NewRedisStandaloneMetricsPoller(cluster cluster.Cluster) (*RedisStandaloneMetricsPoller, error) {
	poller := &RedisStandaloneMetricsPoller{
		basePoller: basePoller{
			logger:         util.GetLogger("standalone-metrics"),
			metricsManager: NewMetricsManager(),
			cluster:        cluster,
		},
	}
	poller.delegate = poller

	return poller, nil
}

// doPollMetrics retrieves all nodes in the configured namespace and labelSelector, then
// polls both cluster-level metrics and Redis INFO per node.
func (p *RedisStandaloneMetricsPoller) doPollMetrics(ctx context.Context) error {
	// node-level info (GetInfo)
	if err := p.pollRedisNodeLevelMetrics(ctx); err != nil {
		p.metricsManager.ResetMetrics()
		return err
	}

	return nil
}

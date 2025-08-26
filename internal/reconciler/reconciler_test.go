// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package reconciler

import (
	"fmt"
	"testing"
	"time"

	"github.com/inditextech/redisrobin/internal/cluster"
	"github.com/inditextech/redisrobin/internal/config"
	"github.com/inditextech/redisrobin/internal/redis"
	"github.com/stretchr/testify/assert"
)

func TestNewRedKeyClusterReconciler(t *testing.T) {
	reconciler, err := NewRedKeyClusterReconciler(&cluster.RedKeyCluster{}, make(chan struct{}))

	assert.NoError(t, err)
	assert.NotNil(t, reconciler)
}

func TestRedKeyClusterReconcilerReconcile(t *testing.T) {
	tests := []struct {
		name          string
		config        *config.Configuration
		expectedError error
	}{
		{
			name: "Ready",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedKeyClusterConfig{
						Status:                   "Ready",
						Replicas:                 3,
						Name:                     "test",
						Namespace:                "test",
						MaxRetries:               1,
						BackOff:                  time.Microsecond * 10,
						HealingTimeSeconds:       55,
						HealthProbePeriodSeconds: 40,
					},
				},
			},
			expectedError: fmt.Errorf("Error reconciling cluster: error getting and checking Redis client: failed to connect after 1 retries"),
		},
		{
			name: "Scaling up",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedKeyClusterConfig{
						Status:                   "ScalingUp",
						Replicas:                 3,
						Name:                     "test",
						Namespace:                "test",
						MaxRetries:               1,
						BackOff:                  time.Microsecond * 10,
						HealingTimeSeconds:       55,
						HealthProbePeriodSeconds: 40,
					},
				},
			},
			expectedError: fmt.Errorf("Error reconciling cluster: error getting and checking Redis client: failed to connect after 1 retries"),
		},
		{
			name: "Scaling down",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedKeyClusterConfig{
						Status:                   "ScalingDown",
						Replicas:                 3,
						Name:                     "test",
						Namespace:                "test",
						MaxRetries:               1,
						BackOff:                  time.Microsecond * 10,
						HealingTimeSeconds:       55,
						HealthProbePeriodSeconds: 40,
					},
				},
			},
			expectedError: fmt.Errorf("Error reconciling cluster: error getting and checking Redis client: failed to connect after 1 retries"),
		},
		{
			name: "Upgrading",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedKeyClusterConfig{
						Status:                   "Upgrading",
						Replicas:                 3,
						Name:                     "test",
						Namespace:                "test",
						MaxRetries:               1,
						BackOff:                  time.Microsecond * 10,
						HealingTimeSeconds:       55,
						HealthProbePeriodSeconds: 40,
					},
				},
			},
			expectedError: fmt.Errorf("Error reconciling cluster: error getting and checking Redis client: failed to connect after 1 retries"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdcl := cluster.NewFakeRedKeyCluster(
				t.Context(),
				tt.config,
				"Unknown",
				map[string]*redis.RedisNode{},
				map[string][]cluster.RedisOperation{},
				make(chan struct{}, 5),
			)

			reconciler, _ := NewRedKeyClusterReconciler(rdcl, make(chan struct{}))
			reconciler.reconcile()
			err := reconciler.doReconcile()

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.IsType(t, tt.expectedError, err)
			} else {
				assert.NoError(t, err)
			}

		})
	}
}

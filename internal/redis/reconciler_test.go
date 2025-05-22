// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"testing"
	"time"

	"github.com/inditextech/redisrobin/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisClusterReconciler(t *testing.T) {
	reconciler, err := NewRedisClusterReconciler(&RedisCluster{}, make(chan struct{}))

	assert.NoError(t, err)
	assert.NotNil(t, reconciler)
}

// func TestRedisClusterReconcilerReconcile(t *testing.T) {
// 	redisCluster := NewFakeRedisCluster(
// 		t.Context(),
// 		&config.Configuration{
// 			Redis: config.RedisConfig{
// 				Cluster: config.RedisClusterConfig{
// 					Status:                   "Ready",
// 					Replicas:                 3,
// 					Name:                     "test",
// 					Namespace:                "test",
// 					MaxRetries:               1,
// 					BackOff:                  time.Microsecond * 10,
// 					HealingTimeSeconds:       55,
// 					HealthProbePeriodSeconds: 40,
// 				},
// 			},
// 		},
// 		"Unknown",
// 		map[string]*RedisNode{},
// 		map[string][]*RedisOperation{},
// 	)
// 	reconciler, _ := NewRedisClusterReconciler(redisCluster, make(chan struct{}))

// 	reconciler.Reconcile()
// 	err := reconciler.doReconcile()
// 	assert.NoError(t, err)
// }

func TestRedisClusterReconcilerReconcile(t *testing.T) {
	tests := []struct {
		name           string
		config 	   *config.Configuration
		expectedError  error
	}{
		{
			name: "Ready",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedisClusterConfig{
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
			expectedError:  nil,
		},
		{
			name: "Scaling up",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedisClusterConfig{
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
			expectedError:  nil,
		},
		{
			name: "Scaling down",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedisClusterConfig{
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
			expectedError:  nil,
		},
		{
			name: "Upgrading",
			config: &config.Configuration{
				Redis: config.RedisConfig{
					Cluster: config.RedisClusterConfig{
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
			expectedError:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdcl := NewFakeRedisCluster(
				t.Context(),
				tt.config,
				"Unknown",
				map[string]*RedisNode{},
				map[string][]*RedisOperation{},
			)

			reconciler, _ := NewRedisClusterReconciler(rdcl, make(chan struct{}))
			reconciler.Reconcile()
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

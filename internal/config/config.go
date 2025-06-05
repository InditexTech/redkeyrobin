// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/inditextech/redisrobin/internal/util"
	"gopkg.in/yaml.v3"
)

// RedisOperatorConfig holds operator-level Redis configuration.
type RedisReconcilerConfig struct {
	IntervalSeconds                 int `yaml:"interval_seconds"`
	OperationCleanupIntervalSeconds int `yaml:"operation_cleanup_interval_seconds"`
}

// RedisClusterConfig holds cluster-level Redis configuration.
type RedisClusterConfig struct {
	Namespace                string        `yaml:"namespace"`
	Name                     string        `yaml:"name"`
	Replicas                 int           `yaml:"replicas"`
	ReplicasPerMaster        int           `yaml:"replicas_per_master"`
	Status                   string        `yaml:"status"`
	Ephemeral                bool          `yaml:"ephemeral"`
	HealthProbePeriodSeconds int           `yaml:"health_probe_interval_seconds"`
	HealingTimeSeconds       int           `yaml:"healing_time_seconds"`
	MaxRetries               int           `yaml:"max_retries"`
	BackOff                  time.Duration `yaml:"back_off"`
}

// RedisMetricsConfig holds metrics-related Redis configuration.
type RedisMetricsConfig struct {
	IntervalSeconds int      `yaml:"interval_seconds"`
	RedisInfoKeys   []string `yaml:"redis_info_keys"`
}

// RedisConfig groups all Redis related configuration.
type RedisConfig struct {
	Reconciler RedisReconcilerConfig `yaml:"reconciler"`
	Cluster    RedisClusterConfig    `yaml:"cluster"`
	Metrics    RedisMetricsConfig    `yaml:"metrics"`
}

// Configuration is the top-level configuration struct.
type Configuration struct {
	Metadata map[string]string `yaml:"metadata"`
	Redis    RedisConfig       `yaml:"redis"`
}

// String returns a formatted string of the configuration.
func (c *Configuration) String() string {
	return fmt.Sprintf(`Configuration properties:
Metadata: %s
ReconcilerIntervalSeconds: %d
ClusterHealthProbePeriodSeconds: %d
ClusterHealingTimeSeconds: %d
RedisInfoKeys: %v
RedisMetricsIntervalSeconds: %d`,
		util.MapToString(c.Metadata),
		c.Redis.Reconciler.IntervalSeconds,
		c.Redis.Cluster.HealthProbePeriodSeconds,
		c.Redis.Cluster.HealingTimeSeconds,
		c.Redis.Metrics.RedisInfoKeys,
		c.Redis.Metrics.IntervalSeconds)
}

// validate checks for missing required configuration fields.
func (cfg *Configuration) validate() []string {
	var missing []string

	if len(cfg.Metadata) == 0 {
		missing = append(missing, "metadata")
	}
	if cfg.Redis.Reconciler.IntervalSeconds == 0 {
		missing = append(missing, "redis.reconciler.interval_seconds")
	}
	if cfg.Redis.Cluster.Namespace == "" {
		missing = append(missing, "redis.cluster.namespace")
	}
	if cfg.Redis.Cluster.Name == "" {
		missing = append(missing, "redis.cluster.name")
	}
	if cfg.Redis.Cluster.Replicas == 0 {
		missing = append(missing, "redis.cluster.replicas")
	}
	if cfg.Redis.Cluster.Status == "" {
		missing = append(missing, "redis.cluster.status")
	}
	if cfg.Redis.Cluster.HealthProbePeriodSeconds == 0 {
		missing = append(missing, "redis.cluster.health_probe_interval_seconds")
	}
	if cfg.Redis.Cluster.HealingTimeSeconds == 0 {
		missing = append(missing, "redis.cluster.healing_time_seconds")
	}
	if cfg.Redis.Metrics.IntervalSeconds == 0 {
		missing = append(missing, "redis.metrics.interval_seconds")
	}
	if cfg.Redis.Reconciler.OperationCleanupIntervalSeconds == 0 {
		cfg.Redis.Reconciler.OperationCleanupIntervalSeconds = 60
	}
	return missing
}

// ConfigLoader defines the interface for loading a configuration.
type ConfigLoader interface {
	LoadConfig(path string) (*Configuration, error)
}

// YAMLConfigLoader implements ConfigLoader by reading YAML files.
type YAMLConfigLoader struct{}

// LoadConfig reads and decodes the YAML configuration from the specified file path.
func (y *YAMLConfigLoader) LoadConfig(path string) (*Configuration, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration file %s: %w", path, err)
	}
	var cfg Configuration
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration file %s: %w", path, err)
	}
	if missing := cfg.validate(); len(missing) > 0 {
		return nil, fmt.Errorf("missing required configuration fields: %v", missing)
	}
	return &cfg, nil
}

// GetConfiguration returns the singleton Configuration loaded from the default file.
// The configuration is loaded only once using sync.Once.
func GetConfiguration(filePath string) (*Configuration, error) {
	var config *Configuration
	var err error
	var once sync.Once
	once.Do(func() {
		loader := &YAMLConfigLoader{}
		config, err = loader.LoadConfig(filePath)
	})
	return config, err
}

// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// ResettableRegistry is a Prometheus registry that can be atomically replaced.
// It lets Robin rebuild RedKey dynamic metric descriptors when their label
// schema changes, without touching process-wide default collectors.
type ResettableRegistry struct {
	mu       sync.RWMutex
	registry *prometheus.Registry
}

// NewResettableRegistry creates an empty resettable Prometheus registry.
func NewResettableRegistry() *ResettableRegistry {
	return &ResettableRegistry{registry: prometheus.NewRegistry()}
}

func (r *ResettableRegistry) Register(collector prometheus.Collector) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registry.Register(collector)
}

func (r *ResettableRegistry) MustRegister(collectors ...prometheus.Collector) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.registry.MustRegister(collectors...)
}

func (r *ResettableRegistry) Unregister(collector prometheus.Collector) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registry.Unregister(collector)
}

func (r *ResettableRegistry) Gather() ([]*dto.MetricFamily, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.registry.Gather()
}

// Reset replaces the current registry with an empty one.
func (r *ResettableRegistry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registry = prometheus.NewRegistry()
}

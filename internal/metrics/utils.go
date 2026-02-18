// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"maps"
	"sort"
	"strconv"

	"github.com/inditextech/redkeyrobin/internal/redis"
	"github.com/prometheus/client_golang/prometheus"
)

// mergeLabels merges two label maps and returns a combined map along with a
// sorted slice of unique label keys. This ensures consistent ordering for metric registration.
func mergeLabels(a, b map[string]string) (prometheus.Labels, []string) {
	merged := make(prometheus.Labels)
	uniqueKeys := make(map[string]struct{})

	for k, v := range a {
		merged[k] = v
		uniqueKeys[k] = struct{}{}
	}
	for k, v := range b {
		merged[k] = v
		uniqueKeys[k] = struct{}{}
	}

	keys := make([]string, 0, len(uniqueKeys))
	for k := range uniqueKeys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return merged, keys
}

// updateGaugeWithCurrentTime is a helper that pulls labels by the provided keys
// from 'tags' and calls SetToCurrentTime() on the matching gauge.
func updateGaugeWithCurrentTime(
	gaugeVec *prometheus.GaugeVec,
	tags map[string]string,
	labelKeys []string,
) {
	labelMap := buildLabels(tags, labelKeys)
	gaugeVec.With(labelMap).SetToCurrentTime()
}

// buildLabels constructs a prometheus.Labels map using the given tag keys.
func buildLabels(tags map[string]string, labelKeys []string) prometheus.Labels {
	out := make(prometheus.Labels, len(labelKeys))
	for _, key := range labelKeys {
		out[key] = tags[key]
	}
	return out
}

// gatherAllFieldsAsStrings flattens RedisInfo numeric fields into strings.
func gatherAllFieldsAsStrings(ri *redis.RedisInfo) map[string]string {
	allFields := make(map[string]string)

	// Copy maps that are string->string
	maps.Copy(allFields, ri.Server)
	maps.Copy(allFields, ri.Persistence)
	maps.Copy(allFields, ri.Replication)
	maps.Copy(allFields, ri.Cluster)
	maps.Copy(allFields, ri.ErrorStats)
	maps.Copy(allFields, ri.LatencyStats)
	maps.Copy(allFields, ri.Memory)
	maps.Copy(allFields, ri.CommandStats)
	maps.Copy(allFields, ri.Stats)
	// Convert int fields to strings
	for k, v := range ri.Clients {
		allFields[k] = strconv.FormatInt(v, 10)
	}

	// Convert float fields to strings
	for k, v := range ri.CPU {
		allFields[k] = fmt.Sprintf("%f", v)
	}
	return allFields
}

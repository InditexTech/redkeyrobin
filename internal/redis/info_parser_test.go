// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"testing"
)

const sampleInfoAll = `# Server
redis_version:7.0.15
redis_git_sha1:00000000
redis_mode:cluster
os:Linux 5.15.0-91-generic x86_64
tcp_port:6379
uptime_in_seconds:86400

# Clients
connected_clients:42
blocked_clients:0

# Memory
used_memory:1048576
used_memory_rss:2097152
maxmemory:4294967296

# Stats
total_commands_processed:123456
keyspace_hits:9000
keyspace_misses:100
evicted_keys:5
expired_keys:200
total_net_input_bytes:500000
total_net_output_bytes:1000000

# CPU
used_cpu_sys:10.5
used_cpu_user:20.3
used_cpu_sys_children:1.2
used_cpu_user_children:2.4

# Keyspace
db0:keys=1000,expires=50,avg_ttl=3600000
`

func TestParseInfoAll_BasicParsing(t *testing.T) {
	result := ParseInfoAll(sampleInfoAll)

	tests := []struct {
		key      string
		expected string
	}{
		{"redis_version", "7.0.15"},
		{"redis_mode", "cluster"},
		{"tcp_port", "6379"},
		{"uptime_in_seconds", "86400"},
		{"connected_clients", "42"},
		{"used_memory", "1048576"},
		{"used_memory_rss", "2097152"},
		{"maxmemory", "4294967296"},
		{"total_commands_processed", "123456"},
		{"keyspace_hits", "9000"},
		{"keyspace_misses", "100"},
		{"evicted_keys", "5"},
		{"expired_keys", "200"},
		{"used_cpu_sys", "10.5"},
		{"used_cpu_user", "20.3"},
		{"db0", "keys=1000,expires=50,avg_ttl=3600000"},
	}

	for _, tt := range tests {
		v, ok := result[tt.key]
		if !ok {
			t.Errorf("key %q not found in parsed result", tt.key)
			continue
		}
		if v != tt.expected {
			t.Errorf("key %q: expected %q, got %q", tt.key, tt.expected, v)
		}
	}
}

func TestParseInfoAll_SkipsSections(t *testing.T) {
	result := ParseInfoAll(sampleInfoAll)
	for k := range result {
		if k == "" || k[0] == '#' {
			t.Errorf("section header or empty key found: %q", k)
		}
	}
}

func TestParseInfoAll_EmptyInput(t *testing.T) {
	result := ParseInfoAll("")
	if len(result) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(result))
	}
}

func TestParseInfoAll_WindowsLineEndings(t *testing.T) {
	input := "# Server\r\nredis_version:7.0.15\r\nconnected_clients:10\r\n"
	result := ParseInfoAll(input)
	if result["redis_version"] != "7.0.15" {
		t.Errorf("expected redis_version=7.0.15, got %q", result["redis_version"])
	}
	if result["connected_clients"] != "10" {
		t.Errorf("expected connected_clients=10, got %q", result["connected_clients"])
	}
}

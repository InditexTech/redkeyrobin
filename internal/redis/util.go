// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"fmt"
	"context"
	"strings"
	"bufio"
	"slices"
	"strconv"
	"math"

	"github.com/inditextech/redisrobin/internal/util"
)

// parseClusterCheckOutput processes Redis cluster check output into a structured format.
func parseClusterCheckOutput(output string) *ClusterCheckResult {
	scanner := bufio.NewScanner(strings.NewReader(output))

	result := &ClusterCheckResult{
		Errors:   []string{},
		Warnings: []string{},
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Capture errors
		if strings.Contains(line, "[ERR]") {
			stripedLine := strings.TrimSpace(strings.ReplaceAll(line, "[ERR]", ""))
			if !slices.Contains(result.Errors, stripedLine) {
				result.Errors = append(result.Errors, stripedLine)
			}
		}

		// Capture general warnings
		if strings.Contains(line, "[WARNING]") {
			stripedLine := strings.TrimSpace(strings.ReplaceAll(line, "[WARNING]", ""))
			if !slices.Contains(result.Warnings, stripedLine) {
				result.Warnings = append(result.Warnings, stripedLine)
			}
		}
	}

	// Check for any scanning error.
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading cluster check output: %v", err)
	}

	return result
}

// parseRedisSlotRange processes Redis slot range output into a structured format.
func parseRedisSlotRange(values ...string) []RedisSlotRange {
	// TODO: add importing and migrating slots?
	slots := []RedisSlotRange{}

	for _, value := range values {
		for _, slotRange := range strings.Split(value, " ") {
			if strings.Contains(slotRange, "-") {
				splitSlotRange := strings.Split(slotRange, "-")
				start, err := strconv.Atoi(splitSlotRange[0])
				if err != nil {
					continue
				}
				end, err := strconv.Atoi(splitSlotRange[1])
				if err != nil {
					continue
				}
				slots = append(slots, RedisSlotRange{Start: start, End: end})
			} else {
				slot, err := strconv.Atoi(slotRange)
				if err != nil {
					continue
				}
				slots = append(slots, RedisSlotRange{Start: slot, End: slot})
			}
		}
	}

	return slots
}

// parseRedisInfo processes Redis INFO output into a structured format.
func parseRedisInfo(info string) *RedisInfo {
	parsedInfo := &RedisInfo{
		Server:       make(map[string]string),
		Clients:      make(map[string]int64),
		Memory:       make(map[string]string),
		Persistence:  make(map[string]string),
		Stats:        make(map[string]string),
		Replication:  make(map[string]string),
		CPU:          make(map[string]float64),
		Cluster:      make(map[string]string),
		Keyspace:     make(map[string]string),
		CommandStats: make(map[string]string),
		ErrorStats:   make(map[string]string),
		LatencyStats: make(map[string]string),
	}

	section := ""
	lines := strings.Split(strings.TrimSpace(info), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Identify the current section if line starts with '#'
		if strings.HasPrefix(line, "#") {
			section = strings.ToLower(strings.TrimPrefix(line, "#"))
			section = strings.TrimSpace(section)
			continue
		}

		// Parse "key:value" structure
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Populate parsedInfo based on the recognized section
		switch section {
		case SectionServer:
			parsedInfo.Server[key] = value
		case SectionClients:
			intVal := util.ParseInt64(value)
			parsedInfo.Clients[key] = intVal
		case SectionMemory:
			parsedInfo.Memory[key] = value
		case SectionPersistence:
			parsedInfo.Persistence[key] = value
		case SectionStats:
			parsedInfo.Stats[key] = value
		case SectionReplication:
			parsedInfo.Replication[key] = value
		case SectionCPU:
			floatVal := util.ParseFloat(value)
			parsedInfo.CPU[key] = floatVal
		case SectionCluster:
			parsedInfo.Cluster[key] = value
		case SectionKeyspace:
			parsedInfo.Keyspace[key] = value
		case SectionCmdStats:
			parsedInfo.CommandStats[key] = value
		case SectionErrorStats:
			parsedInfo.ErrorStats[key] = value
		case SectionLatency:
			parsedInfo.LatencyStats[key] = value
		default:
			fmt.Printf("Ignoring unknown redis info. Section:%s, key: %s", section, key)
		}
	}

	return parsedInfo
}

func calculateMaxSlotsPerMaster(slots int, masters int) int {
	return int(math.Ceil(float64(slots) / float64(masters)))
}

// runRedisCLICommand executes a Redis CLI command synchronously and returns the command reference.
func runRedisCLICommand(ctx context.Context, command string) *RedisCLICommand {
	// Build and run the command
	cmd := NewRedisCLICommand(ctx, command)
	cmd.Run()

	return cmd
}

// runRedisCLICommandAsync executes a Redis CLI command asynchronously and returns the command reference.
func runRedisCLICommandAsync(ctx context.Context, command string) *RedisCLICommand {
	// Build and start the command
	cmd := NewRedisCLICommand(ctx, command)
	cmd.Start()

	return cmd
}

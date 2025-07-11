// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
)

// ClusterCheckResult aggregates the overall cluster state similar to "redis-cli --cluster check".
type ClusterCheckResult struct {
	CommandCodeOutput int
	Errors            []string
	Warnings          []string
}

// ClusterCheck executes "redis-cli --cluster check <addr>" and parses its output.
func (rc *RedisClient) ClusterCheck(ctx context.Context) (*ClusterCheckResult, error) {
	// Build the command: "redis-cli --cluster check <host:port>"
	command := fmt.Sprintf("redis-cli --cluster check %s", rc.client.Options().Addr)

	// Execute the command and parse the output
	cmd := runRedisCLICommand(ctx, command)
	if cmd.Err != nil {
		// If the context was canceled or timed out, return immediately.
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis-cli command canceled or timed out: %w", ctx.Err())
		}

		rc.logger.Error("Error executing 'redis-cli --cluster check'", "output", cmd.GetCombinedOutput(), "error", cmd.Err)
	}

	// Parse the CLI output (whether complete or partial) to fill a ClusterCheckResult.
	result := parseClusterCheckOutput(cmd.GetStdout())
	result.CommandCodeOutput = cmd.ExitCode

	return result, nil
}

// ClusterFix executes "redis-cli --cluster fix <addr>" asynchrously and returns the command reference.
func (rc *RedisClient) ClusterFix(ctx context.Context) *RedisCLICommand {
	// Build the command: "redis-cli --cluster fix <host:port>"
	command := fmt.Sprintf("echo 'yes' | redis-cli --cluster fix %s", rc.client.Options().Addr)

	// Execute the command and return command reference
	return runRedisCLICommandAsync(ctx, command)
}

// ReshardNode executes "redis-cli --cluster reshard <addr> --cluster-from <source> --cluster-to <target> --cluster-slots <slots> --cluster-yes" asynchronously and returns the command reference.
func (rc *RedisClient) ReshardNode(ctx context.Context, source, target RedisNode, slots int) *RedisCLICommand {
	if slots == 0 {
		rc.logger.Info("No slots to reshard")
		return nil
	}

	// Build the command: "redis-cli --cluster reshard <host:port>"
	command := fmt.Sprintf("redis-cli --cluster reshard %s --cluster-from %s --cluster-to %s --cluster-slots %v --cluster-yes", rc.client.Options().Addr, source.ID, target.ID, slots)

	// Execute the command and return command reference
	return runRedisCLICommandAsync(ctx, command)
}

// ReshardNode executes "redis-cli --cluster rebalance <addr> --cluster-use-empty-masters" asynchronously and returns the command reference.
func (rc *RedisClient) ClusterRebalance(ctx context.Context, weights map[string]int) *RedisCLICommand {
	// Build the command: "redis-cli --cluster rebalance <host:port>"
	command := fmt.Sprintf("redis-cli --cluster rebalance %s --cluster-use-empty-masters", rc.client.Options().Addr)

	// Add weights if provided
	if len(weights) > 0 {
		command = fmt.Sprintf("%s --cluster-weight", command)

		for nodeId, weight := range weights {
			command = fmt.Sprintf("%s %s=%v", command, nodeId, weight)
		}
	}

	// Execute the command and return command reference
	return runRedisCLICommandAsync(ctx, command)
}

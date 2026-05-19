// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const redisCLIAuthEnv = "REDISCLI_AUTH"

var newRedisCLICommand = NewRedisCLICommand

// ClusterCheckResult aggregates the overall cluster state reported by redis-cli --cluster check.
type ClusterCheckResult struct {
	CommandCodeOutput int
	Errors            []string
	Warnings          []string
}

// ClusterCheck executes redis-cli --cluster check against the current node address.
func (c *Client) ClusterCheck(ctx context.Context) (*ClusterCheckResult, error) {
	cmd := newRedisCLICommand(ctx, []string{"--cluster", "check", c.addr}, c.redisCLIEnv())
	cmd.Run()

	if cmd.Err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis-cli command canceled or timed out: %w", ctx.Err())
		}
		if cmd.ExitCode == -1 {
			return nil, fmt.Errorf("redis-cli --cluster check on %s: %w", c.addr, cmd.Err)
		}
	}

	result := parseClusterCheckOutput(cmd.GetStdout())
	result.CommandCodeOutput = cmd.ExitCode
	return result, nil
}

func (c *Client) redisCLIEnv() map[string]string {
	if c.password == "" {
		return nil
	}
	return map[string]string{redisCLIAuthEnv: c.password}
}

func parseClusterCheckOutput(output string) *ClusterCheckResult {
	scanner := bufio.NewScanner(strings.NewReader(output))
	result := &ClusterCheckResult{
		Errors:   []string{},
		Warnings: []string{},
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.Contains(line, "[ERR]") {
			trimmed := strings.TrimSpace(strings.ReplaceAll(line, "[ERR]", ""))
			if !slices.Contains(result.Errors, trimmed) {
				result.Errors = append(result.Errors, trimmed)
			}
		}

		if strings.Contains(line, "[WARNING]") {
			trimmed := strings.TrimSpace(strings.ReplaceAll(line, "[WARNING]", ""))
			if !slices.Contains(result.Warnings, trimmed) {
				result.Warnings = append(result.Warnings, trimmed)
			}
		}
	}

	return result
}

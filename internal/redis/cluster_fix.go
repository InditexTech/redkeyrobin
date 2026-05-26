// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
)

const clusterCLIErrorOutputLimit = 500

// ClusterFixResult holds the outcome of redis-cli --cluster fix.
type ClusterFixResult struct {
	CommandCodeOutput int
	Output            string
}

// ClusterFix executes redis-cli --cluster fix against the current node address.
// It uses --cluster-yes so remediation can run non-interactively.
func (c *Client) ClusterFix(ctx context.Context) (*ClusterFixResult, error) {
	cmd := newRedisCLICommand(ctx, []string{"--cluster", "fix", c.addr, "--cluster-yes"}, c.redisCLIEnv())
	cmd.Run()

	if cmd.Err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis-cli --cluster fix canceled or timed out: %w", ctx.Err())
		}
		if cmd.ExitCode == -1 {
			return nil, fmt.Errorf("redis-cli --cluster fix on %s: %w", c.addr, cmd.Err)
		}
	}

	if cmd.ExitCode != 0 {
		return nil, formatClusterCLIExitCodeError("fix", c.addr, cmd.ExitCode, cmd.GetCombinedOutput())
	}

	return &ClusterFixResult{
		CommandCodeOutput: cmd.ExitCode,
		Output:            cmd.GetCombinedOutput(),
	}, nil
}

func formatClusterCLIExitCodeError(operation, addr string, exitCode int, output string) error {
	return fmt.Errorf(
		"redis-cli --cluster %s on %s exited with code %d: %s",
		operation,
		addr,
		exitCode,
		truncateClusterCLIOutput(output, clusterCLIErrorOutputLimit),
	)
}

func truncateClusterCLIOutput(output string, maxLen int) string {
	if len(output) <= maxLen {
		return output
	}
	return output[:maxLen] + "...(truncated)"
}

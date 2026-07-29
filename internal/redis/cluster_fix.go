// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const clusterCLIErrorOutputLimit = 2000

// clusterFixConfirmations answers the interactive prompts that redis-cli
// --cluster fix raises while repairing slot coverage. redis-cli deliberately
// ignores --cluster-yes for the slot-coverage prompts ("Fix these slots by
// covering with a random node?", etc.) and always reads "yes" from stdin, so
// the operator must provide the confirmations explicitly. There are up to three
// such prompts (slots with keys in no node, one node, and multiple nodes); a
// handful of "yes" lines safely covers every case.
const clusterFixConfirmations = "yes\nyes\nyes\nyes\nyes\n"

// ClusterFixResult holds the outcome of redis-cli --cluster fix.
type ClusterFixResult struct {
	CommandCodeOutput int
	Output            string
}

// ClusterFix executes redis-cli --cluster fix against the current node address.
// It uses --cluster-yes for the prompts that honor it and feeds "yes" on stdin
// for the slot-coverage prompts that redis-cli always reads interactively, so
// remediation can run non-interactively.
func (c *Client) ClusterFix(ctx context.Context) (*ClusterFixResult, error) {
	cmd := newRedisCLICommand(ctx, []string{"--cluster", "fix", c.addr, "--cluster-yes"}, c.redisCLIEnv())
	cmd.SetStdin(strings.NewReader(clusterFixConfirmations))
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

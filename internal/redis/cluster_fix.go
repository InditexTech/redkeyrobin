// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
)

// ClusterFixResult holds the outcome of redis-cli --cluster fix.
type ClusterFixResult struct {
	CommandCodeOutput int
	Output            string
}

// ClusterFix executes redis-cli --cluster fix against the current node address.
// It pipes "yes" to stdin to automatically confirm slot migration prompts.
func (c *Client) ClusterFix(ctx context.Context) (*ClusterFixResult, error) {
	cmd := newRedisCLICommand(ctx, []string{"--cluster", "fix", c.addr}, c.redisCLIEnv())
	cmd.Run()

	if cmd.Err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis-cli --cluster fix canceled or timed out: %w", ctx.Err())
		}
		if cmd.ExitCode == -1 {
			return nil, fmt.Errorf("redis-cli --cluster fix on %s: %w", c.addr, cmd.Err)
		}
	}

	return &ClusterFixResult{
		CommandCodeOutput: cmd.ExitCode,
		Output:            cmd.GetCombinedOutput(),
	}, nil
}

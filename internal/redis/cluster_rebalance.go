// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
)

// ClusterRebalanceResult holds the outcome of redis-cli --cluster rebalance.
type ClusterRebalanceResult struct {
	CommandCodeOutput int
	Output            string
}

// ClusterRebalance executes redis-cli --cluster rebalance against the current node address.
// It uses --cluster-use-empty-masters to include primaries with no slots.
// It also sets --cluster-yes so rebalancing runs non-interactively.
func (c *Client) ClusterRebalance(ctx context.Context) (*ClusterRebalanceResult, error) {
	cmd := newRedisCLICommand(ctx, []string{
		"--cluster", "rebalance", c.addr,
		"--cluster-use-empty-masters",
		"--cluster-yes",
	}, c.redisCLIEnv())
	cmd.Run()

	if cmd.Err != nil {
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("redis-cli --cluster rebalance canceled or timed out: %w", ctx.Err())
		}
		if cmd.ExitCode == -1 {
			return nil, fmt.Errorf("redis-cli --cluster rebalance on %s: %w", c.addr, cmd.Err)
		}
	}

	if cmd.ExitCode != 0 {
		return nil, formatClusterCLIExitCodeError("rebalance", c.addr, cmd.ExitCode, cmd.GetCombinedOutput())
	}

	return &ClusterRebalanceResult{
		CommandCodeOutput: cmd.ExitCode,
		Output:            cmd.GetCombinedOutput(),
	}, nil
}

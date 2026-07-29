// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

// ClusterRebalanceResult holds the outcome of redis-cli --cluster rebalance.
type ClusterRebalanceResult struct {
	CommandCodeOutput int
	Output            string
}

// rebalancePipeline is the number of keys migrated per MIGRATE batch during a
// rebalance. redis-cli defaults to 10, which makes resharding clusters that hold data
// slow enough to exceed the rebalance timeout. A larger batch size drastically reduces
// the number of round-trips and lets a full reshard finish within the budget.
const rebalancePipeline = 50

// ClusterRebalance executes redis-cli --cluster rebalance against the current node address.
// It uses --cluster-use-empty-masters to include primaries with no slots.
// It also sets --cluster-yes so rebalancing runs non-interactively.
//
// --cluster-replace makes each MIGRATE use the REPLACE option. This is what makes a
// rebalance safe to re-run: if a previous attempt was interrupted mid-slot it can leave
// a partial copy of a key on the destination node, and a plain MIGRATE would then abort
// with "BUSYKEY Target key name already exists", stalling every subsequent rebalance.
// With REPLACE the authoritative copy from the source overwrites the stale partial copy,
// so an interrupted reshard can resume and the cluster converges.
func (c *Client) ClusterRebalance(ctx context.Context) (*ClusterRebalanceResult, error) {
	return c.runRebalance(ctx, []string{
		"--cluster", "rebalance", c.addr,
		"--cluster-use-empty-masters",
		"--cluster-pipeline", strconv.Itoa(rebalancePipeline),
		"--cluster-replace",
		"--cluster-yes",
	})
}

// ClusterRebalanceWithWeights executes redis-cli --cluster rebalance using per-node
// weights. Nodes assigned a weight of 0 are emptied of slots, which is the mechanism
// used to drain primaries during a scale-down. The weights map keys are node IDs.
//
// --cluster-use-empty-masters is included so that primaries that currently hold no
// slots (for example, freshly added nodes during a scale-up) still participate in the
// redistribution.
func (c *Client) ClusterRebalanceWithWeights(ctx context.Context, weights map[string]int) (*ClusterRebalanceResult, error) {
	if len(weights) == 0 {
		return c.ClusterRebalance(ctx)
	}

	args := []string{
		"--cluster", "rebalance", c.addr,
		"--cluster-use-empty-masters",
		"--cluster-pipeline", strconv.Itoa(rebalancePipeline),
		"--cluster-replace",
	}

	// Sort node IDs for deterministic command construction.
	ids := make([]string, 0, len(weights))
	for id := range weights {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	args = append(args, "--cluster-weight")
	for _, id := range ids {
		args = append(args, fmt.Sprintf("%s=%d", id, weights[id]))
	}

	args = append(args, "--cluster-yes")

	return c.runRebalance(ctx, args)
}

func (c *Client) runRebalance(ctx context.Context, args []string) (*ClusterRebalanceResult, error) {
	cmd := newRedisCLICommand(ctx, args, c.redisCLIEnv())
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

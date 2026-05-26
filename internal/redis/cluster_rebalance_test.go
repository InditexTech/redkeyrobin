// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"testing"
	"time"
)

func TestClient_ClusterRebalance_Success(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
printf 'Rebalancing...\nDone.\n'
exit 0
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "secret")
	result, err := client.ClusterRebalance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CommandCodeOutput != 0 {
		t.Fatalf("expected exit code 0, got %d", result.CommandCodeOutput)
	}
	if result.Output == "" {
		t.Fatal("expected non-empty output")
	}
}

func TestClient_ClusterRebalance_NonZeroExit(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
printf 'rebalance error\n'
exit 1
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "")
	result, err := client.ClusterRebalance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CommandCodeOutput != 1 {
		t.Fatalf("expected exit code 1, got %d", result.CommandCodeOutput)
	}
}

func TestClient_ClusterRebalance_Timeout(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", "sleep 60"}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterRebalance(ctx)
	if err == nil {
		t.Fatal("expected error on timeout")
	}
}

func TestClient_ClusterRebalance_UsesAuth(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
if [ "$REDISCLI_AUTH" != "mypass" ]; then
  echo "auth missing" >&2
  exit 9
fi
echo ok
exit 0
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "mypass")
	result, err := client.ClusterRebalance(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CommandCodeOutput != 0 {
		t.Fatalf("expected exit code 0, got %d", result.CommandCodeOutput)
	}
}

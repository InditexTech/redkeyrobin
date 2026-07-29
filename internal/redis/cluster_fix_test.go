// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestClient_ClusterFix_Success(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
printf 'Fixing cluster...\nAll slots covered.\n'
exit 0
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "secret")
	result, err := client.ClusterFix(context.Background())
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

func TestClient_ClusterFix_NonZeroExit(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
printf 'error fixing\n'
exit 1
`}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterFix(context.Background())
	if err == nil {
		t.Fatal("expected error on non-zero exit code")
	}
}

func TestClient_ClusterFix_Timeout(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", "sleep 60"}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterFix(ctx)
	if err == nil {
		t.Fatal("expected error on timeout")
	}
}

func TestClient_ClusterFix_UsesAuth(t *testing.T) {
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
	result, err := client.ClusterFix(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CommandCodeOutput != 0 {
		t.Fatalf("expected exit code 0, got %d", result.CommandCodeOutput)
	}
}

func TestClient_ClusterFix_UsesClusterYes(t *testing.T) {
	originalFactory := newRedisCLICommand
	var seenArgs []string
	newRedisCLICommand = func(ctx context.Context, args []string, env map[string]string) *RedisCLICommand {
		seenArgs = append([]string(nil), args...)
		return newCLICommand(ctx, "sh", []string{"-c", "echo ok; exit 0"}, env)
	}
	defer func() { newRedisCLICommand = originalFactory }()

	client := NewClient("127.0.0.1:6379", "")
	_, err := client.ClusterFix(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Contains(seenArgs, "--cluster-yes") {
		t.Fatalf("expected --cluster-yes in args, got %v", seenArgs)
	}
}

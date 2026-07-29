// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"testing"
)

func TestParseClusterCheckOutput(t *testing.T) {
	input := `
line without markers
[ERR] Node 1: Node is not empty. Keys found: 1
[ERR] Node 1: Node is not empty. Keys found: 1
[WARNING] Node 1: Mismatching hash slots and slots configuration.
[WARNING] Node 1: Mismatching hash slots and slots configuration.
`

	result := parseClusterCheckOutput(input)
	if len(result.Errors) != 1 {
		t.Fatalf("expected 1 unique error, got %d", len(result.Errors))
	}
	if len(result.Warnings) != 1 {
		t.Fatalf("expected 1 unique warning, got %d", len(result.Warnings))
	}
	if result.Errors[0] != "Node 1: Node is not empty. Keys found: 1" {
		t.Fatalf("unexpected error %q", result.Errors[0])
	}
}

func TestClient_ClusterCheck_UsesRedisCLIAuthEnv(t *testing.T) {
	originalFactory := newRedisCLICommand
	newRedisCLICommand = func(ctx context.Context, _ []string, env map[string]string) *RedisCLICommand {
		return newCLICommand(ctx, "sh", []string{"-c", `
if [ "$REDISCLI_AUTH" != "secret" ]; then
  echo missing-auth >&2
  exit 9
fi
printf '[WARNING] warning one\n[ERR] error one\n'
exit 1
`}, env)
	}
	defer func() {
		newRedisCLICommand = originalFactory
	}()

	client := NewClient("127.0.0.1:6379", "secret")
	result, err := client.ClusterCheck(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.CommandCodeOutput != 1 {
		t.Fatalf("expected command output code 1, got %d", result.CommandCodeOutput)
	}
	if len(result.Errors) != 1 || result.Errors[0] != "error one" {
		t.Fatalf("unexpected errors %#v", result.Errors)
	}
	if len(result.Warnings) != 1 || result.Warnings[0] != "warning one" {
		t.Fatalf("unexpected warnings %#v", result.Warnings)
	}
}

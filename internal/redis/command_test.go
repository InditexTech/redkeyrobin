// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRedisCLICommand_Run_ExitNonZero(t *testing.T) {
	cmd := newCLICommand(context.Background(), "sh", []string{"-c", "echo hello; echo err >&2; exit 2"}, nil)

	cmd.Run()

	if cmd.Err == nil {
		t.Fatal("expected non-nil error")
	}
	if cmd.ExitCode != 2 {
		t.Fatalf("expected exit code 2, got %d", cmd.ExitCode)
	}
	if !strings.Contains(cmd.GetStdout(), "hello") {
		t.Fatalf("expected stdout to contain hello, got %q", cmd.GetStdout())
	}
	if !strings.Contains(cmd.GetStderr(), "err") {
		t.Fatalf("expected stderr to contain err, got %q", cmd.GetStderr())
	}
}

func TestRedisCLICommand_Wait_ExitNonZero(t *testing.T) {
	cmd := newCLICommand(context.Background(), "sh", []string{"-c", "echo out; echo err >&2; exit 3"}, nil)

	cmd.Start()
	cmd.Wait()

	if cmd.Err == nil {
		t.Fatal("expected non-nil error")
	}
	if cmd.ExitCode != 3 {
		t.Fatalf("expected exit code 3, got %d", cmd.ExitCode)
	}
	if !strings.Contains(cmd.GetCombinedOutput(), "out") || !strings.Contains(cmd.GetCombinedOutput(), "err") {
		t.Fatalf("unexpected combined output %q", cmd.GetCombinedOutput())
	}
}

func TestRedisCLICommand_Run_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	cmd := newCLICommand(ctx, "sh", []string{"-c", "sleep 1"}, nil)
	start := time.Now()
	cmd.Run()

	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("expected command to stop shortly after cancellation, took %v", time.Since(start))
	}
	if cmd.ExitCode == 0 {
		t.Fatalf("expected non-zero exit code on cancellation, got %d", cmd.ExitCode)
	}
}

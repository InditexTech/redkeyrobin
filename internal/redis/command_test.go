// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"strings"
	"testing"
)

// Test that Run() captures non-zero exit codes and command output.
func TestRedisCLICommand_Run_ExitNonZero(t *testing.T) {
	ctx := context.Background()

	// This command prints to stdout and stderr then exits with code 2.
	cmd := "echo hello; echo err >&2; exit 2"
	rcc := NewRedisCLICommand(ctx, cmd)

	rcc.Run()

	if rcc.Err == nil {
		t.Fatalf("expected non-nil Err for command that exits non-zero")
	}

	if rcc.ExitCode != 2 {
		t.Fatalf("expected ExitCode 2, got %d", rcc.ExitCode)
	}

	stdout := rcc.GetStdout()
	stderr := rcc.GetStderr()
	combined := rcc.GetCombinedOutput()

	if !strings.Contains(stdout, "hello") {
		t.Fatalf("stdout did not contain expected text; stdout=%q", stdout)
	}
	if !strings.Contains(stderr, "err") {
		t.Fatalf("stderr did not contain expected text; stderr=%q", stderr)
	}
	if !strings.Contains(combined, "hello") || !strings.Contains(combined, "err") {
		t.Fatalf("combined output missing parts; combined=%q", combined)
	}
}

// Test that Start()+Wait() captures non-zero exit codes and command output.
func TestRedisCLICommand_Wait_ExitNonZero(t *testing.T) {
	ctx := context.Background()

	// This command prints to stdout and stderr then exits with code 3.
	cmd := "echo out; echo err >&2; exit 3"
	rcc := NewRedisCLICommand(ctx, cmd)

	rcc.Start()
	rcc.Wait()

	if rcc.Err == nil {
		t.Fatalf("expected non-nil Err for command that exits non-zero (Wait)")
	}

	if rcc.ExitCode != 3 {
		t.Fatalf("expected ExitCode 3, got %d", rcc.ExitCode)
	}

	stdout := rcc.GetStdout()
	stderr := rcc.GetStderr()
	combined := rcc.GetCombinedOutput()

	if !strings.Contains(stdout, "out") {
		t.Fatalf("stdout did not contain expected text; stdout=%q", stdout)
	}
	if !strings.Contains(stderr, "err") {
		t.Fatalf("stderr did not contain expected text; stderr=%q", stderr)
	}
	if !strings.Contains(combined, "out") || !strings.Contains(combined, "err") {
		t.Fatalf("combined output missing parts; combined=%q", combined)
	}
}

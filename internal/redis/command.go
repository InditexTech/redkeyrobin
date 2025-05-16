// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"bytes"
	"context"
	"os/exec"
)

// RedisCLICommand represents a Redis CLI command.
type RedisCLICommand struct {
	cmd      *exec.Cmd
	stdout   *bytes.Buffer
	stderr   *bytes.Buffer
	ExitCode int
	Err      error
}

// NewRedisCLICommand creates a new Redis CLI command.
func NewRedisCLICommand(ctx context.Context, command string) *RedisCLICommand {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	return &RedisCLICommand{
		cmd:    cmd,
		stdout: &stdout,
		stderr: &stderr,
	}
}

// Run executes the Redis CLI command synchronously and captures the exit code.
func (rcc *RedisCLICommand) Run() {
	rcc.Err = rcc.cmd.Run()
	rcc.CheckStatusCode()
}

// Start executes the Redis CLI command asynchronously.
func (rcc *RedisCLICommand) Start() {
	rcc.Err = rcc.cmd.Start()
}

// CheckStatusCode captures the exit code of the Redis CLI command.
func (rcc *RedisCLICommand) CheckStatusCode() {
	exitCode := -1
	if rcc.cmd.ProcessState != nil {
		exitCode = rcc.cmd.ProcessState.ExitCode()
	}

	rcc.ExitCode = exitCode
}

// Wait waits for the Redis CLI command to finish and captures any errors.
func (rcc *RedisCLICommand) Wait() {
	rcc.Err = nil

	// Wait for command to finish
	rcc.Err = rcc.cmd.Wait()
	rcc.CheckStatusCode()
}

// GetStdout returns the standard output of the Redis CLI command.
func (rcc *RedisCLICommand) GetStdout() string {
	return rcc.stdout.String()
}

// GetStderr returns the standard error of the Redis CLI command.
func (rcc *RedisCLICommand) GetStderr() string {
	return rcc.stderr.String()
}

// GetCombinedOutput returns the combined output (both stdout and stderr) of the Redis CLI command.
func (rcc *RedisCLICommand) GetCombinedOutput() string {
	return rcc.GetStdout() + rcc.GetStderr()
}

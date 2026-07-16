// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"time"
)

const commandWaitDelay = 100 * time.Millisecond

// RedisCommand represents a Redis command execution.
type RedisCommand interface {
	Run()
	Start()
	Wait()
	Cancel()
	GetStdout() string
	GetStderr() string
	GetCombinedOutput() string
	Error() error
}

// RedisBaseCommand stores common command execution state.
type RedisBaseCommand struct {
	ExitCode int
	Err      error
}

// Error returns the execution error.
func (rbc *RedisBaseCommand) Error() error {
	return rbc.Err
}

// RedisCLICommand represents a redis-cli process execution.
type RedisCLICommand struct {
	RedisBaseCommand
	cmd    *exec.Cmd
	stdout *bytes.Buffer
	stderr *bytes.Buffer
}

// NewRedisCLICommand creates a new redis-cli command with the provided arguments and environment.
func NewRedisCLICommand(ctx context.Context, args []string, env map[string]string) *RedisCLICommand {
	return newCLICommand(ctx, "redis-cli", args, env)
}

func newCLICommand(ctx context.Context, executable string, args []string, env map[string]string) *RedisCLICommand {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, executable, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.WaitDelay = commandWaitDelay
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), formatCommandEnv(env)...)
	}

	return &RedisCLICommand{
		cmd:    cmd,
		stdout: &stdout,
		stderr: &stderr,
	}
}

// SetStdin sets the standard input for the command. It must be called before Run or Start.
func (rcc *RedisCLICommand) SetStdin(r io.Reader) {
	rcc.cmd.Stdin = r
}

func formatCommandEnv(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	formatted := make([]string, 0, len(keys))
	for _, key := range keys {
		formatted = append(formatted, key+"="+env[key])
	}
	return formatted
}

// Run executes the command synchronously.
func (rcc *RedisCLICommand) Run() {
	if err := rcc.cmd.Run(); err != nil {
		rcc.Err = fmt.Errorf("%w: %s", err, rcc.GetCombinedOutput())
	} else {
		rcc.Err = nil
	}

	rcc.checkStatusCode()
}

// Start executes the command asynchronously.
func (rcc *RedisCLICommand) Start() {
	rcc.Err = rcc.cmd.Start()
	if rcc.Err != nil {
		rcc.checkStatusCode()
	}
}

func (rcc *RedisCLICommand) checkStatusCode() {
	exitCode := -1
	if rcc.cmd.ProcessState != nil {
		exitCode = rcc.cmd.ProcessState.ExitCode()
	}
	rcc.ExitCode = exitCode
}

// Wait waits for the asynchronous command to finish.
func (rcc *RedisCLICommand) Wait() {
	if err := rcc.cmd.Wait(); err != nil {
		rcc.Err = fmt.Errorf("%w: %s", err, rcc.GetCombinedOutput())
	} else {
		rcc.Err = nil
	}

	rcc.checkStatusCode()
}

// Cancel cancels the running command.
func (rcc *RedisCLICommand) Cancel() {
	if rcc.cmd.Cancel != nil {
		_ = rcc.cmd.Cancel()
		return
	}
	if rcc.cmd.Process != nil {
		_ = rcc.cmd.Process.Kill()
	}
}

// GetStdout returns the standard output.
func (rcc *RedisCLICommand) GetStdout() string {
	return rcc.stdout.String()
}

// GetStderr returns the standard error.
func (rcc *RedisCLICommand) GetStderr() string {
	return rcc.stderr.String()
}

// GetCombinedOutput returns the combined output.
func (rcc *RedisCLICommand) GetCombinedOutput() string {
	return rcc.GetStdout() + rcc.GetStderr()
}

// Error returns the combined command output when execution failed.
func (rcc *RedisCLICommand) Error() error {
	if rcc.Err == nil {
		return nil
	}
	return fmt.Errorf("%s", rcc.GetCombinedOutput())
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sync"
)

// RedisCommand represents a Redis command.
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

// RedisBaseCommand represents a base Redis command.
type RedisBaseCommand struct {
	ExitCode int
	Err      error
}

// ExitCode returns the error of the Redis command.
func (rbc *RedisBaseCommand) Error() error {
	return rbc.Err
}

// RedisCLICommand represents a Redis CLI command.
type RedisCLICommand struct {
	RedisBaseCommand
	cmd    *exec.Cmd
	stdout *bytes.Buffer
	stderr *bytes.Buffer
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
	if err := rcc.cmd.Run(); err != nil {
		// preserve original error while attaching command output
		rcc.Err = fmt.Errorf("%w: %s", err, rcc.GetCombinedOutput())
	} else {
		rcc.Err = nil
	}

	rcc.checkStatusCode()
}

// Start executes the Redis CLI command asynchronously.
func (rcc *RedisCLICommand) Start() {
	rcc.Err = rcc.cmd.Start()
}

// CheckStatusCode captures the exit code of the Redis CLI command.
func (rcc *RedisCLICommand) checkStatusCode() {
	exitCode := -1
	if rcc.cmd.ProcessState != nil {
		exitCode = rcc.cmd.ProcessState.ExitCode()
	}

	rcc.ExitCode = exitCode
}

// Wait waits for the Redis CLI command to finish and captures any errors.
func (rcc *RedisCLICommand) Wait() {
	if err := rcc.cmd.Wait(); err != nil {
		rcc.Err = fmt.Errorf("%w: %s", err, rcc.GetCombinedOutput())
	} else {
		rcc.Err = nil
	}

	rcc.checkStatusCode()
}

// Cancel cancels the Redis CLI command.
func (rcc *RedisCLICommand) Cancel() {
	if rcc.cmd.Process != nil {
		rcc.cmd.Cancel()
	}
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

func (rcc *RedisCLICommand) Error() error {
	if rcc.Err == nil {
		return nil
	}
	return fmt.Errorf("%s", rcc.GetCombinedOutput())
}

// RedisLibraryCommand represents a Redis library command.
type RedisLibraryCommand struct {
	RedisBaseCommand
	ctx    context.Context
	wg     sync.WaitGroup
	cmd    func(context.Context) error
	cancel context.CancelFunc
}

// NewRedisLibraryCommand creates a new Redis library command.
func NewRedisLibraryCommand(ctx context.Context, cmd func(context.Context) error) *RedisLibraryCommand {
	ctx, cancel := context.WithCancel(ctx)

	return &RedisLibraryCommand{
		ctx:    ctx,
		cmd:    cmd,
		cancel: cancel,
		wg:     sync.WaitGroup{},
	}
}

// Run executes the Redis library command synchronously and captures the exit code.
func (rlc *RedisLibraryCommand) Run() {
	rlc.Start()
	rlc.Wait()
}

// Start executes the Redis library command asynchronously.
func (rlc *RedisLibraryCommand) Start() {
	rlc.wg.Add(1)
	go func() {
		defer rlc.wg.Done()
		rlc.Err = rlc.cmd(rlc.ctx)
	}()
}

// Wait waits for the Redis library command to finish and captures any errors.
func (rlc *RedisLibraryCommand) Wait() {
	rlc.wg.Wait()
	rlc.checkStatusCode()
}

// Cancel cancels the Redis library command.
func (rlc *RedisLibraryCommand) Cancel() {
	rlc.cancel()
}

// GetStdout returns the standard output of the Redis library command.
func (rlc *RedisLibraryCommand) GetStdout() string {
	return ""
}

// GetStderr returns the standard error of the Redis library command.
func (rlc *RedisLibraryCommand) GetStderr() string {
	return ""
}

// GetCombinedOutput returns the combined output (both stdout and stderr) of the Redis library command.
func (rlc *RedisLibraryCommand) GetCombinedOutput() string {
	return ""
}

// CheckStatusCode captures the exit code of the Redis library command.
func (rlc *RedisLibraryCommand) checkStatusCode() {
	rlc.ExitCode = 0

	if rlc.Err != nil {
		rlc.ExitCode = 1
		return
	}
	if rlc.ctx.Err() != nil {
		rlc.Err = rlc.ctx.Err()
		rlc.ExitCode = 1
	}
}

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

type RedisBaseCommand struct {
	ExitCode int
	Err      error
}

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
	rcc.Err = rcc.cmd.Run()
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
	rcc.Err = nil

	// Wait for command to finish
	rcc.Err = rcc.cmd.Wait()
	rcc.checkStatusCode()
}

// Cancel cancels the Redis CLI command.
func (rcc *RedisCLICommand) Cancel() {
	rcc.Err = rcc.cmd.Cancel()
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

type RedisLibraryCommand struct {
	RedisBaseCommand
	ctx    context.Context
	wg     sync.WaitGroup
	cmd    func(context.Context) error
	cancel context.CancelFunc
}

func NewRedisLibraryCommand(ctx context.Context, cmd func(context.Context) error) *RedisLibraryCommand {
	ctx, cancel := context.WithCancel(ctx)

	return &RedisLibraryCommand{
		ctx:    ctx,
		cmd:    cmd,
		cancel: cancel,
		wg:     sync.WaitGroup{},
	}
}

func (rlc *RedisLibraryCommand) Run() {
	rlc.Start()
	rlc.Wait()
}

func (rlc *RedisLibraryCommand) Start() {
	rlc.wg.Add(1)
	go func() {
		defer rlc.wg.Done()
		rlc.Err = rlc.cmd(rlc.ctx)
	}()
}

func (rlc *RedisLibraryCommand) Wait() {
	rlc.wg.Wait()
	rlc.checkStatusCode()
}

func (rlc *RedisLibraryCommand) Cancel() {
	rlc.cancel()
}

func (rlc *RedisLibraryCommand) GetStdout() string {
	return ""
}

func (rlc *RedisLibraryCommand) GetStderr() string {
	return ""
}

func (rlc *RedisLibraryCommand) GetCombinedOutput() string {
	return ""
}

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

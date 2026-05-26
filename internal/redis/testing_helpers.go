// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import "context"

// RedisCLICommandFactory is the type of newRedisCLICommand for test overriding.
type RedisCLICommandFactory = func(ctx context.Context, args []string, env map[string]string) *RedisCLICommand

// ExportNewRedisCLICommand returns the current factory for tests to save/restore.
func ExportNewRedisCLICommand() RedisCLICommandFactory {
	return newRedisCLICommand
}

// SetNewRedisCLICommand overrides the factory (for external test packages).
func SetNewRedisCLICommand(f RedisCLICommandFactory) {
	newRedisCLICommand = f
}

// NewCLICommandExported exposes newCLICommand for external test packages.
func NewCLICommandExported(ctx context.Context, executable string, args []string, env map[string]string) *RedisCLICommand {
	return newCLICommand(ctx, executable, args, env)
}

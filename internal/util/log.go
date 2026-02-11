// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"log/slog"
	"os"
)

// rootLogger is the root logger for the application.
var rootLogger *slog.Logger

// InitLogger initializes the root logger for the application.
func InitLogger() *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, nil)

	rootLogger = slog.New(handler)
	return rootLogger
}

// GetLogger returns a logger with the specified name.
func GetLogger(name string) *slog.Logger {
	if rootLogger == nil {
		rootLogger = InitLogger()
	}

	return rootLogger.With("component", fmt.Sprintf("robin.%s", name))
}

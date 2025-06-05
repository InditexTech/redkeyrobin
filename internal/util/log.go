// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"log/slog"
	"os"
)

var rootLogger *slog.Logger

func InitLogger() *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, nil)

	rootLogger = slog.New(handler)
	return rootLogger
}

func GetLogger(name string) *slog.Logger {
	if rootLogger == nil {
		rootLogger = InitLogger()
	}

	return rootLogger.With("component", fmt.Sprintf("robin.%s", name))
}

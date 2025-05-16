// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"flag"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var rootLogger logr.Logger

func InitLogger() logr.Logger {
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)

	rootLogger = zap.New(zap.UseFlagOptions(&opts)).WithName("robin")
	ctrl.SetLogger(rootLogger)

	return rootLogger
}

func GetLogger(name string) logr.Logger {
	return rootLogger.WithName(name)
}

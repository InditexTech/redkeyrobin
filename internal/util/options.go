// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"flag"
)

const (
	defaultHttpServeAddress  = ":8080"
	defaultConfigmapFilePath = "/opt/conf/configmap/application-configmap.yml"
)

// Options represents the command-line options.
type Options struct {
	ConfigMapPath  string
	Address        string
	DisableMetrics bool
}

// ParseOptions parses the command-line options.
func ParseOptions() *Options {
	var httpServerAddress, configmapFilePath string
	var disableMetrics bool

	// Parse CLI flags
	flag.StringVar(&configmapFilePath, "configmap", defaultConfigmapFilePath, "The path to the ConfigMap file.")
	flag.StringVar(&httpServerAddress, "address", defaultHttpServeAddress, "The address the HTTP server binds to.")
	flag.BoolVar(&disableMetrics, "disable-metrics", false, "If included, disables the metrics.")
	flag.Parse()

	return &Options{
		ConfigMapPath:  configmapFilePath,
		Address:        httpServerAddress,
		DisableMetrics: disableMetrics,
	}
}

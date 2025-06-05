package util

import (
	"flag"
)

const (
	defaultHttpServeAddress  = ":8080"
	defaultConfigmapFilePath = "/opt/conf/configmap/application-configmap.yml"
)

type Options struct {
	ConfigMapPath  string
	Address        string
	DisableMetrics bool
}

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

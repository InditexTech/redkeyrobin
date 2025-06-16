// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"slices"
	"strings"

	"github.com/inditextech/redisrobin/internal/redis"
)

// nodeInfoReplacer is used to clean up node info strings.
var nodeInfoReplacer = strings.NewReplacer("{", "", "}", "", "\r", "", "[", "", "]", "")

// ClusterManager encapsulates cluster-level logic such as maintaining an IP list and
// resetting metrics if the node list changes.
type ClusterManager struct {
	ipList         []string
	metricsManager *MetricsManager
}

// NewClusterManager constructs a manager with an empty IP list initially.
func NewClusterManager(mm *MetricsManager) *ClusterManager {
	return &ClusterManager{
		ipList:         make([]string, 0),
		metricsManager: mm,
	}
}

// CheckClusterNodes compares known node IPs with the current node list and resets metrics
// if there is a change. The gatherIPList logic is encapsulated here rather than using a global IpList.
func (cm *ClusterManager) CheckClusterNodes(nodesInfo []*redis.RedisNode) {
	if cm.needsRefresh(nodesInfo) {
		cm.generateIpList(nodesInfo)
		cm.metricsManager.ResetMetrics()
	}
}

// generateIpList rebuilds the ipList from the given nodesInfo.
func (cm *ClusterManager) generateIpList(nodesInfo []*redis.RedisNode) {
	ipNodes := make([]string, 0, len(nodesInfo))
	for _, node := range nodesInfo {
		nodeInfoStringList := strings.Split(nodeInfoReplacer.Replace(fmt.Sprintf("%v", node)), " ")
		if len(nodeInfoStringList) > 1 {
			ipNodes = append(ipNodes, nodeInfoStringList[1])
		}
	}
	cm.ipList = ipNodes
}

// needsRefresh detects if there's a mismatch between the current ipList and the new node set.
func (cm *ClusterManager) needsRefresh(nodesInfo []*redis.RedisNode) bool {
	if len(cm.ipList) != len(nodesInfo) {
		return true
	}

	for _, node := range nodesInfo {
		nodeInfoStringList := strings.Split(nodeInfoReplacer.Replace(fmt.Sprintf("%v", node)), " ")
		if len(nodeInfoStringList) < 2 {
			return true
		}
		nodeIP := nodeInfoStringList[1]
		if !slices.Contains(cm.ipList, nodeIP) {
			return true
		}
	}
	return false
}

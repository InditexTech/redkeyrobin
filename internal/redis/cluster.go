// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/inditextech/redisrobin/internal/config"
)

const (
	Initializing     = "Initializing"
	Ready            = "Ready"
	Error            = "Error"
	Upgrading        = "Upgrading"
	ScalingDown      = "ScalingDown"
	ScalingUp        = "ScalingUp"
	Maintenance      = "Maintenance"
	Unknown          = "Unknown"
	Meeting          = "Meeting"
	Resharding       = "Resharding"
	ReshardingError  = "ReshardingError"
	Forgetting       = "Forgetting"
	Rebalancing      = "Rebalancing"
	RebalancingError = "RebalancingError"
	Fixing           = "Fixing"
	EnsuringRatio    = "EnsuringRatio"
)


// NewRedisCluster creates a new Redis cluster
func NewRedisCluster(conf *config.Configuration) *RedisCluster {
	return &RedisCluster{
		conf:       conf,
		status:     Unknown,
		nodes:      make(map[string]*Node),
		operations: make(map[string][]*RedisOperation),
	}
}

// RedisCluster represents a Redis cluster
type RedisCluster struct {
	conf       *config.Configuration
	status     string
	nodes      map[string]*Node
	operations map[string][]*RedisOperation
}

// GetRedisClusterStatus returns the status of the Redis cluster from Operator perspective
func (rc *RedisCluster) GetRedisClusterStatus() string {
	return rc.conf.Redis.Cluster.Status
}

// GetStatus returns the status of the Redis cluster from Robin perspective
func (rc *RedisCluster) GetStatus() string {
	return rc.status
}

// GetReplicas returns the number of replicas in the Redis cluster
func (rc *RedisCluster) GetReplicas() int {
	return rc.conf.Redis.Cluster.Replicas
}

// GetName returns the name of the Redis cluster
func (rc *RedisCluster) GetName() string {
	return rc.conf.Redis.Cluster.Name
}

// GetNamespace returns the namespace of the Redis cluster
func (rc *RedisCluster) GetNamespace() string {
	return rc.conf.Redis.Cluster.Namespace
}

// GetAddress returns the address of the Redis cluster
func (rc *RedisCluster) GetAddress() string {
	return rc.conf.Redis.Cluster.Name
}

// GetReconcilerInterval returns the interval of the Redis cluster reconciler
func (rc *RedisCluster) GetReconcilerInterval() int {
	return rc.conf.Redis.Reconciler.IntervalSeconds
}

// GetClusterMaxRetries returns the maximum number of retries for a Redis cluster check connection operation
func (rc *RedisCluster) GetClusterMaxRetries() int {
	return rc.conf.Redis.Cluster.MaxRetries
}

// GetClusterBackOff returns the backoff duration for a Redis cluster check connection operation
func (rc *RedisCluster) GetClusterBackOff() time.Duration {
	return rc.conf.Redis.Cluster.BackOff
}

// GetClusterHealingTime returns the healing time for a Redis cluster
func (rc *RedisCluster) GetClusterHealingTime() int {
	return rc.conf.Redis.Cluster.HealingTimeSeconds
}

// GetClusterHealthProbePeriod returns the health probe period for a Redis cluster
func (rc *RedisCluster) GetClusterHealthProbePeriod() int {
	return rc.conf.Redis.Cluster.HealthProbePeriodSeconds
}

// GetMetricsRedisInfoKeys returns the Redis info keys to be collected
func (rc *RedisCluster) GetMetricsRedisInfoKeys() []string {
	return rc.conf.Redis.Metrics.RedisInfoKeys
}

// GetMetricsInterval returns the interval for collecting Redis metrics
func (rc *RedisCluster) GetMetricsInterval() int {
	return rc.conf.Redis.Metrics.IntervalSeconds
}

// GetNodes returns the Redis nodes in the cluster
func (rc *RedisCluster) GetNodes() []*Node {
	nodes := []*Node{}
	for _, node := range rc.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// GetMetadata returns the metadata of the Redis cluster
func (rc *RedisCluster) GetMetadata() map[string]string {
	return rc.conf.Metadata
}

func (rc *RedisCluster) GetNode(name string) *Node {
	return rc.nodes[name]
}

// Init initializes the Redis cluster. It should be called after creating a new Redis cluster. 
// It sets the Robin status from the Redis Cluster status and creates the nodes and initializes them.
func (rc *RedisCluster) Init() error {
	// Initialize nodes
	for i := range rc.GetReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
		nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
		node := rc.addNode(nodeName, nodeAddr)

		if err := node.Init(); err != nil {
			log.Printf("Error initializing node %s: %v", nodeName, err)
			continue
		}
	}

	// Get Redis client and check connection
	_, redisClient, err := rc.getAndCheckRedisClient(false)
	if err != nil {
		return err
	}
	defer redisClient.Close()

	// Get nodes info
	nodesInfo, err := redisClient.GetNodesInfo()
	if err != nil {
		return err
	}

	// Update nodes info
	rc.updateNodesInfo(nodesInfo)

	return nil
}

func (rc *RedisCluster) updateNodesInfo(nodesInfo []Node) {
	for _, nodeInfo := range nodesInfo {
		node := rc.getNodeFromID(nodeInfo.ID)
		if node == nil {
			log.Printf("Node with ID %s not found in Redis cluster", nodeInfo.ID)
			continue
		}
		node.UpdateInfo(nodeInfo)
	}
}

func (rc *RedisCluster) getNodeFromID(nodeID string) *Node {
	for _, node := range rc.nodes {
		if node.ID == nodeID {
			return node
		}
	}
	return nil
}

// SetReplicas sets the number of replicas in the Redis cluster.
// It adds or removes nodes to match the desired number of replicas.
// It returns an OperationAlreadyDoneError if the current number of replicas is equal to the desired number of replicas.
func (rc *RedisCluster) SetReplicas(replicas int) error {
	currentReplicas := rc.GetReplicas()
	log.Printf("Changing Redis Cluster replicas from %d to %d", currentReplicas, replicas)
	
	if replicas == currentReplicas {
		return &OperationAlreadyDoneError{Operation: "SetReplicas"}
	}

	rc.conf.Redis.Cluster.Replicas = replicas

	if replicas > currentReplicas {
		for i := currentReplicas; i < replicas; i++ {
			nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
			nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())
			rc.addNode(nodeName, nodeAddr)
		}
	} else {
		for i := currentReplicas; i > replicas; i-- {
			nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
			rc.removeNode(nodeName)
		}
	}

	return nil
}

func (rc *RedisCluster) SetRedisClusterStatus(status string) error {
	log.Printf("Changing Redis Cluster status from %s to %s", rc.GetStatus(), status)
	rc.conf.Redis.Cluster.Status = status
	return nil
}

// Rebalance rebalances the Redis cluster
// It launches the cluster rebalance command and, depending on the async flag, waits for it to finish synchronously or asynchronously
// It returns an OperationInProgressError if the cluster is already rebalancing
// It returns an OperationAlreadyDoneError if the cluster has already been rebalanced
func (rc *RedisCluster) Rebalance(async bool) error {
	log.Printf("Rebalancing cluster %s", rc.GetName())

	// Check if the cluster is already rebalancing or has been rebalanced
	if rc.IsRebalancing() {
		log.Printf("Cluster is already rebalancing")
		return &OperationInProgressError{Operation: "Rebalance"}
	} else if rc.HasBeenRebalanced() {
		log.Printf("Cluster has already been rebalanced")
		return &OperationAlreadyDoneError{Operation: "Rebalance"}
	}

	// Get Redis client and check connection
	ctx, redisClient, err := rc.getAndCheckRedisClient(true)
	if err != nil {
		return err
	}

	// Launch cluster rebalance
	cmd := redisClient.ClusterRebalance(ctx)
	if cmd.Err != nil {
		return cmd.Err
	}

	// Launch wait for rebalance to finish synchronously or asynchronously depending on the async flag
	if async {
		go rc.waitForRebalanceToFinish(cmd)
	} else {
		rc.waitForRebalanceToFinish(cmd)
	}

	return nil
}

func (rc *RedisCluster) MoveSlots(from, to *Node, slots int) error {
	log.Printf("Moving %v slots from %s to %s", slots, from.Name, to.Name)

	// Check if there is an ongoing reshard between the specified nodes
	if rc.IsResharding(*from, *to) {
		log.Printf("There is already a reshard operation between nodes %s and %s", from.Name, to.Name)
		return &OperationInProgressError{Operation: "Resharding"}
	} else if rc.HasBeenResharded(*from, *to) {
		log.Printf("Slots have already been moved from node %s to node %s", from.Name, to.Name)
		return &OperationAlreadyDoneError{Operation: "Resharding"}
	}

	// Get Redis client and check connection
	ctx, redisClient, err := rc.getAndCheckRedisClient(true)
	if err != nil {
		return err
	}

	// Launch node reshard
	cmd := redisClient.ReshardNode(ctx, *from, *to, slots)
	if cmd.Err != nil {
		return cmd.Err
	}

	// Wait for reshard to finish asynchronously
	go rc.waitForReshardToFinish(cmd, from, to)

	return nil
}

func (rc *RedisCluster) Check() error {
	return nil
}

func (rc *RedisCluster) Fix() error {
	return nil
}

// IsRebalancing returns true if the cluster is rebalancing right now
func (rc *RedisCluster) IsRebalancing() bool {
	return rc.hasOperation(Rebalancing, "Running")
}

func (rc *RedisCluster) IsResharding(from, to Node) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Running", from, to)
}

// IsForgetting returns true if the cluster is forgetting a node right now
func (rc *RedisCluster) IsForgetting(node Node) bool {
	return rc.hasOperationInNode(Forgetting, "Running", node)
}

func (rc *RedisCluster) IsMeeting(from, to Node) bool {
	return rc.hasOperation(Meeting, "Running")
}

func (rc *RedisCluster) IsEnsuringRatio() bool {
	return rc.hasOperation(EnsuringRatio, "Running")
}

func (rc *RedisCluster) IsFixing() bool {
	return rc.hasOperation(Fixing, "Running")
}

// HasBeenRebalanced returns true if the cluster has been rebalanced recently
func (rc *RedisCluster) HasBeenRebalanced() bool {
	return rc.hasOperation(Rebalancing, "Finished")
}

func (rc *RedisCluster) HasBeenResharded(from, to Node) bool {
	return rc.hasOperationBetweenNodes(Resharding, "Finished", from, to)
}

// addNode adds a new Redis node to the cluster
func (rc *RedisCluster) addNode(name, addr string) *Node {
	node := &Node{
		Name:     name,
		Addr:     addr,
	}

	// TODO: cluster meet, cluster rebalance, etc.

	rc.nodes[name] = node
	return node
}

// removeNode removes a Redis node from the cluster
func (rc *RedisCluster) removeNode(name string) {
	// TODO: cluster forget, cluster rebalance, etc.

	delete(rc.nodes, name)
}

// hasOperation returns true if the cluster has an operation with the specified name and status
func (rc *RedisCluster) hasOperation(name string, status string) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.Status == status {
			return true
		}
	}
	return false
}

// hasOperationInNode returns true if the cluster has an operation with the specified name and status in the specified node
func (rc *RedisCluster) hasOperationInNode(name string, status string, node Node) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.Status != status {
			continue
		}

		if operation.NodeFrom == nil || operation.NodeTo == nil {
			continue
		}

		if operation.NodeFrom.Name == node.Name {
			return true
		}
	}
	return false
}

// hasOperationBetweenNodes returns true if the cluster has an operation with the specified name and status between the specified nodes
func (rc *RedisCluster) hasOperationBetweenNodes(name string, status string, from Node, to Node) bool {
	operations, ok := rc.operations[name]
	if !ok {
		return false
	}

	for _, operation := range operations {
		if operation.Status != status {
			continue
		}

		if operation.NodeFrom == nil || operation.NodeTo == nil {
			continue
		}

		if operation.NodeFrom.Name == from.Name && operation.NodeTo.Name == to.Name {
			return true
		}
	}
	return false
}

func (rc *RedisCluster) waitForReshardToFinish(cmd *RedisCLICommand, from, to *Node) {
	rc.status = Resharding
	operation := rc.addOperation(Resharding, cmd, from, to)

	// Wait for reshard to finish
	err := operation.Wait()

	// Reshard failed
	if err != nil {
		rc.status = ReshardingError
		log.Printf("Error resharding cluster: %v", err)
		return
	}

	// Reshard finished successfully
	rc.status = Ready
	log.Printf("Slots of node %v moved successfully to node %v", from.Name, to.Name)
}

// waitForRebalanceToFinish waits for the cluster rebalance to finish and updates the status
func (rc *RedisCluster) waitForRebalanceToFinish(cmd *RedisCLICommand) {
	rc.status = Rebalancing
	operation := rc.addOperation(Rebalancing, cmd, nil, nil)

	// Wait for cluster rebalance to finish
	err := operation.Wait()

	// Rebalance failed
	if err != nil {
		rc.status = RebalancingError
		log.Printf("Error rebalancing cluster: %v", err)
		return
	}

	// Rebalance finished successfully
	rc.status = Ready
	log.Printf("Cluster rebalanced successfully")
}

// getAndCheckRedisClient creates a Redis client and checks the connection
func (rc *RedisCluster) getAndCheckRedisClient(close bool) (context.Context, *RedisClient, error) {
	// Create Redis client
	ctx := context.Background()
	redisClient := NewRedisClient(ctx, rc.GetAddress(), os.Getenv("REDISAUTH"), 0)
	if close {
		defer redisClient.Close()
	}
	
	// Check connection
	if err := redisClient.CheckConnection(rc.GetClusterMaxRetries(), rc.GetClusterBackOff()); err != nil {
		return nil, nil, err
	}

	return ctx, redisClient, nil
}

// addOperation adds a new operation to the cluster operations map
func (rc *RedisCluster) addOperation(operationName string, cmd *RedisCLICommand, nodeFrom, nodeTo *Node) *RedisOperation {
	operation := &RedisOperation{
		Name:          operationName,
		Status:        "Running",
		InitTimestamp: time.Now(),
		Cmd:           cmd,
		NodeFrom: 	nodeFrom,
		NodeTo: 	nodeTo,
	}
	if rc.operations[operationName] == nil {
		rc.operations[operationName] = make([]*RedisOperation, 0)
	}

	rc.operations[operationName] = append(rc.operations[operationName], operation)
	return operation
}

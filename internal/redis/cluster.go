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
	Forgetting       = "Forgetting"
	Rebalancing      = "Rebalancing"
	RebalancingError = "RebalancingError"
	Fixing           = "Fixing"
	EnsuringRatio    = "EnsuringRatio"
)

type RedisCluster struct {
	conf       *config.Configuration
	status     string
	nodes      map[string]RedisNode
	operations map[string][]*RedisOperation
}

func (rc *RedisCluster) GetStatus() string {
	return rc.status
}

func (rc *RedisCluster) GetReplicas() int {
	return rc.conf.Redis.Cluster.Replicas
}

func (rc *RedisCluster) GetRedisClusterStatus() string {
	return rc.conf.Redis.Cluster.Status
}

func (rc *RedisCluster) GetName() string {
	return rc.conf.Redis.Cluster.Name
}

func (rc *RedisCluster) GetNamespace() string {
	return rc.conf.Redis.Cluster.Namespace
}

func (rc *RedisCluster) GetAddress() string {
	return rc.conf.Redis.Cluster.Name
}

func (rc *RedisCluster) GetReconcilerInterval() int {
	return rc.conf.Redis.Reconciler.IntervalSeconds
}

func (rc *RedisCluster) GetClusterMaxRetries() int {
	return rc.conf.Redis.Cluster.MaxRetries
}

func (rc *RedisCluster) GetClusterBackOff() time.Duration {
	return rc.conf.Redis.Cluster.BackOff
}

func (rc *RedisCluster) GetClusterHealingTime() int {
	return rc.conf.Redis.Cluster.HealingTimeSeconds
}

func (rc *RedisCluster) GetClusterHealthProbePeriod() int {
	return rc.conf.Redis.Cluster.HealthProbePeriodSeconds
}

func (rc *RedisCluster) GetMetricsRedisInfoKeys() []string {
	return rc.conf.Redis.Metrics.RedisInfoKeys
}

func (rc *RedisCluster) GetMetricsInterval() int {
	return rc.conf.Redis.Metrics.IntervalSeconds
}

func (rc *RedisCluster) GetNodes() map[string]RedisNode {
	return rc.nodes
}

func (rc *RedisCluster) GetMetadata() map[string]string {
	return rc.conf.Metadata
}

type RedisNode struct {
	ID       string
	Name     string
	Addr     string
	Slots    []int
	Status   string
	Role     string
	MasterID string
}

type RedisOperation struct {
	Name          string
	Status        string
	NodeFrom      *RedisNode
	NodeTo        *RedisNode
	InitTimestamp time.Time
	EndTimestamp  time.Time
	Cmd           *RedisCLICommand
}

func (ro *RedisOperation) Wait() error {
	// Wait for command to finish
	ro.Cmd.Wait()
	ro.EndTimestamp = time.Now()

	// Check if command failed
	if err := ro.Cmd.Err; err != nil {
		ro.Status = Error
		return err
	}

	// Command finished successfully
	ro.Status = "Finished"
	return nil
}

func NewRedisCluster(conf *config.Configuration) *RedisCluster {
	return &RedisCluster{
		conf:       conf,
		status:     Initializing,
		nodes:      make(map[string]RedisNode),
		operations: make(map[string][]*RedisOperation),
	}
}

func (rc *RedisCluster) Init() error {
	// Initialize nodes
	for i := range rc.GetReplicas() {
		nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
		nodeAddr := fmt.Sprintf("%s.%s", nodeName, rc.GetAddress())

		rc.nodes[nodeName] = RedisNode{
			ID:       "",
			Name:     nodeName,
			Addr:     nodeAddr,
			Slots:    []int{},
			Status:   "Unknown",
			Role:     "",
			MasterID: "",
		}
	}

	return nil
}

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

			rc.nodes[nodeName] = RedisNode{
				ID:       "",
				Name:     nodeName,
				Addr:     nodeAddr,
				Slots:    []int{},
				Status:   "Unknown",
				Role:     "",
				MasterID: "",
			}
		}
	} else {
		for i := currentReplicas; i > replicas; i-- {
			nodeName := fmt.Sprintf("%s-%d", rc.GetName(), i)
			delete(rc.nodes, nodeName)
		}
	}

	return nil
}

func (rc *RedisCluster) SetRedisClusterStatus(status string) error {
	log.Printf("Changing Redis Cluster status from %s to %s", rc.GetStatus(), status)
	rc.conf.Redis.Cluster.Status = status
	return nil
}

func (rc *RedisCluster) Rebalance() error {
	log.Printf("Rebalancing cluster %s", rc.GetName())

	if rc.IsRebalancing() {
		log.Printf("Cluster is already rebalancing")
		return &OperationInProgressError{Operation: "Rebalance"}
	} else if rc.HasBeenRebalanced() {
		log.Printf("Cluster has already been rebalanced")
		return &OperationAlreadyDoneError{Operation: "Rebalance"}
	}

	// Get Redis client and check connection
	ctx, redisClient, err := rc.getAndCheckRedisClient()
	if err != nil {
		return err
	}

	// Launch cluster rebalance
	cmd := redisClient.ClusterRebalance(ctx)
	if cmd.Err != nil {
		return cmd.Err
	}

	// Launch goroutine to wait for rebalance to finish
	go rc.waitForRebalanceToFinish(cmd)

	return nil
}

func (rc *RedisCluster) IsRebalancing() bool {
	return rc.hasOperation(Rebalancing, "Running")
}

func (rc *RedisCluster) HasBeenRebalanced() bool {
	return rc.hasOperation(Rebalancing, "Finished")
}

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

func (rc *RedisCluster) hasOperationBetweenNodes(name string, status string, from RedisNode, to RedisNode) bool {
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

func (rc *RedisCluster) waitForRebalanceToFinish(cmd *RedisCLICommand) {
	rc.status = Rebalancing
	operation := rc.addOperation(Rebalancing, cmd)

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

func (rc *RedisCluster) MoveSlots(from, to RedisNode) error {
	return nil
}

func (rc *RedisCluster) Check() error {
	return nil
}

func (rc *RedisCluster) Fix() error {
	return nil
}

func (rc *RedisCluster) getAndCheckRedisClient() (context.Context, *RedisClient, error) {
	// Create Redis client
	ctx := context.Background()
	redisClient := NewRedisClient(ctx, rc.GetAddress(), os.Getenv("REDISAUTH"), 0)
	defer redisClient.Close()

	// Check connection
	if err := redisClient.CheckConnection(rc.GetClusterMaxRetries(), rc.GetClusterBackOff()); err != nil {
		return nil, nil, err
	}

	return ctx, redisClient, nil
}

func (rc *RedisCluster) addOperation(operationName string, cmd *RedisCLICommand) *RedisOperation {
	operation := &RedisOperation{
		Name:          operationName,
		Status:        "Running",
		InitTimestamp: time.Now(),
		Cmd:           cmd,
	}
	if rc.operations[operationName] == nil {
		rc.operations[operationName] = make([]*RedisOperation, 0)
	}

	rc.operations[operationName] = append(rc.operations[operationName], operation)
	return operation
}

package redis

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/inditextech/redisrobin/internal/config"
)

const (
	Initializing = "Initializing"
	Ready = "Ready"
	Error = "Error"
	Upgrading = "Upgrading"
	ScalingDown = "ScalingDown"
	ScalingUp = "ScalingUp"
	Maintenance = "Maintenance"
	Unknown = "Unknown"
	Meeting = "Meeting"
	Resharding = "Resharding"
	Forgetting = "Forgetting"
	Rebalancing = "Rebalancing"
	RebalancingError = "RebalancingError"
	Fixing = "Fixing"
	EnsuringRatio = "EnsuringRatio"
)


type RedisCluster struct {
	Conf           *config.Configuration
	Status string
	Nodes map[string]RedisNode
	Operations map[string][]*RedisOperation
}

func (rc *RedisCluster) GetAddress() string {
	return rc.Conf.Redis.Cluster.Name
}

type RedisNode struct {
	ID string
	Name string
	Addr string
	Slots []int
	Status string
	Role  string
	MasterID string
}


type RedisOperation struct {
	Name string
	Status string
	NodeFrom RedisNode
	NodeTo RedisNode
	InitTimestamp time.Time
	EndTimestamp time.Time
	Cmd *RedisCLICommand
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
		Conf: conf,
		Status: "",
		Nodes: make(map[string]RedisNode),
		Operations: make(map[string][]*RedisOperation),
	}
}

func (rc *RedisCluster) SetReplicas(replicas int) error{
	return nil
}

func (rc *RedisCluster) SetStatus(status string) error {
	rc.Conf.Redis.Cluster.Status = status
	return nil
}

func (rc *RedisCluster) Rebalance() error {
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

	go rc.waitForRebalanceToFinish(cmd)

	return nil
}

func (rc *RedisCluster) waitForRebalanceToFinish(cmd *RedisCLICommand) {
	rc.Status = Rebalancing
	operation := rc.addOperation(Rebalancing, cmd)

	// Wait for cluster rebalance to finish
	err := operation.Wait()

	// Rebalance failed
	if err != nil {
		rc.Status = RebalancingError
		log.Printf("Error rebalancing cluster: %v", err)
		return
	}

	// Rebalance finished successfully
	rc.Status = Ready
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
	if err := redisClient.CheckConnection(rc.Conf.Redis.Cluster.MaxRetries, rc.Conf.Redis.Cluster.BackOff); err != nil {
		return nil, nil, err
	}

	return ctx, redisClient, nil
}

func (rc *RedisCluster) addOperation(operationName string, cmd *RedisCLICommand) *RedisOperation {
	operation := &RedisOperation{
		Name: operationName,
		Status: "Running",
		InitTimestamp: time.Now(),
		Cmd: cmd,
	}
	if rc.Operations[operationName] == nil {
		rc.Operations[operationName] = make([]*RedisOperation, 0)
	}

	rc.Operations[operationName] = append(rc.Operations[operationName], operation)
	return operation
}
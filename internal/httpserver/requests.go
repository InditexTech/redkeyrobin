// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import(
	"fmt"
	"encoding/json"
	"net/http"

	"github.com/inditextech/redisrobin/internal/util"
	"github.com/inditextech/redisrobin/internal/redis"
)


var (
	ValidRedisClusterStatus = []string{redis.Initializing, redis.Ready, redis.Error, redis.Upgrading, redis.ScalingDown, redis.ScalingUp, redis.Maintenance, redis.Unknown}
)

type RequestInterface interface {
	Validate() error
}

func ParseRequest(input *http.Request, output RequestInterface) error {
	// Unmarshall request
	err := json.NewDecoder(input.Body).Decode(&output)
	if err != nil {
		return err
	}
	// Validate the request
	if err := output.Validate(); err != nil {
		return err
	}
	return nil
}

type RedisClusterStatusRequest struct {
	Status string `json:"status"`
}

func (r *RedisClusterStatusRequest) Validate() error {
	if !util.StringInSlice(r.Status, ValidRedisClusterStatus) {
		return fmt.Errorf("invalid status '%s'", r.Status)
	}
	return nil
}

type ClusterReplicasRequest struct {
	Replicas int `json:"replicas"`
}

func (r *ClusterReplicasRequest) Validate() error {
	if r.Replicas < 0 {
		return fmt.Errorf("invalid replicas '%d'", r.Replicas)
	}
	return nil
}

type ClusterMoveRequest struct {
	From string `json:"from"`
	To string `json:"to"`
	Slots int `json:"slots"`
}

func (r *ClusterMoveRequest) Validate() error {
	if r.From == "" {
		return fmt.Errorf("from cannot be empty")
	}
	if r.To == "" {
		return fmt.Errorf("to cannot be empty")
	}
	if r.Slots < 0 {
		return fmt.Errorf("slots must be positive")
	}

	return nil
}
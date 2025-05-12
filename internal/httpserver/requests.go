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


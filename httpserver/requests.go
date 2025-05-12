package httpserver

import(
	"fmt"
	"encoding/json"
	"net/http"

	"github.com/inditextech/redisrobin/util"
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
	Fixing = "Fixing"
	EnsuringRatio = "EnsuringRatio"
)

var (
	ValidRedisClusterStatus = []string{Initializing, Ready, Error, Upgrading, ScalingDown, ScalingUp, Maintenance, Unknown}
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


package httpserver

import(
	"fmt"
	"encoding/json"
	"net/http"

	"github.com/inditextech/redisrobin/util"
)

var (
	ValidRedisClusterStatus = []string{"Ready", "NotReady"}
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
		return fmt.Errorf("validation error: %v", err)
	}
	return nil
}

type RedisClusterStatusRequest struct {
	Status string `json:"status"`
}

func (r *RedisClusterStatusRequest) Validate() error {
	if !util.StringInSlice(r.Status, ValidRedisClusterStatus) {
		return fmt.Errorf("invalid status: %s", r.Status)
	}
	return nil
}


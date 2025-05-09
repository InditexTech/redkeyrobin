// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"fmt"
	"log"
	"strings"
	"net/http"
	
	"github.com/inditextech/redisrobin/config"
	"github.com/inditextech/redisrobin/util"

	ctrl "sigs.k8s.io/controller-runtime"
)


// Server represents an HTTP server with a dependency on a ConfigProvider.
type Server struct {
	Config config.APIConfig
}

// Init initializes the Server using the Config in the provided Manager.
func (s *Server) Init(mgr ctrl.Manager) error {
	// Set up the HTTP server with the provided Config
	for path, pathConfiguration := range s.Config.Endpoints {
		// Check the path configuration and delete it if it is invalid
		if err := s.checkPathConfiguration(pathConfiguration); err != nil {
			log.Printf("Error checking path %s configuration: %v", path, err)
			delete(s.Config.Endpoints, path)
			continue
		}

		// Attach the handler to the metrics server
		if err := mgr.AddMetricsServerExtraHandler(path, s); err != nil {
			return fmt.Errorf("unable to attach %s handler: %v", path, err)
		}
	}
	return nil
}

// ServeHTTP routes incoming HTTP requests to the appropriate handler methods.
// It implements the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received %s request on path %s", r.Method, r.URL.Path)

	// Check if the path is configured
	pathConfiguration, found := s.Config.Endpoints[r.URL.Path]
	if !found {
		s.sendError(w, http.StatusNotFound, fmt.Sprintf("Unknown path %s", r.URL.Path))
		return
	}

	// Check if the method is allowed
	methodConfiguration, found := pathConfiguration[strings.ToLower(r.Method)].(map[string]interface{})
	if !found {
		s.sendError(w, http.StatusMethodNotAllowed, fmt.Sprintf("Method %s not allowed in path %s", r.Method, r.URL.Path))
		return
	}

	// Invoke the method that handles the request
	util.Invoke(s, methodConfiguration["operationId"].(string), w, r)
}

func (s *Server) checkPathConfiguration(pathConfiguration map[string]interface{}) error {
	// Path configuration is not empty
	if len(pathConfiguration) == 0 {
		return fmt.Errorf("no methods configured for path")
	}

	for method, methodConfiguration := range pathConfiguration {
		// Method configuration is a valid map
		methodConfigurationMap, ok := methodConfiguration.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid method configuration for method %s", method)
		}

		// Method configuration has an operationId and it is an string
		operationID, ok := methodConfigurationMap["operationId"].(string)
		if !ok {
			return fmt.Errorf("invalid operationId for method %s", method)
		}

		// Check if Server has the method
		if !util.MethodExists(s, operationID) {
			return fmt.Errorf("method %s not found", operationID)
		}
	}
	
	return nil
}

func (s *Server) sendResponse(w http.ResponseWriter, code int, object ResponseInterface) {
	response := Response{
		Code:    code,
		Headers: map[string][]string{"Content-Type": {"application/json"}},
		Object:  object,
	}
	response.WriteResponse(w)
}

func (s *Server) sendError(w http.ResponseWriter, code int, message string) {
	response := ErrorResponse{
		Error: message,
	}
	s.sendResponse(w, code, response)
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"fmt"
	"log"
	"net/http"

	"github.com/inditextech/redisrobin/internal/redis"
)

// GetRedisClusterStatus returns the status of the Redis cluster
func (s *Server) GetRedisClusterStatus(w http.ResponseWriter, r *http.Request) {
	response := RedisClusterStatusResponse{
		Status: s.redisCluster.GetRedisClusterStatus(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

// UpdateRedisClusterStatus updates the status of the Redis Cluster
func (s *Server) UpdateRedisClusterStatus(w http.ResponseWriter, r *http.Request) {
	// Parse the request body
	request := RedisClusterStatusRequest{}
	if err := ParseRequest(r, &request); err != nil {
		log.Printf("Invalid request: %v", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Update the status
	s.redisCluster.SetRedisClusterStatus(request.Status)

	// Send the response
	response := RedisClusterStatusResponse{
		Status: request.Status,
	}
	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) GetClusterReplicas(w http.ResponseWriter, r *http.Request) {
	response := ClusterReplicasResponse{
		Replicas: s.redisCluster.GetReplicas(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) UpdateClusterReplicas(w http.ResponseWriter, r *http.Request) {
	// Parse the request body
	request := ClusterReplicasRequest{}
	if err := ParseRequest(r, &request); err != nil {
		log.Printf("Invalid request: %v", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Update the replicas
	s.redisCluster.SetReplicas(request.Replicas)

	// Send the response
	response := ClusterReplicasResponse{
		Replicas: request.Replicas,
	}
	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) GetClusterStatus(w http.ResponseWriter, r *http.Request) {
	response := ClusterStatusResponse{
		Status: s.redisCluster.GetStatus(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) MoveNodeSlots(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) CheckCluster(w http.ResponseWriter, r *http.Request) {

}

func (s *Server) FixCluster(w http.ResponseWriter, r *http.Request) {
	err := s.redisCluster.Rebalance()
	if err != nil {
		if _, ok := err.(*redis.OperationInProgressError); ok {
			s.sendResponse(w, http.StatusAccepted, nil)
			return
		} else if _, ok := err.(*redis.OperationAlreadyDoneError); ok {
			s.sendResponse(w, http.StatusOK, nil)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error rebalancing cluster: %v", err))
		return
	}
	s.sendResponse(w, http.StatusCreated, nil)
}

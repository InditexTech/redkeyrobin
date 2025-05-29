// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"fmt"
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
		s.logger.Info("Invalid request", "error", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Update the status
	s.redisCluster.SetRedisClusterStatus(request.Status)

	// Send the response
	response := RedisClusterStatusResponse{
		Status: s.redisCluster.GetRedisClusterStatus(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) GetClusterReplicas(w http.ResponseWriter, r *http.Request) {
	response := ClusterReplicasResponse{
		Replicas:          s.redisCluster.GetReplicas(),
		ReplicasPerMaster: s.redisCluster.GetReplicasPerMaster(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) UpdateClusterReplicas(w http.ResponseWriter, r *http.Request) {
	// Parse the request body
	request := ClusterReplicasRequest{}
	if err := ParseRequest(r, &request); err != nil {
		s.logger.Info("Invalid request", "error", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Update the replicas
	err := s.redisCluster.SetReplicas(request.Replicas, request.ReplicasPerMaster)

	// Send the response
	response := ClusterReplicasResponse{
		Replicas:          s.redisCluster.GetReplicas(),
		ReplicasPerMaster: s.redisCluster.GetReplicasPerMaster(),
	}
	if err != nil {
		if _, ok := err.(*redis.OperationCompletedError); ok {
			s.sendResponse(w, http.StatusAccepted, response)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error updating replicas: %v", err))
		return
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
	// Parse the request body
	request := ClusterMoveSlotsRequest{}
	if err := ParseRequest(r, &request); err != nil {
		s.logger.Info("Invalid request", "error", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Get the nodes
	if request.From == request.To {
		s.sendError(w, http.StatusBadRequest, "Source and destination nodes cannot be the same")
		return
	}

	from := s.redisCluster.GetNode(request.From)
	if from == nil {
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Node '%s' not found", request.From))
		return
	}
	to := s.redisCluster.GetNode(request.To)
	if to == nil {
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Node '%s' not found", request.To))
		return
	}

	// Get the slots
	slots := request.Slots
	if slots == 0 {
		slots = from.GetNumberOfSlots()
	}

	// Launch the move
	err := s.redisCluster.MoveSlots(from, to, slots)
	response := ClusterMoveSlotsResponse{}
	if err != nil {
		if _, ok := err.(*redis.OperationInProgressError); ok {
			response.Status = "In progress"
			s.sendResponse(w, http.StatusAccepted, response)
			return
		} else if _, ok := err.(*redis.OperationCompletedError); ok {
			response.Status = "Completed"
			s.sendResponse(w, http.StatusOK, response)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error rebalancing cluster: %v", err))
		return
	}
	response.Status = "In progress"
	s.sendResponse(w, http.StatusCreated, response)
}

func (s *Server) CheckCluster(w http.ResponseWriter, r *http.Request) {
	// Launch the check
	result, err := s.redisCluster.Check()

	// Send the response
	if err != nil {
		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error checking cluster: %v", err))
		return
	}
	response := ClusterCheckResponse{
		Errors:   result.Errors,
		Warnings: result.Warnings,
	}
	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) FixCluster(w http.ResponseWriter, r *http.Request) {
	// Launch the fix
	err := s.redisCluster.CheckIntegrity(true, false)

	// Send the response
	response := ClusterFixResponse{
		Status: "In progress",
	}
	if err != nil {
		if _, ok := err.(*redis.OperationInProgressError); ok {
			s.sendResponse(w, http.StatusAccepted, response)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error fixing cluster: %v", err))
		return
	}
	s.sendResponse(w, http.StatusCreated, response)
}

func (s *Server) ResetNode(w http.ResponseWriter, r *http.Request) {
	// Parse the request path to get the node index
	nodeIndex := r.PathValue("nodeIndex")

	// Get the node
	node := s.redisCluster.GetNode(nodeIndex)
	if node == nil {
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Node '%s' not found", nodeIndex))
		return
	}

	// Launch the reset
	err := s.redisCluster.ResetNode(node)

	// Send the response
	response := ClusterResetNodeResponse{
		Status: "Completed",
	}
	if err != nil {
		if _, ok := err.(*redis.OperationInProgressError); ok {
			response.Status = "In progress"
			s.sendResponse(w, http.StatusAccepted, response)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error reseting node: %v", err))
		return
	}

	s.sendResponse(w, http.StatusOK, response)
}

func (s *Server) GetNodes(w http.ResponseWriter, r *http.Request) {
	response := ClusterNodesResponse{
		Nodes: s.redisCluster.GetNodes(),
	}

	s.sendResponse(w, http.StatusOK, response)
}

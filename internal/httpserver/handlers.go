// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"fmt"
	"net/http"

	"github.com/inditextech/redkeyrobin/internal/cluster"
)

// GetRedKeyClusterStatus handles the GET /v1/redkeycluster/status endpoint. It returns the current status of the RedKey cluster  from the Operator's perspective.
func (s *Server) GetRedKeyClusterStatus(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Get redkey cluster status")

	response := RedKeyClusterStatusResponse{
		Status: s.cluster.GetRedKeyClusterStatus(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

// UpdateRedKeyClusterStatus handles the PUT /v1/redkeycluster/status endpoint. It updates the status of the RedKey cluster  from the Operator's perspective.
func (s *Server) UpdateRedKeyClusterStatus(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Update redkey cluster status")

	// Parse the request body
	request := RedKeyClusterStatusRequest{}
	if err := ParseRequest(r, &request); err != nil {
		s.logger.Error("Invalid request", "error", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Update the status
	s.cluster.SetRedKeyClusterStatus(request.Status)

	// Send the response
	response := RedKeyClusterStatusResponse{
		Status: s.cluster.GetRedKeyClusterStatus(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

// GetRedKeyClusterReplicas handles the GET /v1/redkeycluster/replicas endpoint. It returns the current number of replicas in the RedKey cluster .
func (s *Server) GetRedKeyClusterReplicas(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Get redkey cluster replicas")

	response := ClusterReplicasResponse{
		Replicas:          s.cluster.GetReplicas(),
		ReplicasPerMaster: s.cluster.GetReplicasPerMaster(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

// UpdateRedKeyClusterReplicas handles the PUT /v1/redkeycluster/replicas endpoint. It updates the number of replicas in the RedKey cluster .
func (s *Server) UpdateRedKeyClusterReplicas(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Update redkey cluster replicas")

	// Parse the request body
	request := ClusterReplicasRequest{}
	if err := ParseRequest(r, &request); err != nil {
		s.logger.Error("Invalid request", "error", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Update the replicas
	err := s.cluster.SetReplicas(request.Replicas, request.ReplicasPerMaster)

	// Send the response
	response := ClusterReplicasResponse{
		Replicas:          s.cluster.GetReplicas(),
		ReplicasPerMaster: s.cluster.GetReplicasPerMaster(),
	}
	if err != nil {
		if _, ok := err.(*cluster.OperationCompletedError); ok {
			s.sendResponse(w, http.StatusOK, response)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error updating replicas: %v", err))
		return
	}
	s.sendResponse(w, http.StatusCreated, response)
}

// GetClusterStatus handles the GET /v1/cluster/status endpoint. It returns the current status of the RedKey cluster  from the Robin's perspective.
func (s *Server) GetClusterStatus(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Get cluster status")

	response := ClusterStatusResponse{
		Status: s.cluster.GetStatus(),
	}
	s.sendResponse(w, http.StatusOK, response)
}

// GetClusterNodes handles the PUT /v1/cluster/move endpoint. It moves slots from one node to another.
func (s *Server) MoveNodeSlots(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Move cluster node slots")

	// Parse the request body
	request := ClusterMoveSlotsRequest{}
	if err := ParseRequest(r, &request); err != nil {
		s.logger.Error("Invalid request", "error", err)
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid request: %v", err))
		return
	}

	// Get the nodes
	if request.From == request.To {
		s.sendError(w, http.StatusBadRequest, "Source and destination nodes cannot be the same")
		return
	}

	from := s.cluster.GetNode(request.From)
	if from == nil {
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Node '%s' not found", request.From))
		return
	}
	to := s.cluster.GetNode(request.To)
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
	err := s.cluster.MoveSlots(from, to, slots)
	response := ClusterMoveSlotsResponse{}
	if err != nil {
		if _, ok := err.(*cluster.OperationInProgressError); ok {
			response.Status = "In progress"
			s.sendResponse(w, http.StatusAccepted, response)
			return
		} else if _, ok := err.(*cluster.OperationCompletedError); ok {
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

// CheckCluster handles the GET /v1/cluster/check endpoint. It checks the integrity of the RedKey cluster  and returns a list of errors and warnings.
func (s *Server) CheckCluster(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Check cluster")

	// Launch the check
	result, err := s.cluster.Check()

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

// FixCluster handles the POST /v1/cluster/fix endpoint. It fixes the integrity of the RedKey cluster .
func (s *Server) FixCluster(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Fix cluster")

	// Launch the fix
	err := s.cluster.CheckIntegrity(true, false)

	// Send the response
	response := ClusterFixResponse{
		Status: "In progress",
	}
	if err != nil {
		if _, ok := err.(*cluster.OperationInProgressError); ok {
			s.sendResponse(w, http.StatusAccepted, response)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error fixing cluster: %v", err))
		return
	}
	s.sendResponse(w, http.StatusCreated, response)
}

// ResetNode handles the POST /v1/cluster/reset/{nodeIndex} endpoint. It resets a Redis node.
func (s *Server) ResetNode(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Reset node")

	// Parse the request path to get the node index
	nodeIndex := r.PathValue("nodeIndex")

	// Get the node
	node := s.cluster.GetNode(nodeIndex)
	if node == nil {
		s.sendError(w, http.StatusBadRequest, fmt.Sprintf("Node '%s' not found", nodeIndex))
		return
	}

	// Launch the reset
	err := s.cluster.ResetNode(node)

	// Send the response
	response := ClusterResetNodeResponse{
		Status: "Completed",
	}
	if err != nil {
		if _, ok := err.(*cluster.OperationInProgressError); ok {
			response.Status = "In progress"
			s.sendResponse(w, http.StatusAccepted, response)
			return
		}

		s.sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error reseting node: %v", err))
		return
	}

	s.sendResponse(w, http.StatusOK, response)
}

// GetNodes handles the GET /v1/cluster/nodes endpoint. It returns the list of nodes in the RedKey cluster .
func (s *Server) GetNodes(w http.ResponseWriter, r *http.Request) {
	s.logger.Info("Get cluster nodes")

	response := ClusterNodesResponse{
		Nodes: s.cluster.GetNodes(),
	}

	s.sendResponse(w, http.StatusOK, response)
}

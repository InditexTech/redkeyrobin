// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"encoding/json"
	"net/http"

	"github.com/inditextech/redkeyrobin/internal/redis"
)

type ResponseInterface interface {
	GetKeys() []string
}

type Response struct {
	Code    int
	Headers map[string][]string
	Body    []byte
	Object  interface{}
}

func (r *Response) WriteResponse(w http.ResponseWriter) {
	// Set the status code
	w.WriteHeader(r.Code)

	// Write the headers
	for k, vs := range r.Headers {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}

	// Set the object as body if provided
	if r.Body == nil && r.Object != nil {
		r.SetJSONBody(r.Object)
	}

	// Write the body
	if r.Body != nil {
		w.Write(r.Body)
	}
}

func (r *Response) SetBody(body []byte) {
	r.Body = body
}

func (r *Response) SetJSONBody(body interface{}) {
	r.Body, _ = json.Marshal(body)
}

func (r *Response) SetHeaders(headers map[string][]string) {
	r.Headers = headers
}

func (r *Response) SetCode(code int) {
	r.Code = code
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func (e ErrorResponse) GetKeys() []string {
	return []string{"error"}
}

type HealthResponse struct {
	Status string `json:"status"`
}

func (r HealthResponse) GetKeys() []string {
	return []string{"status"}
}

type RedKeyClusterStatusResponse struct {
	Status string `json:"status"`
}

func (r RedKeyClusterStatusResponse) GetKeys() []string {
	return []string{"status"}
}

type ClusterReplicasResponse struct {
	Replicas          int `json:"replicas"`
	ReplicasPerMaster int `json:"replicas_per_master"`
}

func (r ClusterReplicasResponse) GetKeys() []string {
	return []string{"replicas", "replicas_per_master"}
}

type ClusterStatusResponse struct {
	Status string `json:"status"`
}

func (r ClusterStatusResponse) GetKeys() []string {
	return []string{"status"}
}

type ClusterMoveSlotsResponse struct {
	Status string `json:"status"`
}

func (r ClusterMoveSlotsResponse) GetKeys() []string {
	return []string{"status"}
}

type ClusterCheckResponse struct {
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`
}

func (r ClusterCheckResponse) GetKeys() []string {
	return []string{"errors", "warnings"}
}

type ClusterFixResponse struct {
	Status string `json:"status"`
}

func (r ClusterFixResponse) GetKeys() []string {
	return []string{"status"}
}

type ClusterResetNodeResponse struct {
	Status string `json:"status"`
}

func (r ClusterResetNodeResponse) GetKeys() []string {
	return []string{"status"}
}

type ClusterNodesResponse struct {
	Nodes []*redis.RedisNode `json:"nodes"`
}

func (r ClusterNodesResponse) GetKeys() []string {
	return []string{"nodes"}
}

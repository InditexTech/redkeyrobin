// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package httpserver

import (
	"encoding/json"
	"net/http"
)

type ResponseInterface interface {
	GetKeys() []string
}

type ErrorableResponseInterface interface {
	ResponseInterface
	SetStatus(status string)
	AddError(err string)
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
	Primaries          int `json:"primaries"`
	ReplicasPerPrimary int `json:"replicas_per_primary"`
}

func (r ClusterReplicasResponse) GetKeys() []string {
	return []string{"primaries", "replicas_per_primary"}
}

func (r ClusterReplicasResponse) SetStatus(status string) {
	// No-op for this response type
}

func (r ClusterReplicasResponse) AddError(err string) {
	// No-op for this response type
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

func (r *ClusterMoveSlotsResponse) SetStatus(status string) {
	r.Status = status
}

func (r ClusterMoveSlotsResponse) AddError(err string) {
	// No-op for this response type
}

type ClusterCheckResponse struct {
	Errors   []string `json:"errors"`
	Warnings []string `json:"warnings"`
}

func (r ClusterCheckResponse) GetKeys() []string {
	return []string{"errors", "warnings"}
}

func (r ClusterCheckResponse) SetStatus(status string) {
	// No-op for this response type
}

func (r *ClusterCheckResponse) AddError(err string) {
	r.Errors = append(r.Errors, err)
}

type ClusterFixResponse struct {
	Status string `json:"status"`
}

func (r ClusterFixResponse) GetKeys() []string {
	return []string{"status"}
}

func (r *ClusterFixResponse) SetStatus(status string) {
	r.Status = status
}

func (r ClusterFixResponse) AddError(err string) {
	// No-op for this response type
}

type ClusterResetNodeResponse struct {
	Status string `json:"status"`
}

func (r ClusterResetNodeResponse) GetKeys() []string {
	return []string{"status"}
}

func (r *ClusterResetNodeResponse) SetStatus(status string) {
	r.Status = status
}

func (r ClusterResetNodeResponse) AddError(err string) {
	// No-op for this response type
}

type ClusterNodesResponse struct {
	Nodes []RedisNode `json:"nodes"`
}

type RedisNode struct {
	Name       string `json:"name"`
	ID         string `json:"id"`
	IP         string `json:"ip"`
	Role       string `json:"role"`
	PrimaryID  string `json:"primaryId"`
	Failures   int    `json:"failures"`
	Sent       int    `json:"sent"`
	Recv       int    `json:"recv"`
	LinkStatus string `json:"linkStatus"`
}

func (r ClusterNodesResponse) GetKeys() []string {
	return []string{"nodes"}
}

type ClusterRecreateResponse struct {
	Status string `json:"status"`
}

func (r ClusterRecreateResponse) GetKeys() []string {
	return []string{"status"}
}

func (r *ClusterRecreateResponse) SetStatus(status string) {
	r.Status = status
}

func (r ClusterRecreateResponse) AddError(err string) {
	// No-op for this response type
}

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

type RedisClusterStatusResponse struct {
	Status string `json:"status"`
}

func (r RedisClusterStatusResponse) GetKeys() []string {
	return []string{"status"}
}

type ClusterReplicasResponse struct {
	Replicas int `json:"replicas"`
}

func (r ClusterReplicasResponse) GetKeys() []string {
	return []string{"replicas"}
}

type ClusterStatusResponse struct {
	Status string `json:"status"`
}

func (r ClusterStatusResponse) GetKeys() []string {
	return []string{"status"}
}

// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	Number int
}

func (t *TestStruct) Method1() int {
	return t.Number
}

func (t *TestStruct) Method2(w http.ResponseWriter, r *http.Request) {	
}

func (t *TestStruct) Method3(input int) int {
	t.Number = input
	return t.Number
}

var testStruct = &TestStruct{
	Number: 123,
}

func TestMethodIsValid(t *testing.T) {
	tests := []struct {
		name               string
		method string
		expected error
	}{
		{
			name: "not found",
			method: "bad",
			expected: fmt.Errorf("method bad not found"),
		},
		{
			name: "bad signature",
			method: "Method1",
			expected: fmt.Errorf("method Method1 has invalid signature"),
		},
		{
			name: "good",
			method: "Method2",
			expected: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := MethodIsValid(testStruct, tt.method)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestInvoke(t *testing.T) {
	tests := []struct {
		name               string
		method string
		expected error
		args []interface{}
	}{
		{
			name: "no inputs",
			method: "Method1",
		},
		{
			name: "inputs",
			method: "Method3",
			args: []interface{}{456},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Invoke(testStruct, tt.method, tt.args...)
		})
	}
}
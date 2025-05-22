// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseInt(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "bad",
			input:    "bad",
			expected: 0,
		},
		{
			name:     "good",
			input:    "123",
			expected: 123,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ParseInt(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestParseInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{
			name:     "bad",
			input:    "bad",
			expected: 0,
		},
		{
			name:     "good",
			input:    "123",
			expected: 123,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ParseInt64(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected float64
	}{
		{
			name:     "bad",
			input:    "bad",
			expected: 0,
		},
		{
			name:     "good",
			input:    "123.123",
			expected: 123.123,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ParseFloat(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

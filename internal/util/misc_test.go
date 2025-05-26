// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapToString(t *testing.T) {
	testMap := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	result := MapToString(testMap)
	assert.Contains(t, result, "key1: value1")
	assert.Contains(t, result, "key2: value2")
}

func TestGetIPFromAddress(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expected      string
		expectedError error
	}{
		{
			name:          "bad",
			input:         "bad",
			expected:      "",
			expectedError: fmt.Errorf("lookup bad"),
		},
		{
			name:          "good",
			input:         "localhost",
			expected:      "127.0.0.1",
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := GetIPFromAddress(tt.input)
			assert.Equal(t, tt.expected, actual)

			if tt.expectedError != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStringInSlice(t *testing.T) {
	testSlice := []string{"item1", "item2", "item3"}

	result := StringInSlice("item2", testSlice)
	assert.True(t, result)

	result = StringInSlice("item4", testSlice)
	assert.False(t, result)
}

func TestMakeRangeMap(t *testing.T) {
	expectedMap := map[int]interface{}{
		1: "",
		2: "",
		3: "",
		4: "",
		5: "",
	}

	actualMap := MakeRangeMap(1, 5)
	assert.Equal(t, expectedMap, actualMap)
}

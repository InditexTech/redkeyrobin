// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
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

func TestStringInSlice(t *testing.T) {
	testSlice := []string{"item1", "item2", "item3"}

	result := StringInSlice("item2", testSlice)
	assert.True(t, result)

	result = StringInSlice("item4", testSlice)
	assert.False(t, result)
}

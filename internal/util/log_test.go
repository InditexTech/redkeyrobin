// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)


func TestLogger(t *testing.T) {
	logger := InitLogger()
	assert.NotNil(t, logger)

	secondLogger := GetLogger("test")
	assert.NotNil(t, secondLogger)
}
// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"strings"
)

// MapToString converts a map[string]string to a string.
func MapToString(m map[string]string) string {
	var b strings.Builder
	b.WriteString("{")
	for k, v := range m {
		b.WriteString(k)
		b.WriteString(": ")
		b.WriteString(v)
		b.WriteString(", ")
	}
	b.WriteString("}")
	return b.String()
}

// StringInSlice checks if a string is in a slice of strings.
func StringInSlice(item string, list []string) bool {
	for _, i := range list {
		if i == item {
			return true
		}
	}
	return false
}

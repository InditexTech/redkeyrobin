// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"net"
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

// GetIPFromAddress returns the IP address of a given address.
func GetIPFromAddress(address string) (string, error) {
	ips, err := net.LookupIP(address)
	if err != nil {
		return "", err
	}

	if len(ips) == 0 {
		return "", fmt.Errorf("no IPs found for address %s", address)
	}

	return ips[0].String(), nil
}

// MakeRangeMap creates a map with a range of integers as keys.
func MakeRangeMap(min int, max int) map[int]interface{} {
	result := map[int]interface{}{}
	a := make([]int, max-min+1)
	for i := range a {
		result[min+i] = ""
	}
	return result
}

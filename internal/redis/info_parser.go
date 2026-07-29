// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"strings"
)

// ParseInfoAll parses the raw output of Redis INFO ALL into a flat key→value map.
// Section headers (lines starting with #) and empty lines are skipped.
func ParseInfoAll(raw string) map[string]string {
	result := make(map[string]string)
	lines := strings.SplitSeq(raw, "\n")

	for line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		result[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return result
}

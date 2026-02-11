// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"math"
)

type contextKey string

const (
	nodeNameKey contextKey = "nodeName"
)

// calculateMaxSlotsPerPrimary calculates the maximum number of slots per primary.
// It takes into account the number of slots and the number of primaries.
func calculateMaxSlotsPerPrimary(slots int, primaries int) int {
	return int(math.Ceil(float64(slots) / float64(primaries)))
}

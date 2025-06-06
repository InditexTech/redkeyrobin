// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package rediscluster

import (
	"math"
)

func calculateMaxSlotsPerMaster(slots int, masters int) int {
	return int(math.Ceil(float64(slots) / float64(masters)))
}
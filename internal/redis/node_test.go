// SPDX-FileCopyrightText: 2025 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeGetNumberOfSlots(t *testing.T) {
	tests := []struct {
		name               string
		node 		*RedisNode
		expectedSlots 	int
	}{
		{
			name: "zero slots",
			node: &RedisNode{
				Slots: []RedisSlotRange{},
			},
			expectedSlots: 0,
		},
		{
			name: "one slot",
			node: &RedisNode{
				Slots: []RedisSlotRange{
					{
						Start: 1,
						End: 1,
					},
				},
			},
			expectedSlots: 1,
		},
		{
			name: "several slots",
			node: &RedisNode{
				Slots: []RedisSlotRange{
					{
						Start: 1,
						End: 10,
					},
					{
						Start: 20,
						End: 20,
					},
				},
			},
			expectedSlots: 11,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.node.GetNumberOfSlots()
			assert.Equal(t, tt.expectedSlots, actual)
		})
	}
}

func TestNodeUpdateInfo(t *testing.T) {
	tests := []struct {
		name               string
		node 		*RedisNode
		update 		RedisNode
	}{
		{
			name: "zero slots",
			node: &RedisNode{
				IP: "aaa",
				Role: "master",
				Slots: []RedisSlotRange{},
				Failures: 1,
			},
			update: RedisNode{
				IP: "bbb",
				Role: "slave",
				Slots: []RedisSlotRange{
					{
						Start: 1,
						End: 1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.node.UpdateInfo(tt.update)
			assert.Equal(t, tt.node.IP, tt.update.IP)
			assert.Equal(t, tt.node.Role, tt.update.Role)
			assert.Equal(t, tt.node.Slots, tt.update.Slots)
			assert.Equal(t, tt.node.MasterID, tt.update.MasterID)
			assert.Equal(t, tt.node.Failures, tt.update.Failures)
		})
	}
}


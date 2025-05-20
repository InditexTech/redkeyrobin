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
		name          string
		node          *RedisNode
		expectedSlots int
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
						End:   1,
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
						End:   10,
					},
					{
						Start: 20,
						End:   20,
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
		name   string
		node   *RedisNode
		update RedisNode
	}{
		{
			name: "zero slots",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			update: RedisNode{
				IP:    "bbb",
				Flags: "slave",
				Slots: []RedisSlotRange{
					{
						Start: 1,
						End:   1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.node.UpdateInfo(tt.update)
			assert.Equal(t, tt.node.IP, tt.update.IP)
			assert.Equal(t, tt.node.Flags, tt.update.Flags)
			assert.Equal(t, tt.node.Slots, tt.update.Slots)
			assert.Equal(t, tt.node.MasterID, tt.update.MasterID)
			assert.Equal(t, tt.node.Failures, tt.update.Failures)
		})
	}
}


func TestNodeIsMaster(t *testing.T) {
	tests := []struct {
		name   string
		node   *RedisNode
		expected bool
	}{
		{
			name: "master node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: true,
		},
		{
			name: "master node with flags",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master,noaddr,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: true,
		},
		{
			name: "slave node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "slave",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: false,
		},
		{
			name: "slave node with flags",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "fail,slave,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.node.IsMaster()
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestNodeHasFlag(t *testing.T) {
	tests := []struct {
		name   string
		node   *RedisNode
		flag  string
		expected bool
	}{
		{
			name: "master node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag: "master",
			expected: true,
		},
		{
			name: "master node with flags",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "master,noaddr,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			expected: true,
		},
		{
			name: "slave node",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "slave",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag: "master",
			expected: false,
		},
		{
			name: "fail",
			node: &RedisNode{
				IP:       "aaa",
				Flags:    "fail,slave,myself",
				Slots:    []RedisSlotRange{},
				Failures: 1,
			},
			flag: "fail",
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.node.hasFlag(tt.flag)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
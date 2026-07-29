// SPDX-FileCopyrightText: 2026 INDUSTRIA DE DISEÑO TEXTIL, S.A. (INDITEX, S.A.)
//
// SPDX-License-Identifier: Apache-2.0

package redis

import "testing"

func TestParseSlotRanges_ParsesWhitespaceAndCommas(t *testing.T) {
	ranges, err := ParseSlotRanges("0-1,2-3 6")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ranges) != 3 {
		t.Fatalf("expected 3 ranges, got %d", len(ranges))
	}
	if ranges[0] != (SlotRange{Start: 0, End: 1}) {
		t.Fatalf("unexpected first range %#v", ranges[0])
	}
	if ranges[2] != (SlotRange{Start: 6, End: 6}) {
		t.Fatalf("unexpected last range %#v", ranges[2])
	}
	if CountSlots(ranges) != 5 {
		t.Fatalf("expected 5 slots, got %d", CountSlots(ranges))
	}
}

func TestParseSlotRanges_EmptyInput(t *testing.T) {
	ranges, err := ParseSlotRanges("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ranges) != 0 {
		t.Fatalf("expected no ranges, got %d", len(ranges))
	}
}

func TestParseSlotRanges_RejectsUnsupportedTokens(t *testing.T) {
	_, err := ParseSlotRanges("[1234->-node]")
	if err == nil {
		t.Fatal("expected parse error for unsupported slot token")
	}
}

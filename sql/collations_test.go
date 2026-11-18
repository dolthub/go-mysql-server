// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql/encodings"
)

func TestParseCollation(t *testing.T) {
	tests := []struct {
		charset           string
		collation         string
		binaryAttribute   bool
		expectedCollation CollationID
		expectedErr       bool
	}{
		{"", "", false, Collation_Unspecified, false},
		{"", "", true, Collation_Unspecified, false},
		{CharacterSet_big5.String(), "", false, CharacterSet_big5.DefaultCollation(), false},
		{CharacterSet_eucjpms.String(), "", true, CharacterSet_eucjpms.BinaryCollation(), false},
		{"", Collation_big5_chinese_ci.String(), false, Collation_big5_chinese_ci, false},
		{"", Collation_armscii8_general_ci.String(), true, Collation_armscii8_bin, false},
		{CharacterSet_sjis.String(), Collation_sjis_japanese_ci.String(), false, Collation_sjis_japanese_ci, false},
		{CharacterSet_gbk.String(), Collation_gbk_chinese_ci.String(), true, Collation_gbk_chinese_ci, false},

		{CharacterSet_armscii8.String(), Collation_cp1251_bin.String(), false, Collation_Default, true},
		{CharacterSet_eucjpms.String(), Collation_latin5_turkish_ci.String(), false, Collation_Default, true},
		{CharacterSet_binary.String(), Collation_utf8_bin.String(), false, Collation_Default, true},
	}

	for _, test := range tests {
		if test.charset == "" {
			testParseCollation(t, "", test.collation, test.binaryAttribute, test.expectedCollation, test.expectedErr)
		}
		if test.collation == "" {
			testParseCollation(t, test.charset, "", test.binaryAttribute, test.expectedCollation, test.expectedErr)
		}
		if test.charset == "" && test.collation == "" {
			testParseCollation(t, "", "", test.binaryAttribute, test.expectedCollation, test.expectedErr)
		}
		testParseCollation(t, test.charset, test.collation, test.binaryAttribute, test.expectedCollation, test.expectedErr)
	}
}

func testParseCollation(t *testing.T, charset string, collation string, binaryAttribute bool, expectedCollation CollationID, expectedErr bool) {
	t.Run(fmt.Sprintf("%v %v %v", charset, collation, binaryAttribute), func(t *testing.T) {
		col, err := ParseCollation(charset, collation, binaryAttribute)
		if expectedErr {
			assert.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.True(t, expectedCollation.Equals(col))
		}
	})
}

func TestConvertCollationID(t *testing.T) {
	tests := []struct {
		input    any
		expected string
	}{
		{uint64(33), "utf8mb3_general_ci"},
		{int64(33), "utf8mb3_general_ci"},
		{[]byte("33"), "utf8mb3_general_ci"},
		{uint64(8), "latin1_swedish_ci"},
		{int32(8), "latin1_swedish_ci"},

		{45, "utf8mb4_general_ci"},
		{uint64(46), "utf8mb4_bin"},
		{255, "utf8mb4_0900_ai_ci"},
		{uint64(309), "utf8mb4_0900_bin"},

		{83, "utf8mb3_bin"},
		{uint64(223), "utf8mb3_general_mysql500_ci"},

		{uint64(47), "latin1_bin"},
		{48, "latin1_general_ci"},
		{49, "latin1_general_cs"},

		{uint64(63), "binary"},

		{uint64(11), "ascii_general_ci"},
		{65, "ascii_bin"},

		{uint64(15), "latin1_danish_ci"},
		{31, "latin1_german2_ci"},
		{94, "latin1_spanish_ci"},

		{int8(8), "latin1_swedish_ci"},
		{int16(8), "latin1_swedish_ci"},
		{int(8), "latin1_swedish_ci"},
		{uint8(8), "latin1_swedish_ci"},
		{uint16(8), "latin1_swedish_ci"},
		{uint(8), "latin1_swedish_ci"},
		{uint32(8), "latin1_swedish_ci"},

		{"utf8mb4_0900_bin", "utf8mb4_0900_bin"},
		{"utf8mb3_general_ci", "utf8mb3_general_ci"},
		{"", ""},

		{uint64(99999), "99999"},
		{uint64(1000), "1000"},
		{int(500), "500"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%T(%v)", tt.input, tt.input), func(t *testing.T) {
			result, err := ConvertCollationID(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCollationIsBinaryMetadata(t *testing.T) {
	require.Equal(t, Collation_Default.IsBinary(), Collation_Unspecified.IsBinary())

	iter := NewCollationsIterator()
	for {
		c, ok := iter.Next()
		if !ok {
			break
		}

		expected := c.ID == Collation_binary || c.CharacterSet == CharacterSet_binary || strings.HasSuffix(c.Name, "_bin")
		require.Equal(t, expected, c.IsBinary, "Collation %s (ID %d) IsBinary flag mismatch", c.Name, c.ID)
		require.Equal(t, expected, c.ID.IsBinary(), "CollationID %s (ID %d) IsBinary() method mismatch", c.Name, c.ID)
	}
}

func TestWeightScannerMalformedAndBinary(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12342
	sorter := Collation_utf8mb4_unicode_ci.Sorter()
	ws := NewWeightScanner(Collation_utf8mb4_unicode_ci, "b\xff\u200bob")
	var got []int32
	for {
		w, ok := ws.Next()
		if !ok {
			break
		}
		got = append(got, w)
	}
	require.Equal(t, []int32{sorter('b'), sorter(utf8.RuneError), sorter('o'), sorter('b')}, got)

	var buf bytes.Buffer
	require.NoError(t, Collation_utf8mb4_unicode_ci.WriteWeightString(&buf, "b\xff\u200bob"))
	var wantBuf bytes.Buffer
	require.NoError(t, Collation_utf8mb4_unicode_ci.WriteWeightString(&wantBuf, "b\ufffdob"))
	require.Equal(t, wantBuf.Bytes(), buf.Bytes())

	binScanner := NewWeightScanner(Collation_binary, "\x00\xff")
	w, ok := binScanner.Next()
	require.True(t, ok)
	require.Equal(t, int32(0), w)

	w, ok = binScanner.Next()
	require.True(t, ok)
	require.Equal(t, int32(0xff), w)

	_, ok = binScanner.Next()
	require.False(t, ok)
}

func TestUCACollationWeights(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12342
	require.Equal(t, Collation_Default.IsUCA(), Collation_Unspecified.IsUCA())

	iter := NewCollationsIterator()
	for {
		c, ok := iter.Next()
		if !ok {
			break
		}
		if c.Sorter == nil {
			continue
		}
		isUCA := encodings.IsUCACollation(c.Name)
		require.Equal(t, isUCA, c.ID.IsUCA(), "collation %s", c.Name)
		if isUCA {
			require.Equal(t, encodings.IgnorableWeight, c.Sorter('\u0000'), "collation %s NUL", c.Name)
			require.Equal(t, encodings.IgnorableWeight, c.Sorter('\u200b'), "collation %s U+200B", c.Name)
			require.Less(t, encodings.IgnorableWeight, c.Sorter('\t'), "collation %s TAB", c.Name)
			require.Less(t, c.Sorter('\t'), c.Sorter(' '), "collation %s TAB < SPACE", c.Name)
			require.Less(t, c.Sorter(' '), c.Sorter('a'), "collation %s SPACE < a", c.Name)
		} else {
			ws := NewWeightScanner(c.ID, "\x00")
			_, hasWeight := ws.Next()
			require.True(t, hasWeight, "non-UCA collation %s must not skip NUL", c.Name)
		}
	}

	// Pin exact ignorable scalar-value counts per UCA family so a Go toolchain
	// Unicode table update cannot silently alter generated maps.
	pinnedIgnorableCounts := []struct {
		collation CollationID
		expected  int
	}{
		{Collation_utf8mb4_unicode_ci, 470},
		{Collation_utf8mb4_persian_ci, 450},
		{Collation_utf8mb4_sinhala_ci, 468},
		{Collation_utf8mb4_unicode_520_ci, 1045},
		{Collation_utf8mb4_0900_ai_ci, 1367},
		{Collation_utf8mb4_0900_as_cs, 835},
		{Collation_utf8mb4_ja_0900_as_cs, 835},
		{Collation_utf8mb4_zh_0900_as_cs, 835},
	}
	for _, tc := range pinnedIgnorableCounts {
		sorter := tc.collation.Sorter()
		count := 0
		for r := rune(0); r <= 0x10FFFF; r++ {
			if r >= 0xD800 && r <= 0xDFFF {
				continue
			}
			if sorter(r) == encodings.IgnorableWeight {
				count++
			}
		}
		require.Equal(t, tc.expected, count, "collation %s ignorable count", tc.collation.Name())
	}
}

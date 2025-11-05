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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

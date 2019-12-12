package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCollation(t *testing.T) {
	tests := []struct{
		charset string
		collation string
		binaryAttribute bool
		expectedCollation Collation
		expectedErr bool
	}{
		{"", "", false, Collation_Default, false},
		{"", "", true, Collation_Default.CharacterSet().BinaryCollation(), false},
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
			testParseCollation(t, nil, &test.collation, test.binaryAttribute, test.expectedCollation, test.expectedErr)
		}
		if test.collation == "" {
			testParseCollation(t, &test.charset, nil, test.binaryAttribute, test.expectedCollation, test.expectedErr)
		}
		if test.charset == "" && test.collation == "" {
			testParseCollation(t, nil, nil, test.binaryAttribute, test.expectedCollation, test.expectedErr)
		}
		testParseCollation(t, &test.charset, &test.collation, test.binaryAttribute, test.expectedCollation, test.expectedErr)
	}
}

func testParseCollation(t *testing.T, charset *string, collation *string, binaryAttribute bool, expectedCollation Collation, expectedErr bool) {
	t.Run(fmt.Sprintf("%v %v %v", charset, collation, binaryAttribute), func(t *testing.T) {
		col, err := ParseCollation(charset, collation, binaryAttribute)
		if expectedErr {
			assert.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, expectedCollation, col)
		}
	})
}

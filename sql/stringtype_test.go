package sql

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringChangeCollation(t *testing.T) {
	tests := []struct{
		inputType StringType
		newColl Collation
		outputType StringType
		expectedErr bool
	}{
		{
			MustCreateString(sqltypes.Char, 8, Collation_Default),
			Collation_Default,
			MustCreateString(sqltypes.Char, 8, Collation_Default),
			false,
		},
		{
			MustCreateString(sqltypes.Char, 40, Collation_big5_chinese_ci),
			Collation_ascii_general_ci,
			MustCreateString(sqltypes.Char, 40, Collation_ascii_general_ci),
			false,
		},
		{
			MustCreateString(sqltypes.Char, 40, Collation_eucjpms_bin),
			Collation_binary,
			MustCreateBinary(sqltypes.Binary, 40),
			false,
		},
		{
			MustCreateString(sqltypes.VarChar, 1, Collation_Default),
			Collation_Default,
			MustCreateString(sqltypes.VarChar, 1, Collation_Default),
			false,
		},
		{
			MustCreateString(sqltypes.VarChar, 255, Collation_sjis_japanese_ci),
			Collation_gbk_chinese_ci,
			MustCreateString(sqltypes.VarChar, 255, Collation_gbk_chinese_ci),
			false,
		},
		{
			MustCreateString(sqltypes.VarChar, 10000, Collation_eucjpms_bin),
			Collation_binary,
			MustCreateBinary(sqltypes.VarBinary, 10000),
			false,
		},
		{
			MustCreateString(sqltypes.VarChar, 16384, Collation_utf8mb3_general_ci),
			Collation_utf8mb4_general_ci,
			nil,
			true,
		},
		{
			MustCreateString(sqltypes.Text, tinyTextBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default),
			Collation_Default,
			MustCreateString(sqltypes.Text, tinyTextBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default),
			false,
		},
		{
			MustCreateString(sqltypes.Text, textBlobMax / CharacterSet_big5.MaxLength(), Collation_big5_chinese_ci),
			Collation_geostd8_bin,
			MustCreateString(sqltypes.Text, textBlobMax / CharacterSet_geostd8.MaxLength(), Collation_geostd8_bin),
			false,
		},
		{
			MustCreateString(sqltypes.Text, mediumTextBlobMax / CharacterSet_gb18030.MaxLength(), Collation_gb18030_unicode_520_ci),
			Collation_ascii_bin,
			MustCreateString(sqltypes.Text, mediumTextBlobMax, Collation_ascii_bin),
			false,
		},
		{
			MustCreateString(sqltypes.Text, longTextBlobMax, Collation_hebrew_general_ci),
			Collation_utf16_esperanto_ci,
			MustCreateString(sqltypes.Text, longTextBlobMax, Collation_utf16_esperanto_ci),
			false,
		},
		{
			MustCreateString(sqltypes.Text, mediumTextBlobMax / CharacterSet_euckr.MaxLength(), Collation_euckr_korean_ci),
			Collation_binary,
			MustCreateBinary(sqltypes.Blob, mediumTextBlobMax),
			false,
		},
		{
			MustCreateBinary(sqltypes.Binary, 21),
			Collation_binary,
			MustCreateBinary(sqltypes.Binary, 21),
			false,
		},
		{
			MustCreateBinary(sqltypes.Binary, 40),
			Collation_ucs2_general_mysql500_ci,
			MustCreateString(sqltypes.Char, 40, Collation_ucs2_general_mysql500_ci),
			false,
		},
		{
			MustCreateBinary(sqltypes.VarBinary, 73),
			Collation_binary,
			MustCreateBinary(sqltypes.VarBinary, 73),
			false,
		},
		{
			MustCreateBinary(sqltypes.VarBinary, 396),
			Collation_hp8_english_ci,
			MustCreateString(sqltypes.VarChar, 396, Collation_hp8_english_ci),
			false,
		},
		{
			MustCreateBinary(sqltypes.VarBinary, 53982),
			Collation_eucjpms_japanese_ci,
			nil,
			true,
		},
		{
			TinyBlob,
			Collation_binary,
			TinyBlob,
			false,
		},
		{
			Blob,
			Collation_cp932_bin,
			MustCreateString(sqltypes.Text, textBlobMax / CharacterSet_cp932.MaxLength(), Collation_cp932_bin),
			false,
		},
		{
			MediumBlob,
			Collation_big5_bin,
			MustCreateString(sqltypes.Text, mediumTextBlobMax / CharacterSet_big5.MaxLength(), Collation_big5_bin),
			false,
		},
		{
			LongBlob,
			Collation_utf8mb4_es_trad_0900_as_cs,
			MustCreateString(sqltypes.Text, longTextBlobMax, Collation_utf8mb4_es_trad_0900_as_cs),
			false,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.inputType.String(), test.newColl.String()), func(t *testing.T) {
			resType, err := test.inputType.ChangeCollation(test.newColl)
			if !test.expectedErr {
				require.NoError(t, err)
				resTypeCast, ok := resType.(stringType)
				require.True(t, ok)
				outputTypeCast, ok := test.outputType.(stringType)
				require.True(t, ok)
				assert.Equal(t, outputTypeCast, resTypeCast)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestStringCompare(t *testing.T) {
	tests := []struct {
		typ StringType
		val1 interface{}
		val2 interface{}
		expectedCmp int
	}{
		{MustCreateBinary(sqltypes.Binary, 10), nil, 0, -1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), 0, nil, 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), nil, nil, 0},

		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 0, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte{0}, true, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), false, 1, 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, 0, 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), true, "false", 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, false, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, 1, 0},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), true, 1, 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), "True", true, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), false, true, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), "0x12345de", "0xed54321", -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), "0xed54321", "0x12345de", 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte("254"), 254, 0},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte("254"), 254.5, -1},

		// Sanity checks that behavior is consistent
		{MustCreateBinary(sqltypes.Binary, 10), 0, 1, -1},
		{MustCreateBinary(sqltypes.Binary, 10), []byte{0}, true, -1},
		{MustCreateBinary(sqltypes.Binary, 10), false, 1, 1},
		{MustCreateBinary(sqltypes.Binary, 10), []byte("254"), 254, 0},
		{MustCreateBinary(sqltypes.Blob, 10), 0, 1, -1},
		{MustCreateBinary(sqltypes.Blob, 10), []byte{0}, true, -1},
		{MustCreateBinary(sqltypes.Blob, 10), false, 1, 1},
		{MustCreateBinary(sqltypes.Blob, 10), []byte("254"), 254, 0},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), 0, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), []byte{0}, true, -1},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), false, 1, 1},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), []byte("254"), 254, 0},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), 0, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), []byte{0}, true, -1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), false, 1, 1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), []byte("254"), 254, 0},
		{MustCreateBinary(sqltypes.VarBinary, 10), []byte{0}, true, -1},
		{MustCreateBinary(sqltypes.VarBinary, 10), false, 1, 1},
		{MustCreateBinary(sqltypes.VarBinary, 10), 0, 1, -1},
		{MustCreateBinary(sqltypes.VarBinary, 10), []byte("254"), 254, 0},

	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := test.typ.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestStringCreateBlob(t *testing.T) {
	tests := []struct {
		baseType query.Type
		length int64
		expectedType stringType
		expectedErr bool
	}{
		{sqltypes.Binary, 10,
			stringType{sqltypes.Binary, 10, Collation_binary}, false},
		{sqltypes.Blob, 10,
			stringType{sqltypes.Blob, tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.Char, 10,
			stringType{sqltypes.Binary, 10, Collation_binary}, false},
		{sqltypes.Text, 10,
			stringType{sqltypes.Blob, tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.VarBinary, 10,
			stringType{sqltypes.VarBinary, 10, Collation_binary}, false},
		{sqltypes.VarChar, 10,
			stringType{sqltypes.VarBinary, 10, Collation_binary}, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.baseType, test.length), func(t *testing.T) {
			typ, err := CreateBinary(test.baseType, test.length)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestStringCreateBlobInvalidBaseTypes(t *testing.T) {
	tests := []struct {
		baseType query.Type
		length int64
		expectedType stringType
		expectedErr bool
	}{
		{sqltypes.Bit, 10, stringType{}, true},
		{sqltypes.Date, 10, stringType{}, true},
		{sqltypes.Datetime, 10, stringType{}, true},
		{sqltypes.Decimal, 10, stringType{}, true},
		{sqltypes.Enum, 10, stringType{}, true},
		{sqltypes.Expression, 10, stringType{}, true},
		{sqltypes.Float32, 10, stringType{}, true},
		{sqltypes.Float64, 10, stringType{}, true},
		{sqltypes.Geometry, 10, stringType{}, true},
		{sqltypes.Int16, 10, stringType{}, true},
		{sqltypes.Int24, 10, stringType{}, true},
		{sqltypes.Int32, 10, stringType{}, true},
		{sqltypes.Int64, 10, stringType{}, true},
		{sqltypes.Int8, 10, stringType{}, true},
		{sqltypes.Null, 10, stringType{}, true},
		{sqltypes.Set, 10, stringType{}, true},
		{sqltypes.Time, 10, stringType{}, true},
		{sqltypes.Timestamp, 10, stringType{}, true},
		{sqltypes.TypeJSON, 10, stringType{}, true},
		{sqltypes.Uint16, 10, stringType{}, true},
		{sqltypes.Uint24, 10, stringType{}, true},
		{sqltypes.Uint32, 10, stringType{}, true},
		{sqltypes.Uint64, 10, stringType{}, true},
		{sqltypes.Uint8, 10, stringType{}, true},
		{sqltypes.Year, 10, stringType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.baseType, test.length), func(t *testing.T) {
			typ, err := CreateBinary(test.baseType, test.length)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestStringCreateString(t *testing.T) {
	tests := []struct {
		baseType query.Type
		length int64
		collation Collation
		expectedType stringType
		expectedErr bool
	}{
		{sqltypes.Binary, 10, Collation_binary,
			stringType{sqltypes.Binary, 10, Collation_binary}, false},
		{sqltypes.Blob, 10, Collation_binary,
			stringType{sqltypes.Blob, tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.Char, 10, Collation_Default,
			stringType{sqltypes.Char, 10, Collation_Default}, false},
		{sqltypes.Text, 10, Collation_Default,
			stringType{sqltypes.Text, tinyTextBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default}, false},
		{sqltypes.Text, 1000, Collation_Default,
			stringType{sqltypes.Text, textBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default}, false},
		{sqltypes.Text, 1000000, Collation_Default,
			stringType{sqltypes.Text, mediumTextBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default}, false},
		{sqltypes.Text, longTextBlobMax, Collation_Default,
			stringType{sqltypes.Text, longTextBlobMax, Collation_Default}, false},
		{sqltypes.VarBinary, 10, Collation_binary,
			stringType{sqltypes.VarBinary, 10, Collation_binary}, false},
		{sqltypes.VarChar, 10, Collation_Default,
			stringType{sqltypes.VarChar, 10, Collation_Default}, false},

		{sqltypes.Char, 10, Collation_binary,
			stringType{sqltypes.Binary, 10, Collation_binary}, false},
		{sqltypes.Text, 10, Collation_binary,
			stringType{sqltypes.Blob, tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.VarChar, 10, Collation_binary,
			stringType{sqltypes.VarBinary, 10, Collation_binary}, false},

		{sqltypes.Binary, charBinaryMax + 1, Collation_binary, stringType{}, true},
		{sqltypes.Blob, longTextBlobMax + 1, Collation_binary, stringType{}, true},
		{sqltypes.Char, charBinaryMax + 1, Collation_Default, stringType{}, true},
		{sqltypes.Text, longTextBlobMax + 1, Collation_Default, stringType{}, true},
		{sqltypes.VarBinary, varcharVarbinaryMax + 1, Collation_binary, stringType{}, true},
		{sqltypes.VarChar, varcharVarbinaryMax, Collation_Default, stringType{}, true},

		// Default collation is not valid for these types
		{sqltypes.Binary, 10, Collation_Default, stringType{}, true},
		{sqltypes.Blob, 10, Collation_Default, stringType{}, true},
		{sqltypes.VarBinary, 10, Collation_Default, stringType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.baseType, test.length, test.collation), func(t *testing.T) {
			typ, err := CreateString(test.baseType, test.length, test.collation)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestStringCreateStringInvalidBaseTypes(t *testing.T) {
	tests := []struct {
		baseType query.Type
		length int64
		collation Collation
		expectedType stringType
		expectedErr bool
	}{
		{sqltypes.Bit, 10, Collation_Default, stringType{}, true},
		{sqltypes.Date, 10, Collation_Default, stringType{}, true},
		{sqltypes.Datetime, 10, Collation_Default, stringType{}, true},
		{sqltypes.Decimal, 10, Collation_Default, stringType{}, true},
		{sqltypes.Enum, 10, Collation_Default, stringType{}, true},
		{sqltypes.Expression, 10, Collation_Default, stringType{}, true},
		{sqltypes.Float32, 10, Collation_Default, stringType{}, true},
		{sqltypes.Float64, 10, Collation_Default, stringType{}, true},
		{sqltypes.Geometry, 10, Collation_Default, stringType{}, true},
		{sqltypes.Int16, 10, Collation_Default, stringType{}, true},
		{sqltypes.Int24, 10, Collation_Default, stringType{}, true},
		{sqltypes.Int32, 10, Collation_Default, stringType{}, true},
		{sqltypes.Int64, 10, Collation_Default, stringType{}, true},
		{sqltypes.Int8, 10, Collation_Default, stringType{}, true},
		{sqltypes.Null, 10, Collation_Default, stringType{}, true},
		{sqltypes.Set, 10, Collation_Default, stringType{}, true},
		{sqltypes.Time, 10, Collation_Default, stringType{}, true},
		{sqltypes.Timestamp, 10, Collation_Default, stringType{}, true},
		{sqltypes.TypeJSON, 10, Collation_Default, stringType{}, true},
		{sqltypes.Uint16, 10, Collation_Default, stringType{}, true},
		{sqltypes.Uint24, 10, Collation_Default, stringType{}, true},
		{sqltypes.Uint32, 10, Collation_Default, stringType{}, true},
		{sqltypes.Uint64, 10, Collation_Default, stringType{}, true},
		{sqltypes.Uint8, 10, Collation_Default, stringType{}, true},
		{sqltypes.Year, 10, Collation_Default, stringType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.baseType, test.length, test.collation), func(t *testing.T) {
			typ, err := CreateString(test.baseType, test.length, test.collation)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestStringConvert(t *testing.T) {
	tests := []struct {
		typ StringType
		val interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{MustCreateBinary(sqltypes.Binary, 3), nil, nil, false},
		{MustCreateBinary(sqltypes.Blob, 3), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.Char, 7), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.Text, 7), nil, nil, false},
		{MustCreateBinary(sqltypes.VarBinary, 3), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 7), nil, nil, false},

		{MustCreateBinary(sqltypes.Binary, 4), []byte{'1'}, string([]byte{'1', 0, 0, 0}), false},
		{MustCreateBinary(sqltypes.Blob, 4), []byte{'1'}, string([]byte{'1'}), false},
		{MustCreateStringWithDefaults(sqltypes.Char, 7), "abcde", "abcde", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 7), "abcde", "abcde", false},
		{MustCreateBinary(sqltypes.VarBinary, 7), "abcde", "abcde", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 7), "abcde", "abcde", false},

		{MustCreateStringWithDefaults(sqltypes.Char, 4), int(1), "1", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), int8(2), "2", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 4), int16(3), "3", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), int32(4), "4", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), int64(5), "5", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 4), uint(10), "10", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), uint8(11), "11", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), uint16(12), "12", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 4), uint32(13), "13", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), uint64(14), "14", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), float32(9.875), "9.875", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 7), float64(11583.5), "11583.5", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), []byte("abcd"), "abcd", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 40), time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), "2019-12-12 12:12:12", false},

		{MustCreateBinary(sqltypes.Binary, 3), "abcd", nil, true},
		{MustCreateBinary(sqltypes.Blob, 3), strings.Repeat("0", tinyTextBlobMax + 1), nil, true},
		{MustCreateStringWithDefaults(sqltypes.Char, 3), "abcd", nil, true},
		{MustCreateStringWithDefaults(sqltypes.Text, 3), strings.Repeat("0", int(tinyTextBlobMax / Collation_Default.CharacterSet().MaxLength()) + 1), nil, true},
		{MustCreateBinary(sqltypes.VarBinary, 3), []byte{01, 02, 03, 04}, nil, true},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 3), []byte("abcd"), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val, test.expectedVal), func(t *testing.T) {
			val, err := test.typ.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
			}
		})
	}
}

func TestStringString(t *testing.T) {
	tests := []struct {
		typ Type
		expectedStr string
	}{
		{MustCreateBinary(sqltypes.Binary, 10), "BINARY(10)"},
		{MustCreateBinary(sqltypes.Binary, charBinaryMax), fmt.Sprintf("BINARY(%v)", charBinaryMax)},
		{MustCreateBinary(sqltypes.Blob, 0), "TINYBLOB"},
		{MustCreateBinary(sqltypes.Blob, tinyTextBlobMax - 1), "TINYBLOB"},
		{MustCreateBinary(sqltypes.Blob, tinyTextBlobMax), "TINYBLOB"},
		{MustCreateBinary(sqltypes.Blob, tinyTextBlobMax + 1), "BLOB"},
		{MustCreateBinary(sqltypes.Blob, textBlobMax - 1), "BLOB"},
		{MustCreateBinary(sqltypes.Blob, textBlobMax), "BLOB"},
		{MustCreateBinary(sqltypes.Blob, textBlobMax + 1), "MEDIUMBLOB"},
		{MustCreateBinary(sqltypes.Blob, mediumTextBlobMax - 1), "MEDIUMBLOB"},
		{MustCreateBinary(sqltypes.Blob, mediumTextBlobMax), "MEDIUMBLOB"},
		{MustCreateBinary(sqltypes.Blob, mediumTextBlobMax + 1), "LONGBLOB"},
		{MustCreateBinary(sqltypes.Blob, longTextBlobMax), "LONGBLOB"},
		{MustCreateString(sqltypes.Char, 10, Collation_Default), "CHAR(10)"},
		{MustCreateString(sqltypes.Char, charBinaryMax, Collation_Default), fmt.Sprintf("CHAR(%v)", charBinaryMax)},
		{MustCreateString(sqltypes.Text, 0, Collation_Default), "TINYTEXT"},
		{MustCreateString(sqltypes.Text, tinyTextBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default), "TINYTEXT"},
		{MustCreateString(sqltypes.Text, tinyTextBlobMax, Collation_Default), "TEXT"},
		{MustCreateString(sqltypes.Text, textBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default), "TEXT"},
		{MustCreateString(sqltypes.Text, textBlobMax, Collation_Default), "MEDIUMTEXT"},
		{MustCreateString(sqltypes.Text, mediumTextBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default), "MEDIUMTEXT"},
		{MustCreateString(sqltypes.Text, mediumTextBlobMax, Collation_Default), "LONGTEXT"},
		{MustCreateString(sqltypes.Text, longTextBlobMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default), "LONGTEXT"},
		{MustCreateString(sqltypes.Text, longTextBlobMax, Collation_Default), "LONGTEXT"},
		{MustCreateBinary(sqltypes.VarBinary, 10), "VARBINARY(10)"},
		{MustCreateBinary(sqltypes.VarBinary, varcharVarbinaryMax), fmt.Sprintf("VARBINARY(%v)", varcharVarbinaryMax)},
		{MustCreateString(sqltypes.VarChar, 10, Collation_Default), "VARCHAR(10)"},
		{MustCreateString(sqltypes.VarChar, varcharVarbinaryMax / Collation_Default.CharacterSet().MaxLength(), Collation_Default),
			fmt.Sprintf("VARCHAR(%v)", varcharVarbinaryMax / Collation_Default.CharacterSet().MaxLength())},

		{MustCreateString(sqltypes.Char, 10, Collation_Default.CharacterSet().BinaryCollation()),
			fmt.Sprintf("CHAR(10) COLLATE %v", Collation_Default.CharacterSet().BinaryCollation())},
		{MustCreateString(sqltypes.Char, 10, Collation_tis620_thai_ci), "CHAR(10) CHARACTER SET tis620 COLLATE tis620_thai_ci"},
		{MustCreateString(sqltypes.Text, 10, Collation_ascii_general_ci), "TINYTEXT CHARACTER SET ascii COLLATE ascii_general_ci"},
		{MustCreateString(sqltypes.VarChar, 10, Collation_cp1251_bin), "VARCHAR(10) CHARACTER SET cp1251 COLLATE cp1251_bin"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}
// Copyright 2022 Dolthub, Inc.
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

package types

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestStringCompare(t *testing.T) {
	tests := []struct {
		typ         sql.StringType
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{MustCreateBinary(sqltypes.Binary, 10), nil, 0, 1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), 0, nil, -1},
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
		baseType     query.Type
		length       int64
		expectedType StringType
		expectedErr  bool
	}{
		{sqltypes.Binary, 10,
			StringType{sqltypes.Binary, 10, 10, 10, sql.Collation_binary}, false},
		{sqltypes.Blob, 10,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary}, false},
		{sqltypes.Char, 10,
			StringType{sqltypes.Binary, 10, 10, 10, sql.Collation_binary}, false},
		{sqltypes.Text, 10,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary}, false},
		{sqltypes.VarBinary, 10,
			StringType{sqltypes.VarBinary, 10, 10, 10, sql.Collation_binary}, false},
		{sqltypes.VarChar, 10,
			StringType{sqltypes.VarBinary, 10, 10, 10, sql.Collation_binary}, false},
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
		baseType     query.Type
		length       int64
		expectedType StringType
		expectedErr  bool
	}{
		{sqltypes.Bit, 10, StringType{}, true},
		{sqltypes.Date, 10, StringType{}, true},
		{sqltypes.Datetime, 10, StringType{}, true},
		{sqltypes.Decimal, 10, StringType{}, true},
		{sqltypes.Enum, 10, StringType{}, true},
		{sqltypes.Expression, 10, StringType{}, true},
		{sqltypes.Float32, 10, StringType{}, true},
		{sqltypes.Float64, 10, StringType{}, true},
		{sqltypes.Geometry, 10, StringType{}, true},
		{sqltypes.Int16, 10, StringType{}, true},
		{sqltypes.Int24, 10, StringType{}, true},
		{sqltypes.Int32, 10, StringType{}, true},
		{sqltypes.Int64, 10, StringType{}, true},
		{sqltypes.Int8, 10, StringType{}, true},
		{sqltypes.Null, 10, StringType{}, true},
		{sqltypes.Set, 10, StringType{}, true},
		{sqltypes.Time, 10, StringType{}, true},
		{sqltypes.Timestamp, 10, StringType{}, true},
		{sqltypes.TypeJSON, 10, StringType{}, true},
		{sqltypes.Uint16, 10, StringType{}, true},
		{sqltypes.Uint24, 10, StringType{}, true},
		{sqltypes.Uint32, 10, StringType{}, true},
		{sqltypes.Uint64, 10, StringType{}, true},
		{sqltypes.Uint8, 10, StringType{}, true},
		{sqltypes.Year, 10, StringType{}, true},
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
		baseType     query.Type
		length       int64
		collation    sql.CollationID
		expectedType StringType
		expectedErr  bool
	}{
		{sqltypes.Binary, 10, sql.Collation_binary,
			StringType{sqltypes.Binary, 10, 10, 10, sql.Collation_binary}, false},
		{sqltypes.Blob, 10, sql.Collation_binary,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary}, false},
		{sqltypes.Char, 10, sql.Collation_Default,
			StringType{sqltypes.Char, 10, 40, 40, sql.Collation_Default}, false},
		{sqltypes.Text, 10, sql.Collation_Default,
			StringType{sqltypes.Text, TinyTextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), TinyTextBlobMax, uint32(TinyTextBlobMax * sql.Collation_Default.CharacterSet().MaxLength()), sql.Collation_Default}, false},
		{sqltypes.Text, 1000, sql.Collation_Default,
			StringType{sqltypes.Text, TextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), TextBlobMax, uint32(TextBlobMax * sql.Collation_Default.CharacterSet().MaxLength()), sql.Collation_Default}, false},
		{sqltypes.Text, 1000000, sql.Collation_Default,
			StringType{sqltypes.Text, MediumTextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), MediumTextBlobMax, uint32(MediumTextBlobMax * sql.Collation_Default.CharacterSet().MaxLength()), sql.Collation_Default}, false},
		{sqltypes.Text, LongTextBlobMax, sql.Collation_Default,
			StringType{sqltypes.Text, LongTextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), LongTextBlobMax, uint32(LongTextBlobMax), sql.Collation_Default}, false},
		{sqltypes.VarBinary, 10, sql.Collation_binary,
			StringType{sqltypes.VarBinary, 10, 10, 10, sql.Collation_binary}, false},
		{sqltypes.VarChar, 10, sql.Collation_Default,
			StringType{sqltypes.VarChar, 10, 40, 40, sql.Collation_Default}, false},
		{sqltypes.Char, 10, sql.Collation_binary,
			StringType{sqltypes.Binary, 10, 10, 10, sql.Collation_binary}, false},
		{sqltypes.Text, 10, sql.Collation_binary,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary}, false},
		{sqltypes.VarChar, 10, sql.Collation_binary,
			StringType{sqltypes.VarBinary, 10, 10, 10, sql.Collation_binary}, false},

		{sqltypes.Binary, charBinaryMax + 1, sql.Collation_binary, StringType{}, true},
		{sqltypes.Blob, LongTextBlobMax + 1, sql.Collation_binary, StringType{}, true},
		{sqltypes.Char, charBinaryMax + 1, sql.Collation_Default, StringType{}, true},
		{sqltypes.Text, LongTextBlobMax + 1, sql.Collation_Default, StringType{}, true},

		// JSON strings can also come in over the wire as VARBINARY types, and JSON allows a much larger length limit (1GB).
		{sqltypes.VarBinary, MaxJsonFieldByteLength + 1, sql.Collation_binary, StringType{}, true},
		{sqltypes.VarChar, varcharVarbinaryMax + 1, sql.Collation_Default, StringType{}, true},

		// Default collation is not valid for these types
		{sqltypes.Binary, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Blob, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.VarBinary, 10, sql.Collation_Default, StringType{}, true},
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
		baseType     query.Type
		length       int64
		collation    sql.CollationID
		expectedType StringType
		expectedErr  bool
	}{
		{sqltypes.Bit, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Date, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Datetime, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Decimal, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Enum, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Expression, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Float32, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Float64, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Geometry, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int16, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int24, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int32, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int64, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int8, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Null, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Set, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Time, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Timestamp, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.TypeJSON, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint16, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint24, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint32, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint64, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint8, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Year, 10, sql.Collation_Default, StringType{}, true},
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
		typ         sql.StringType
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{MustCreateBinary(sqltypes.Binary, 3), nil, nil, false},
		{MustCreateBinary(sqltypes.Blob, 3), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.Char, 7), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.Text, 7), nil, nil, false},
		{MustCreateBinary(sqltypes.VarBinary, 3), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 7), nil, nil, false},

		{MustCreateBinary(sqltypes.Binary, 4), []byte{'1'}, []byte{'1', 0, 0, 0}, false},
		{MustCreateBinary(sqltypes.Blob, 4), []byte{'1'}, []byte{'1'}, false},
		{MustCreateStringWithDefaults(sqltypes.Char, 7), "abcde", "abcde", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 7), "abcde", "abcde", false},
		{MustCreateBinary(sqltypes.VarBinary, 7), "abcde", []byte("abcde"), false},
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
		{MustCreateBinary(sqltypes.Blob, 3), strings.Repeat("0", TinyTextBlobMax+1), nil, true},
		{MustCreateStringWithDefaults(sqltypes.Char, 3), "abcd", nil, true},
		{MustCreateStringWithDefaults(sqltypes.Text, 3),
			strings.Repeat("𒁏", int(TinyTextBlobMax/sql.Collation_Default.CharacterSet().MaxLength())+1),
			nil, true},
		{MustCreateBinary(sqltypes.VarBinary, 3), []byte{01, 02, 03, 04}, nil, true},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 3), []byte("abcd"), nil, true},
		{MustCreateStringWithDefaults(sqltypes.Char, 20), JSONDocument{Val: nil}, "null", false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val, test.expectedVal), func(t *testing.T) {
			val, err := test.typ.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
				if val != nil {
					assert.Equal(t, test.typ.ValueType(), reflect.TypeOf(val))
				}
			}
		})
	}
}

func TestStringString(t *testing.T) {
	tests := []struct {
		typ         sql.Type
		expectedStr string
	}{
		{MustCreateBinary(sqltypes.Binary, 10), "binary(10)"},
		{MustCreateBinary(sqltypes.Binary, charBinaryMax), fmt.Sprintf("binary(%v)", charBinaryMax)},
		{MustCreateBinary(sqltypes.Blob, 0), "tinyblob"},
		{MustCreateBinary(sqltypes.Blob, TinyTextBlobMax-1), "tinyblob"},
		{MustCreateBinary(sqltypes.Blob, TinyTextBlobMax), "tinyblob"},
		{MustCreateBinary(sqltypes.Blob, TinyTextBlobMax+1), "blob"},
		{MustCreateBinary(sqltypes.Blob, TextBlobMax-1), "blob"},
		{MustCreateBinary(sqltypes.Blob, TextBlobMax), "blob"},
		{MustCreateBinary(sqltypes.Blob, TextBlobMax+1), "mediumblob"},
		{MustCreateBinary(sqltypes.Blob, MediumTextBlobMax-1), "mediumblob"},
		{MustCreateBinary(sqltypes.Blob, MediumTextBlobMax), "mediumblob"},
		{MustCreateBinary(sqltypes.Blob, MediumTextBlobMax+1), "longblob"},
		{MustCreateBinary(sqltypes.Blob, LongTextBlobMax), "longblob"},
		{MustCreateString(sqltypes.Char, 10, sql.Collation_Default), "char(10)"},
		{MustCreateString(sqltypes.Char, charBinaryMax, sql.Collation_Default), fmt.Sprintf("char(%v)", charBinaryMax)},
		{MustCreateString(sqltypes.Text, 0, sql.Collation_Default), "tinytext"},
		{MustCreateString(sqltypes.Text, TinyTextBlobMax, sql.Collation_Default), "tinytext"},
		{MustCreateString(sqltypes.Text, TinyTextBlobMax+1, sql.Collation_Default), "text"},
		{MustCreateString(sqltypes.Text, TextBlobMax, sql.Collation_Default), "text"},
		{MustCreateString(sqltypes.Text, TextBlobMax+1, sql.Collation_Default), "mediumtext"},
		{MustCreateString(sqltypes.Text, MediumTextBlobMax, sql.Collation_Default), "mediumtext"},
		{MustCreateString(sqltypes.Text, MediumTextBlobMax+1, sql.Collation_Default), "longtext"},
		{MustCreateString(sqltypes.Text, LongTextBlobMax-1, sql.Collation_Default), "longtext"},
		{MustCreateString(sqltypes.Text, LongTextBlobMax, sql.Collation_Default), "longtext"},
		{MustCreateBinary(sqltypes.VarBinary, 10), "varbinary(10)"},
		{MustCreateBinary(sqltypes.VarBinary, varcharVarbinaryMax), fmt.Sprintf("varbinary(%v)", varcharVarbinaryMax)},
		{MustCreateString(sqltypes.VarChar, 10, sql.Collation_Default), "varchar(10)"},
		{MustCreateString(sqltypes.VarChar, varcharVarbinaryMax, sql.Collation_Default),
			fmt.Sprintf("varchar(%v)", varcharVarbinaryMax)},
		{MustCreateString(sqltypes.Char, 10, sql.Collation_Default.CharacterSet().BinaryCollation()),
			fmt.Sprintf("char(10) COLLATE %v", sql.Collation_Default.CharacterSet().BinaryCollation())},
		{MustCreateString(sqltypes.Char, 10, sql.Collation_utf16_general_ci), "char(10) CHARACTER SET utf16 COLLATE utf16_general_ci"},
		{MustCreateString(sqltypes.Text, 10, sql.Collation_ascii_general_ci), "tinytext CHARACTER SET ascii COLLATE ascii_general_ci"},
		{MustCreateString(sqltypes.VarChar, 10, sql.Collation_latin1_bin), "varchar(10) CHARACTER SET latin1 COLLATE latin1_bin"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

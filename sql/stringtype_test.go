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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringCompare(t *testing.T) {
	tests := []struct {
		typ  types.StringType
		val1 interface{}
		val2        interface{}
		expectedCmp int
	}{
		{types.MustCreateBinary(sqltypes.Binary, 10), nil, 0, 1},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 10), 0, nil, -1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), nil, nil, 0},

		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), 0, 1, -1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte{0}, true, -1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), false, 1, 1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, 0, 1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), true, "false", 1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, false, -1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, 1, 0},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), true, 1, 1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), "True", true, -1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), false, true, -1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), "0x12345de", "0xed54321", -1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), "0xed54321", "0x12345de", 1},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte("254"), 254, 0},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte("254"), 254.5, -1},

		// Sanity checks that behavior is consistent
		{types.MustCreateBinary(sqltypes.Binary, 10), 0, 1, -1},
		{types.MustCreateBinary(sqltypes.Binary, 10), []byte{0}, true, -1},
		{types.MustCreateBinary(sqltypes.Binary, 10), false, 1, 1},
		{types.MustCreateBinary(sqltypes.Binary, 10), []byte("254"), 254, 0},
		{types.MustCreateBinary(sqltypes.Blob, 10), 0, 1, -1},
		{types.MustCreateBinary(sqltypes.Blob, 10), []byte{0}, true, -1},
		{types.MustCreateBinary(sqltypes.Blob, 10), false, 1, 1},
		{types.MustCreateBinary(sqltypes.Blob, 10), []byte("254"), 254, 0},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 10), 0, 1, -1},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 10), []byte{0}, true, -1},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 10), false, 1, 1},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 10), []byte("254"), 254, 0},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 10), 0, 1, -1},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 10), []byte{0}, true, -1},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 10), false, 1, 1},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 10), []byte("254"), 254, 0},
		{types.MustCreateBinary(sqltypes.VarBinary, 10), []byte{0}, true, -1},
		{types.MustCreateBinary(sqltypes.VarBinary, 10), false, 1, 1},
		{types.MustCreateBinary(sqltypes.VarBinary, 10), 0, 1, -1},
		{types.MustCreateBinary(sqltypes.VarBinary, 10), []byte("254"), 254, 0},
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
		expectedType types.StringType_
		expectedErr  bool
	}{
		{sqltypes.Binary, 10,
			types.StringType_{sqltypes.Binary, 10, 10, 10, Collation_binary}, false},
		{sqltypes.Blob, 10,
			types.StringType_{sqltypes.Blob, types.tinyTextBlobMax, types.tinyTextBlobMax, types.tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.Char, 10,
			types.StringType_{sqltypes.Binary, 10, 10, 10, Collation_binary}, false},
		{sqltypes.Text, 10,
			types.StringType_{sqltypes.Blob, types.tinyTextBlobMax, types.tinyTextBlobMax, types.tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.VarBinary, 10,
			types.StringType_{sqltypes.VarBinary, 10, 10, 10, Collation_binary}, false},
		{sqltypes.VarChar, 10,
			types.StringType_{sqltypes.VarBinary, 10, 10, 10, Collation_binary}, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.baseType, test.length), func(t *testing.T) {
			typ, err := types.CreateBinary(test.baseType, test.length)
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
		expectedType types.StringType_
		expectedErr  bool
	}{
		{sqltypes.Bit, 10, types.StringType_{}, true},
		{sqltypes.Date, 10, types.StringType_{}, true},
		{sqltypes.Datetime, 10, types.StringType_{}, true},
		{sqltypes.Decimal, 10, types.StringType_{}, true},
		{sqltypes.Enum, 10, types.StringType_{}, true},
		{sqltypes.Expression, 10, types.StringType_{}, true},
		{sqltypes.Float32, 10, types.StringType_{}, true},
		{sqltypes.Float64, 10, types.StringType_{}, true},
		{sqltypes.Geometry, 10, types.StringType_{}, true},
		{sqltypes.Int16, 10, types.StringType_{}, true},
		{sqltypes.Int24, 10, types.StringType_{}, true},
		{sqltypes.Int32, 10, types.StringType_{}, true},
		{sqltypes.Int64, 10, types.StringType_{}, true},
		{sqltypes.Int8, 10, types.StringType_{}, true},
		{sqltypes.Null, 10, types.StringType_{}, true},
		{sqltypes.Set, 10, types.StringType_{}, true},
		{sqltypes.Time, 10, types.StringType_{}, true},
		{sqltypes.Timestamp, 10, types.StringType_{}, true},
		{sqltypes.TypeJSON, 10, types.StringType_{}, true},
		{sqltypes.Uint16, 10, types.StringType_{}, true},
		{sqltypes.Uint24, 10, types.StringType_{}, true},
		{sqltypes.Uint32, 10, types.StringType_{}, true},
		{sqltypes.Uint64, 10, types.StringType_{}, true},
		{sqltypes.Uint8, 10, types.StringType_{}, true},
		{sqltypes.Year, 10, types.StringType_{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.baseType, test.length), func(t *testing.T) {
			typ, err := types.CreateBinary(test.baseType, test.length)
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
		collation    CollationID
		expectedType types.StringType_
		expectedErr  bool
	}{
		{sqltypes.Binary, 10, Collation_binary,
			types.StringType_{sqltypes.Binary, 10, 10, 10, Collation_binary}, false},
		{sqltypes.Blob, 10, Collation_binary,
			types.StringType_{sqltypes.Blob, types.tinyTextBlobMax, types.tinyTextBlobMax, types.tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.Char, 10, Collation_Default,
			types.StringType_{sqltypes.Char, 10, 40, 40, Collation_Default}, false},
		{sqltypes.Text, 10, Collation_Default,
			types.StringType_{sqltypes.Text, types.tinyTextBlobMax / Collation_Default.CharacterSet().MaxLength(), types.tinyTextBlobMax, uint32(types.tinyTextBlobMax * Collation_Default.CharacterSet().MaxLength()), Collation_Default}, false},
		{sqltypes.Text, 1000, Collation_Default,
			types.StringType_{sqltypes.Text, types.textBlobMax / Collation_Default.CharacterSet().MaxLength(), types.textBlobMax, uint32(types.textBlobMax * Collation_Default.CharacterSet().MaxLength()), Collation_Default}, false},
		{sqltypes.Text, 1000000, Collation_Default,
			types.StringType_{sqltypes.Text, types.mediumTextBlobMax / Collation_Default.CharacterSet().MaxLength(), types.mediumTextBlobMax, uint32(types.mediumTextBlobMax * Collation_Default.CharacterSet().MaxLength()), Collation_Default}, false},
		{sqltypes.Text, types.longTextBlobMax, Collation_Default,
			types.StringType_{sqltypes.Text, types.longTextBlobMax / Collation_Default.CharacterSet().MaxLength(), types.longTextBlobMax, uint32(types.longTextBlobMax), Collation_Default}, false},
		{sqltypes.VarBinary, 10, Collation_binary,
			types.StringType_{sqltypes.VarBinary, 10, 10, 10, Collation_binary}, false},
		{sqltypes.VarChar, 10, Collation_Default,
			types.StringType_{sqltypes.VarChar, 10, 40, 40, Collation_Default}, false},
		{sqltypes.Char, 10, Collation_binary,
			types.StringType_{sqltypes.Binary, 10, 10, 10, Collation_binary}, false},
		{sqltypes.Text, 10, Collation_binary,
			types.StringType_{sqltypes.Blob, types.tinyTextBlobMax, types.tinyTextBlobMax, types.tinyTextBlobMax, Collation_binary}, false},
		{sqltypes.VarChar, 10, Collation_binary,
			types.StringType_{sqltypes.VarBinary, 10, 10, 10, Collation_binary}, false},

		{sqltypes.Binary, types.charBinaryMax + 1, Collation_binary, types.StringType_{}, true},
		{sqltypes.Blob, types.longTextBlobMax + 1, Collation_binary, types.StringType_{}, true},
		{sqltypes.Char, types.charBinaryMax + 1, Collation_Default, types.StringType_{}, true},
		{sqltypes.Text, types.longTextBlobMax + 1, Collation_Default, types.StringType_{}, true},

		// JSON strings can also come in over the wire as VARBINARY types, and JSON allows a much larger length limit (1GB).
		{sqltypes.VarBinary, MaxJsonFieldByteLength + 1, Collation_binary, types.StringType_{}, true},
		{sqltypes.VarChar, types.varcharVarbinaryMax + 1, Collation_Default, types.StringType_{}, true},

		// Default collation is not valid for these types
		{sqltypes.Binary, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Blob, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.VarBinary, 10, Collation_Default, types.StringType_{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.baseType, test.length, test.collation), func(t *testing.T) {
			typ, err := types.CreateString(test.baseType, test.length, test.collation)
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
		collation    CollationID
		expectedType types.StringType_
		expectedErr  bool
	}{
		{sqltypes.Bit, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Date, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Datetime, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Decimal, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Enum, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Expression, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Float32, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Float64, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Geometry, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Int16, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Int24, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Int32, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Int64, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Int8, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Null, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Set, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Time, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Timestamp, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.TypeJSON, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Uint16, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Uint24, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Uint32, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Uint64, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Uint8, 10, Collation_Default, types.StringType_{}, true},
		{sqltypes.Year, 10, Collation_Default, types.StringType_{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.baseType, test.length, test.collation), func(t *testing.T) {
			typ, err := types.CreateString(test.baseType, test.length, test.collation)
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
		typ types.StringType
		val interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{types.MustCreateBinary(sqltypes.Binary, 3), nil, nil, false},
		{types.MustCreateBinary(sqltypes.Blob, 3), nil, nil, false},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 7), nil, nil, false},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 7), nil, nil, false},
		{types.MustCreateBinary(sqltypes.VarBinary, 3), nil, nil, false},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 7), nil, nil, false},

		{types.MustCreateBinary(sqltypes.Binary, 4), []byte{'1'}, []byte{'1', 0, 0, 0}, false},
		{types.MustCreateBinary(sqltypes.Blob, 4), []byte{'1'}, []byte{'1'}, false},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 7), "abcde", "abcde", false},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 7), "abcde", "abcde", false},
		{types.MustCreateBinary(sqltypes.VarBinary, 7), "abcde", []byte("abcde"), false},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 7), "abcde", "abcde", false},

		{types.MustCreateStringWithDefaults(sqltypes.Char, 4), int(1), "1", false},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 4), int8(2), "2", false},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 4), int16(3), "3", false},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 4), int32(4), "4", false},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 4), int64(5), "5", false},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 4), uint(10), "10", false},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 4), uint8(11), "11", false},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 4), uint16(12), "12", false},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 4), uint32(13), "13", false},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 4), uint64(14), "14", false},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 4), float32(9.875), "9.875", false},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 7), float64(11583.5), "11583.5", false},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 4), []byte("abcd"), "abcd", false},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 40), time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), "2019-12-12 12:12:12", false},

		{types.MustCreateBinary(sqltypes.Binary, 3), "abcd", nil, true},
		{types.MustCreateBinary(sqltypes.Blob, 3), strings.Repeat("0", types.tinyTextBlobMax+1), nil, true},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 3), "abcd", nil, true},
		{types.MustCreateStringWithDefaults(sqltypes.Text, 3),
			strings.Repeat("𒁏", int(types.tinyTextBlobMax/Collation_Default.CharacterSet().MaxLength())+1),
			nil, true},
		{types.MustCreateBinary(sqltypes.VarBinary, 3), []byte{01, 02, 03, 04}, nil, true},
		{types.MustCreateStringWithDefaults(sqltypes.VarChar, 3), []byte("abcd"), nil, true},
		{types.MustCreateStringWithDefaults(sqltypes.Char, 20), JSONDocument{Val: nil}, "null", false},
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
		typ         Type
		expectedStr string
	}{
		{types.MustCreateBinary(sqltypes.Binary, 10), "binary(10)"},
		{types.MustCreateBinary(sqltypes.Binary, types.charBinaryMax), fmt.Sprintf("binary(%v)", types.charBinaryMax)},
		{types.MustCreateBinary(sqltypes.Blob, 0), "tinyblob"},
		{types.MustCreateBinary(sqltypes.Blob, types.tinyTextBlobMax-1), "tinyblob"},
		{types.MustCreateBinary(sqltypes.Blob, types.tinyTextBlobMax), "tinyblob"},
		{types.MustCreateBinary(sqltypes.Blob, types.tinyTextBlobMax+1), "blob"},
		{types.MustCreateBinary(sqltypes.Blob, types.textBlobMax-1), "blob"},
		{types.MustCreateBinary(sqltypes.Blob, types.textBlobMax), "blob"},
		{types.MustCreateBinary(sqltypes.Blob, types.textBlobMax+1), "mediumblob"},
		{types.MustCreateBinary(sqltypes.Blob, types.mediumTextBlobMax-1), "mediumblob"},
		{types.MustCreateBinary(sqltypes.Blob, types.mediumTextBlobMax), "mediumblob"},
		{types.MustCreateBinary(sqltypes.Blob, types.mediumTextBlobMax+1), "longblob"},
		{types.MustCreateBinary(sqltypes.Blob, types.longTextBlobMax), "longblob"},
		{types.MustCreateString(sqltypes.Char, 10, Collation_Default), "char(10)"},
		{types.MustCreateString(sqltypes.Char, types.charBinaryMax, Collation_Default), fmt.Sprintf("char(%v)", types.charBinaryMax)},
		{types.MustCreateString(sqltypes.Text, 0, Collation_Default), "tinytext"},
		{types.MustCreateString(sqltypes.Text, types.tinyTextBlobMax, Collation_Default), "tinytext"},
		{types.MustCreateString(sqltypes.Text, types.tinyTextBlobMax+1, Collation_Default), "text"},
		{types.MustCreateString(sqltypes.Text, types.textBlobMax, Collation_Default), "text"},
		{types.MustCreateString(sqltypes.Text, types.textBlobMax+1, Collation_Default), "mediumtext"},
		{types.MustCreateString(sqltypes.Text, types.mediumTextBlobMax, Collation_Default), "mediumtext"},
		{types.MustCreateString(sqltypes.Text, types.mediumTextBlobMax+1, Collation_Default), "longtext"},
		{types.MustCreateString(sqltypes.Text, types.longTextBlobMax-1, Collation_Default), "longtext"},
		{types.MustCreateString(sqltypes.Text, types.longTextBlobMax, Collation_Default), "longtext"},
		{types.MustCreateBinary(sqltypes.VarBinary, 10), "varbinary(10)"},
		{types.MustCreateBinary(sqltypes.VarBinary, types.varcharVarbinaryMax), fmt.Sprintf("varbinary(%v)", types.varcharVarbinaryMax)},
		{types.MustCreateString(sqltypes.VarChar, 10, Collation_Default), "varchar(10)"},
		{types.MustCreateString(sqltypes.VarChar, types.varcharVarbinaryMax, Collation_Default),
			fmt.Sprintf("varchar(%v)", types.varcharVarbinaryMax)},
		{types.MustCreateString(sqltypes.Char, 10, Collation_Default.CharacterSet().BinaryCollation()),
			fmt.Sprintf("char(10) COLLATE %v", Collation_Default.CharacterSet().BinaryCollation())},
		{types.MustCreateString(sqltypes.Char, 10, Collation_utf16_general_ci), "char(10) CHARACTER SET utf16 COLLATE utf16_general_ci"},
		{types.MustCreateString(sqltypes.Text, 10, Collation_ascii_general_ci), "tinytext CHARACTER SET ascii COLLATE ascii_general_ci"},
		{types.MustCreateString(sqltypes.VarChar, 10, Collation_latin1_bin), "varchar(10) CHARACTER SET latin1 COLLATE latin1_bin"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

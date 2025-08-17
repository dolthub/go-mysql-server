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
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/assert"

	"github.com/gabereiser/go-mysql-server/sql"
)

func TestFloatCovert(t *testing.T) {
	tests := []struct {
		length   string
		scale    string
		expected sql.Type
		err      bool
	}{
		{"20", "2", Float32, false},
		{"-1", "", nil, true},
		{"54", "", nil, true},
		{"", "", Float32, false},
		{"0", "", Float32, false},
		{"24", "", Float32, false},
		{"25", "", Float64, false},
		{"53", "", Float64, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.length, test.scale, test.err), func(t *testing.T) {
			var precision *sqlparser.SQLVal = nil
			var scale *sqlparser.SQLVal = nil

			if test.length != "" {
				precision = &sqlparser.SQLVal{
					Type: sqlparser.IntVal,
					Val:  []byte(test.length),
				}
			}

			if test.scale != "" {
				scale = &sqlparser.SQLVal{
					Type: sqlparser.IntVal,
					Val:  []byte(test.scale),
				}
			}

			ct := &sqlparser.ColumnType{
				Type:   "FLOAT",
				Scale:  scale,
				Length: precision,
			}
			res, err := ColumnTypeToType(ct)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestColumnTypeToType_Time(t *testing.T) {
	tests := []struct {
		length   string
		expected sql.Type
		err      bool
	}{
		{"", Time, false},
		{"0", nil, true},
		{"1", nil, true},
		{"2", nil, true},
		{"3", nil, true},
		{"4", nil, true},
		{"5", nil, true},
		{"6", Time, false},
		{"7", nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.length, test.err), func(t *testing.T) {
			var precision *sqlparser.SQLVal

			if test.length != "" {
				precision = &sqlparser.SQLVal{
					Type: sqlparser.IntVal,
					Val:  []byte(test.length),
				}
			}

			ct := &sqlparser.ColumnType{
				Type:   "TIME",
				Length: precision,
			}
			res, err := ColumnTypeToType(ct)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}

func TestColumnCharTypes(t *testing.T) {
	tests := []struct {
		typ string
		len int64
		exp sql.Type
	}{
		{
			typ: "nchar varchar",
			len: 10,
			exp: StringType{baseType: sqltypes.VarChar, maxCharLength: 10, maxByteLength: 30, collation: 33},
		},
		{
			typ: "char varying",
			len: 10,
			exp: StringType{baseType: sqltypes.VarChar, maxCharLength: 10, maxByteLength: 40},
		},
		{
			typ: "nchar varying",
			len: 10,
			exp: StringType{baseType: sqltypes.VarChar, maxCharLength: 10, maxByteLength: 30, collation: 33},
		},
		{
			typ: "national char varying",
			len: 10,
			exp: StringType{baseType: sqltypes.VarChar, maxCharLength: 10, maxByteLength: 30, collation: 33},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.exp), func(t *testing.T) {
			ct := &sqlparser.ColumnType{
				Type:   test.typ,
				Length: &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte(fmt.Sprintf("%v", test.len))},
			}
			res, err := ColumnTypeToType(ct)
			assert.NoError(t, err)
			assert.Equal(t, test.exp, res)
		})
	}
}

func TestGeneralizeTypes(t *testing.T) {
	decimalType := MustCreateDecimalType(DecimalTypeMaxPrecision, DecimalTypeMaxScale)
	uint64DecimalType := MustCreateDecimalType(DecimalTypeMaxPrecision, 0)

	tests := []struct {
		typeA    sql.Type
		typeB    sql.Type
		expected sql.Type
	}{
		{Float64, Float32, Float64},
		{Float64, Int32, Float64},
		{Int24, Float32, Float64},
		{decimalType, Float64, Float64},
		{decimalType, Int32, decimalType},
		{Int64, decimalType, decimalType},
		{Uint64, Int32, uint64DecimalType},
		{Int24, Uint64, uint64DecimalType},
		{Uint64, Uint8, Uint64},
		{Uint24, Uint64, Uint64},
		{Int64, Uint32, Int64},
		{Int24, Int64, Int64},
		{Int8, Int64, Int64},
		{Uint32, Int24, Int64},
		{Uint24, Uint32, Uint32},
		{Int32, Int8, Int32},
		{Uint24, Int32, Int32},
		{Uint24, Int24, Int32},
		{Uint8, Uint24, Uint24},
		{Int24, Uint8, Int24},
		{Int8, Int24, Int24},
		{Int8, Uint16, Int24},
		{Uint16, Uint8, Uint16},
		{Int16, Int16, Int16},
		{Int8, Int16, Int16},
		{Uint8, Int8, Int16},
		{Uint8, Uint8, Uint8},
		{Int8, Int8, Int8},
		{Boolean, Int64, Int64},
		{Boolean, Boolean, Boolean},
		{Text, Text, Text},
		{Text, LongText, LongText},
		{Text, Float64, LongText},
		{Int64, Text, LongText},
		{Int8, Null, Int8},
		{Time, Time, Time},
		{Time, Date, DatetimeMaxPrecision},
		{Date, Date, Date},
		{Date, Timestamp, DatetimeMaxPrecision},
		{Timestamp, Timestamp, Timestamp},
		{Timestamp, TimestampMaxPrecision, TimestampMaxPrecision},
		{Timestamp, Datetime, DatetimeMaxPrecision},
		{Null, Int64, Int64},
		{Null, Null, Null},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typeA, test.typeB, test.expected), func(t *testing.T) {
			res := GeneralizeTypes(test.typeA, test.typeB)
			assert.Equal(t, test.expected, res)
		})
	}
}

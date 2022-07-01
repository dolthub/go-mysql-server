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
	"strconv"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

const (
	// EnumTypeMinElements returns the minimum number of enumerations for the Enum type.
	EnumTypeMinElements = 1
	// EnumTypeMaxElements returns the maximum number of enumerations for the Enum type.
	EnumTypeMaxElements = 65535
	/// An ENUM column can have a maximum of 65,535 distinct elements.
)

var (
	ErrConvertingToEnum  = errors.NewKind("value %v is not valid for this Enum")
	ErrUnmarshallingEnum = errors.NewKind("value %v is not a marshalled value for this Enum")

	enumValueType = reflect.TypeOf(uint16(0))
)

// Comments with three slashes were taken directly from the linked documentation.

// EnumType represents the ENUM type.
// https://dev.mysql.com/doc/refman/8.0/en/enum.html
// The type of the returned value is uint16.
type EnumType interface {
	Type
	// At returns the string at the given index, as well if the string was found.
	At(index int) (string, bool)
	CharacterSet() CharacterSetID
	Collation() CollationID
	// IndexOf returns the index of the given string. If the string was not found, then this returns -1.
	IndexOf(v string) int
	// NumberOfElements returns the number of enumerations.
	NumberOfElements() uint16
	// Values returns the elements, in order, of every enumeration.
	Values() []string
}

type enumType struct {
	collation  CollationID
	valToIndex map[string]int
	indexToVal []string
}

// CreateEnumType creates a EnumType.
func CreateEnumType(values []string, collation CollationID) (EnumType, error) {
	if len(values) < EnumTypeMinElements {
		return nil, fmt.Errorf("number of values may not be zero")
	}
	if len(values) > EnumTypeMaxElements {
		return nil, fmt.Errorf("number of values is too large")
	}
	valToIndex := make(map[string]int)
	for i, value := range values {
		if !collation.Equals(Collation_binary) {
			/// Trailing spaces are automatically deleted from ENUM member values in the table definition when a table is created.
			value = strings.TrimRight(value, " ")
		}
		values[i] = value
		if _, ok := valToIndex[value]; ok {
			return nil, fmt.Errorf("duplicate entry: %v", value)
		}
		/// The elements listed in the column specification are assigned index numbers, beginning with 1.
		valToIndex[value] = i + 1
	}
	return enumType{
		collation:  collation,
		valToIndex: valToIndex,
		indexToVal: values,
	}, nil
}

// MustCreateEnumType is the same as CreateEnumType except it panics on errors.
func MustCreateEnumType(values []string, collation CollationID) EnumType {
	et, err := CreateEnumType(values, collation)
	if err != nil {
		panic(err)
	}
	return et
}

// Compare implements Type interface.
func (t enumType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	ai, err := t.Convert(a)
	if err != nil {
		return 0, err
	}
	bi, err := t.Convert(b)
	if err != nil {
		return 0, err
	}
	au := ai.(uint16)
	bu := bi.(uint16)

	if au < bu {
		return -1, nil
	} else if au > bu {
		return 1, nil
	}
	return 0, nil
}

// Convert implements Type interface.
func (t enumType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch value := v.(type) {
	case int:
		if _, ok := t.At(value); ok {
			return uint16(value), nil
		}
	case uint:
		return t.Convert(int(value))
	case int8:
		return t.Convert(int(value))
	case uint8:
		return t.Convert(int(value))
	case int16:
		return t.Convert(int(value))
	case uint16:
		return t.Convert(int(value))
	case int32:
		return t.Convert(int(value))
	case uint32:
		return t.Convert(int(value))
	case int64:
		return t.Convert(int(value))
	case uint64:
		return t.Convert(int(value))
	case float32:
		return t.Convert(int(value))
	case float64:
		return t.Convert(int(value))
	case decimal.Decimal:
		return t.Convert(value.IntPart())
	case decimal.NullDecimal:
		if !value.Valid {
			return nil, nil
		}
		return t.Convert(value.Decimal.IntPart())
	case string:
		if index := t.IndexOf(value); index != -1 {
			return uint16(index), nil
		}
	case []byte:
		return t.Convert(string(value))
	}

	return nil, ErrConvertingToEnum.New(v)
}

// MustConvert implements the Type interface.
func (t enumType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t enumType) Equals(otherType Type) bool {
	if ot, ok := otherType.(enumType); ok && t.collation.Equals(ot.collation) && len(t.indexToVal) == len(ot.indexToVal) {
		for i, val := range t.indexToVal {
			if ot.indexToVal[i] != val {
				return false
			}
		}
		return true
	}
	return false
}

// Promote implements the Type interface.
func (t enumType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t enumType) SQL(dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	convertedValue, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	value, _ := t.At(int(convertedValue.(uint16)))

	val := appendAndSliceString(dest, value)

	return sqltypes.MakeTrusted(sqltypes.Enum, val), nil
}

// String implements Type interface.
func (t enumType) String() string {
	s := fmt.Sprintf("ENUM('%v')", strings.Join(t.indexToVal, `','`))
	if t.CharacterSet() != Collation_Default.CharacterSet() {
		s += " CHARACTER SET " + t.CharacterSet().String()
	}
	if !t.collation.Equals(Collation_Default) {
		s += " COLLATE " + t.collation.String()
	}
	return s
}

// Type implements Type interface.
func (t enumType) Type() query.Type {
	return sqltypes.Enum
}

// ValueType implements Type interface.
func (t enumType) ValueType() reflect.Type {
	return enumValueType
}

// Zero implements Type interface.
func (t enumType) Zero() interface{} {
	/// If an ENUM column is declared NOT NULL, its default value is the first element of the list of permitted values.
	return t.indexToVal[0]
}

// At implements EnumType interface.
func (t enumType) At(index int) (string, bool) {
	/// The elements listed in the column specification are assigned index numbers, beginning with 1.
	index -= 1
	if index < 0 || index >= len(t.indexToVal) {
		return "", false
	}
	return t.indexToVal[index], true
}

// CharacterSet implements EnumType interface.
func (t enumType) CharacterSet() CharacterSetID {
	return t.collation.CharacterSet()
}

// Collation implements EnumType interface.
func (t enumType) Collation() CollationID {
	return t.collation
}

// IndexOf implements EnumType interface.
func (t enumType) IndexOf(v string) int {
	if index, ok := t.valToIndex[v]; ok {
		return index
	}
	/// ENUM('0','1','2')
	/// If you store '3', it does not match any enumeration value, so it is treated as an index and becomes '2' (the value with index 3).
	if parsedIndex, err := strconv.ParseInt(v, 10, 32); err == nil {
		if realV, ok := t.At(int(parsedIndex)); ok {
			if index, ok := t.valToIndex[realV]; ok {
				return index
			}
		}
	}
	return -1
}

// NumberOfElements implements EnumType interface.
func (t enumType) NumberOfElements() uint16 {
	return uint16(len(t.indexToVal))
}

// Values implements EnumType interface.
func (t enumType) Values() []string {
	vals := make([]string, len(t.indexToVal))
	copy(vals, t.indexToVal)
	return vals
}

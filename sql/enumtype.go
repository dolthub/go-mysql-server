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
	"strconv"
	"strings"

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
)

// Comments with three slashes were taken directly from the linked documentation.

// Represents the ENUM type.
// https://dev.mysql.com/doc/refman/8.0/en/enum.html
type EnumType interface {
	Type
	At(index int) (string, bool)
	CharacterSet() CharacterSet
	Collation() Collation
	ConvertToIndex(v interface{}) (int, error)
	IndexOf(v string) int
	//TODO: move this out of go-mysql-server and into the Dolt layer
	Marshal(v interface{}) (int64, error)
	NumberOfElements() uint16
	Unmarshal(v int64) (string, error)
	Values() []string
}

type enumType struct {
	collation  Collation
	valToIndex map[string]int
	indexToVal []string
}

// CreateEnumType creates a EnumType.
func CreateEnumType(values []string, collation Collation) (EnumType, error) {
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
func MustCreateEnumType(values []string, collation Collation) EnumType {
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

	ai, err := t.ConvertToIndex(a)
	if err != nil {
		return 0, err
	}
	bi, err := t.ConvertToIndex(b)
	if err != nil {
		return 0, err
	}

	if ai < bi {
		return -1, nil
	} else if ai > bi {
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
		if str, ok := t.At(value); ok {
			return str, nil
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
	case string:
		if index := t.IndexOf(value); index != -1 {
			realStr, _ := t.At(index)
			return realStr, nil
		}
		return nil, ErrConvertingToEnum.New(`"` + value + `"`)
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

// ConvertToIndex is similar to Convert, except that it converts to the index rather than the value.
// Returns an error on nil.
func (t enumType) ConvertToIndex(v interface{}) (int, error) {
	switch value := v.(type) {
	case int:
		if _, ok := t.At(value); ok {
			return value, nil
		}
	case uint:
		return t.ConvertToIndex(int(value))
	case int8:
		return t.ConvertToIndex(int(value))
	case uint8:
		return t.ConvertToIndex(int(value))
	case int16:
		return t.ConvertToIndex(int(value))
	case uint16:
		return t.ConvertToIndex(int(value))
	case int32:
		return t.ConvertToIndex(int(value))
	case uint32:
		return t.ConvertToIndex(int(value))
	case int64:
		return t.ConvertToIndex(int(value))
	case uint64:
		return t.ConvertToIndex(int(value))
	case float32:
		return t.ConvertToIndex(int(value))
	case float64:
		return t.ConvertToIndex(int(value))
	case string:
		if index := t.IndexOf(value); index != -1 {
			return index, nil
		}
	case []byte:
		return t.ConvertToIndex(string(value))
	}

	return -1, ErrConvertingToEnum.New(v)
}

// Promote implements the Type interface.
func (t enumType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t enumType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Enum, []byte(value.(string))), nil
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

// Zero implements Type interface.
func (t enumType) Zero() interface{} {
	/// If an ENUM column is declared NOT NULL, its default value is the first element of the list of permitted values.
	return t.indexToVal[0]
}

// At returns the string at the given index, as well if the string was found.
func (t enumType) At(index int) (string, bool) {
	/// The elements listed in the column specification are assigned index numbers, beginning with 1.
	index -= 1
	if index < 0 || index >= len(t.indexToVal) {
		return "", false
	}
	return t.indexToVal[index], true
}

func (t enumType) CharacterSet() CharacterSet {
	return t.collation.CharacterSet()
}

func (t enumType) Collation() Collation {
	return t.collation
}

// IndexOf returns the index of the given string. If the string was not found, then this returns -1.
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

// Marshal takes a valid Enum value and returns it as an int64.
func (t enumType) Marshal(v interface{}) (int64, error) {
	i, err := t.ConvertToIndex(v)
	return int64(i), err
}

// NumberOfElements returns the number of enumerations.
func (t enumType) NumberOfElements() uint16 {
	return uint16(len(t.indexToVal))
}

// Unmarshal takes a previously-marshalled value and returns it as a string.
func (t enumType) Unmarshal(v int64) (string, error) {
	str, found := t.At(int(v))
	if !found {
		return "", ErrUnmarshallingEnum.New(v)
	}
	return str, nil
}

// Values returns the elements, in order, of every enumeration.
func (t enumType) Values() []string {
	vals := make([]string, len(t.indexToVal))
	copy(vals, t.indexToVal)
	return vals
}

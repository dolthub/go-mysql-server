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
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"
)

const (
	// EnumTypeMinElements returns the minimum number of enumerations for the Enum type.
	EnumTypeMinElements = 1
	// EnumTypeMaxElements returns the maximum number of enumerations for the Enum type.
	EnumTypeMaxElements = 65535
	// / An ENUM column can have a maximum of 65,535 distinct elements.
)

var (
	ErrConvertingToEnum = errors.NewKind("value %v is not valid for this Enum")

	enumValueType = reflect.TypeOf(uint16(0))
)

type EnumType struct {
	collation             sql.CollationID
	hashedValToIndex      map[uint64]int
	indexToVal            []string
	maxResponseByteLength uint32
}

var _ sql.EnumType = EnumType{}
var _ sql.TypeWithCollation = EnumType{}

// CreateEnumType creates a EnumType.
func CreateEnumType(values []string, collation sql.CollationID) (sql.EnumType, error) {
	if len(values) < EnumTypeMinElements {
		return nil, fmt.Errorf("number of values may not be zero")
	}
	if len(values) > EnumTypeMaxElements {
		return nil, fmt.Errorf("number of values is too large")
	}

	// maxResponseByteLength for an enum type is the bytes required to send back the largest enum value,
	// including accounting for multibyte character representations.
	var maxResponseByteLength uint32
	maxCharLength := collation.Collation().CharacterSet.MaxLength()
	valToIndex := make(map[uint64]int)
	for i, value := range values {
		if !collation.Equals(sql.Collation_binary) {
			// Trailing spaces are automatically deleted from ENUM member values in the table definition when a table
			// is created, unless the binary charset and collation is in use
			value = strings.TrimRight(value, " ")
		}
		values[i] = value
		hashedVal, err := collation.HashToUint(value)
		if err != nil {
			return nil, err
		}
		if _, ok := valToIndex[hashedVal]; ok {
			return nil, fmt.Errorf("duplicate entry: %v", value)
		}
		// The elements listed in the column specification are assigned index numbers, beginning with 1.
		valToIndex[hashedVal] = i + 1

		byteLength := uint32(utf8.RuneCountInString(value) * int(maxCharLength))
		if byteLength > maxResponseByteLength {
			maxResponseByteLength = byteLength
		}
	}
	return EnumType{
		collation:             collation,
		hashedValToIndex:      valToIndex,
		indexToVal:            values,
		maxResponseByteLength: maxResponseByteLength,
	}, nil
}

// MustCreateEnumType is the same as CreateEnumType except it panics on errors.
func MustCreateEnumType(values []string, collation sql.CollationID) sql.EnumType {
	et, err := CreateEnumType(values, collation)
	if err != nil {
		panic(err)
	}
	return et
}

// MaxTextResponseByteLength implements the Type interface
func (t EnumType) MaxTextResponseByteLength() uint32 {
	return t.maxResponseByteLength
}

// Compare implements Type interface.
func (t EnumType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	// Attempt to convert the values to their enum values, but don't error
	// out if they aren't valid enum values.
	ai, err := t.Convert(a)
	if err != nil && !ErrConvertingToEnum.Is(err) {
		return 0, err
	}
	bi, err := t.Convert(b)
	if err != nil && !ErrConvertingToEnum.Is(err) {
		return 0, err
	}

	if ai == nil && bi == nil {
		return 0, nil
	} else if ai == nil {
		return -1, nil
	} else if bi == nil {
		return 1, nil
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
func (t EnumType) Convert(v interface{}) (interface{}, error) {
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
func (t EnumType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t EnumType) Equals(otherType sql.Type) bool {
	if ot, ok := otherType.(EnumType); ok && t.collation.Equals(ot.collation) && len(t.indexToVal) == len(ot.indexToVal) {
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
func (t EnumType) Promote() sql.Type {
	return t
}

// SQL implements Type interface.
func (t EnumType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	convertedValue, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	value, _ := t.At(int(convertedValue.(uint16)))

	resultCharset := ctx.GetCharacterSetResults()
	if resultCharset == sql.CharacterSet_Unspecified || resultCharset == sql.CharacterSet_binary {
		resultCharset = t.collation.CharacterSet()
	}
	encodedBytes, ok := resultCharset.Encoder().Encode(encodings.StringToBytes(value))
	if !ok {
		return sqltypes.Value{}, sql.ErrCharSetFailedToEncode.New(t.collation.CharacterSet().Name())
	}
	val := AppendAndSliceBytes(dest, encodedBytes)

	return sqltypes.MakeTrusted(sqltypes.Enum, val), nil
}

// String implements Type interface.
func (t EnumType) String() string {
	s := fmt.Sprintf("enum('%v')", strings.Join(t.indexToVal, `','`))
	if t.CharacterSet() != sql.Collation_Default.CharacterSet() {
		s += " CHARACTER SET " + t.CharacterSet().String()
	}
	if !t.collation.Equals(sql.Collation_Default) {
		s += " COLLATE " + t.collation.String()
	}
	return s
}

// Type implements Type interface.
func (t EnumType) Type() query.Type {
	return sqltypes.Enum
}

// ValueType implements Type interface.
func (t EnumType) ValueType() reflect.Type {
	return enumValueType
}

// Zero implements Type interface.
func (t EnumType) Zero() interface{} {
	// / If an ENUM column is declared NOT NULL, its default value is the first element of the list of permitted values.
	return uint16(1)
}

// At implements EnumType interface.
func (t EnumType) At(index int) (string, bool) {
	// / The elements listed in the column specification are assigned index numbers, beginning with 1.
	index -= 1
	if index < 0 || index >= len(t.indexToVal) {
		return "", false
	}
	return t.indexToVal[index], true
}

// CharacterSet implements EnumType interface.
func (t EnumType) CharacterSet() sql.CharacterSetID {
	return t.collation.CharacterSet()
}

// Collation implements EnumType interface.
func (t EnumType) Collation() sql.CollationID {
	return t.collation
}

// IndexOf implements EnumType interface.
func (t EnumType) IndexOf(v string) int {
	hashedVal, err := t.collation.HashToUint(v)
	if err == nil {
		if index, ok := t.hashedValToIndex[hashedVal]; ok {
			return index
		}
	}
	// / ENUM('0','1','2')
	// / If you store '3', it does not match any enumeration value, so it is treated as an index and becomes '2' (the value with index 3).
	if parsedIndex, err := strconv.ParseInt(v, 10, 32); err == nil {
		if _, ok := t.At(int(parsedIndex)); ok {
			return int(parsedIndex)
		}
	}
	return -1
}

// NumberOfElements implements EnumType interface.
func (t EnumType) NumberOfElements() uint16 {
	return uint16(len(t.indexToVal))
}

// Values implements EnumType interface.
func (t EnumType) Values() []string {
	vals := make([]string, len(t.indexToVal))
	copy(vals, t.indexToVal)
	return vals
}

// WithNewCollation implements TypeWithCollation interface.
func (t EnumType) WithNewCollation(collation sql.CollationID) (sql.Type, error) {
	return CreateEnumType(t.indexToVal, collation)
}

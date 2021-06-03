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
	"math"
	"math/bits"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"
)

const (
	// SetTypeMaxElements returns the maximum number of elements for the Set type.
	SetTypeMaxElements = 64
)

var (
	ErrConvertingToSet   = errors.NewKind("value %v is not valid for this set")
	ErrDuplicateEntrySet = errors.NewKind("duplicate entry: %v")
	ErrInvalidSetValue   = errors.NewKind("value %v was not found in the set")
	ErrTooLargeForSet    = errors.NewKind(`value "%v" is too large for this set`)
)

// Comments with three slashes were taken directly from the linked documentation.

// Represents the SET type.
// https://dev.mysql.com/doc/refman/8.0/en/set.html
type SetType interface {
	Type
	CharacterSet() CharacterSet
	Collation() Collation
	//TODO: move this out of go-mysql-server and into the Dolt layer
	Marshal(v interface{}) (uint64, error)
	NumberOfElements() uint16
	Unmarshal(bits uint64) (string, error)
	Values() []string
}

type setType struct {
	collation         Collation
	compareToOriginal map[string]string
	valToBit          map[string]uint64
	bitToVal          map[uint64]string
}

// CreateSetType creates a SetType.
func CreateSetType(values []string, collation Collation) (SetType, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("number of values may not be zero")
	}
	/// A SET column can have a maximum of 64 distinct members.
	if len(values) > SetTypeMaxElements {
		return nil, fmt.Errorf("number of values is too large")
	}
	compareToOriginal := make(map[string]string)
	valToBit := make(map[string]uint64)
	bitToVal := make(map[uint64]string)
	for i, value := range values {
		/// ...SET member values should not themselves contain commas.
		if strings.Contains(value, ",") {
			return nil, fmt.Errorf("values cannot contain a comma")
		}
		/// For binary or case-sensitive collations, lettercase is taken into account when assigning values to the column.
		//TODO: add the other case-sensitive collations
		switch collation.Name {
		case Collation_binary.Name:
			if _, ok := compareToOriginal[value]; ok {
				return nil, ErrDuplicateEntrySet.New(value)
			}
			compareToOriginal[value] = value
		default:
			/// Trailing spaces are automatically deleted from SET member values in the table definition when a table is created.
			value = strings.TrimRight(value, " ")
			lowercaseValue := strings.ToLower(value)
			if _, ok := compareToOriginal[lowercaseValue]; ok {
				return nil, ErrDuplicateEntrySet.New(value)
			}
			compareToOriginal[lowercaseValue] = value
		}
		bit := uint64(1 << uint64(i))
		valToBit[value] = bit
		bitToVal[bit] = value
	}
	return setType{
		collation:         collation,
		compareToOriginal: compareToOriginal,
		valToBit:          valToBit,
		bitToVal:          bitToVal,
	}, nil
}

// MustCreateSetType is the same as CreateSetType except it panics on errors.
func MustCreateSetType(values []string, collation Collation) SetType {
	et, err := CreateSetType(values, collation)
	if err != nil {
		panic(err)
	}
	return et
}

// Compare implements Type interface.
func (t setType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	ai, err := t.Marshal(a)
	if err != nil {
		return 0, err
	}
	bi, err := t.Marshal(b)
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
// Returns the string representing the given value if applicable.
func (t setType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	switch value := v.(type) {
	case int:
		return t.Convert(uint64(value))
	case uint:
		return t.Convert(uint64(value))
	case int8:
		return t.Convert(uint64(value))
	case uint8:
		return t.Convert(uint64(value))
	case int16:
		return t.Convert(uint64(value))
	case uint16:
		return t.Convert(uint64(value))
	case int32:
		return t.Convert(uint64(value))
	case uint32:
		return t.Convert(uint64(value))
	case int64:
		return t.Convert(uint64(value))
	case uint64:
		if value <= t.allValuesBitField() {
			return t.convertBitFieldToString(value)
		}
		return nil, ErrConvertingToSet.New(v)
	case float32:
		return t.Convert(uint64(value))
	case float64:
		return t.Convert(uint64(value))
	case string:
		// For SET('a','b') and given a string 'b,a,a', we would return 'a,b', so we can't return the input.
		bitField, err := t.convertStringToBitField(value)
		if err != nil {
			return nil, err
		}
		setStr, _ := t.convertBitFieldToString(bitField)
		return setStr, nil
	case []byte:
		return t.Convert(string(value))
	}

	return nil, ErrConvertingToSet.New(v)
}

// MustConvert implements the Type interface.
func (t setType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Promote implements the Type interface.
func (t setType) Promote() Type {
	return t
}

// SQL implements Type interface.
func (t setType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	value, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return sqltypes.MakeTrusted(sqltypes.Set, []byte(value.(string))), nil
}

// String implements Type interface.
func (t setType) String() string {
	s := fmt.Sprintf("SET('%v')", strings.Join(t.Values(), `','`))
	if t.CharacterSet() != Collation_Default.CharacterSet() {
		s += " CHARACTER SET " + t.CharacterSet().String()
	}
	if !t.collation.Equals(Collation_Default) {
		s += " COLLATE " + t.collation.String()
	}
	return s
}

// Type implements Type interface.
func (t setType) Type() query.Type {
	return sqltypes.Set
}

// Zero implements Type interface.
func (t setType) Zero() interface{} {
	return ""
}

func (t setType) CharacterSet() CharacterSet {
	return t.collation.CharacterSet()
}

func (t setType) Collation() Collation {
	return t.collation
}

// Marshal takes a valid Set value and returns it as an uint64.
func (t setType) Marshal(v interface{}) (uint64, error) {
	switch value := v.(type) {
	case int:
		return t.Marshal(uint64(value))
	case uint:
		return t.Marshal(uint64(value))
	case int8:
		return t.Marshal(uint64(value))
	case uint8:
		return t.Marshal(uint64(value))
	case int16:
		return t.Marshal(uint64(value))
	case uint16:
		return t.Marshal(uint64(value))
	case int32:
		return t.Marshal(uint64(value))
	case uint32:
		return t.Marshal(uint64(value))
	case int64:
		return t.Marshal(uint64(value))
	case uint64:
		if value <= t.allValuesBitField() {
			return value, nil
		}
	case float32:
		return t.Marshal(uint64(value))
	case float64:
		return t.Marshal(uint64(value))
	case string:
		return t.convertStringToBitField(value)
	case []byte:
		return t.Marshal(string(value))
	}

	return uint64(0), ErrConvertingToSet.New(v)
}

// NumberOfElements returns the number of elements in this set.
func (t setType) NumberOfElements() uint16 {
	return uint16(len(t.valToBit))
}

// Unmarshal takes a previously-marshalled value and returns it as a string.
func (t setType) Unmarshal(v uint64) (string, error) {
	return t.convertBitFieldToString(v)
}

// Values returns all of the set's values in ascending order according to their corresponding bit value.
func (t setType) Values() []string {
	bitEdge := 64 - bits.LeadingZeros64(t.allValuesBitField())
	valArray := make([]string, bitEdge)
	for i := 0; i < bitEdge; i++ {
		bit := uint64(1 << uint64(i))
		valArray[i] = t.bitToVal[bit]
	}
	return valArray
}

// allValuesBitField returns a bit field that references every value that the set contains.
func (t setType) allValuesBitField() uint64 {
	valCount := uint64(len(t.valToBit))
	if valCount == 64 {
		return math.MaxUint64
	}
	// A set with 3 values will have an upper bound of 8, or 0b1000.
	// 8 - 1 == 7, and 7 is 0b0111, which would map to every value in the set.
	return uint64(1<<valCount) - 1
}

// convertBitFieldToString converts the given bit field into the equivalent comma-delimited string.
func (t setType) convertBitFieldToString(bitField uint64) (string, error) {
	strBuilder := strings.Builder{}
	bitEdge := 64 - bits.LeadingZeros64(bitField)
	writeCommas := false
	if bitEdge > len(t.bitToVal) {
		return "", ErrTooLargeForSet.New(bitField)
	}
	for i := 0; i < bitEdge; i++ {
		bit := uint64(1 << uint64(i))
		if bit&bitField != 0 {
			val, ok := t.bitToVal[bit]
			if !ok {
				return "", ErrInvalidSetValue.New(bitField)
			}
			if writeCommas {
				strBuilder.WriteByte(',')
			} else {
				writeCommas = true
			}
			strBuilder.WriteString(val)
		}
	}
	return strBuilder.String(), nil
}

// convertStringToBitField converts the given string into a bit field.
func (t setType) convertStringToBitField(str string) (uint64, error) {
	if str == "" {
		return 0, nil
	}
	var bitField uint64
	vals := strings.Split(str, ",")
	for _, val := range vals {
		var compareVal string
		switch t.collation.Name {
		case Collation_binary.Name:
			compareVal = val
		default:
			compareVal = strings.ToLower(strings.TrimRight(val, " "))
		}
		if originalVal, ok := t.compareToOriginal[compareVal]; ok {
			bitField |= t.valToBit[originalVal]
		} else {
			asUint, err := strconv.ParseUint(val, 10, 64)
			if err == nil {
				if asUint == 0 {
					continue
				}
				if _, ok := t.bitToVal[asUint]; ok {
					bitField |= asUint
					continue
				}
			}
			return 0, ErrInvalidSetValue.New(val)
		}
	}
	return bitField, nil
}

package sql

import (
	"fmt"
	"math/bits"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

const (
	// SetTypeMaxElements returns the maximum number of elements for the Set type.
	SetTypeMaxElements = 64
)

var (
	ErrConvertingToSet = errors.NewKind("value %v is not valid for this Set")
	ErrDuplicateEntrySet = errors.NewKind("duplicate entry: %v")
	ErrInvalidSetValue = errors.NewKind("value %v was not found in the set")
)

// Comments with three slashes were taken directly from the linked documentation.

// Represents the SET type.
// https://dev.mysql.com/doc/refman/8.0/en/set.html
type SetType interface {
	Type
	CharacterSet() CharacterSet
	Collation() Collation
	NumberOfElements() uint16
	Values() []string
}

type setType struct{
	collation Collation
	compareToOriginal map[string]string
	valToBit map[string]uint64
	bitToVal map[uint64]string
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
		switch collation {
		case Collation_binary:
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
		collation: collation,
		compareToOriginal: compareToOriginal,
		valToBit: valToBit,
		bitToVal: bitToVal,
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

	ai, err := t.ConvertToBits(a)
	if err != nil {
		return 0, err
	}
	bi, err := t.ConvertToBits(b)
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
		if value >= t.upperBound() {
			return nil, ErrConvertingToSet.New(v)
		}
		vals, _ := t.convertBitFieldToString(value)
		return vals, nil
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

// ConvertToBits is similar to Convert, except that it converts to the number representing the bits rather than the string.
// Returns an error on nil.
func (t setType) ConvertToBits(v interface{}) (uint64, error) {
	switch value := v.(type) {
	case int:
		return t.ConvertToBits(uint64(value))
	case uint:
		return t.ConvertToBits(uint64(value))
	case int8:
		return t.ConvertToBits(uint64(value))
	case uint8:
		return t.ConvertToBits(uint64(value))
	case int16:
		return t.ConvertToBits(uint64(value))
	case uint16:
		return t.ConvertToBits(uint64(value))
	case int32:
		return t.ConvertToBits(uint64(value))
	case uint32:
		return t.ConvertToBits(uint64(value))
	case int64:
		return t.ConvertToBits(uint64(value))
	case uint64:
		if value < t.upperBound() {
			return value, nil
		}
	case float32:
		return t.ConvertToBits(uint64(value))
	case float64:
		return t.ConvertToBits(uint64(value))
	case string:
		bitField, err := t.convertStringToBitField(value)
		if err != nil {
			return uint64(0), err
		}
		return bitField, nil
	case []byte:
		return t.ConvertToBits(string(value))
	}

	return uint64(0), ErrConvertingToSet.New(v)
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
	s := fmt.Sprintf("SET('%v')", strings.Join(t.Values(), ","))
	if t.CharacterSet() != Collation_Default.CharacterSet() {
		s += " CHARACTER SET " + t.CharacterSet().String()
	}
	if t.collation != Collation_Default {
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

// NumberOfElements returns the number of elements in this set.
func (t setType) NumberOfElements() uint16 {
	return uint16(len(t.valToBit))
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
	// A set with 3 values will have an upper bound of 8, or 0b1000.
	// 8 - 1 == 7, and 7 is 0b0111, which would map to every value in the set.
	return t.upperBound() - 1
}

// convertBitFieldToString converts the given bit field into the equivalent comma-delimited string.
func (t setType) convertBitFieldToString(bitField uint64) (string, error) {
	strBuilder := strings.Builder{}
	bitEdge := 64 - bits.LeadingZeros64(bitField)
	writeCommas := false
	for i := 0; i < bitEdge; i++ {
		bit := uint64(1 << uint64(i))
		if bit & bitField != 0 {
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
		switch t.collation {
		case Collation_binary:
			compareVal = val
		default:
			compareVal = strings.ToLower(strings.TrimRight(val, " "))
		}
		if originalVal, ok := t.compareToOriginal[compareVal]; ok {
			bitField |= t.valToBit[originalVal]
		} else {
			return 0, ErrInvalidSetValue.New(val)
		}
	}
	return bitField, nil
}

// upperBound returns the exclusive upper bound for valid numbers representing a bit collection.
func (t setType) upperBound() uint64 {
	return uint64(1 << uint64(len(t.valToBit)))
}
package sql

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
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
	ConvertBitArrayToString(bitArray []uint64) (string, error)
	ConvertStringToBitArray(str string) ([]uint64, error)
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
	if len(values) > 64 {
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
		bitArray := t.decomposeToBitArray(value)
		vals, _ := t.ConvertBitArrayToString(bitArray)
		return vals, nil
	case float32:
		return t.Convert(uint64(value))
	case float64:
		return t.Convert(uint64(value))
	case string:
		// For SET('a','b') and given a string 'b,a,a', we would return 'a,b', so we can't return the input.
		bitArray, err := t.ConvertStringToBitArray(value)
		if err != nil {
			return nil, err
		}
		setStr, _ := t.ConvertBitArrayToString(bitArray)
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
		bitArray, err := t.ConvertStringToBitArray(value)
		if err != nil {
			return uint64(0), err
		}
		return t.sumBitArray(bitArray), nil
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

// ConvertBitArrayToString converts the given bit array into the equivalent comma-delimited string.
// The given bit array must have a form where representing the value 12 would have values {4,8}.
func (t setType) ConvertBitArrayToString(bitArray []uint64) (string, error) {
	strBuilder := strings.Builder{}
	for i, bit := range bitArray {
		val, ok := t.bitToVal[bit]
		if !ok {
			return "", ErrInvalidSetValue.New(t.sumBitArray(bitArray))
		}
		if i != 0 {
			strBuilder.WriteByte(',')
		}
		strBuilder.WriteString(val)
	}
	return strBuilder.String(), nil
}

// ConvertStringToBitArray converts the given string into a bit array.
// Ignores duplicate set values and always returns the bits in ascending order.
// The returned bit array representing the value 12 will have values {4,8}.
func (t setType) ConvertStringToBitArray(str string) ([]uint64, error) {
	if str == "" {
		return nil, nil
	}
	var bitArray []uint64
	seen := make(map[string]struct{})
	vals := strings.Split(str, ",")
	for _, val := range vals {
		if _, ok := seen[val]; ok {
			continue
		}
		seen[val] = struct{}{}
		var compareVal string
		switch t.collation {
		case Collation_binary:
			compareVal = val
		default:
			compareVal = strings.ToLower(strings.TrimRight(val, " "))
		}
		if originalVal, ok := t.compareToOriginal[compareVal]; ok {
			bitArray = append(bitArray, t.valToBit[originalVal])
		} else {
			return nil, ErrInvalidSetValue.New(val)
		}
	}
	sort.Slice(bitArray, func(i, j int) bool {
		return bitArray[i] < bitArray[j]
	})
	return bitArray, nil
}

// Values returns all of the set's values in ascending order according to their corresponding bit value.
func (t setType) Values() []string {
	// A set with 3 values will have an upper bound of 8, or 0b1000.
	// 8 - 1 == 7, and 7 is 0b0111, which would map to every value in the set.
	bitArray := t.decomposeToBitArray(t.upperBound() - 1)
	valArray := make([]string, len(bitArray))
	for i, bit := range bitArray {
		valArray[i] = t.bitToVal[bit]
	}
	return valArray
}

// decomposeToBitArray returns an array of all of the bits that were set to 1.
// For example, 12 will return {4,8} for 0b1100.
func (t setType) decomposeToBitArray(num uint64) []uint64 {
	bitEdge := 64 - bits.LeadingZeros64(num)
	var bitArray []uint64
	for i := 0; i < bitEdge; i++ {
		bit := uint64(1 << uint64(i))
		if num & bit != 0 {
			bitArray = append(bitArray, bit)
		}
	}
	return bitArray
}

// sumBitArray returns an unsigned integer representing the bit array.
// The given bit array is expected to have a form where representing the value 12 would have values {4,8}.
func (t setType) sumBitArray(bitArray []uint64) uint64 {
	sum := uint64(0)
	for _, bit := range bitArray {
		sum += bit
	}
	return sum
}

// upperBound returns the exclusive upper bound for valid numbers representing a bit collection.
func (t setType) upperBound() uint64 {
	return uint64(1 << uint64(len(t.valToBit)))
}
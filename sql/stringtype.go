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
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	istrings "github.com/dolthub/go-mysql-server/internal/strings"
	"github.com/dolthub/go-mysql-server/sql/encodings"
)

const (
	charBinaryMax       = 255
	varcharVarbinaryMax = 65_535

	tinyTextBlobMax   = charBinaryMax
	textBlobMax       = varcharVarbinaryMax
	mediumTextBlobMax = 16_777_215
	longTextBlobMax   = int64(4_294_967_295)
)

var (
	// ErrLengthTooLarge is thrown when a string's length is too large given the other parameters.
	ErrLengthTooLarge    = errors.NewKind("length is %v but max allowed is %v")
	ErrLengthBeyondLimit = errors.NewKind("string '%v' is too large for column '%v'")
	ErrBinaryCollation   = errors.NewKind("binary types must have the binary collation")

	TinyText   = MustCreateStringWithDefaults(sqltypes.Text, tinyTextBlobMax)
	Text       = MustCreateStringWithDefaults(sqltypes.Text, textBlobMax)
	MediumText = MustCreateStringWithDefaults(sqltypes.Text, mediumTextBlobMax)
	LongText   = MustCreateStringWithDefaults(sqltypes.Text, longTextBlobMax)
	TinyBlob   = MustCreateBinary(sqltypes.Blob, tinyTextBlobMax)
	Blob       = MustCreateBinary(sqltypes.Blob, textBlobMax)
	MediumBlob = MustCreateBinary(sqltypes.Blob, mediumTextBlobMax)
	LongBlob   = MustCreateBinary(sqltypes.Blob, longTextBlobMax)

	stringValueType = reflect.TypeOf(string(""))
	byteValueType   = reflect.TypeOf(([]byte)(nil))
)

// StringType represents all string types, including VARCHAR and BLOB.
// https://dev.mysql.com/doc/refman/8.0/en/char.html
// https://dev.mysql.com/doc/refman/8.0/en/binary-varbinary.html
// https://dev.mysql.com/doc/refman/8.0/en/blob.html
// The type of the returned value is string.
type StringType interface {
	Type
	CharacterSet() CharacterSetID
	Collation() CollationID
	// MaxCharacterLength returns the maximum number of chars that can safely be stored in this type, based on
	// the current character set.
	MaxCharacterLength() int64
	// MaxByteLength returns the maximum number of bytes that may be consumed by a value stored in this type.
	MaxByteLength() int64
	// Length returns the maximum length, in characters, allowed for this string type.
	Length() int64
}

type stringType struct {
	baseType              query.Type
	maxCharLength         int64
	maxByteLength         int64
	maxResponseByteLength uint32
	collation             CollationID
}

var _ StringType = stringType{}
var _ TypeWithCollation = stringType{}

// CreateString creates a new StringType based on the specified type, length, and collation. Length is interpreted as
// the length of bytes in the new StringType for SQL types that are based on bytes (i.e. TEXT, BLOB, BINARY, and
// VARBINARY). For all other char-based SQL types, length is interpreted as the length of chars in the new
// StringType (i.e. CHAR, and VARCHAR).
func CreateString(baseType query.Type, length int64, collation CollationID) (StringType, error) {
	//TODO: remove character set and collation validity checks once all collations have been implemented (delete errors as well)
	if collation.CharacterSet().Encoder() == nil {
		return nil, ErrCharSetNotYetImplementedTemp.New(collation.CharacterSet().Name())
	} else if collation.Sorter() == nil {
		return nil, ErrCollationNotYetImplementedTemp.New(collation.Name())
	}

	// Check the base type first and fail immediately if it's unknown
	switch baseType {
	case sqltypes.Char, sqltypes.Binary, sqltypes.VarChar, sqltypes.VarBinary, sqltypes.Text, sqltypes.Blob:
	default:
		return nil, ErrInvalidBaseType.New(baseType.String(), "string")
	}

	// We accept a length of zero, but a negative length is not valid
	if length < 0 {
		return nil, fmt.Errorf("length of %v is less than the minimum of 0", length)
	}

	switch baseType {
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		if collation != Collation_binary {
			return nil, ErrBinaryCollation.New(collation.Name, Collation_binary)
		}
	}

	// If the CharacterSet is binary, then we convert the type to the binary equivalent
	if collation.Equals(Collation_binary) {
		switch baseType {
		case sqltypes.Char:
			baseType = sqltypes.Binary
		case sqltypes.VarChar:
			baseType = sqltypes.VarBinary
		case sqltypes.Text:
			baseType = sqltypes.Blob
		}
	}

	// Determine the max byte length and max char length based on whether the base type is byte-based or char-based
	charsetMaxLength := collation.CharacterSet().MaxLength()
	maxCharLength := length
	maxByteLength := length
	switch baseType {
	case sqltypes.Char, sqltypes.VarChar:
		maxByteLength = length * charsetMaxLength
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Text, sqltypes.Blob:
		maxCharLength = length / charsetMaxLength
	}
	maxResponseByteLength := maxByteLength

	// Make sure that length is valid depending on the base type, since they each handle lengths differently
	switch baseType {
	case sqltypes.Char:
		if maxCharLength > charBinaryMax {
			return nil, ErrLengthTooLarge.New(length, charBinaryMax)
		}
	case sqltypes.VarChar:
		if maxCharLength > varcharVarbinaryMax {
			return nil, ErrLengthTooLarge.New(length, varcharVarbinaryMax/charsetMaxLength)
		}
	case sqltypes.Binary:
		if maxByteLength > charBinaryMax {
			return nil, ErrLengthTooLarge.New(length, charBinaryMax)
		}
	case sqltypes.VarBinary:
		// VarBinary fields transmitted over the wire could be for a VarBinary field,
		// or a JSON field, so we validate against JSON's larger limit (1GB)
		// instead of VarBinary's smaller limit (65k).
		if maxByteLength > MaxJsonFieldByteLength {
			return nil, ErrLengthTooLarge.New(length, MaxJsonFieldByteLength/charsetMaxLength)
		}
	case sqltypes.Text, sqltypes.Blob:
		if maxByteLength > longTextBlobMax {
			return nil, ErrLengthTooLarge.New(length, longTextBlobMax)
		}
		if maxByteLength <= tinyTextBlobMax {
			maxByteLength = tinyTextBlobMax
			maxCharLength = tinyTextBlobMax / charsetMaxLength
		} else if maxByteLength <= textBlobMax {
			maxByteLength = textBlobMax
			maxCharLength = textBlobMax / charsetMaxLength
		} else if maxByteLength <= mediumTextBlobMax {
			maxByteLength = mediumTextBlobMax
			maxCharLength = mediumTextBlobMax / charsetMaxLength
		} else {
			maxByteLength = longTextBlobMax
			maxCharLength = longTextBlobMax / charsetMaxLength
		}

		maxResponseByteLength = maxByteLength
		if baseType == sqltypes.Text && maxByteLength != longTextBlobMax {
			// For TEXT types, MySQL returns the maxByteLength multiplied by the size of the largest
			// multibyte character in the associated charset for the maximum field bytes in the response
			// metadata. It seems like returning the maxByteLength would be sufficient, but we do this to
			// emulate MySQL's behavior exactly.
			// The one exception is LongText types, which cannot be multiplied by a multibyte char multiplier,
			// since the max bytes field in a column definition response over the wire is a uint32 and multiplying
			// longTextBlobMax by anything over 1 would cause it to overflow.
			maxResponseByteLength = maxByteLength * charsetMaxLength
		}
	}

	if maxResponseByteLength > math.MaxUint32 {
		return nil, fmt.Errorf("length of %v is greater than the maximum of uint32", maxResponseByteLength)
	}

	return stringType{baseType, maxCharLength, maxByteLength, uint32(maxResponseByteLength), collation}, nil
}

// MustCreateString is the same as CreateString except it panics on errors.
func MustCreateString(baseType query.Type, length int64, collation CollationID) StringType {
	st, err := CreateString(baseType, length, collation)
	if err != nil {
		panic(err)
	}
	return st
}

// CreateStringWithDefaults creates a StringType with the default character set and collation of the given size.
func CreateStringWithDefaults(baseType query.Type, length int64) (StringType, error) {
	return CreateString(baseType, length, Collation_Default)
}

// MustCreateStringWithDefaults creates a StringType with the default CharacterSet and Collation.
func MustCreateStringWithDefaults(baseType query.Type, length int64) StringType {
	return MustCreateString(baseType, length, Collation_Default)
}

// CreateBinary creates a StringType with a binary collation and character set of the given size.
func CreateBinary(baseType query.Type, lengthHint int64) (StringType, error) {
	return CreateString(baseType, lengthHint, Collation_binary)
}

// MustCreateBinary is the same as CreateBinary except it panics on errors.
func MustCreateBinary(baseType query.Type, lengthHint int64) StringType {
	return MustCreateString(baseType, lengthHint, Collation_binary)
}

// CreateTinyText creates a TINYTEXT with the given collation.
func CreateTinyText(collation CollationID) StringType {
	return MustCreateString(sqltypes.Text, tinyTextBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// CreateText creates a TEXT with the given collation.
func CreateText(collation CollationID) StringType {
	return MustCreateString(sqltypes.Text, textBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// CreateMediumText creates a MEDIUMTEXT with the given collation.
func CreateMediumText(collation CollationID) StringType {
	return MustCreateString(sqltypes.Text, mediumTextBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// CreateLongText creates a LONGTEXT with the given collation.
func CreateLongText(collation CollationID) StringType {
	return MustCreateString(sqltypes.Text, longTextBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// MaxTextResponseByteLength implements the Type interface
func (t stringType) MaxTextResponseByteLength() uint32 {
	return t.maxResponseByteLength
}

func (t stringType) Length() int64 {
	return t.maxCharLength
}

// Compare implements Type interface.
func (t stringType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := compareNulls(a, b); hasNulls {
		return res, nil
	}

	var as string
	var bs string
	var ok bool
	if as, ok = a.(string); !ok {
		ai, err := t.Convert(a)
		if err != nil {
			return 0, err
		}
		if IsBinaryType(t) {
			as = encodings.BytesToString(ai.([]byte))
		} else {
			as = ai.(string)
		}
	}
	if bs, ok = b.(string); !ok {
		bi, err := t.Convert(b)
		if err != nil {
			return 0, err
		}
		if IsBinaryType(t) {
			bs = encodings.BytesToString(bi.([]byte))
		} else {
			bs = bi.(string)
		}
	}

	encoder := t.collation.CharacterSet().Encoder()
	getRuneWeight := t.collation.Sorter()
	for len(as) > 0 && len(bs) > 0 {
		ar, aRead := encoder.NextRune(as)
		br, bRead := encoder.NextRune(bs)
		if aRead == 0 || bRead == 0 || aRead == utf8.RuneError || bRead == utf8.RuneError {
			//TODO: return a real error
			return 0, fmt.Errorf("malformed string encountered while comparing")
		}
		aWeight := getRuneWeight(ar)
		bWeight := getRuneWeight(br)
		if aWeight < bWeight {
			return -1, nil
		} else if aWeight > bWeight {
			return 1, nil
		}
		as = as[aRead:]
		bs = bs[bRead:]
	}

	// Strings are equal up to the compared length, so shorter strings sort before longer strings
	if len(as) < len(bs) {
		return -1, nil
	} else if len(as) > len(bs) {
		return 1, nil
	} else {
		return 0, nil
	}
}

// Convert implements Type interface.
func (t stringType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	val, err := ConvertToString(v, t)
	if err != nil {
		return nil, err
	}

	if IsBinaryType(t) {
		return []byte(val), nil
	}
	return val, nil
}

func ConvertToString(v interface{}, t StringType) (string, error) {
	var val string
	switch s := v.(type) {
	case bool:
		val = strconv.FormatBool(s)
	case float64:
		val = strconv.FormatFloat(s, 'f', -1, 64)
	case float32:
		val = strconv.FormatFloat(float64(s), 'f', -1, 32)
	case int:
		val = strconv.FormatInt(int64(s), 10)
	case int8:
		val = strconv.FormatInt(int64(s), 10)
	case int16:
		val = strconv.FormatInt(int64(s), 10)
	case int32:
		val = strconv.FormatInt(int64(s), 10)
	case int64:
		val = strconv.FormatInt(s, 10)
	case uint:
		val = strconv.FormatUint(uint64(s), 10)
	case uint8:
		val = strconv.FormatUint(uint64(s), 10)
	case uint16:
		val = strconv.FormatUint(uint64(s), 10)
	case uint32:
		val = strconv.FormatUint(uint64(s), 10)
	case uint64:
		val = strconv.FormatUint(s, 10)
	case string:
		val = s
	case []byte:
		val = string(s)
	case time.Time:
		val = s.Format(TimestampDatetimeLayout)
	case decimal.Decimal:
		val = s.StringFixed(s.Exponent() * -1)
	case decimal.NullDecimal:
		if !s.Valid {
			return "", nil
		}
		val = s.Decimal.String()
	case JSONValue:
		str, err := s.ToString(nil)
		if err != nil {
			return "", err
		}

		val, err = istrings.Unquote(str)
		if err != nil {
			return "", err
		}
	case GeometryValue:
		return string(s.Serialize()), nil
	default:
		return "", ErrConvertToSQL.New(t)
	}

	s := t.(stringType)
	if s.baseType == sqltypes.Text {
		// for TEXT types, we use the byte length instead of the character length
		if int64(len(val)) > s.maxByteLength {
			return "", ErrLengthBeyondLimit.New(val, t.String())
		}
	} else {
		if t.CharacterSet().MaxLength() == 1 {
			// if the character set only has a max size of 1, we can just count the bytes
			if int64(len(val)) > s.maxCharLength {
				return "", ErrLengthBeyondLimit.New(val, t.String())
			}
		} else {
			//TODO: this should count the string's length properly according to the character set
			//convert 'val' string to rune to count the character length, not byte length
			if int64(len([]rune(val))) > s.maxCharLength {
				return "", ErrLengthBeyondLimit.New(val, t.String())
			}
		}
	}

	if s.baseType == sqltypes.Binary {
		val += strings.Repeat(string([]byte{0}), int(s.maxCharLength)-len(val))
	}

	return val, nil
}

// ConvertToCollatedString returns the given interface as a string, along with its collation. If the Type possess a
// collation, then that collation is returned. If the Type does not possess a collation (such as an integer), then the
// value is converted to a string and the default collation is used. If the value is already a string then no additional
// conversions are made. If the value is a byte slice then a non-copying conversion is made, which means that the
// original byte slice MUST NOT be modified after being passed to this function. If modifications need to be made, then
// you must allocate a new byte slice and pass that new one in.
func ConvertToCollatedString(val interface{}, typ Type) (string, CollationID, error) {
	var content string
	var collation CollationID
	var err error
	if typeWithCollation, ok := typ.(TypeWithCollation); ok {
		collation = typeWithCollation.Collation()
		if strVal, ok := val.(string); ok {
			content = strVal
		} else if byteVal, ok := val.([]byte); ok {
			content = encodings.BytesToString(byteVal)
		} else {
			val, err = LongText.Convert(val)
			if err != nil {
				return "", Collation_Unspecified, err
			}
			content = val.(string)
		}
	} else {
		collation = Collation_Default
		val, err = LongText.Convert(val)
		if err != nil {
			return "", Collation_Unspecified, err
		}
		content = val.(string)
	}
	return content, collation, nil
}

// MustConvert implements the Type interface.
func (t stringType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t stringType) Equals(otherType Type) bool {
	if ot, ok := otherType.(stringType); ok {
		return t.baseType == ot.baseType && t.collation == ot.collation && t.maxCharLength == ot.maxCharLength
	}
	return false
}

// Promote implements the Type interface.
func (t stringType) Promote() Type {
	switch t.baseType {
	case sqltypes.Char, sqltypes.VarChar, sqltypes.Text:
		return MustCreateString(sqltypes.Text, longTextBlobMax, t.collation)
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		return LongBlob
	default:
		panic(ErrInvalidBaseType.New(t.baseType.String(), "string"))
	}
}

// SQL implements Type interface.
func (t stringType) SQL(ctx *Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	var val []byte
	if IsBinaryType(t) {
		v, err := t.Convert(v)
		if err != nil {
			return sqltypes.Value{}, err
		}
		val = appendAndSliceBytes(dest, v.([]byte))
	} else {
		v, err := ConvertToString(v, t)
		if err != nil {
			return sqltypes.Value{}, err
		}
		resultCharset := ctx.GetCharacterSetResults()
		if resultCharset == CharacterSet_Unspecified || resultCharset == CharacterSet_binary {
			resultCharset = t.collation.CharacterSet()
		}
		encodedBytes, ok := resultCharset.Encoder().Encode(encodings.StringToBytes(v))
		if !ok {
			return sqltypes.Value{}, ErrCharSetFailedToEncode.New(t.collation.CharacterSet().Name())
		}
		val = appendAndSliceBytes(dest, encodedBytes)
	}

	return sqltypes.MakeTrusted(t.baseType, val), nil
}

// String implements Type interface.
func (t stringType) String() string {
	var s string

	switch t.baseType {
	case sqltypes.Char:
		s = fmt.Sprintf("char(%v)", t.maxCharLength)
	case sqltypes.Binary:
		s = fmt.Sprintf("binary(%v)", t.maxCharLength)
	case sqltypes.VarChar:
		s = fmt.Sprintf("varchar(%v)", t.maxCharLength)
	case sqltypes.VarBinary:
		s = fmt.Sprintf("varbinary(%v)", t.maxCharLength)
	case sqltypes.Text:
		if t.maxByteLength <= tinyTextBlobMax {
			s = "tinytext"
		} else if t.maxByteLength <= textBlobMax {
			s = "text"
		} else if t.maxByteLength <= mediumTextBlobMax {
			s = "mediumtext"
		} else {
			s = "longtext"
		}
	case sqltypes.Blob:
		if t.maxByteLength <= tinyTextBlobMax {
			s = "tinyblob"
		} else if t.maxByteLength <= textBlobMax {
			s = "blob"
		} else if t.maxByteLength <= mediumTextBlobMax {
			s = "mediumblob"
		} else {
			s = "longblob"
		}
	}

	if t.CharacterSet() != CharacterSet_binary {
		if t.CharacterSet() != Collation_Default.CharacterSet() {
			s += " CHARACTER SET " + t.CharacterSet().String()
		}
		if t.collation != Collation_Default {
			s += " COLLATE " + t.collation.Name()
		}
	}

	return s
}

// Type implements Type interface.
func (t stringType) Type() query.Type {
	return t.baseType
}

// ValueType implements Type interface.
func (t stringType) ValueType() reflect.Type {
	if IsBinaryType(t) {
		return byteValueType
	}
	return stringValueType
}

// Zero implements Type interface.
func (t stringType) Zero() interface{} {
	return ""
}

func (t stringType) CharacterSet() CharacterSetID {
	return t.collation.CharacterSet()
}

func (t stringType) Collation() CollationID {
	return t.collation
}

// WithNewCollation implements TypeWithCollation interface.
func (t stringType) WithNewCollation(collation CollationID) (Type, error) {
	// Blobs are special as, although they use collations, they don't change like a standard collated type
	if t.baseType == sqltypes.Blob || t.baseType == sqltypes.Binary || t.baseType == sqltypes.VarBinary {
		return t, nil
	}
	return CreateString(t.baseType, t.maxCharLength, collation)
}

// MaxCharacterLength is the maximum character length for this type.
func (t stringType) MaxCharacterLength() int64 {
	return t.maxCharLength
}

// MaxByteLength is the maximum number of bytes that may be consumed by a string that conforms to this type.
func (t stringType) MaxByteLength() int64 {
	return t.maxByteLength
}

func appendAndSliceString(buffer []byte, addition string) (slice []byte) {
	stop := len(buffer)
	buffer = append(buffer, addition...)
	slice = buffer[stop:]
	return
}

func appendAndSliceBytes(buffer, addition []byte) (slice []byte) {
	stop := len(buffer)
	buffer = append(buffer, addition...)
	slice = buffer[stop:]
	return
}

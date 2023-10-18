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
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	strings2 "strings"
	"time"
	"unicode/utf8"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/strings"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/encodings"
)

const (
	charBinaryMax       = 255
	varcharVarbinaryMax = 65_535

	TinyTextBlobMax   = charBinaryMax
	TextBlobMax       = varcharVarbinaryMax
	MediumTextBlobMax = 16_777_215
	LongTextBlobMax   = int64(4_294_967_295)
)

var (
	// ErrLengthTooLarge is thrown when a string's length is too large given the other parameters.
	ErrLengthTooLarge    = errors.NewKind("length is %v but max allowed is %v")
	ErrLengthBeyondLimit = errors.NewKind("string '%v' is too large for column '%v'")
	ErrBinaryCollation   = errors.NewKind("binary types must have the binary collation: %v")

	TinyText   = MustCreateStringWithDefaults(sqltypes.Text, TinyTextBlobMax)
	Text       = MustCreateStringWithDefaults(sqltypes.Text, TextBlobMax)
	MediumText = MustCreateStringWithDefaults(sqltypes.Text, MediumTextBlobMax)
	LongText   = MustCreateStringWithDefaults(sqltypes.Text, LongTextBlobMax)
	TinyBlob   = MustCreateBinary(sqltypes.Blob, TinyTextBlobMax)
	Blob       = MustCreateBinary(sqltypes.Blob, TextBlobMax)
	MediumBlob = MustCreateBinary(sqltypes.Blob, MediumTextBlobMax)
	LongBlob   = MustCreateBinary(sqltypes.Blob, LongTextBlobMax)

	stringValueType = reflect.TypeOf(string(""))
	byteValueType   = reflect.TypeOf(([]byte)(nil))
)

type StringType struct {
	baseType      query.Type
	maxCharLength int64
	maxByteLength int64
	collation     sql.CollationID
}

var _ sql.StringType = StringType{}
var _ sql.TypeWithCollation = StringType{}
var _ sql.CollationCoercible = StringType{}

// CreateString creates a new StringType based on the specified type, length, and collation. Length is interpreted as
// the length of bytes in the new StringType for SQL types that are based on bytes (i.e. TEXT, BLOB, BINARY, and
// VARBINARY). For all other char-based SQL types, length is interpreted as the length of chars in the new
// StringType (i.e. CHAR, and VARCHAR).
func CreateString(baseType query.Type, length int64, collation sql.CollationID) (sql.StringType, error) {
	// TODO: remove character set and collation validity checks once all collations have been implemented (delete errors as well)
	if collation.CharacterSet().Encoder() == nil {
		return nil, sql.ErrCharSetNotYetImplementedTemp.New(collation.CharacterSet().Name())
	} else if collation.Sorter() == nil {
		return nil, sql.ErrCollationNotYetImplementedTemp.New(collation.Name())
	}

	// Check the base type first and fail immediately if it's unknown
	switch baseType {
	case sqltypes.Char, sqltypes.Binary, sqltypes.VarChar, sqltypes.VarBinary, sqltypes.Text, sqltypes.Blob:
	default:
		return nil, sql.ErrInvalidBaseType.New(baseType.String(), "string")
	}

	// We accept a length of zero, but a negative length is not valid
	if length < 0 {
		return nil, fmt.Errorf("length of %v is less than the minimum of 0", length)
	}

	switch baseType {
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		if collation != sql.Collation_binary {
			return nil, ErrBinaryCollation.New(collation.Name())
		}
	}

	// If the CharacterSet is binary, then we convert the type to the binary equivalent
	if collation.Equals(sql.Collation_binary) {
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
		if maxByteLength > LongTextBlobMax {
			return nil, ErrLengthTooLarge.New(length, LongTextBlobMax)
		}
		if maxByteLength <= TinyTextBlobMax {
			maxByteLength = TinyTextBlobMax
			maxCharLength = TinyTextBlobMax / charsetMaxLength
		} else if maxByteLength <= TextBlobMax {
			maxByteLength = TextBlobMax
			maxCharLength = TextBlobMax / charsetMaxLength
		} else if maxByteLength <= MediumTextBlobMax {
			maxByteLength = MediumTextBlobMax
			maxCharLength = MediumTextBlobMax / charsetMaxLength
		} else {
			maxByteLength = LongTextBlobMax
			maxCharLength = LongTextBlobMax / charsetMaxLength
		}
	}

	return StringType{baseType, maxCharLength, maxByteLength, collation}, nil
}

// MustCreateString is the same as CreateString except it panics on errors.
func MustCreateString(baseType query.Type, length int64, collation sql.CollationID) sql.StringType {
	st, err := CreateString(baseType, length, collation)
	if err != nil {
		panic(err)
	}
	return st
}

// CreateStringWithDefaults creates a StringType with the default character set and collation of the given size.
func CreateStringWithDefaults(baseType query.Type, length int64) (sql.StringType, error) {
	return CreateString(baseType, length, sql.Collation_Default)
}

// MustCreateStringWithDefaults creates a StringType with the default CharacterSet and Collation.
func MustCreateStringWithDefaults(baseType query.Type, length int64) sql.StringType {
	return MustCreateString(baseType, length, sql.Collation_Default)
}

// CreateBinary creates a StringType with a binary collation and character set of the given size.
func CreateBinary(baseType query.Type, lengthHint int64) (sql.StringType, error) {
	return CreateString(baseType, lengthHint, sql.Collation_binary)
}

// MustCreateBinary is the same as CreateBinary except it panics on errors.
func MustCreateBinary(baseType query.Type, lengthHint int64) sql.StringType {
	return MustCreateString(baseType, lengthHint, sql.Collation_binary)
}

// CreateTinyText creates a TINYTEXT with the given collation.
func CreateTinyText(collation sql.CollationID) sql.StringType {
	return MustCreateString(sqltypes.Text, TinyTextBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// CreateText creates a TEXT with the given collation.
func CreateText(collation sql.CollationID) sql.StringType {
	return MustCreateString(sqltypes.Text, TextBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// CreateMediumText creates a MEDIUMTEXT with the given collation.
func CreateMediumText(collation sql.CollationID) sql.StringType {
	return MustCreateString(sqltypes.Text, MediumTextBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// CreateLongText creates a LONGTEXT with the given collation.
func CreateLongText(collation sql.CollationID) sql.StringType {
	return MustCreateString(sqltypes.Text, LongTextBlobMax/collation.CharacterSet().MaxLength(), collation)
}

// MaxTextResponseByteLength implements the Type interface
func (t StringType) MaxTextResponseByteLength(ctx *sql.Context) uint32 {
	// For TEXT types, MySQL returns the maxByteLength multiplied by the size of the largest
	// multibyte character in the associated charset for the maximum field bytes in the response
	// metadata.
	// The one exception is LongText types, which cannot be multiplied by a multibyte char multiplier,
	// since the max bytes field in a column definition response over the wire is a uint32 and multiplying
	// longTextBlobMax by anything over 1 would cause it to overflow.
	if t.baseType == sqltypes.Text && t.maxByteLength != LongTextBlobMax {
		characterSetResults := ctx.GetCharacterSetResults()
		charsetMaxLength := uint32(characterSetResults.MaxLength())
		return uint32(t.maxByteLength) * charsetMaxLength
	} else {
		return uint32(t.maxByteLength)
	}
}

func (t StringType) Length() int64 {
	return t.maxCharLength
}

// Compare implements Type interface.
func (t StringType) Compare(a interface{}, b interface{}) (int, error) {
	if hasNulls, res := CompareNulls(a, b); hasNulls {
		return res, nil
	}

	var as string
	var bs string
	var ok bool
	if as, ok = a.(string); !ok {
		ai, _, err := t.Convert(a)
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
		bi, _, err := t.Convert(b)
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
			// TODO: return a real error
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
func (t StringType) Convert(v interface{}) (interface{}, sql.ConvertInRange, error) {
	if v == nil {
		return nil, sql.InRange, nil
	}

	val, err := ConvertToString(v, t)
	if err != nil {
		return nil, sql.OutOfRange, err
	}

	if IsBinaryType(t) {
		return []byte(val), sql.InRange, nil
	}
	return val, sql.InRange, nil
}

func ConvertToString(v interface{}, t sql.StringType) (string, error) {
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
		val = s.Format(sql.TimestampDatetimeLayout)
	case decimal.Decimal:
		val = s.StringFixed(s.Exponent() * -1)
	case decimal.NullDecimal:
		if !s.Valid {
			return "", nil
		}
		val = s.Decimal.String()

	case JSONStringer:
		var err error
		val, err = s.JSONString()
		if err != nil {
			return "", err
		}
		val, err = strings.Unquote(val)
		if err != nil {
			return "", err
		}
	case sql.JSONWrapper:
		jsonInterface := s.ToInterface()
		jsonBytes, err := json.Marshal(jsonInterface)
		if err != nil {
			return "", err
		}
		val, err = strings.Unquote(string(jsonBytes))
		if err != nil {
			return "", err
		}
	case GeometryValue:
		return string(s.Serialize()), nil
	default:
		return "", sql.ErrConvertToSQL.New(s, t)
	}

	s := t.(StringType)
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
			// TODO: this should count the string's length properly according to the character set
			// convert 'val' string to rune to count the character length, not byte length
			if int64(len([]rune(val))) > s.maxCharLength {
				return "", ErrLengthBeyondLimit.New(val, t.String())
			}
		}
	}

	if s.baseType == sqltypes.Binary {
		val += strings2.Repeat(string([]byte{0}), int(s.maxCharLength)-len(val))
	}

	return val, nil
}

// ConvertToCollatedString returns the given interface as a string, along with its collation. If the Type possess a
// collation, then that collation is returned. If the Type does not possess a collation (such as an integer), then the
// value is converted to a string and the default collation is used. If the value is already a string then no additional
// conversions are made. If the value is a byte slice then a non-copying conversion is made, which means that the
// original byte slice MUST NOT be modified after being passed to this function. If modifications need to be made, then
// you must allocate a new byte slice and pass that new one in.
func ConvertToCollatedString(val interface{}, typ sql.Type) (string, sql.CollationID, error) {
	var content string
	var collation sql.CollationID
	var err error
	if typeWithCollation, ok := typ.(sql.TypeWithCollation); ok {
		collation = typeWithCollation.Collation()
		if strVal, ok := val.(string); ok {
			content = strVal
		} else if byteVal, ok := val.([]byte); ok {
			content = encodings.BytesToString(byteVal)
		} else {
			val, _, err = LongText.Convert(val)
			if err != nil {
				return "", sql.Collation_Unspecified, err
			}
			content = val.(string)
		}
	} else {
		collation = sql.Collation_Default
		val, _, err = LongText.Convert(val)
		if err != nil {
			return "", sql.Collation_Unspecified, err
		}
		content = val.(string)
	}
	return content, collation, nil
}

// MustConvert implements the Type interface.
func (t StringType) MustConvert(v interface{}) interface{} {
	value, _, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
}

// Equals implements the Type interface.
func (t StringType) Equals(otherType sql.Type) bool {
	if ot, ok := otherType.(StringType); ok {
		return t.baseType == ot.baseType && t.collation == ot.collation && t.maxCharLength == ot.maxCharLength
	}
	return false
}

// Promote implements the Type interface.
func (t StringType) Promote() sql.Type {
	switch t.baseType {
	case sqltypes.Char, sqltypes.VarChar, sqltypes.Text:
		return MustCreateString(sqltypes.Text, LongTextBlobMax, t.collation)
	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob:
		return LongBlob
	default:
		panic(sql.ErrInvalidBaseType.New(t.baseType.String(), "string"))
	}
}

// SQL implements Type interface.
func (t StringType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	var val []byte
	if IsBinaryType(t) {
		v, _, err := t.Convert(v)
		if err != nil {
			return sqltypes.Value{}, err
		}
		val = AppendAndSliceBytes(dest, v.([]byte))
	} else {
		v, err := ConvertToString(v, t)
		if err != nil {
			return sqltypes.Value{}, err
		}
		resultCharset := ctx.GetCharacterSetResults()
		if resultCharset == sql.CharacterSet_Unspecified || resultCharset == sql.CharacterSet_binary {
			resultCharset = t.collation.CharacterSet()
		}
		encodedBytes, ok := resultCharset.Encoder().Encode(encodings.StringToBytes(v))
		if !ok {
			snippet := v
			if len(snippet) > 50 {
				snippet = snippet[:50]
			}
			snippet = strings2.ToValidUTF8(snippet, string(utf8.RuneError))
			return sqltypes.Value{}, sql.ErrCharSetFailedToEncode.New(resultCharset.Name(), utf8.ValidString(v), snippet)
		}
		val = AppendAndSliceBytes(dest, encodedBytes)
	}

	return sqltypes.MakeTrusted(t.baseType, val), nil
}

// String implements Type interface.
func (t StringType) String() string {
	return t.StringWithTableCollation(sql.Collation_Default)
}

// Type implements Type interface.
func (t StringType) Type() query.Type {
	return t.baseType
}

// ValueType implements Type interface.
func (t StringType) ValueType() reflect.Type {
	if IsBinaryType(t) {
		return byteValueType
	}
	return stringValueType
}

// Zero implements Type interface.
func (t StringType) Zero() interface{} {
	return ""
}

// CollationCoercibility implements sql.CollationCoercible interface.
func (t StringType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return t.collation, 4
}

func (t StringType) CharacterSet() sql.CharacterSetID {
	return t.collation.CharacterSet()
}

func (t StringType) Collation() sql.CollationID {
	return t.collation
}

// StringWithTableCollation implements sql.TypeWithCollation interface.
func (t StringType) StringWithTableCollation(tableCollation sql.CollationID) string {
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
		if t.maxByteLength <= TinyTextBlobMax {
			s = "tinytext"
		} else if t.maxByteLength <= TextBlobMax {
			s = "text"
		} else if t.maxByteLength <= MediumTextBlobMax {
			s = "mediumtext"
		} else {
			s = "longtext"
		}
	case sqltypes.Blob:
		if t.maxByteLength <= TinyTextBlobMax {
			s = "tinyblob"
		} else if t.maxByteLength <= TextBlobMax {
			s = "blob"
		} else if t.maxByteLength <= MediumTextBlobMax {
			s = "mediumblob"
		} else {
			s = "longblob"
		}
	}

	if t.CharacterSet() != sql.CharacterSet_binary {
		if t.CharacterSet() != tableCollation.CharacterSet() {
			s += " CHARACTER SET " + t.CharacterSet().String()
		}
		if t.collation != tableCollation {
			s += " COLLATE " + t.collation.Name()
		}
	}

	return s
}

// WithNewCollation implements TypeWithCollation interface.
func (t StringType) WithNewCollation(collation sql.CollationID) (sql.Type, error) {
	// Blobs are special as, although they use collations, they don't change like a standard collated type
	if t.baseType == sqltypes.Blob || t.baseType == sqltypes.Binary || t.baseType == sqltypes.VarBinary {
		return t, nil
	}
	return CreateString(t.baseType, t.maxCharLength, collation)
}

// MaxCharacterLength is the maximum character length for this type.
func (t StringType) MaxCharacterLength() int64 {
	return t.maxCharLength
}

// MaxByteLength is the maximum number of bytes that may be consumed by a string that conforms to this type.
func (t StringType) MaxByteLength() int64 {
	return t.maxByteLength
}

// TODO: move me
func AppendAndSliceString(buffer []byte, addition string) (slice []byte) {
	stop := len(buffer)
	buffer = append(buffer, addition...)
	slice = buffer[stop:]
	return
}

func AppendAndSliceBytes(buffer, addition []byte) (slice []byte) {
	stop := len(buffer)
	buffer = append(buffer, addition...)
	slice = buffer[stop:]
	return
}

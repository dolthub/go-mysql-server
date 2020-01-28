package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cast"
	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)


const (
	charBinaryMax = 255
	varcharVarbinaryMax = 65535

	tinyTextBlobMax = charBinaryMax
	textBlobMax = varcharVarbinaryMax
	mediumTextBlobMax = 16777215
	longTextBlobMax = int64(4294967295)
)

var (
	// ErrLengthTooLarge is thrown when a string's length is too large given the other parameters.
	ErrLengthTooLarge = errors.NewKind("length is %v but max allowed is %v")
	ErrLengthBeyondLimit = errors.NewKind("string is too large for column")
	ErrBinaryCollation = errors.NewKind("binary types must have the binary collation")

	TinyText   = MustCreateStringWithDefaults(sqltypes.Text, tinyTextBlobMax / Collation_Default.CharacterSet().MaxLength())
	Text   = MustCreateStringWithDefaults(sqltypes.Text, textBlobMax / Collation_Default.CharacterSet().MaxLength())
	MediumText = MustCreateStringWithDefaults(sqltypes.Text, mediumTextBlobMax / Collation_Default.CharacterSet().MaxLength())
	LongText = MustCreateStringWithDefaults(sqltypes.Text, longTextBlobMax)
	TinyBlob = MustCreateBinary(sqltypes.Blob, tinyTextBlobMax)
	Blob = MustCreateBinary(sqltypes.Blob, textBlobMax)
	MediumBlob = MustCreateBinary(sqltypes.Blob, mediumTextBlobMax)
	LongBlob = MustCreateBinary(sqltypes.Blob, longTextBlobMax)
)

// StringType represents all string types, including VARCHAR and BLOB.
// https://dev.mysql.com/doc/refman/8.0/en/char.html
// https://dev.mysql.com/doc/refman/8.0/en/binary-varbinary.html
// https://dev.mysql.com/doc/refman/8.0/en/blob.html
type StringType interface {
	Type
	ChangeCollation(newCollation Collation) (StringType, error)
	CharacterSet() CharacterSet
	Collation() Collation
	MaxCharacterLength() int64
	MaxByteLength() int64
}

type stringType struct {
	baseType query.Type
	charLength int64
	collation Collation
}

// CreateString creates a StringType.
func CreateString(baseType query.Type, length int64, collation Collation) (StringType, error) {
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
			return nil, ErrBinaryCollation.New()
		}
	}

	// If the CharacterSet is binary, then we convert the type to the binary equivalent
	if collation == Collation_binary {
		switch baseType {
		case sqltypes.Char:
			baseType = sqltypes.Binary
		case sqltypes.VarChar:
			baseType = sqltypes.VarBinary
		case sqltypes.Text:
			baseType = sqltypes.Blob
		}
	}

	// Make sure that length is valid depending on the base type, since they each handle lengths differently
	charsetMaxLength := collation.CharacterSet().MaxLength()
	byteLength := length * charsetMaxLength
	switch baseType {
	case sqltypes.Char, sqltypes.Binary:
		// We limit on length, so storage requirements are variable
		if length > charBinaryMax {
			return nil, ErrLengthTooLarge.New(length, charBinaryMax)
		}
	case sqltypes.VarChar, sqltypes.VarBinary:
		// We limit on byte length, so acceptable character lengths are variable
		if byteLength > varcharVarbinaryMax {
			return nil, ErrLengthTooLarge.New(length, varcharVarbinaryMax / charsetMaxLength)
		}
	case sqltypes.Text, sqltypes.Blob:
		// We overall limit on character length, but determine tiny, medium, etc. based on byte length.
		if length > longTextBlobMax {
			return nil, ErrLengthTooLarge.New(length, longTextBlobMax)
		}
		if byteLength <= tinyTextBlobMax {
			length = tinyTextBlobMax / charsetMaxLength
		} else if byteLength <= textBlobMax {
			length = textBlobMax / charsetMaxLength
		} else if byteLength <= mediumTextBlobMax {
			length = mediumTextBlobMax / charsetMaxLength
		} else {
			// Unlike the others, we just limit on character length rather than byte length.
			length = longTextBlobMax
		}
	}

	return stringType{baseType, length, collation}, nil
}

// MustCreateString is the same as CreateString except it panics on errors.
func MustCreateString(baseType query.Type, length int64, collation Collation) StringType {
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
		as = ai.(string)
	}
	if bs, ok = b.(string); !ok {
		bi, err := t.Convert(b)
		if err != nil {
			return 0, err
		}
		bs = bi.(string)
	}

	return strings.Compare(as, bs), nil
}

// Convert implements Type interface.
func (t stringType) Convert(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}

	if ti, ok := v.(time.Time); ok {
		v = ti.Format(TimestampDatetimeLayout)
	}

	val, err := cast.ToStringE(v)
	if err != nil {
		return nil, ErrConvertToSQL.New(t)
	}

	if t.CharacterSet().MaxLength() == 1 {
		if int64(len(val)) > t.charLength {
			return nil, ErrLengthBeyondLimit.New()
		}
	} else {
		//TODO: this should count the string's length properly according to the character set
		if int64(len(val)) > t.charLength {
			return nil, ErrLengthBeyondLimit.New()
		}
	}

	if t.baseType == sqltypes.Binary {
		val += strings.Repeat(string([]byte{0}), int(t.charLength) - len(val))
	}

	return val, nil
}

// MustConvert implements the Type interface.
func (t stringType) MustConvert(v interface{}) interface{} {
	value, err := t.Convert(v)
	if err != nil {
		panic(err)
	}
	return value
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
func (t stringType) SQL(v interface{}) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}

	v, err := t.Convert(v)
	if err != nil {
		return sqltypes.Value{}, err
	}

	return sqltypes.MakeTrusted(t.baseType, []byte(v.(string))), nil
}

// String implements Type interface.
func (t stringType) String() string {
	byteLength := t.MaxByteLength()
	var s string

	switch t.baseType {
	case sqltypes.Char:
		s = fmt.Sprintf("CHAR(%v)", t.charLength)
	case sqltypes.Binary:
		s = fmt.Sprintf("BINARY(%v)", t.charLength)
	case sqltypes.VarChar:
		s = fmt.Sprintf("VARCHAR(%v)", t.charLength)
	case sqltypes.VarBinary:
		s = fmt.Sprintf("VARBINARY(%v)", t.charLength)
	case sqltypes.Text:
		if byteLength <= tinyTextBlobMax {
			s = "TINYTEXT"
		} else if byteLength <= textBlobMax {
			s = "TEXT"
		} else if byteLength <= mediumTextBlobMax {
			s = "MEDIUMTEXT"
		} else {
			s = "LONGTEXT"
		}
	case sqltypes.Blob:
		if byteLength <= tinyTextBlobMax {
			s = "TINYBLOB"
		} else if byteLength <= textBlobMax {
			s = "BLOB"
		} else if byteLength <= mediumTextBlobMax {
			s = "MEDIUMBLOB"
		} else {
			s = "LONGBLOB"
		}
	}

	if t.CharacterSet() != CharacterSet_binary {
		if t.CharacterSet() != Collation_Default.CharacterSet() {
			s += " CHARACTER SET " + t.CharacterSet().String()
		}
		if t.collation != Collation_Default {
			s += " COLLATE " + t.collation.String()
		}
	}

	return s
}

// Type implements Type interface.
func (t stringType) Type() query.Type {
	return t.baseType
}

// Zero implements Type interface.
func (t stringType) Zero() interface{} {
	return ""
}

// ChangeCollation returns a new StringType that is equivalent to this type with the new collation.
// Returns an error if the new collation causes a VarChar type to go beyond the byte limit.
// Of note, Text and Blob types may change their character limit, since it's determined by byte length.
// For example, `TINYTEXT COLLATE utf8mb4_bin` has a limit of 63 characters, where as
// `TINYTEXT COLLATE ascii_bin` has a limit of 255 characters.
func (t stringType) ChangeCollation(newCollation Collation) (StringType, error) {
	if t.collation == newCollation {
		return t, nil
	}
	switch t.baseType {
	case sqltypes.Char, sqltypes.Binary:
		// Pass in Char since a binary collation will turn it into Binary
		return CreateString(sqltypes.Char, t.charLength, newCollation)
	case sqltypes.VarChar, sqltypes.VarBinary:
		// Pass in VarChar since a binary collation will turn it into VarBinary
		return CreateString(sqltypes.VarChar, t.charLength, newCollation)
	case sqltypes.Text, sqltypes.Blob:
		maxByteLength := t.MaxByteLength()
		if maxByteLength <= tinyTextBlobMax {
			return CreateString(sqltypes.Text, tinyTextBlobMax / newCollation.CharacterSet().MaxLength(), newCollation)
		} else if maxByteLength <= textBlobMax {
			return CreateString(sqltypes.Text, textBlobMax / newCollation.CharacterSet().MaxLength(), newCollation)
		} else if maxByteLength <= mediumTextBlobMax {
			return CreateString(sqltypes.Text, mediumTextBlobMax / newCollation.CharacterSet().MaxLength(), newCollation)
		} else {
			return CreateString(sqltypes.Text, longTextBlobMax, newCollation)
		}
	default:
		return nil, ErrInvalidBaseType.New(t.baseType.String(), "string")
	}
}

func (t stringType) CharacterSet() CharacterSet {
	return t.collation.CharacterSet()
}

func (t stringType) Collation() Collation {
	return t.collation
}

// MaxCharacterLength is the maximum character length for this type.
func (t stringType) MaxCharacterLength() int64 {
	return t.charLength
}

// MaxByteLength is the maximum number of bytes that may be consumed by a string that conforms to this type.
func (t stringType) MaxByteLength() int64 {
	return t.charLength * t.CharacterSet().MaxLength()
}

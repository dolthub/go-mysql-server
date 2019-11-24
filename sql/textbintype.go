package sql

import (
	"fmt"
	"strings"

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
	longTextBlobMax = 4294967295
)

var (
	// ErrLengthTooLarge is thrown when a string's length is too large given the other parameters.
	ErrLengthTooLarge = errors.NewKind("length is %v but max allowed is %v")

	defaultCharset = CharacterSet_utf8mb4

	Text = MustCreateStringWithDefaults(sqltypes.Text, textBlobMax)
	Blob = MustCreateBlob(sqltypes.Blob, textBlobMax)
)

// StringType represents all string types, including VARCHAR and BLOB
type StringType interface {
	Type
	CharacterSet() CharacterSet
	Collation() Collation
	MaxCharacterLength() int64
	MaxByteLength() int64
}

type stringType struct {
	baseType query.Type
	charLength int64
	charset CharacterSet
	collation Collation
}

// CreateString creates a StringType. If a CharacterSet or Collation was not specified, then use Unknown.
// If the binary attribute (not collation) was used, then use CharacterSetBin as the Collation.
func CreateString(baseType query.Type, length int64, charset CharacterSet, collation Collation) (StringType, error) {
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

	// Figure out what an Unknown CharacterSet or Collation should be or validate the given combination.
	if charset == CharacterSet_Unknown {
		if collation == Collation_Unknown {
			charset = defaultCharset
			collation = charset.DefaultCollation()
		} else if collation == Collation_CharacterSetBin {
			charset = defaultCharset
			collation = charset.BinaryCollation()
		} else {
			charset = collation.CharacterSet()
		}
	} else {
		if collation == Collation_Unknown {
			collation = charset.DefaultCollation()
		} else if collation == Collation_CharacterSetBin {
			collation = charset.BinaryCollation()
		} else {
			if !collation.IsValid(charset) {
				return nil, fmt.Errorf("%v is not a valid character set for %v", charset, collation)
			}
		}
	}

	// If the CharacterSet is binary, then we convert the type to the binary equivalent
	switch baseType {
	case sqltypes.Char:
		if charset == CharacterSet_binary {
			baseType = sqltypes.Binary
		}
	case sqltypes.VarChar:
		if charset == CharacterSet_binary {
			baseType = sqltypes.VarBinary
		}
	case sqltypes.Text:
		if charset == CharacterSet_binary {
			baseType = sqltypes.Blob
		}
	}

	// Make sure that length is valid depending on the base type, since they each handle lengths differently
	charsetMaxLength := charset.MaxLength()
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

	return stringType{baseType, length, charset, collation}, nil
}

// MustCreateString creates a StringType. If a CharacterSet or Collation was not specified, then use Unknown.
// If the binary attribute (not collation) was used, then use CharacterSetBin as the Collation.
func MustCreateString(baseType query.Type, length int64, charset CharacterSet, collation Collation) StringType {
	st, err := CreateString(baseType, length, charset, collation)
	if err != nil {
		panic(err)
	}
	return st
}

// CreateStringWithDefaults creates a StringType with the default character set and collation of the given size.
func CreateStringWithDefaults(baseType query.Type, length int64) (StringType, error) {
	return CreateString(baseType, length, defaultCharset, defaultCharset.DefaultCollation())
}

// MustCreateStringWithDefaults creates a StringType with the default CharacterSet and Collation.
func MustCreateStringWithDefaults(baseType query.Type, length int64) StringType {
	return MustCreateString(baseType, length, defaultCharset, defaultCharset.DefaultCollation())
}

// CreateBlob creates a StringType with a binary collation and character set of the given size.
func CreateBlob(baseType query.Type, lengthHint int64) (StringType, error) {
	return CreateString(baseType, lengthHint, CharacterSet_binary, Collation_binary)
}

// MustCreateBlob is the same as CreateBlob except it panics on errors.
func MustCreateBlob(baseType query.Type, lengthHint int64) StringType {
	return MustCreateString(baseType, lengthHint, CharacterSet_binary, Collation_binary)
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
	val, err := cast.ToStringE(v)
	if err != nil {
		return nil, ErrConvertToSQL.New(t)
	}
	return val, nil
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

	if t.charset != CharacterSet_binary {
		if t.charset != defaultCharset {
			s += " CHARACTER SET " + t.charset.String()
		}
		if t.collation != defaultCharset.DefaultCollation() {
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

func (t stringType) CharacterSet() CharacterSet {
	return t.charset
}

func (t stringType) Collation() Collation {
	return t.collation
}

func (t stringType) MaxCharacterLength() int64 {
	return t.charLength
}

func (t stringType) MaxByteLength() int64 {
	return t.charLength * int64(t.charset.MaxLength())
}

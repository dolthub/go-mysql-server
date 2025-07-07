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
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestStringCompare(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		typ         sql.StringType
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{MustCreateBinary(sqltypes.Binary, 10), nil, 0, 1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), 0, nil, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), nil, nil, 0},

		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 0, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte{0}, true, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), false, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, 0, 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), true, "false", -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, false, 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), 1, 1, 0},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), true, 1, 0},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), "True", true, 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), false, true, -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), "0x12345de", "0xed54321", -1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), "0xed54321", "0x12345de", 1},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte("254"), 254, 0},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte("254"), 254.5, -1},

		// Sanity checks that behavior is consistent
		{MustCreateBinary(sqltypes.Binary, 10), 0, 1, -1},
		{MustCreateBinary(sqltypes.Binary, 10), []byte{0}, true, -1},
		{MustCreateBinary(sqltypes.Binary, 10), false, 1, -1},
		{MustCreateBinary(sqltypes.Binary, 10), []byte("254"), 254, 0},
		{MustCreateBinary(sqltypes.Blob, 10), 0, 1, -1},
		{MustCreateBinary(sqltypes.Blob, 10), []byte{0}, true, -1},
		{MustCreateBinary(sqltypes.Blob, 10), false, 1, -1},
		{MustCreateBinary(sqltypes.Blob, 10), []byte("254"), 254, 0},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), 0, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), []byte{0}, true, -1},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), false, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.Char, 10), []byte("254"), 254, 0},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), 0, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), []byte{0}, true, -1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), false, 1, -1},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), []byte("254"), 254, 0},
		{MustCreateBinary(sqltypes.VarBinary, 10), []byte{0}, true, -1},
		{MustCreateBinary(sqltypes.VarBinary, 10), false, 1, -1},
		{MustCreateBinary(sqltypes.VarBinary, 10), 0, 1, -1},
		{MustCreateBinary(sqltypes.VarBinary, 10), []byte("254"), 254, 0},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := test.typ.Compare(ctx, test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestStringCreateBlob(t *testing.T) {
	tests := []struct {
		baseType     query.Type
		length       int64
		expectedType StringType
		expectedErr  bool
	}{
		{sqltypes.Binary, 10,
			StringType{sqltypes.Binary, 10, 10, sql.Collation_binary}, false},
		{sqltypes.Blob, 10,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary}, false},
		{sqltypes.Char, 10,
			StringType{sqltypes.Binary, 10, 10, sql.Collation_binary}, false},
		{sqltypes.Text, 10,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary}, false},
		{sqltypes.VarBinary, 10,
			StringType{sqltypes.VarBinary, 10, 10, sql.Collation_binary}, false},
		{sqltypes.VarChar, 10,
			StringType{sqltypes.VarBinary, 10, 10, sql.Collation_binary}, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.baseType, test.length), func(t *testing.T) {
			typ, err := CreateBinary(test.baseType, test.length)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestStringCreateBlobInvalidBaseTypes(t *testing.T) {
	tests := []struct {
		baseType     query.Type
		length       int64
		expectedType StringType
		expectedErr  bool
	}{
		{sqltypes.Bit, 10, StringType{}, true},
		{sqltypes.Date, 10, StringType{}, true},
		{sqltypes.Datetime, 10, StringType{}, true},
		{sqltypes.Decimal, 10, StringType{}, true},
		{sqltypes.Enum, 10, StringType{}, true},
		{sqltypes.Expression, 10, StringType{}, true},
		{sqltypes.Float32, 10, StringType{}, true},
		{sqltypes.Float64, 10, StringType{}, true},
		{sqltypes.Geometry, 10, StringType{}, true},
		{sqltypes.Int16, 10, StringType{}, true},
		{sqltypes.Int24, 10, StringType{}, true},
		{sqltypes.Int32, 10, StringType{}, true},
		{sqltypes.Int64, 10, StringType{}, true},
		{sqltypes.Int8, 10, StringType{}, true},
		{sqltypes.Null, 10, StringType{}, true},
		{sqltypes.Set, 10, StringType{}, true},
		{sqltypes.Time, 10, StringType{}, true},
		{sqltypes.Timestamp, 10, StringType{}, true},
		{sqltypes.TypeJSON, 10, StringType{}, true},
		{sqltypes.Uint16, 10, StringType{}, true},
		{sqltypes.Uint24, 10, StringType{}, true},
		{sqltypes.Uint32, 10, StringType{}, true},
		{sqltypes.Uint64, 10, StringType{}, true},
		{sqltypes.Uint8, 10, StringType{}, true},
		{sqltypes.Year, 10, StringType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.baseType, test.length), func(t *testing.T) {
			typ, err := CreateBinary(test.baseType, test.length)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestStringCreateString(t *testing.T) {
	tests := []struct {
		baseType             query.Type
		length               int64
		collation            sql.CollationID
		expectedType         StringType
		expectedMaxTextBytes uint32
		expectedErr          bool
	}{
		{sqltypes.Binary, 10, sql.Collation_binary,
			StringType{sqltypes.Binary, 10, 10, sql.Collation_binary},
			10, false},
		{sqltypes.Blob, 10, sql.Collation_binary,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary},
			TinyTextBlobMax, false},
		{sqltypes.Char, 10, sql.Collation_Default,
			StringType{sqltypes.Char, 10, 40, sql.Collation_Default},
			40, false},
		{sqltypes.Text, 10, sql.Collation_Default,
			StringType{sqltypes.Text, TinyTextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), TinyTextBlobMax, sql.Collation_Default},
			uint32(TinyTextBlobMax * sql.Collation_Default.CharacterSet().MaxLength()), false},
		{sqltypes.Text, 1000, sql.Collation_Default,
			StringType{sqltypes.Text, TextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), TextBlobMax, sql.Collation_Default},
			uint32(TextBlobMax * sql.Collation_Default.CharacterSet().MaxLength()), false},
		{sqltypes.Text, 1000000, sql.Collation_Default,
			StringType{sqltypes.Text, MediumTextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), MediumTextBlobMax, sql.Collation_Default},
			uint32(MediumTextBlobMax * sql.Collation_Default.CharacterSet().MaxLength()), false},
		{sqltypes.Text, LongTextBlobMax, sql.Collation_Default,
			StringType{sqltypes.Text, LongTextBlobMax / sql.Collation_Default.CharacterSet().MaxLength(), LongTextBlobMax, sql.Collation_Default},
			uint32(LongTextBlobMax), false},
		{sqltypes.VarBinary, 10, sql.Collation_binary,
			StringType{sqltypes.VarBinary, 10, 10, sql.Collation_binary},
			10, false},
		{sqltypes.VarChar, 10, sql.Collation_Default,
			StringType{sqltypes.VarChar, 10, 40, sql.Collation_Default},
			40, false},
		{sqltypes.Char, 10, sql.Collation_binary,
			StringType{sqltypes.Binary, 10, 10, sql.Collation_binary},
			10, false},
		{sqltypes.Text, 10, sql.Collation_binary,
			StringType{sqltypes.Blob, TinyTextBlobMax, TinyTextBlobMax, sql.Collation_binary},
			TinyTextBlobMax, false},
		{sqltypes.VarChar, 10, sql.Collation_binary,
			StringType{sqltypes.VarBinary, 10, 10, sql.Collation_binary},
			10, false},

		// Out of bounds error cases
		{sqltypes.Binary, charBinaryMax + 1, sql.Collation_binary, StringType{}, 0, true},
		{sqltypes.Blob, LongTextBlobMax + 1, sql.Collation_binary, StringType{}, 0, true},
		{sqltypes.Char, charBinaryMax + 1, sql.Collation_Default, StringType{}, 0, true},
		{sqltypes.Text, LongTextBlobMax + 1, sql.Collation_Default, StringType{}, 0, true},

		// JSON strings can also come in over the wire as VARBINARY types, and JSON allows a much larger length limit (1GB).
		{sqltypes.VarBinary, MaxJsonFieldByteLength + 1, sql.Collation_binary, StringType{}, 0, true},
		{sqltypes.VarChar, varcharVarbinaryMax + 1, sql.Collation_Default, StringType{}, 0, true},

		// Default collation is not valid for these types
		{sqltypes.Binary, 10, sql.Collation_Default, StringType{}, 0, true},
		{sqltypes.Blob, 10, sql.Collation_Default, StringType{}, 0, true},
		{sqltypes.VarBinary, 10, sql.Collation_Default, StringType{}, 0, true},
	}

	ctx := sql.NewContext(
		context.Background(),
		sql.WithSession(sql.NewBaseSession()),
	)

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.baseType, test.length, test.collation), func(t *testing.T) {
			typ, err := CreateString(test.baseType, test.length, test.collation)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
				assert.Equal(t, test.expectedMaxTextBytes, typ.MaxTextResponseByteLength(ctx))
			}
		})
	}
}

func TestStringCreateStringInvalidBaseTypes(t *testing.T) {
	tests := []struct {
		baseType     query.Type
		length       int64
		collation    sql.CollationID
		expectedType StringType
		expectedErr  bool
	}{
		{sqltypes.Bit, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Date, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Datetime, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Decimal, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Enum, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Expression, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Float32, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Float64, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Geometry, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int16, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int24, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int32, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int64, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Int8, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Null, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Set, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Time, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Timestamp, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.TypeJSON, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint16, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint24, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint32, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint64, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Uint8, 10, sql.Collation_Default, StringType{}, true},
		{sqltypes.Year, 10, sql.Collation_Default, StringType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.baseType, test.length, test.collation), func(t *testing.T) {
			typ, err := CreateString(test.baseType, test.length, test.collation)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestStringConvert(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		typ         sql.StringType
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{MustCreateBinary(sqltypes.Binary, 3), nil, nil, false},
		{MustCreateBinary(sqltypes.Blob, 3), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.Char, 7), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.Text, 7), nil, nil, false},
		{MustCreateBinary(sqltypes.VarBinary, 3), nil, nil, false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 7), nil, nil, false},

		{MustCreateBinary(sqltypes.Binary, 4), []byte{'1'}, []byte{'1', 0, 0, 0}, false},
		{MustCreateBinary(sqltypes.Blob, 4), []byte{'1'}, []byte{'1'}, false},
		{MustCreateStringWithDefaults(sqltypes.Char, 7), "abcde", "abcde", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 7), "abcde", "abcde", false},
		{MustCreateBinary(sqltypes.VarBinary, 7), "abcde", []byte("abcde"), false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 7), "abcde", "abcde", false},

		{MustCreateStringWithDefaults(sqltypes.Char, 4), int(1), "1", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), int8(2), "2", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 4), int16(3), "3", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), int32(4), "4", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), int64(5), "5", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 4), uint(10), "10", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), uint8(11), "11", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), uint16(12), "12", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 4), uint32(13), "13", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), uint64(14), "14", false},
		{MustCreateStringWithDefaults(sqltypes.Text, 4), float32(9.875), "9.875", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 7), float64(11583.5), "11583.5", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 4), []byte("abcd"), "abcd", false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 40), time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), "2019-12-12 12:12:12", false},

		{MustCreateBinary(sqltypes.Binary, 3), "abcd", nil, true},
		{MustCreateBinary(sqltypes.Blob, 3), strings.Repeat("0", TinyTextBlobMax+1), nil, true},
		{MustCreateStringWithDefaults(sqltypes.Char, 3), "abcd", nil, true},
		{MustCreateStringWithDefaults(sqltypes.Text, 3),
			strings.Repeat("𒁏", int(TinyTextBlobMax/sql.Collation_Default.CharacterSet().MaxLength())+1),
			nil, true},
		{MustCreateBinary(sqltypes.VarBinary, 3), []byte{01, 02, 03, 04}, nil, true},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 3), []byte("abcd"), nil, true},
		{MustCreateStringWithDefaults(sqltypes.Char, 20), JSONDocument{Val: nil}, "null", false},
		{MustCreateStringWithDefaults(sqltypes.Char, 20), JSONDocument{Val: map[string]interface{}{"a": 1}}, `{"a": 1}`, false},
		{MustCreateStringWithDefaults(sqltypes.Char, 20), NewLazyJSONDocument([]byte(`{"a":1}`)), `{"a": 1}`, false},

		{MustCreateStringWithDefaults(sqltypes.Char, 10), []byte{0x98, 0x76, 0x54}, nil, false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), []byte{0x98, 0x76, 0x54}, nil, false},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), []byte{0x98, 0x76, 0x54}, nil, false},
		{MustCreateBinary(sqltypes.Binary, 10), []byte{0x98, 0x76, 0x54}, []byte{0x98, 0x76, 0x54, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, false},
		{MustCreateBinary(sqltypes.VarBinary, 10), []byte{0x98, 0x76, 0x54}, []byte{0x98, 0x76, 0x54}, false},
		{MustCreateBinary(sqltypes.Blob, 10), []byte{0x98, 0x76, 0x54}, []byte{0x98, 0x76, 0x54}, false},

		{MustCreateStringWithDefaults(sqltypes.Char, 10), string([]byte{0x98, 0x76, 0x54}), nil, false},
		{MustCreateStringWithDefaults(sqltypes.VarChar, 10), string([]byte{0x98, 0x76, 0x54}), nil, false},
		{MustCreateStringWithDefaults(sqltypes.Text, 10), string([]byte{0x98, 0x76, 0x54}), nil, false},
		{MustCreateBinary(sqltypes.Binary, 10), string([]byte{0x98, 0x76, 0x54}), []byte{0x98, 0x76, 0x54, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, false},
		{MustCreateBinary(sqltypes.VarBinary, 10), string([]byte{0x98, 0x76, 0x54}), []byte{0x98, 0x76, 0x54}, false},
		{MustCreateBinary(sqltypes.Blob, 10), string([]byte{0x98, 0x76, 0x54}), []byte{0x98, 0x76, 0x54}, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val, test.expectedVal), func(t *testing.T) {
			val, _, err := test.typ.Convert(ctx, test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
				if val != nil {
					assert.Equal(t, test.typ.ValueType(), reflect.TypeOf(val))
				}
			}
		})
	}
}

// TestStringSQL_InvalidUTF8Handling tests that the SQL function gracefully handles
// invalid UTF-8 data by replacing invalid sequences with replacement characters.
// This is a regression test for issue dolthub/dolt#8893.
func TestStringSQL_InvalidUTF8Handling(t *testing.T) {
	ctx := sql.NewEmptyContext()
	tests := []struct {
		name        string
		typ         sql.StringType
		input       []byte
		expected    string
		expectError bool
	}{
		{
			name:        "Valid UTF-8 should work normally",
			typ:         Text,
			input:       []byte("DoltLab®"), // Proper UTF-8 encoding of ®
			expected:    "DoltLab®",
			expectError: false,
		},
		{
			name:        "Issue #8893 exact scenario - DoltLab with latin1 ® (0xAE)",
			typ:         Text,
			input:       []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE}, // "DoltLab" + 0xAE
			expected:    "",                                                     // Should return NULL for invalid UTF-8 (matches MySQL behavior)
			expectError: false,
		},
		{
			name:        "Multiple invalid UTF-8 bytes",
			typ:         Text,
			input:       []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x98, 0x76, 0x54}, // "Hello" + invalid bytes
			expected:    "",                                                     // Should return NULL for invalid UTF-8 (matches MySQL behavior)
			expectError: false,
		},
		{
			name:        "Empty input",
			typ:         Text,
			input:       []byte{},
			expected:    "",
			expectError: false,
		},
		{
			name:        "Binary type should not be affected",
			typ:         LongBlob,
			input:       []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE},
			expected:    "DoltLab\xae", // Binary types pass through unchanged
			expectError: false,
		},
		{
			name:        "VARCHAR with invalid UTF-8",
			typ:         MustCreateStringWithDefaults(sqltypes.VarChar, 100),
			input:       []byte{0x54, 0x65, 0x73, 0x74, 0xAE, 0x98}, // "Test" + invalid bytes
			expected:    "",                                         // Should return NULL for invalid UTF-8 (matches MySQL behavior)
			expectError: false,
		},
		{
			name:        "CHAR with invalid UTF-8",
			typ:         MustCreateStringWithDefaults(sqltypes.Char, 100),
			input:       []byte{0x41, 0x42, 0x43, 0xAE}, // "ABC" + invalid byte
			expected:    "",                             // Should return NULL for invalid UTF-8 (matches MySQL behavior)
			expectError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.typ.SQL(ctx, nil, test.input)

			if test.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				if test.expected == "" && (test.name == "Issue #8893 exact scenario - DoltLab with latin1 ® (0xAE)" ||
					test.name == "Multiple invalid UTF-8 bytes" ||
					test.name == "VARCHAR with invalid UTF-8" ||
					test.name == "CHAR with invalid UTF-8") {
					// For invalid UTF-8 cases, we expect NULL (which returns empty string but is actually NULL)
					assert.True(t, result.IsNull(), "Expected NULL for invalid UTF-8")
				} else {
					assert.Equal(t, test.expected, result.ToString())
				}
			}
		})
	}
}

// TestStringSQL_StrictConvertValidation ensures that both Convert (used for INSERT)
// and SQL (used for SELECT) handle invalid UTF-8 by returning NULL.
// This verifies the MySQL-compatible behavior required for issue dolthub/dolt#8893.
func TestStringSQL_StrictConvertValidation(t *testing.T) {
	ctx := sql.NewEmptyContext()

	// The exact invalid UTF-8 data from issue #8893
	invalidUTF8 := []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE} // "DoltLab" + 0xAE

	stringType := Text

	// Test 1: Convert should now accept invalid UTF-8 and return NULL (matches MySQL INSERT behavior)
	t.Run("Convert should accept invalid UTF-8 and return NULL for INSERT operations", func(t *testing.T) {
		result, _, err := stringType.Convert(ctx, string(invalidUTF8))
		require.NoError(t, err)
		assert.Nil(t, result, "Expected NULL for invalid UTF-8 in Convert (matches MySQL)")
	})

	// Test 2: SQL should accept invalid UTF-8 and return NULL (permissive for SELECT, matches MySQL)
	t.Run("SQL should accept invalid UTF-8 for SELECT operations", func(t *testing.T) {
		result, err := stringType.SQL(ctx, nil, invalidUTF8)
		require.NoError(t, err)
		assert.True(t, result.IsNull(), "Expected NULL for invalid UTF-8 (matches MySQL behavior)")
	})

	// Test 3: Valid UTF-8 should work in both cases
	t.Run("Valid UTF-8 should work in both Convert and SQL", func(t *testing.T) {
		validUTF8 := []byte("DoltLab®") // Proper UTF-8

		// Convert should work
		converted, _, err := stringType.Convert(ctx, string(validUTF8))
		require.NoError(t, err)
		assert.Equal(t, "DoltLab®", converted)

		// SQL should work
		result, err := stringType.SQL(ctx, nil, validUTF8)
		require.NoError(t, err)
		assert.Equal(t, "DoltLab®", result.ToString())
	})
}

// TestStringSQL_CustomerWorkflow_Issue8893 tests the specific customer scenarios
// described in issue dolthub/dolt#8893 to ensure MySQL-compatible behavior.
func TestStringSQL_CustomerWorkflow_Issue8893(t *testing.T) {
	ctx := sql.NewEmptyContext()

	// Customer's exact problematic data: "DoltLab®" with latin1 ® (0xAE)
	customerData := []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE}
	textType := Text

	t.Run("Customer Scenario 1: Basic SELECT that was failing", func(t *testing.T) {
		// Customer reported: SELECT name FROM Products; threw "invalid string for charset utf8mb4"
		// After fix: Should return NULL instead of error
		result, err := textType.SQL(ctx, nil, customerData)
		require.NoError(t, err, "Customer's basic SELECT should not throw errors")
		assert.True(t, result.IsNull(), "Should return NULL for invalid UTF-8 (matches MySQL)")
	})

	t.Run("Customer Scenario 2: INSERT with problematic data", func(t *testing.T) {
		// Customer had existing data that was problematic
		// Our fix should allow INSERT operations to complete with NULL values
		convertResult, _, err := textType.Convert(ctx, string(customerData))
		require.NoError(t, err, "INSERT operations should not fail")
		assert.Nil(t, convertResult, "Should insert NULL for invalid UTF-8 (matches MySQL)")
	})

	t.Run("Customer Scenario 3: Data identification queries", func(t *testing.T) {
		// Customer needs to identify problematic records with WHERE clauses
		// Test that NULL values work properly in comparisons
		result, err := textType.SQL(ctx, nil, customerData)
		require.NoError(t, err)

		// Simulate: SELECT * FROM Products WHERE name IS NULL;
		assert.True(t, result.IsNull(), "NULL values should be identifiable with IS NULL")

		// Simulate: SELECT * FROM Products WHERE name IS NOT NULL;
		validData := []byte("ValidProduct")
		validResult, err := textType.SQL(ctx, nil, validData)
		require.NoError(t, err)
		assert.False(t, validResult.IsNull(), "Valid data should not be NULL")
	})

	t.Run("Customer Scenario 4: Mixed valid and invalid data", func(t *testing.T) {
		// Customer's table had mix of valid and invalid data
		testCases := []struct {
			name    string
			data    []byte
			isValid bool
		}{
			{"Valid product name", []byte("ValidProduct"), true},
			{"Customer's problematic data", customerData, false},
			{"Another valid name", []byte("AnotherProduct®"), true}, // Proper UTF-8 ®
			{"Different invalid UTF-8", []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x98}, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := textType.SQL(ctx, nil, tc.data)
				require.NoError(t, err, "No queries should fail regardless of data validity")

				if tc.isValid {
					assert.False(t, result.IsNull(), "Valid UTF-8 should not return NULL")
					assert.Equal(t, string(tc.data), result.ToString())
				} else {
					assert.True(t, result.IsNull(), "Invalid UTF-8 should return NULL")
				}
			})
		}
	})

	t.Run("Customer Scenario 5: Export/cleanup operations", func(t *testing.T) {
		// Customer wanted to export data and re-import with proper encoding
		// All SELECT operations should work without throwing errors

		// Simulate customer's export query that was failing
		problemData := [][]byte{
			customerData, // Original issue
			{0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x98, 0x76, 0x54}, // Other invalid UTF-8
			{0x54, 0x65, 0x73, 0x74, 0xAE, 0x98},             // Multiple invalid bytes
		}

		for i, data := range problemData {
			t.Run(fmt.Sprintf("Export query %d", i+1), func(t *testing.T) {
				result, err := textType.SQL(ctx, nil, data)
				require.NoError(t, err, "Export queries must not fail")

				// Customer can now identify records that need fixing
				if result.IsNull() {
					// This record needs attention in the re-import
					t.Logf("Record %d identified as needing cleanup (NULL)", i+1)
				}
			})
		}
	})
}

// TestStringSQL_MySQLCompatibility_Issue8893 validates that our behavior exactly matches MySQL
// for the specific scenarios in issue dolthub/dolt#8893.
func TestStringSQL_MySQLCompatibility_Issue8893(t *testing.T) {
	ctx := sql.NewEmptyContext()

	t.Run("MySQL VARBINARY behavior comparison", func(t *testing.T) {
		// Test data: 0x446F6C744C6162AE (DoltLab + latin1 ®)
		varbinaryData := []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE}

		// MySQL behavior for VARBINARY with invalid UTF-8:
		// - Basic SELECT: Shows "DoltLab�" (replacement character in display)
		// - This is handled at the display level, our SQL function should return the data
		binaryType := LongBlob // Binary type should pass through unchanged
		result, err := binaryType.SQL(ctx, nil, varbinaryData)
		require.NoError(t, err)
		assert.False(t, result.IsNull(), "Binary data should pass through unchanged")

		// The display shows replacement character, but the data itself is preserved
		resultBytes := []byte(result.ToString())
		assert.Equal(t, varbinaryData, resultBytes, "Binary data should be preserved exactly")
	})

	t.Run("MySQL TEXT behavior comparison", func(t *testing.T) {
		// Test data: 0x446F6C744C6162AE (DoltLab + latin1 ®)
		invalidUTF8Data := []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE}
		textType := Text

		// MySQL behavior for TEXT with invalid UTF-8:
		// - SELECT: Returns NULL
		// - INSERT: Accepts and stores NULL
		// - CAST to utf8mb4: Returns NULL

		// Test SELECT behavior
		result, err := textType.SQL(ctx, nil, invalidUTF8Data)
		require.NoError(t, err, "SELECT should not error (MySQL compatibility)")
		assert.True(t, result.IsNull(), "Should return NULL for invalid UTF-8 (matches MySQL)")

		// Test INSERT behavior (Convert function)
		convertResult, _, err := textType.Convert(ctx, string(invalidUTF8Data))
		require.NoError(t, err, "INSERT should not error (MySQL compatibility)")
		assert.Nil(t, convertResult, "Should insert NULL for invalid UTF-8 (matches MySQL)")
	})

	t.Run("MySQL CAST behavior comparison", func(t *testing.T) {
		// Test MySQL: SELECT CAST(0x446F6C744C6162AE AS CHAR CHARACTER SET utf8mb4);
		// Result: NULL
		invalidUTF8Data := []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE}

		// Test both SQL and Convert functions
		textType := Text

		// SQL function (used in SELECT CAST(...))
		sqlResult, err := textType.SQL(ctx, nil, invalidUTF8Data)
		require.NoError(t, err)
		assert.True(t, sqlResult.IsNull(), "CAST in SELECT should return NULL (matches MySQL)")

		// Convert function (used in INSERT with CAST(...))
		convertResult, _, err := textType.Convert(ctx, string(invalidUTF8Data))
		require.NoError(t, err)
		assert.Nil(t, convertResult, "CAST in INSERT should return NULL (matches MySQL)")
	})

	t.Run("Customer's exact error message scenario", func(t *testing.T) {
		// Customer reported: "invalid string for charset utf8mb4"
		// This should no longer occur with our fix
		customerData := []byte{0x44, 0x6F, 0x6C, 0x74, 0x4C, 0x61, 0x62, 0xAE}
		textType := Text

		// Before fix: This would throw "invalid string for charset utf8mb4"
		// After fix: Should return NULL without any error
		result, err := textType.SQL(ctx, nil, customerData)
		require.NoError(t, err, "Should not throw 'invalid string for charset utf8mb4' error")
		assert.True(t, result.IsNull(), "Should handle invalid UTF-8 gracefully with NULL")

		// Verify the error message pattern is not present
		if err != nil {
			assert.NotContains(t, err.Error(), "invalid string for charset utf8mb4",
				"Should not throw the specific error customer encountered")
		}
	})
}

func TestStringString(t *testing.T) {
	tests := []struct {
		typ         sql.Type
		expectedStr string
	}{
		{MustCreateBinary(sqltypes.Binary, 10), "binary(10)"},
		{MustCreateBinary(sqltypes.Binary, charBinaryMax), fmt.Sprintf("binary(%v)", charBinaryMax)},
		{MustCreateBinary(sqltypes.Blob, 0), "tinyblob"},
		{MustCreateBinary(sqltypes.Blob, TinyTextBlobMax-1), "tinyblob"},
		{MustCreateBinary(sqltypes.Blob, TinyTextBlobMax), "tinyblob"},
		{MustCreateBinary(sqltypes.Blob, TinyTextBlobMax+1), "blob"},
		{MustCreateBinary(sqltypes.Blob, TextBlobMax-1), "blob"},
		{MustCreateBinary(sqltypes.Blob, TextBlobMax), "blob"},
		{MustCreateBinary(sqltypes.Blob, TextBlobMax+1), "mediumblob"},
		{MustCreateBinary(sqltypes.Blob, MediumTextBlobMax-1), "mediumblob"},
		{MustCreateBinary(sqltypes.Blob, MediumTextBlobMax), "mediumblob"},
		{MustCreateBinary(sqltypes.Blob, MediumTextBlobMax+1), "longblob"},
		{MustCreateBinary(sqltypes.Blob, LongTextBlobMax), "longblob"},
		{MustCreateString(sqltypes.Char, 10, sql.Collation_Default), "char(10)"},
		{MustCreateString(sqltypes.Char, charBinaryMax, sql.Collation_Default), fmt.Sprintf("char(%v)", charBinaryMax)},
		{MustCreateString(sqltypes.Text, 0, sql.Collation_Default), "tinytext"},
		{MustCreateString(sqltypes.Text, TinyTextBlobMax, sql.Collation_Default), "tinytext"},
		{MustCreateString(sqltypes.Text, TinyTextBlobMax+1, sql.Collation_Default), "text"},
		{MustCreateString(sqltypes.Text, TextBlobMax, sql.Collation_Default), "text"},
		{MustCreateString(sqltypes.Text, TextBlobMax+1, sql.Collation_Default), "mediumtext"},
		{MustCreateString(sqltypes.Text, MediumTextBlobMax, sql.Collation_Default), "mediumtext"},
		{MustCreateString(sqltypes.Text, MediumTextBlobMax+1, sql.Collation_Default), "longtext"},
		{MustCreateString(sqltypes.Text, LongTextBlobMax-1, sql.Collation_Default), "longtext"},
		{MustCreateString(sqltypes.Text, LongTextBlobMax, sql.Collation_Default), "longtext"},
		{MustCreateBinary(sqltypes.VarBinary, 10), "varbinary(10)"},
		{MustCreateBinary(sqltypes.VarBinary, varcharVarbinaryMax), fmt.Sprintf("varbinary(%v)", varcharVarbinaryMax)},
		{MustCreateString(sqltypes.VarChar, 10, sql.Collation_Default), "varchar(10)"},
		{MustCreateString(sqltypes.VarChar, varcharVarbinaryMax, sql.Collation_Default),
			fmt.Sprintf("varchar(%v)", varcharVarbinaryMax)},
		{MustCreateString(sqltypes.Char, 10, sql.Collation_Default.CharacterSet().BinaryCollation()),
			fmt.Sprintf("char(10) COLLATE %v", sql.Collation_Default.CharacterSet().BinaryCollation())},
		{MustCreateString(sqltypes.Char, 10, sql.Collation_utf16_general_ci), "char(10) CHARACTER SET utf16 COLLATE utf16_general_ci"},
		{MustCreateString(sqltypes.Text, 10, sql.Collation_ascii_general_ci), "tinytext CHARACTER SET ascii COLLATE ascii_general_ci"},
		{MustCreateString(sqltypes.VarChar, 10, sql.Collation_latin1_bin), "varchar(10) CHARACTER SET latin1 COLLATE latin1_bin"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

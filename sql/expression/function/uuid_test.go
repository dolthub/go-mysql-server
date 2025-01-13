// Copyright 2021 Dolthub, Inc.
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

package function

import (
	"regexp"
	"testing"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestUUID(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// Generate a UUID and validate that is a legitimate uuid
	uuidE := NewUUIDFunc()

	result, err := uuidE.Eval(ctx, sql.UntypedSqlRow{nil})
	require.NoError(t, err)

	myUUID := result.(string)
	_, err = uuid.Parse(myUUID)
	require.NoError(t, err)

	// validate that generated uuid is legitimate for IsUUID
	val := NewIsUUID(uuidE)
	require.Equal(t, true, eval(t, val, sql.UntypedSqlRow{nil}))

	// Use a UUID regex as a sanity check
	re2 := regexp.MustCompile(`\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b`)
	require.True(t, re2.MatchString(myUUID))
}

func TestIsUUID(t *testing.T) {
	testCases := []struct {
		name     string
		rowType  sql.Type
		value    interface{}
		expected interface{}
	}{
		{"uuid form 1", types.LongText, "{12345678-1234-5678-1234-567812345678}", true},
		{"uuid form 2", types.LongText, "12345678123456781234567812345678", true},
		{"uuid form 3", types.LongText, "12345678-1234-5678-1234-567812345678", true},
		{"NULL", types.Null, nil, nil},
		{"random int", types.Int8, 1, false},
		{"random bool", types.Boolean, false, false},
		{"random string", types.LongText, "12345678-dasd-fasdf8", false},
		{"swapped uuid", types.LongText, "5678-1234-12345678-1234-567812345678", false},
	}

	for _, tt := range testCases {
		f := NewIsUUID(expression.NewLiteral(tt.value, tt.rowType))

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, sql.UntypedSqlRow{nil}))
		})

		req := require.New(t)
		req.False(f.IsNullable())
	}
}

func TestUUIDToBinValid(t *testing.T) {
	validTestCases := []struct {
		name      string
		uuidType  sql.Type
		uuid      interface{}
		hasSwap   bool
		swapType  sql.Type
		swapValue interface{}
		expected  interface{}
	}{
		{"valid uuid; swap=0", types.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", true, types.Int8, int8(0), "6CCD780CBABA102695645B8C656024DB"},
		{"valid uuid; swap=nil", types.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", true, types.Null, nil, "6CCD780CBABA102695645B8C656024DB"},
		{"valid uuid; swap=1", types.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", true, types.Int8, int8(1), "1026BABA6CCD780C95645B8C656024DB"},
		{"valid uuid; no swap", types.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", false, nil, nil, "6CCD780CBABA102695645B8C656024DB"},
		{"null uuid; no swap", types.Null, nil, false, nil, nil, nil},
	}

	for _, tt := range validTestCases {
		var f sql.Expression
		var err error

		if tt.hasSwap {
			f, err = NewUUIDToBin(expression.NewLiteral(tt.uuid, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		} else {
			f, err = NewUUIDToBin(expression.NewLiteral(tt.uuid, tt.uuidType))
		}

		require.NoError(t, err)

		// Convert to hex to make testing easier
		h := NewHex(f)

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, h, sql.UntypedSqlRow{nil}))
		})

		req := require.New(t)
		req.False(f.IsNullable())
	}
}

func TestUUIDToBinFailing(t *testing.T) {
	failingTestCases := []struct {
		name      string
		uuidType  sql.Type
		uuid      interface{}
		swapType  sql.Type
		swapValue interface{}
	}{
		{"bad swap value", types.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", types.Int8, int8(2)},
		{"bad uuid value", types.LongText, "sdasdsad", types.Int8, int8(0)},
		{"bad uuid value2", types.Int8, int8(0), types.Int8, int8(0)},
	}

	for _, tt := range failingTestCases {
		f, err := NewUUIDToBin(expression.NewLiteral(tt.uuid, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		require.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			_, err := f.Eval(ctx, sql.UntypedSqlRow{nil})
			require.Error(t, err)
		})
	}
}

func TestBinToUUID(t *testing.T) {
	// Test that UUID_TO_BIN to BIN_TO_UUID is reflexive
	uuidE := eval(t, NewUUIDFunc(), sql.UntypedSqlRow{nil})

	f, err := NewUUIDToBin(expression.NewLiteral(uuidE, types.LongText))
	require.NoError(t, err)

	retUUID, err := NewBinToUUID(f)
	require.NoError(t, err)

	require.Equal(t, uuidE, eval(t, retUUID, sql.UntypedSqlRow{nil}))

	// Run UUID_TO_BIN through a series of test cases.
	validTestCases := []struct {
		name      string
		uuidType  sql.Type
		binary    interface{}
		hasSwap   bool
		swapType  sql.Type
		swapValue interface{}
		expected  interface{}
	}{
		{"valid uuid; swap=0", types.MustCreateBinary(query.Type_VARBINARY, int64(16)), []byte("lxºº & d[e`$Û"), true, types.Int8, int8(0), "6c78c2ba-c2ba-2026-2064-5b656024c39b"},
		{"valid uuid; swap=1", types.MustCreateBinary(query.Type_VARBINARY, int64(16)), []byte("&ººlÍxd[e`$Û"), true, types.Int8, int8(1), "ba6cc38d-bac2-26c2-7864-5b656024c39b"},
		{"valid uuid; no swap", types.MustCreateBinary(query.Type_VARBINARY, int64(16)), []byte("lxºº & d[e`$Û"), false, nil, nil, "6c78c2ba-c2ba-2026-2064-5b656024c39b"},
		{"null input", types.Null, nil, false, nil, nil, nil},
	}

	for _, tt := range validTestCases {
		var f sql.Expression
		var err error

		if tt.hasSwap {
			f, err = NewBinToUUID(expression.NewLiteral(tt.binary, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		} else {
			f, err = NewBinToUUID(expression.NewLiteral(tt.binary, tt.uuidType))
		}
		require.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, sql.UntypedSqlRow{nil}))
		})

		req := require.New(t)
		req.False(f.IsNullable())
	}
}

func TestBinToUUIDFailing(t *testing.T) {
	failingTestCases := []struct {
		name      string
		uuidType  sql.Type
		uuid      interface{}
		swapType  sql.Type
		swapValue interface{}
	}{
		{"bad swap value", types.MustCreateBinary(query.Type_VARBINARY, int64(16)), "helo", types.Int8, int8(2)},
		{"bad binary value", types.MustCreateBinary(query.Type_VARBINARY, int64(16)), "sdasdsad", types.Int8, int8(0)},
		{"bad input value", types.Int8, int8(0), types.Int8, int8(0)},
	}

	for _, tt := range failingTestCases {
		f, err := NewBinToUUID(expression.NewLiteral(tt.uuid, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		require.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			_, err := f.Eval(ctx, sql.UntypedSqlRow{nil})
			require.Error(t, err)
		})
	}
}

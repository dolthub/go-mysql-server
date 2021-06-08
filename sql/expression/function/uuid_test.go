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
)

func TestUUID(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// Generate a UUID and validate that is a legitimate uuid
	uuidE := NewUUIDFunc(ctx)

	result, err := uuidE.Eval(ctx, sql.Row{nil})
	require.NoError(t, err)

	myUUID := result.(string)
	_, err = uuid.Parse(myUUID)
	require.NoError(t, err)

	// validate that generated uuid is legitimate for IsUUID
	val := NewIsUUID(ctx, uuidE)
	require.Equal(t, int8(1), eval(t, val, sql.Row{nil}))

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
		{"uuid form 1", sql.LongText, "{12345678-1234-5678-1234-567812345678}", int8(1)},
		{"uuid form 2", sql.LongText, "12345678123456781234567812345678", int8(1)},
		{"uuid form 3", sql.LongText, "12345678-1234-5678-1234-567812345678", int8(1)},
		{"NULL", sql.Null, nil, nil},
		{"random int", sql.Int8, 1, int8(0)},
		{"random bool", sql.Boolean, false, int8(0)},
		{"random string", sql.LongText, "12345678-dasd-fasdf8", int8(0)},
		{"swapped uuid", sql.LongText, "5678-1234-12345678-1234-567812345678", int8(0)},
	}

	for _, tt := range testCases {
		ctx := sql.NewEmptyContext()
		f := NewIsUUID(ctx, expression.NewLiteral(tt.value, tt.rowType))

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, sql.Row{nil}))
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
		{"valid uuid; swap=0", sql.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", true, sql.Int8, int8(0), "6CCD780CBABA102695645B8C656024DB"},
		{"valid uuid; swap=nil", sql.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", true, sql.Null, nil, "6CCD780CBABA102695645B8C656024DB"},
		{"valid uuid; swap=1", sql.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", true, sql.Int8, int8(1), "1026BABA6CCD780C95645B8C656024DB"},
		{"valid uuid; no swap", sql.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", false, nil, nil, "6CCD780CBABA102695645B8C656024DB"},
		{"null uuid; no swap", sql.Null, nil, false, nil, nil, nil},
	}

	for _, tt := range validTestCases {
		var f sql.Expression
		var err error

		ctx := sql.NewEmptyContext()
		if tt.hasSwap {
			f, err = NewUUIDToBin(ctx, expression.NewLiteral(tt.uuid, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		} else {
			f, err = NewUUIDToBin(ctx, expression.NewLiteral(tt.uuid, tt.uuidType))
		}

		require.NoError(t, err)

		// Convert to hex to make testing easier
		h := NewHex(ctx, f)

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, h, sql.Row{nil}))
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
		{"bad swap value", sql.LongText, "6ccd780c-baba-1026-9564-5b8c656024db", sql.Int8, int8(2)},
		{"bad uuid value", sql.LongText, "sdasdsad", sql.Int8, int8(0)},
		{"bad uuid value2", sql.Int8, int8(0), sql.Int8, int8(0)},
	}

	for _, tt := range failingTestCases {
		ctx := sql.NewEmptyContext()
		f, err := NewUUIDToBin(ctx, expression.NewLiteral(tt.uuid, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		require.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			_, err := f.Eval(ctx, sql.Row{nil})
			require.Error(t, err)
		})
	}
}

func TestBinToUUID(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// Test that UUID_TO_BIN to BIN_TO_UUID is reflexive
	uuidE := eval(t, NewUUIDFunc(ctx), sql.Row{nil})

	f, err := NewUUIDToBin(ctx, expression.NewLiteral(uuidE, sql.LongText))
	require.NoError(t, err)

	retUUID, err := NewBinToUUID(ctx, f)
	require.NoError(t, err)

	require.Equal(t, uuidE, eval(t, retUUID, sql.Row{nil}))

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
		{"valid uuid; swap=0", sql.MustCreateBinary(query.Type_VARBINARY, int64(16)), []byte("lxºº & d[e`$Û"), true, sql.Int8, int8(0), "6c78c2ba-c2ba-2026-2064-5b656024c39b"},
		{"valid uuid; swap=1", sql.MustCreateBinary(query.Type_VARBINARY, int64(16)), []byte("&ººlÍxd[e`$Û"), true, sql.Int8, int8(1), "ba6cc38d-bac2-26c2-7864-5b656024c39b"},
		{"valid uuid; no swap", sql.MustCreateBinary(query.Type_VARBINARY, int64(16)), []byte("lxºº & d[e`$Û"), false, nil, nil, "6c78c2ba-c2ba-2026-2064-5b656024c39b"},
		{"null input", sql.Null, nil, false, nil, nil, nil},
	}

	for _, tt := range validTestCases {
		var f sql.Expression
		var err error

		ctx := sql.NewEmptyContext()
		if tt.hasSwap {
			f, err = NewBinToUUID(ctx, expression.NewLiteral(tt.binary, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		} else {
			f, err = NewBinToUUID(ctx, expression.NewLiteral(tt.binary, tt.uuidType))
		}
		require.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, eval(t, f, sql.Row{nil}))
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
		{"bad swap value", sql.MustCreateBinary(query.Type_VARBINARY, int64(16)), "helo", sql.Int8, int8(2)},
		{"bad binary value", sql.MustCreateBinary(query.Type_VARBINARY, int64(16)), "sdasdsad", sql.Int8, int8(0)},
		{"bad input value", sql.Int8, int8(0), sql.Int8, int8(0)},
	}

	for _, tt := range failingTestCases {
		ctx := sql.NewEmptyContext()
		f, err := NewBinToUUID(ctx, expression.NewLiteral(tt.uuid, tt.uuidType), expression.NewLiteral(tt.swapValue, tt.swapType))
		require.NoError(t, err)

		t.Run(tt.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			_, err := f.Eval(ctx, sql.Row{nil})
			require.Error(t, err)
		})
	}
}

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
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestEnumCompare(t *testing.T) {
	tests := []struct {
		vals        []string
		collation   sql.CollationID
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{[]string{"one", "two"}, sql.Collation_Default, nil, 1, 1},
		{[]string{"one", "two"}, sql.Collation_Default, "one", nil, -1},
		{[]string{"one", "two"}, sql.Collation_Default, nil, nil, 0},
		{[]string{"one", "two"}, sql.Collation_Default, 1, "two", -1},
		{[]string{"one", "two"}, sql.Collation_Default, 2, []byte("one"), 1},
		{[]string{"one", "two"}, sql.Collation_Default, "one", 1, 0},
		{[]string{"one", "two"}, sql.Collation_Default, "one", "two", -1},
		{[]string{"two", "one"}, sql.Collation_Default, "two", "one", -1},
		{[]string{"0", "1", "2"}, sql.Collation_Default, 3, "2", 0},
		{[]string{"0", "1", "2"}, sql.Collation_Default, 2, "1", 0},
		{[]string{"0", "1", "2"}, sql.Collation_Default, "3", "2", 0},
		{[]string{"one", "two"}, sql.Collation_Default, "ten", "twenty", 0},
		{[]string{"one", "two"}, sql.Collation_Default, "one", "hundred", 1},
		{[]string{"one", "two"}, sql.Collation_Default, "hundred", "one", -1},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v %v", test.vals, test.collation, test.val1, test.val2), func(t *testing.T) {
			typ := MustCreateEnumType(test.vals, test.collation)
			cmp, err := typ.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestEnumCreate(t *testing.T) {
	tests := []struct {
		vals               []string
		collation          sql.CollationID
		expectedValToIndex map[string]int
		expectedErr        bool
	}{
		{[]string{"one"}, sql.Collation_Default, map[string]int{"one": 1}, false},
		{[]string{" one ", "  two  "}, sql.Collation_Default,
			map[string]int{" one": 1, "  two": 2}, false},
		{[]string{"0", "1", "2"}, sql.Collation_Default,
			map[string]int{"0": 1, "1": 2, "2": 3}, false},
		{[]string{"one", "one "}, sql.Collation_binary,
			map[string]int{"one": 1, "one ": 2}, false},
		{[]string{}, sql.Collation_Default, nil, true},
		{[]string{"one", "one"}, sql.Collation_Default, nil, true},
		{[]string{"one", "one "}, sql.Collation_Default, nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.vals, test.collation, test.expectedValToIndex), func(t *testing.T) {
			typ, err := CreateEnumType(test.vals, test.collation)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.True(t, test.collation.Equals(typ.Collation()))
				for val, i := range test.expectedValToIndex {
					str, ok := typ.At(i)
					if assert.True(t, ok) {
						assert.Equal(t, val, str)
					}
					index := typ.IndexOf(val)
					assert.Equal(t, i, index)
				}
			}
		})
	}
}

func TestEnumCreateTooLarge(t *testing.T) {
	vals := make([]string, 65536)
	for i := range vals {
		vals[i] = strconv.Itoa(i)
	}
	_, err := CreateEnumType(vals, sql.Collation_Default)
	require.Error(t, err)
}

func TestEnumConvert(t *testing.T) {
	tests := []struct {
		vals        []string
		collation   sql.CollationID
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{[]string{"one", "two"}, sql.Collation_Default, nil, nil, false},
		{[]string{"one", "two"}, sql.Collation_Default, int(1), "one", false},
		{[]string{"one", "two"}, sql.Collation_Default, int8(2), "two", false},
		{[]string{"one", "two"}, sql.Collation_Default, int16(1), "one", false},
		{[]string{"one", "two"}, sql.Collation_Default, int32(2), "two", false},
		{[]string{"one", "two"}, sql.Collation_Default, int64(1), "one", false},
		{[]string{"one", "two"}, sql.Collation_Default, uint(2), "two", false},
		{[]string{"one", "two"}, sql.Collation_Default, uint8(1), "one", false},
		{[]string{"one", "two"}, sql.Collation_Default, uint16(2), "two", false},
		{[]string{"one", "two"}, sql.Collation_Default, uint32(1), "one", false},
		{[]string{"one", "two"}, sql.Collation_Default, uint64(2), "two", false},
		{[]string{"one", "two"}, sql.Collation_Default, "one", "one", false},
		{[]string{"one", "two"}, sql.Collation_Default, []byte("two"), "two", false},
		{[]string{"0", "1", "2"}, sql.Collation_Default, 3, "2", false},
		{[]string{"0", "1", "2"}, sql.Collation_Default, 2, "1", false},
		{[]string{"0", "1", "2"}, sql.Collation_Default, "3", "2", false},
		{[]string{"0", "1", "2"}, sql.Collation_Default, "2", "2", false},

		{[]string{"one", "two"}, sql.Collation_Default, 3, nil, true},
		{[]string{"one", "two"}, sql.Collation_Default, 0, nil, true},
		{[]string{"one", "two"}, sql.Collation_Default, "three", nil, true},
		{[]string{"one", "two"}, sql.Collation_Default, time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.vals, test.collation, test.val), func(t *testing.T) {
			typ := MustCreateEnumType(test.vals, test.collation)
			val, err := typ.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				if test.val != nil {
					umar, ok := typ.At(int(val.(uint16)))
					require.True(t, ok)
					cmp, err := typ.Compare(test.val, umar)
					require.NoError(t, err)
					assert.Equal(t, 0, cmp)
					assert.Equal(t, typ.ValueType(), reflect.TypeOf(val))
				} else {
					assert.Equal(t, test.expectedVal, val)
				}
			}
		})
	}
}

func TestEnumString(t *testing.T) {
	tests := []struct {
		vals        []string
		collation   sql.CollationID
		expectedStr string
	}{
		{[]string{"one"}, sql.Collation_Default, "enum('one')"},
		{[]string{"مرحبا", "こんにちは"}, sql.Collation_Default, "enum('مرحبا','こんにちは')"},
		{[]string{" hi ", "  lo  "}, sql.Collation_Default, "enum(' hi','  lo')"},
		{[]string{"a"}, sql.Collation_Default.CharacterSet().BinaryCollation(),
			fmt.Sprintf("enum('a') COLLATE %v", sql.Collation_Default.CharacterSet().BinaryCollation())},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.vals, test.collation), func(t *testing.T) {
			typ := MustCreateEnumType(test.vals, test.collation)
			assert.Equal(t, test.expectedStr, typ.String())
		})
	}
}

func TestEnumZero(t *testing.T) {
	tests := []struct {
		vals []string
	}{
		{[]string{"a"}},
		{[]string{"a", "b"}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v ok", test.vals), func(t *testing.T) {
			typ := MustCreateEnumType(test.vals, sql.Collation_Default)
			v, ok := typ.Zero().(uint16)
			assert.True(t, ok)
			assert.Equal(t, uint16(1), v)
		})
	}
}

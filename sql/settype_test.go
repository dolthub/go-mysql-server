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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetCompare(t *testing.T) {
	tests := []struct {
		vals        []string
		collation   CollationID
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{[]string{"one", "two"}, Collation_Default, nil, 1, 1},
		{[]string{"one", "two"}, Collation_Default, "one", nil, -1},
		{[]string{"one", "two"}, Collation_Default, nil, nil, 0},
		{[]string{"one", "two"}, Collation_Default, 0, "one", -1},
		{[]string{"one", "two"}, Collation_Default, 1, "two", -1},
		{[]string{"one", "two"}, Collation_Default, 2, []byte("one"), 1},
		{[]string{"one", "two"}, Collation_Default, "one", "", 1},
		{[]string{"one", "two"}, Collation_Default, "one", 1, 0},
		{[]string{"one", "two"}, Collation_Default, "one", "two", -1},
		{[]string{"two", "one"}, Collation_binary, "two", "one", -1},
		{[]string{"one", "two"}, Collation_Default, 3, "one,two", 0},
		{[]string{"one", "two"}, Collation_Default, "two,one,two", "one,two", 0},
		{[]string{"one", "two"}, Collation_Default, "two", "", 1},
		{[]string{"one", "two"}, Collation_Default, "one,two", "two", 1},
		{[]string{"a", "b", "c"}, Collation_Default, "a,b", "b,c", -1},
		{[]string{"a", "b", "c"}, Collation_Default, "a,b,c", "c,c,b", 1},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v %v", test.vals, test.collation, test.val1, test.val2), func(t *testing.T) {
			typ := MustCreateSetType(test.vals, test.collation)
			cmp, err := typ.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestSetCompareErrors(t *testing.T) {
	tests := []struct {
		vals      []string
		collation CollationID
		val1      interface{}
		val2      interface{}
	}{
		{[]string{"one", "two"}, Collation_Default, "three", "two"},
		{[]string{"one", "two"}, Collation_Default, time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), []byte("one")},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v %v", test.vals, test.collation, test.val1, test.val2), func(t *testing.T) {
			typ := MustCreateSetType(test.vals, test.collation)
			_, err := typ.Compare(test.val1, test.val2)
			require.Error(t, err)
		})
	}
}

func TestSetCreate(t *testing.T) {
	tests := []struct {
		vals         []string
		collation    CollationID
		expectedVals map[string]uint64
		expectedErr  bool
	}{
		{[]string{"one"}, Collation_Default,
			map[string]uint64{"one": 1}, false},
		{[]string{" one ", "  two  "}, Collation_Default,
			map[string]uint64{" one": 1, "  two": 2}, false},
		{[]string{"a", "b", "c"}, Collation_Default,
			map[string]uint64{"a": 1, "b": 2, "c": 4}, false},
		{[]string{"one", "one "}, Collation_binary, map[string]uint64{"one": 1, "one ": 2}, false},
		{[]string{"one", "One"}, Collation_binary, map[string]uint64{"one": 1, "One": 2}, false},

		{[]string{}, Collation_Default, nil, true},
		{[]string{"one", "one"}, Collation_Default, nil, true},
		{[]string{"one", "one"}, Collation_binary, nil, true},
		{[]string{"one", "One"}, Collation_utf8mb4_general_ci, nil, true},
		{[]string{"one", "one "}, Collation_Default, nil, true},
		{[]string{"one", "two,"}, Collation_Default, nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.vals, test.collation), func(t *testing.T) {
			typ, err := CreateSetType(test.vals, test.collation)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				concreteType, ok := typ.(setType)
				require.True(t, ok)
				assert.True(t, test.collation.Equals(typ.Collation()))
				for val, bit := range test.expectedVals {
					bitField, err := concreteType.convertStringToBitField(val)
					if assert.NoError(t, err) {
						assert.Equal(t, bit, bitField)
					}
					str, err := concreteType.convertBitFieldToString(bit)
					if assert.NoError(t, err) {
						assert.Equal(t, val, str)
					}
				}
			}
		})
	}
}

func TestSetCreateTooLarge(t *testing.T) {
	vals := make([]string, 65)
	for i := range vals {
		vals[i] = strconv.Itoa(i)
	}
	_, err := CreateSetType(vals, Collation_Default)
	require.Error(t, err)
}

func TestSetConvert(t *testing.T) {
	tests := []struct {
		vals        []string
		collation   CollationID
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{[]string{"one", "two"}, Collation_Default, nil, nil, false},
		{[]string{"one", "two"}, Collation_Default, "", "", false},
		{[]string{"one", "two"}, Collation_Default, int(0), "", false},
		{[]string{"one", "two"}, Collation_Default, int8(2), "two", false},
		{[]string{"one", "two"}, Collation_Default, int16(1), "one", false},
		{[]string{"one", "two"}, Collation_binary, int32(2), "two", false},
		{[]string{"one", "two"}, Collation_Default, int64(1), "one", false},
		{[]string{"one", "two"}, Collation_Default, uint(2), "two", false},
		{[]string{"one", "two"}, Collation_binary, uint8(1), "one", false},
		{[]string{"one", "two"}, Collation_Default, uint16(2), "two", false},
		{[]string{"one", "two"}, Collation_binary, uint32(3), "one,two", false},
		{[]string{"one", "two"}, Collation_Default, uint64(2), "two", false},
		{[]string{"one", "two"}, Collation_Default, "one", "one", false},
		{[]string{"one", "two"}, Collation_Default, []byte("two"), "two", false},
		{[]string{"one", "two"}, Collation_Default, "one,two", "one,two", false},
		{[]string{"one", "two"}, Collation_binary, "two,one", "one,two", false},
		{[]string{"one", "two"}, Collation_Default, "one,two,one", "one,two", false},
		{[]string{"one", "two"}, Collation_binary, "two,one,two", "one,two", false},
		{[]string{"one", "two"}, Collation_Default, "two,one,two", "one,two", false},
		{[]string{"a", "b", "c"}, Collation_Default, "b,c  ,a", "a,b,c", false},
		{[]string{"one", "two"}, Collation_utf8mb4_general_ci, "ONE", "one", false},
		{[]string{"ONE", "two"}, Collation_utf8mb4_general_ci, "one", "ONE", false},

		{[]string{"one", "two"}, Collation_Default, 4, nil, true},
		{[]string{"one", "two"}, Collation_Default, "three", nil, true},
		{[]string{"one", "two"}, Collation_Default, "one,two,three", nil, true},
		{[]string{"a", "b", "c"}, Collation_binary, "b,c  ,a", nil, true},
		{[]string{"one", "two"}, Collation_binary, "ONE", nil, true},
		{[]string{"one", "two"}, Collation_Default, time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v | %v | %v", test.vals, test.collation, test.val), func(t *testing.T) {
			typ := MustCreateSetType(test.vals, test.collation)
			val, err := typ.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				res, err := typ.Compare(test.expectedVal, val)
				require.NoError(t, err)
				assert.Equal(t, 0, res)
				if val != nil {
					assert.Equal(t, typ.ValueType(), reflect.TypeOf(val))
				}
			}
		})
	}
}

func TestSetMarshalMax(t *testing.T) {
	vals := make([]string, 64)
	for i := range vals {
		vals[i] = strconv.Itoa(i)
	}
	typ, err := CreateSetType(vals, Collation_Default)
	require.NoError(t, err)

	tests := []string{
		"",
		"1",
		"1,2",
		"0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63",
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test), func(t *testing.T) {
			bits, err := typ.Convert(test)
			require.NoError(t, err)
			res1, err := typ.BitsToString(bits.(uint64))
			require.NoError(t, err)
			require.Equal(t, test, res1)
			bits2, err := typ.Convert(bits)
			require.NoError(t, err)
			res2, err := typ.BitsToString(bits2.(uint64))
			require.NoError(t, err)
			require.Equal(t, test, res2)
		})
	}
}

func TestSetString(t *testing.T) {
	tests := []struct {
		vals        []string
		collation   CollationID
		expectedStr string
	}{
		{[]string{"one"}, Collation_Default, "set('one')"},
		{[]string{"مرحبا", "こんにちは"}, Collation_Default, "set('مرحبا','こんにちは')"},
		{[]string{" hi ", "  lo  "}, Collation_Default, "set(' hi','  lo')"},
		{[]string{" hi ", "  lo  "}, Collation_binary, "set(' hi ','  lo  ') CHARACTER SET binary COLLATE binary"},
		{[]string{"a"}, Collation_Default.CharacterSet().BinaryCollation(),
			fmt.Sprintf("set('a') COLLATE %v", Collation_Default.CharacterSet().BinaryCollation())},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.vals, test.collation), func(t *testing.T) {
			typ := MustCreateSetType(test.vals, test.collation)
			assert.Equal(t, test.expectedStr, typ.String())
		})
	}
}

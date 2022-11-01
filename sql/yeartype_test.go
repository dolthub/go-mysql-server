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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestYearCompare(t *testing.T) {
	tests := []struct {
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{nil, 0, 1},
		{0, nil, -1},
		{nil, nil, 0},
		{1, 70, 1},
		{80, 30, -1},
		{0, "0", -1},
		{2050, 50, 0},
		{"2050", "2050", 0},
		{10, time.Date(2010, 1, 2, 3, 4, 5, 0, time.UTC), 0},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := Year.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestYearConvert(t *testing.T) {
	tests := []struct {
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{int(0), int16(0), false},
		{uint(1), int16(2001), false},
		{int8(31), int16(2031), false},
		{uint8(32), int16(2032), false},
		{int16(69), int16(2069), false},
		{uint16(70), int16(1970), false},
		{uint16(99), int16(1999), false},
		{int32(1901), int16(1901), false},
		{uint32(2000), int16(2000), false},
		{int64(2100), int16(2100), false},
		{uint64(2155), int16(2155), false},
		{"0", int16(2000), false},
		{"1", int16(2001), false},
		{"31", int16(2031), false},
		{"32", int16(2032), false},
		{"69", int16(2069), false},
		{"70", int16(1970), false},
		{"99", int16(1999), false},
		{"1901", int16(1901), false},
		{"2000", int16(2000), false},
		{"2100", int16(2100), false},
		{"2155", int16(2155), false},
		{time.Date(2010, 1, 2, 3, 4, 5, 0, time.UTC), int16(2010), false},

		{100, nil, true},
		{"100", nil, true},
		{1850, nil, true},
		{"1850", nil, true},
		{[]byte{0}, nil, true},
		{false, nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val, test.expectedVal), func(t *testing.T) {
			val, err := Year.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
				if val != nil {
					assert.Equal(t, Year.ValueType(), reflect.TypeOf(val))
				}
			}
		})
	}
}

func TestYearString(t *testing.T) {
	require.Equal(t, "year", Year.String())
}

func TestYearZero(t *testing.T) {
	_, ok := Year.Zero().(int16)
	require.True(t, ok)
}

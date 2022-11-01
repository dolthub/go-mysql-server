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

func TestBitCompare(t *testing.T) {
	tests := []struct {
		typ         Type
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{MustCreateBitType(1), nil, 0, 1},
		{MustCreateBitType(1), 0, nil, -1},
		{MustCreateBitType(1), nil, nil, 0},
		{MustCreateBitType(1), 0, 1, -1},
		{MustCreateBitType(10), 0, true, -1},
		{MustCreateBitType(64), false, 1, -1},
		{MustCreateBitType(1), 1, 0, 1},
		{MustCreateBitType(10), true, False, 1},
		{MustCreateBitType(64), 1, false, 1},
		{MustCreateBitType(1), 1, 1, 0},
		{MustCreateBitType(10), true, 1, 0},
		{MustCreateBitType(64), True, true, 0},
		{MustCreateBitType(1), true, true, 0},
		{MustCreateBitType(1), false, false, 0},
		{MustCreateBitType(64), 0x12345de, 0xed54321, -1},
		{MustCreateBitType(64), 0xed54321, 0x12345de, 1},
		{MustCreateBitType(64), 3848, 3848, 0},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := test.typ.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestBitCreate(t *testing.T) {
	tests := []struct {
		numOfBits    uint8
		expectedType bitType
		expectedErr  bool
	}{
		{1, bitType{1}, false},
		{10, bitType{10}, false},
		{64, bitType{64}, false},
		{0, bitType{}, true},
		{65, bitType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.numOfBits, test.expectedType), func(t *testing.T) {
			typ, err := CreateBitType(test.numOfBits)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestBitConvert(t *testing.T) {
	tests := []struct {
		typ         Type
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{MustCreateBitType(1), nil, nil, false},
		{MustCreateBitType(1), true, uint64(1), false},
		{MustCreateBitType(1), int32(0), uint64(0), false},
		{MustCreateBitType(1), uint16(1), uint64(1), false},
		{MustCreateBitType(1), false, uint64(0), false},
		{MustCreateBitType(1), true, uint64(1), false},
		{MustCreateBitType(10), int(33), uint64(33), false},
		{MustCreateBitType(11), int8(34), uint64(34), false},
		{MustCreateBitType(12), int16(35), uint64(35), false},
		{MustCreateBitType(13), uint8(36), uint64(36), false},
		{MustCreateBitType(14), uint32(37), uint64(37), false},
		{MustCreateBitType(15), uint(38), uint64(38), false},
		{MustCreateBitType(64), uint64(18446744073709551615), uint64(18446744073709551615), false},
		{MustCreateBitType(64), float32(893.22356), uint64(893), false},
		{MustCreateBitType(64), float64(79234.356), uint64(79234), false},
		{MustCreateBitType(21), "32", uint64(13106), false},
		{MustCreateBitType(64), "12341234", uint64(3544952155950691124), false},
		{MustCreateBitType(64), -1, uint64(18446744073709551615), false},
		{MustCreateBitType(22), []byte{36, 107}, uint64(9323), false},
		{MustCreateBitType(1), int64(2), nil, true},
		{MustCreateBitType(20), 47202753, nil, true},
		{MustCreateBitType(64), float64(-1.0), nil, true},
		{MustCreateBitType(21), "324", nil, true},
		{MustCreateBitType(60), "12341234", nil, true},
		{MustCreateBitType(64), "123412341", nil, true},
		{MustCreateBitType(22), []byte{36, 107, 48, 38}, nil, true},
		{MustCreateBitType(64), time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.typ, test.val, test.expectedVal), func(t *testing.T) {
			val, err := test.typ.Convert(test.val)
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

func TestBitString(t *testing.T) {
	tests := []struct {
		typ         Type
		expectedStr string
	}{
		{MustCreateBitType(1), "bit(1)"},
		{MustCreateBitType(10), "bit(10)"},
		{MustCreateBitType(32), "bit(32)"},
		{MustCreateBitType(64), "bit(64)"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

func TestBitZero(t *testing.T) {
	_, ok := MustCreateBitType(1).Zero().(uint64)
	require.True(t, ok)
}

package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBitCompare(t *testing.T) {
	tests := []struct {
		typ Type
		val1 interface{}
		val2 interface{}
		expectedCmp int
	}{
		{MustCreateBitType(1), nil, 0, -1},
		{MustCreateBitType(1), 0, nil, 1},
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
		numOfBits uint8
		expectedType bitType
		expectedErr bool
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
		typ Type
		val interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{MustCreateBitType(1), nil, nil, false},
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
		{MustCreateBitType(1), int64(2), nil, true},
		{MustCreateBitType(20), 47202753, nil, true},
		{MustCreateBitType(21), "32", nil, true},
		{MustCreateBitType(22), []byte{32}, nil, true},
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
			}
		})
	}
}

func TestBitString(t *testing.T) {
	tests := []struct {
		typ Type
		expectedStr string
	}{
		{MustCreateBitType(1), "BIT(1)"},
		{MustCreateBitType(10), "BIT(10)"},
		{MustCreateBitType(32), "BIT(32)"},
		{MustCreateBitType(64), "BIT(64)"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.typ, test.expectedStr), func(t *testing.T) {
			str := test.typ.String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}
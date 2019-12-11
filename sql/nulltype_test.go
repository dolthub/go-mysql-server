package sql

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNullCompare(t *testing.T) {
	tests := []struct {
		val1 interface{}
		val2 interface{}
	}{
		{true, 1},
		{"blah", nil},
		{time.Now(), []byte{0}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := Null.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, 0, cmp)
		})
	}
}

func TestNullConvert(t *testing.T) {
	tests := []struct {
		val interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{nil, nil, false},
		{int(0), nil, true},
		{int8(0), nil, true},
		{int16(0), nil, true},
		{int32(0), nil, true},
		{int64(0), nil, true},
		{uint(0), nil, true},
		{uint8(0), nil, true},
		{uint16(0), nil, true},
		{uint32(0), nil, true},
		{uint64(0), nil, true},
		{float32(0), nil, true},
		{float64(0), nil, true},
		{"stuff", nil, true},
		{[]byte{0}, nil, true},
		{time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val, test.expectedVal), func(t *testing.T) {
			val, err := Null.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
			}
		})
	}
}

func TestNullString(t *testing.T) {
	require.Equal(t, "NULL", Null.String())
}
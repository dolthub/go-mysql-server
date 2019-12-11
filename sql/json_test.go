package sql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJsonCompare(t *testing.T) {
	tests := []struct {
		val1 interface{}
		val2 interface{}
		expectedCmp int
	}{
		{[]byte("A"), []byte("B"), -1},
		{[]byte("A"), []byte("A"), 0},
		{[]byte("C"), []byte("B"), 1},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := JSON.Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestJsonConvert(t *testing.T) {
	tests := []struct {
		val interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{"", []byte(`""`), false},
		{[]int{1, 2}, []byte("[1,2]"), false},
		{`{"a": true, "b": 3}`, []byte(`{"a":true,"b":3}`), false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val, test.expectedVal), func(t *testing.T) {
			val, err := JSON.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedVal, val)
			}
		})
	}
}

func TestJsonString(t *testing.T) {
	require.Equal(t, "JSON", JSON.String())
}
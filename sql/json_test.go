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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJsonCompare(t *testing.T) {
	tests := []struct {
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{nil, 0, -1},
		{0, nil, 1},
		{nil, nil, 0},
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
		val         interface{}
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

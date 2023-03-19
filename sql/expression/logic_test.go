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

package expression

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestAnd(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right interface{}
		expected    interface{}
	}{
		{"left is true, right is false", true, false, false},
		{"left is true, right is null", true, nil, nil},
		{"left is false, right is true", false, true, false},
		{"left is null, right is true", nil, true, nil},
		{"left is false, right is null", false, nil, false},
		{"left is null, right is false", nil, false, false},
		{"both true", true, true, true},
		{"both false", false, false, false},
		{"both nil", nil, nil, nil},
		{"left is string, right is string", "dsdad", "dasa", false},
		{"left is string, right is nil", "ads", nil, false},
		{"left is nil, right is string", nil, "dada", false},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewAnd(
				NewLiteral(tt.left, types.Boolean),
				NewLiteral(tt.right, types.Boolean),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestOr(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right interface{}
		expected    interface{}
	}{
		{"left is true, right is false", true, false, true},
		{"left is null, right is true", nil, true, true},
		{"left is null, right is false", nil, false, nil},
		{"left is false, right is true", false, true, true},
		{"left is true, right is null", true, nil, true},
		{"left is false, right is null", false, nil, nil},
		{"both true", true, true, true},
		{"both false", false, false, false},
		{"both null", nil, nil, nil},
		{"left is string, right is different string", "abc", "def", false},
		{"left is string, right is nil", "abc", nil, nil},
		{"left is nil, right is string", nil, "def", nil},
		{"left is float, right is string", 2.0, "hello", true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewOr(
				NewLiteral(tt.left, types.Boolean),
				NewLiteral(tt.right, types.Boolean),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestXor(t *testing.T) {
	var testCases = []struct {
		name        string
		left, right interface{}
		expected    interface{}
	}{
		{"left is true, right is false", true, false, true},
		{"left is null, right is true", nil, true, nil},
		{"left is null, right is false", nil, false, nil},
		{"left is false, right is true", false, true, true},
		{"left is true, right is null", true, nil, nil},
		{"left is false, right is null", false, nil, nil},
		{"both true", true, true, false},
		{"both false", false, false, false},
		{"both null", nil, nil, nil},
		{"left is string, right is different string", "abc", "def", false},
		{"left is string, right is nil", "abc", nil, nil},
		{"left is nil, right is string", nil, "def", nil},
		{"left is float, right is string", 2.0, "hello", true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := NewXor(
				NewLiteral(tt.left, types.Boolean),
				NewLiteral(tt.right, types.Boolean),
			).Eval(sql.NewEmptyContext(), sql.NewRow())
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestJoinAnd(t *testing.T) {
	require := require.New(t)

	require.Nil(JoinAnd())

	require.Equal(
		NewNot(nil),
		JoinAnd(NewNot(nil)),
	)

	require.Equal(
		NewAnd(
			NewAnd(
				NewIsNull(nil),
				NewEquals(nil, nil),
			),
			NewNot(nil),
		),
		JoinAnd(
			NewIsNull(nil),
			NewEquals(nil, nil),
			NewNot(nil),
		),
	)
}

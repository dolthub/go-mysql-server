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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestBetween(t *testing.T) {
	b := NewBetween(
		NewGetField(0, types.Int64, "val", true),
		NewGetField(1, types.Int64, "lower", true),
		NewGetField(2, types.Int64, "upper", true),
	)

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"val is null", sql.NewRow(nil, 1, 2), nil, false},
		{"lower is null, out of range", sql.NewRow(1, nil, 2), nil, false},
		{"lower is null, in range", sql.NewRow(2, nil, 2), nil, false},
		{"upper is null, out of range", sql.NewRow(1, 2, nil), false, false},
		{"upper is null, in range", sql.NewRow(2, 2, nil), nil, false},
		{"val is lower", sql.NewRow(1, 1, 3), true, false},
		{"val is upper", sql.NewRow(3, 1, 3), true, false},
		{"val is between lower and upper", sql.NewRow(2, 1, 3), true, false},
		{"val is less than lower", sql.NewRow(0, 1, 3), false, false},
		{"val is more than upper", sql.NewRow(4, 1, 3), false, false},
		{"val type is different than lower", sql.NewRow(4, "lower", 3), false, true},
		{"val type is different than upper", sql.NewRow(4, 1, "upper"), false, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := b.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

func TestBetweenIsNullable(t *testing.T) {
	testCases := []struct {
		name     string
		b        *Between
		nullable bool
	}{
		{
			"val is nullable",
			NewBetween(
				NewGetField(0, types.Int64, "foo", true),
				NewLiteral(1, types.Int64),
				NewLiteral(2, types.Int64),
			),
			true,
		},
		{
			"lower is nullable",
			NewBetween(
				NewLiteral(1, types.Int64),
				NewGetField(0, types.Int64, "foo", true),
				NewLiteral(2, types.Int64),
			),
			true,
		},
		{
			"upper is nullable",
			NewBetween(
				NewLiteral(1, types.Int64),
				NewLiteral(2, types.Int64),
				NewGetField(0, types.Int64, "foo", true),
			),
			true,
		},
		{
			"all are not nullable",
			NewBetween(
				NewLiteral(1, types.Int64),
				NewLiteral(2, types.Int64),
				NewLiteral(3, types.Int64),
			),
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.nullable, tt.b.IsNullable())
		})
	}
}

func TestBetweenResolved(t *testing.T) {
	testCases := []struct {
		name     string
		b        *Between
		resolved bool
	}{
		{
			"val is unresolved",
			NewBetween(
				NewUnresolvedColumn("foo"),
				NewLiteral(1, types.Int64),
				NewLiteral(2, types.Int64),
			),
			false,
		},
		{
			"lower is unresolved",
			NewBetween(
				NewLiteral(1, types.Int64),
				NewUnresolvedColumn("foo"),
				NewLiteral(2, types.Int64),
			),
			false,
		},
		{
			"upper is unresolved",
			NewBetween(
				NewLiteral(1, types.Int64),
				NewLiteral(2, types.Int64),
				NewUnresolvedColumn("foo"),
			),
			false,
		},
		{
			"all are resolved",
			NewBetween(
				NewLiteral(1, types.Int64),
				NewLiteral(2, types.Int64),
				NewLiteral(3, types.Int64),
			),
			true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.resolved, tt.b.Resolved())
		})
	}
}

func TestNotBetween(t *testing.T) {
	n := NewNot(NewBetween(
		NewGetField(0, types.Int64, "val", true),
		NewGetField(1, types.Int64, "lower", true),
		NewGetField(2, types.Int64, "upper", true),
	))

	testCases := []struct {
		name     string
		row      sql.Row
		expected interface{}
		err      bool
	}{
		{"val is null", sql.NewRow(nil, 1, 2), nil, false},
		{"lower is null, out of range", sql.NewRow(1, nil, 2), nil, false},
		{"lower is null, in range", sql.NewRow(2, nil, 2), nil, false},
		{"upper is null, out of range", sql.NewRow(1, 2, nil), true, false},
		{"upper is null, in range", sql.NewRow(2, 2, nil), nil, false},
		{"val is lower", sql.NewRow(1, 1, 3), false, false},
		{"val is upper", sql.NewRow(3, 1, 3), false, false},
		{"val is between lower and upper", sql.NewRow(2, 1, 3), false, false},
		{"val is less than lower", sql.NewRow(0, 1, 3), true, false},
		{"val is more than upper", sql.NewRow(4, 1, 3), true, false},
		{"val type is different than lower", sql.NewRow(4, "lower", 3), true, true},
		{"val type is different than upper", sql.NewRow(4, 1, "upper"), true, true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result, err := n.Eval(sql.NewEmptyContext(), tt.row)
			if tt.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(tt.expected, result)
			}
		})
	}
}

// Copyright 2026 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

func TestHiddenSystemColumnName(t *testing.T) {
	require.Equal(t, "!hidden!idx1!0!0", HiddenSystemColumnName("idx1", 0))
	require.Equal(t, "!hidden!idx1!2!0", HiddenSystemColumnName("idx1", 2))
	// Index names are lowercased, matching MySQL's case-insensitive identifier semantics.
	require.Equal(t, "!hidden!idx1!0!0", HiddenSystemColumnName("IDX1", 0))
}

func TestIsHiddenSystemColumnForIndex(t *testing.T) {
	require.True(t, IsHiddenSystemColumnForIndex("!hidden!idx1!0!0", "idx1"))
	require.True(t, IsHiddenSystemColumnForIndex("!hidden!idx1!2!0", "idx1"))
	// Case-insensitive on both the column name and the index name.
	require.True(t, IsHiddenSystemColumnForIndex("!HIDDEN!IDX1!0!0", "idx1"))
	require.True(t, IsHiddenSystemColumnForIndex("!hidden!idx1!0!0", "IDX1"))

	// Must not match a different index's hidden columns, even with a name that's a prefix of
	// this one (e.g. "idx1" vs "idx10")
	require.False(t, IsHiddenSystemColumnForIndex("!hidden!idx2!0!0", "idx1"))
	require.False(t, IsHiddenSystemColumnForIndex("!hidden!idx10!0!0", "idx1"))
	require.False(t, IsHiddenSystemColumnForIndex("c1", "idx1"))
}

func TestFormatColumnExtra(t *testing.T) {
	testCases := []struct {
		name string
		col  *Column
		want string
	}{
		{
			name: "nil column",
			col:  nil,
			want: "",
		},
		{
			name: "empty extra without default",
			col:  &Column{Name: "c1"},
			want: "",
		},
		{
			name: "auto_increment extra",
			col:  &Column{Name: "c1", AutoIncrement: true},
			want: "auto_increment",
		},
		{
			name: "virtual generated column",
			col: &Column{
				Name:      "c1",
				Virtual:   true,
				Generated: &ColumnDefaultValue{Literal: false},
			},
			want: "VIRTUAL GENERATED",
		},
		{
			name: "stored generated column",
			col: &Column{
				Name:      "c1",
				Virtual:   false,
				Generated: &ColumnDefaultValue{Literal: false},
			},
			want: "STORED GENERATED",
		},
		{
			name: "non-literal default generates DEFAULT_GENERATED",
			col: &Column{
				Name:    "c1",
				Default: &ColumnDefaultValue{Literal: false},
			},
			want: "DEFAULT_GENERATED",
		},
		{
			name: "on update bare current_timestamp",
			col: &Column{
				Name:     "c1",
				OnUpdate: &sqlparser.OnUpdateExpr{Precision: 0},
			},
			want: "on update CURRENT_TIMESTAMP",
		},
		{
			name: "on update with precision and default generated",
			col: &Column{
				Name:     "c1",
				Default:  &ColumnDefaultValue{Literal: false},
				OnUpdate: &sqlparser.OnUpdateExpr{Precision: 3},
			},
			want: "DEFAULT_GENERATED on update CURRENT_TIMESTAMP(3)",
		},
		{
			name: "auto increment combined with on update",
			col: &Column{
				Name:          "c1",
				AutoIncrement: true,
				OnUpdate:      &sqlparser.OnUpdateExpr{Precision: 6},
			},
			want: "auto_increment on update CURRENT_TIMESTAMP(6)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, FormatColumnExtra(tc.col))
		})
	}
}

func TestColumnCopy(t *testing.T) {
	col := &Column{
		Name:      "ts",
		Default:   &ColumnDefaultValue{Literal: true},
		Generated: &ColumnDefaultValue{Literal: false},
		OnUpdate:  &sqlparser.OnUpdateExpr{Precision: 3},
	}
	copied := col.Copy()

	require.Equal(t, col, copied)
	require.False(t, col == copied)
	require.False(t, col.Default == copied.Default)
	require.False(t, col.Generated == copied.Generated)
	require.False(t, col.OnUpdate == copied.OnUpdate)

	copied.OnUpdate.Precision = 6
	require.Equal(t, 3, col.OnUpdate.Precision)
	require.Equal(t, 6, copied.OnUpdate.Precision)
}

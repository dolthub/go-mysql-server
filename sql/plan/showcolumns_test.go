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

package plan_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	. "github.com/dolthub/go-mysql-server/sql/plan"
)

func TestShowColumns(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	schema := sql.Schema{
		{Name: "a", Source: "foo", Type: sql.Text, PrimaryKey: true},
		{Name: "b", Source: "foo", Type: sql.Int64, Nullable: true},
		{Name: "c", Source: "foo", Type: sql.Int64, Default: parse.MustStringToColumnDefaultValue(ctx, "1", sql.Int64, false)},
	}
	table := NewResolvedTable(memory.NewTable("foo", sql.NewPrimaryKeySchema(schema), nil), nil, nil)

	showColumns, err := NewShowColumns(false, table).WithTargetSchema(schema)
	require.NoError(err)

	iter, err := showColumns.RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"a", "text", "NO", "PRI", "", ""},
		{"b", "bigint", "YES", "", "", ""},
		{"c", "bigint", "NO", "", "1", ""},
	}

	require.Equal(expected, rows)
}

func TestShowColumnsWithIndexes(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	schema := sql.Schema{
		{Name: "a", Source: "foo", Type: sql.Text, PrimaryKey: true},
		{Name: "b", Source: "foo", Type: sql.Int64, Nullable: true},
		{Name: "c", Source: "foo", Type: sql.Int64, Default: parse.MustStringToColumnDefaultValue(ctx, "1", sql.Int64, false)},
		{Name: "d", Source: "foo", Type: sql.Int64, Nullable: true},
		{Name: "e", Source: "foo", Type: sql.Int64, Default: parse.MustStringToColumnDefaultValue(ctx, "1", sql.Int64, false)},
	}
	table := NewResolvedTable(memory.NewTable("foo", sql.NewPrimaryKeySchema(schema), nil), nil, nil)

	showColumns, err := NewShowColumns(false, table).WithTargetSchema(schema)
	require.NoError(err)

	// Assign indexes. This mimics what happens during analysis
	showColumns.(*ShowColumns).Indexes = []sql.Index{
		&mockIndex{
			db:    "mydb",
			table: "foo",
			id:    "a",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", true),
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true),
			},
			unique: true,
		},
		&mockIndex{
			db:    "mydb",
			table: "foo",
			id:    "b",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "d", true),
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "e", true),
			},
			unique: false,
		},
	}

	iter, err := showColumns.RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"a", "text", "NO", "PRI", "", ""},
		{"b", "bigint", "YES", "UNI", "", ""},
		{"c", "bigint", "NO", "", "1", ""},
		{"d", "bigint", "YES", "MUL", "", ""},
		{"e", "bigint", "NO", "", "1", ""},
	}

	require.Equal(expected, rows)

	// Test the precedence of key type. PRI > UNI > MUL
	showColumns.(*ShowColumns).Indexes = append(showColumns.(*ShowColumns).Indexes,
		&mockIndex{
			db:    "mydb",
			table: "foo",
			id:    "c",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", true),
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", true),
			},
			unique: true,
		},
		&mockIndex{
			db:    "mydb",
			table: "foo",
			id:    "d",
			exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", true),
				expression.NewGetFieldWithTable(0, sql.Int64, "foo", "d", true),
			},
			unique: false,
		},
	)

	iter, err = showColumns.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	rows, err = sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	require.Equal(expected, rows)
}

func TestShowColumnsFull(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	schema := sql.Schema{
		{Name: "a", Type: sql.Text, PrimaryKey: true},
		{Name: "b", Type: sql.Int64, Nullable: true},
		{Name: "c", Type: sql.Int64, Default: parse.MustStringToColumnDefaultValue(ctx, "1", sql.Int64, false), Comment: "a comment"},
	}
	table := NewResolvedTable(memory.NewTable("foo", sql.NewPrimaryKeySchema(schema), nil), nil, nil)

	showColumns, err := NewShowColumns(true, table).WithTargetSchema(schema)
	require.NoError(err)

	iter, err := showColumns.RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"a", "text", "utf8mb4_0900_bin", "NO", "PRI", "", "", "", ""},
		{"b", "bigint", nil, "YES", "", "", "", "", ""},
		{"c", "bigint", nil, "NO", "", "1", "", "", "a comment"},
	}

	require.Equal(expected, rows)
}

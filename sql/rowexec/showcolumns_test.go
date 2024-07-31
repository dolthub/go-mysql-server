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

package rowexec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	. "github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestShowColumns(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	db := memory.NewDatabase("mydb")
	schema := sql.Schema{
		{Name: "a", Source: "foo", Type: types.Text, PrimaryKey: true},
		{Name: "b", Source: "foo", Type: types.Int64, Nullable: true},
		{Name: "c", Source: "foo", Type: types.Int64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "1", types.Int64, false)},
	}
	table := NewResolvedTable(memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(schema), nil), nil, nil)

	showColumns, err := NewShowColumns(false, table).WithTargetSchema(schema)
	require.NoError(err)

	iter, err := DefaultBuilder.Build(ctx, showColumns, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"a", "text", "NO", "PRI", nil, ""},
		{"b", "bigint", "YES", "", nil, ""},
		{"c", "bigint", "NO", "", "1", ""},
	}

	require.Equal(expected, rows)
}

func TestShowColumnsWithIndexes(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	db := memory.NewDatabase("mydb")
	schema := sql.Schema{
		{Name: "a", Source: "foo", Type: types.Text, PrimaryKey: true},
		{Name: "b", Source: "foo", Type: types.Int64, Nullable: true},
		{Name: "c", Source: "foo", Type: types.Int64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "1", types.Int64, false)},
		{Name: "d", Source: "foo", Type: types.Int64, Nullable: true},
		{Name: "e", Source: "foo", Type: types.Int64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "1", types.Int64, false)},
	}
	memTable := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(schema), nil)
	table := NewResolvedTable(memTable, nil, nil)

	showColumns, err := NewShowColumns(false, table).WithTargetSchema(schema)
	require.NoError(err)

	// Assign indexes. This mimics what happens during analysis
	showColumns.(*ShowColumns).Indexes = []sql.Index{
		&memory.Index{
			DB:        "mydb",
			TableName: "foo",
			Tbl:       memTable,
			Name:      "a",
			Exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "b", true),
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "c", true),
			},
			Unique: true,
		},
		&memory.Index{
			DB:        "mydb",
			TableName: "foo",
			Tbl:       memTable,
			Name:      "b",
			Exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "d", true),
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "e", true),
			},
			Unique: false,
		},
	}

	iter, err := DefaultBuilder.Build(ctx, showColumns, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"a", "text", "NO", "PRI", nil, ""},
		{"b", "bigint", "YES", "MUL", nil, ""},
		{"c", "bigint", "NO", "", "1", ""},
		{"d", "bigint", "YES", "MUL", nil, ""},
		{"e", "bigint", "NO", "", "1", ""},
	}

	require.Equal(expected, rows)

	// Test the precedence of key type. PRI > UNI > MUL
	showColumns.(*ShowColumns).Indexes = append(showColumns.(*ShowColumns).Indexes,
		&memory.Index{
			DB:        "mydb",
			TableName: "foo",
			Tbl:       memTable,
			Name:      "c",
			Exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "a", true),
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "b", true),
			},
			Unique: true,
		},
		&memory.Index{
			DB:        "mydb",
			TableName: "foo",
			Tbl:       memTable,
			Name:      "d",
			Exprs: []sql.Expression{
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "b", true),
				expression.NewGetFieldWithTable(0, 1, types.Int64, "", "foo", "d", true),
			},
			Unique: false,
		},
	)

	iter, err = DefaultBuilder.Build(sql.NewEmptyContext(), showColumns, nil)
	require.NoError(err)

	rows, err = sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	require.Equal(expected, rows)
}

func TestShowColumnsFull(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	db := memory.NewDatabase("mydb")
	schema := sql.Schema{
		{Name: "a", Type: types.Text, PrimaryKey: true},
		{Name: "b", Type: types.Int64, Nullable: true},
		{Name: "c", Type: types.Int64, Default: planbuilder.MustStringToColumnDefaultValue(ctx, "1", types.Int64, false), Comment: "a comment"},
	}
	table := NewResolvedTable(memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(schema), nil), nil, nil)

	showColumns, err := NewShowColumns(true, table).WithTargetSchema(schema)
	require.NoError(err)

	iter, err := DefaultBuilder.Build(ctx, showColumns, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"a", "text", "utf8mb4_0900_bin", "NO", "PRI", nil, "", "", ""},
		{"b", "bigint", nil, "YES", "", nil, "", "", ""},
		{"c", "bigint", nil, "NO", "", "1", "", "", "a comment"},
	}

	require.Equal(expected, rows)
}

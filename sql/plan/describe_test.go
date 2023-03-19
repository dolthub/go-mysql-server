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

package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/memory"
	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

func TestDescribe(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	table := memory.NewTable("test", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Type: types.Text},
		{Name: "c2", Type: types.Int32},
	}), nil)

	d := NewDescribe(NewResolvedTable(table, nil, nil))
	iter, err := d.RowIter(ctx, nil)
	require.NoError(err)
	require.NotNil(iter)

	n, err := iter.Next(ctx)
	require.NoError(err)
	require.Equal(sql.NewRow("c1", "text"), n)

	n, err = iter.Next(ctx)
	require.NoError(err)
	require.Equal(sql.NewRow("c2", "int"), n)

	n, err = iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(n)
}

func TestDescribe_Empty(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	d := NewDescribe(NewUnresolvedTable("test_table", ""))

	iter, err := d.RowIter(ctx, nil)
	require.NoError(err)
	require.NotNil(iter)

	n, err := iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(n)
}

func TestDescribeQuery(t *testing.T) {
	require := require.New(t)

	table := memory.NewTable("foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Source: "foo", Name: "a", Type: types.Text},
		{Source: "foo", Name: "b", Type: types.Text},
	}), nil)

	node := NewDescribeQuery("tree", NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, types.Text, "foo", "a", false),
			expression.NewGetFieldWithTable(1, types.Text, "foo", "b", false),
		},
		NewFilter(
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, types.Text, "foo", "a", false),
				expression.NewLiteral("foo", types.LongText),
			),
			NewResolvedTable(table, nil, nil),
		),
	))

	ctx := sql.NewEmptyContext()
	iter, err := node.RowIter(ctx, nil)
	require.NoError(err)

	rows, err := sql.RowIterToRows(ctx, nil, iter)
	require.NoError(err)

	expected := []sql.Row{
		{"Project"},
		{" ├─ columns: [foo.a, foo.b]"},
		{" └─ Filter"},
		{"     ├─ (foo.a = 'foo')"},
		{"     └─ Table"},
		{"         └─ name: foo"},
	}

	require.Equal(expected, rows)
}

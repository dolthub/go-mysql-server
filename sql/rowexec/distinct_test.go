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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestDistinct(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: types.Text, Nullable: true},
		{Name: "email", Type: types.Text, Nullable: true},
	})
	child := memory.NewTable(db.BaseDatabase, "test", childSchema, nil)

	rows := []sql.Row{
		sql.NewRow("john", "john@doe.com"),
		sql.NewRow("jane", "jane@doe.com"),
		sql.NewRow("john", "johnx@doe.com"),
		sql.NewRow("martha", "marthax@doe.com"),
		sql.NewRow("martha", "martha@doe.com"),
	}

	for _, r := range rows {
		require.NoError(child.Insert(ctx, r))
	}

	p := plan.NewProject([]sql.Expression{
		expression.NewGetField(0, types.Text, "name", true),
	}, plan.NewResolvedTable(child, nil, nil))
	d := plan.NewDistinct(p)

	iter, err := DefaultBuilder.Build(ctx, d, nil)
	require.NoError(err)
	require.NotNil(iter)

	var results []string
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		require.NoError(err)
		result, ok := row[0].(string)
		require.True(ok, "first row column should be string, but is %T", row[0])
		results = append(results, result)
	}

	require.Equal([]string{"john", "jane", "martha"}, results)
}

func TestOrderedDistinct(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "name", Type: types.Text, Nullable: true},
		{Name: "email", Type: types.Text, Nullable: true},
	})
	child := memory.NewTable(db.BaseDatabase, "test", childSchema, nil)

	rows := []sql.Row{
		sql.NewRow("jane", "jane@doe.com"),
		sql.NewRow("john", "john@doe.com"),
		sql.NewRow("john", "johnx@doe.com"),
		sql.NewRow("martha", "martha@doe.com"),
		sql.NewRow("martha", "marthax@doe.com"),
	}

	for _, r := range rows {
		require.NoError(child.Insert(ctx, r))
	}

	p := plan.NewProject([]sql.Expression{
		expression.NewGetField(0, types.Text, "name", true),
	}, plan.NewResolvedTable(child, nil, nil))
	d := plan.NewOrderedDistinct(p)

	iter, err := DefaultBuilder.Build(ctx, d, nil)
	require.NoError(err)
	require.NotNil(iter)

	var results []string
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}

		require.NoError(err)
		result, ok := row[0].(string)
		require.True(ok, "first row column should be string, but is %T", row[0])
		results = append(results, result)
	}

	require.Equal([]string{"jane", "john", "martha"}, results)
}

func BenchmarkDistinct(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()

	for i := 0; i < b.N; i++ {
		p := plan.NewProject([]sql.Expression{
			expression.NewGetField(0, types.Text, "strfield", true),
			expression.NewGetField(1, types.Float64, "floatfield", true),
			expression.NewGetField(2, types.Boolean, "boolfield", false),
			expression.NewGetField(3, types.Int32, "intfield", false),
			expression.NewGetField(4, types.Int64, "bigintfield", false),
			expression.NewGetField(5, types.Blob, "blobfield", false),
		}, plan.NewResolvedTable(benchtable, nil, nil))
		d := plan.NewDistinct(p)

		iter, err := DefaultBuilder.Build(ctx, d, nil)
		require.NoError(err)
		require.NotNil(iter)

		var rows int
		for {
			_, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}

			require.NoError(err)
			rows++
		}
		require.Equal(100, rows)
	}
}

func BenchmarkOrderedDistinct(b *testing.B) {
	require := require.New(b)
	ctx := sql.NewEmptyContext()

	for i := 0; i < b.N; i++ {
		p := plan.NewProject([]sql.Expression{
			expression.NewGetField(0, types.Text, "strfield", true),
			expression.NewGetField(1, types.Float64, "floatfield", true),
			expression.NewGetField(2, types.Boolean, "boolfield", false),
			expression.NewGetField(3, types.Int32, "intfield", false),
			expression.NewGetField(4, types.Int64, "bigintfield", false),
			expression.NewGetField(5, types.Blob, "blobfield", false),
		}, plan.NewResolvedTable(benchtable, nil, nil))
		d := plan.NewOrderedDistinct(p)

		iter, err := DefaultBuilder.Build(ctx, d, nil)
		require.NoError(err)
		require.NotNil(iter)

		var rows int
		for {
			_, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}

			require.NoError(err)
			rows++
		}
		require.Equal(100, rows)
	}
}

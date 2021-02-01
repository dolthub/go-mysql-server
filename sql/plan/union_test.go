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

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

func TestUnion(t *testing.T) {
	require := require.New(t)

	childSchema := sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: true},
		{Name: "email", Type: sql.Text, Nullable: true},
	}
	child := memory.NewTable("test", childSchema)
	empty := memory.NewTable("empty", childSchema)

	rows := []sql.Row{
		sql.NewRow("john", "john@doe.com"),
		sql.NewRow("jane", "jane@doe.com"),
		sql.NewRow("john", "johnx@doe.com"),
		sql.NewRow("martha", "marthax@doe.com"),
		sql.NewRow("martha", "martha@doe.com"),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	name := []sql.Expression{
		expression.NewGetField(0, sql.Text, "name", true),
	}

	cases := []struct {
		node     sql.Node
		expected []string
	}{
		{
			NewUnion(
				NewProject(name, NewResolvedTable(child)),
				NewProject(name, NewResolvedTable(child))),
			[]string{
				"john", "jane", "john", "martha", "martha",
				"john", "jane", "john", "martha", "martha",
			},
		},
		{
			NewUnion(
				NewProject(name, NewResolvedTable(empty)),
				NewProject(name, NewResolvedTable(child))),
			[]string{
				"john", "jane", "john", "martha", "martha",
			},
		},
		{
			NewUnion(
				NewProject(name, NewResolvedTable(child)),
				NewProject(name, NewResolvedTable(empty))),
			[]string{
				"john", "jane", "john", "martha", "martha",
			},
		},
	}

	for _, c := range cases {
		iter, err := c.node.RowIter(sql.NewEmptyContext(), nil)
		require.NoError(err)
		require.NotNil(iter)

		var results []string
		for {
			row, err := iter.Next()
			if err == io.EOF {
				break
			}
			require.NoError(err)
			result, ok := row[0].(string)
			require.True(ok, "first row column should be string, but is %T", row[0])
			results = append(results, result)
		}

		require.Equal(c.expected, results)
	}
}

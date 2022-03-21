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

package visit

import (
	"github.com/dolthub/go-mysql-server/sql/plan"
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/stretchr/testify/require"
)

func TestTransformUp(t *testing.T) {
	require := require.New(t)

	aCol := expression.NewUnresolvedColumn("a")
	bCol := expression.NewUnresolvedColumn("a")
	ur := plan.NewUnresolvedTable("unresolved", "")
	p := plan.NewProject([]sql.Expression{aCol, bCol}, plan.NewFilter(expression.NewEquals(aCol, bCol), ur))

	schema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: sql.Text},
		{Name: "b", Type: sql.Text},
	})
	table := memory.NewTable("resolved", schema)

	pt, err := Nodes(p, func(n sql.Node) (sql.Node, sql.TreeIdentity, error) {
		switch n.(type) {
		case *plan.UnresolvedTable:
			return plan.NewResolvedTable(table, nil, nil), sql.NewTree, nil
		default:
			return n, sql.SameTree, nil
		}
	})
	require.NoError(err)

	ep := plan.NewProject(
		[]sql.Expression{aCol, bCol},
		plan.NewFilter(expression.NewEquals(aCol, bCol),
			plan.NewResolvedTable(table, nil, nil),
		),
	)
	require.Equal(ep, pt)
}

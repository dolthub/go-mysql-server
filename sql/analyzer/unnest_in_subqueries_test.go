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

package analyzer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/transform"
)

// customInSubquery is an integrator-defined IN subquery expression, used to test that the analyzer unnests any
// plan.InSubqueryExpression rather than only *plan.InSubquery.
type customInSubquery struct {
	*plan.InSubquery
	canUnnest bool
}

var _ plan.InSubqueryExpression = (*customInSubquery)(nil)

func (c *customInSubquery) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	in, err := c.InSubquery.WithChildren(ctx, children...)
	if err != nil {
		return nil, err
	}
	return &customInSubquery{
		InSubquery: in.(*plan.InSubquery),
		canUnnest:  c.canUnnest,
	}, nil
}

func (c *customInSubquery) NewJoinFilter(ctx *sql.Context, left, right sql.Expression) (sql.Expression, bool, error) {
	if !c.canUnnest {
		return nil, false, nil
	}
	// We use a null-safe equality so that the test can verify that this filter was used for the join
	return expression.NewNullSafeEquals(left, right), true, nil
}

func TestUnnestCustomInSubquery(t *testing.T) {
	tests := []struct {
		name      string
		canUnnest bool
		exp       string
	}{
		{
			name:      "unnests into a semi join using the custom join filter",
			canUnnest: true,
			exp:       "(xy.x <=> uv.u)",
		},
		{
			name:      "leaves the filter in place when no join filter can be built",
			canUnnest: false,
		},
	}

	db := memory.NewDatabase("mydb")
	cat := newTestCatalog(db)
	pro := memory.NewDBProvider(db)
	sess := memory.NewSession(sql.NewBaseSession(), pro)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	ctx.SetCurrentDatabase("mydb")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := planbuilder.New(ctx, cat, nil)
			node, _, _, qFlags, err := b.Parse("SELECT x FROM xy WHERE x IN (SELECT u FROM uv)", nil, false)
			require.NoError(t, err)

			node, _, err = transform.NodeExprs(ctx, node, func(ctx *sql.Context, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				if in, ok := e.(*plan.InSubquery); ok {
					return &customInSubquery{
						InSubquery: in,
						canUnnest:  tt.canUnnest,
					}, transform.NewTree, nil
				}
				return e, transform.SameTree, nil
			})
			require.NoError(t, err)

			sel := func(RuleId) bool { return true }
			res, _, err := unnestInSubqueries(ctx, nil, node, nil, sel, qFlags)
			require.NoError(t, err)

			child := res.(*plan.Project).Child
			if tt.canUnnest {
				join, ok := child.(*plan.JoinNode)
				require.True(t, ok, "expected a join, found %T", child)
				require.Equal(t, plan.JoinTypeSemi, join.Op)
				require.Equal(t, tt.exp, join.Filter.String())
			} else {
				filter, ok := child.(*plan.Filter)
				require.True(t, ok, "expected a filter, found %T", child)
				_, ok = filter.Expression.(*customInSubquery)
				require.True(t, ok, "expected a custom IN subquery, found %T", filter.Expression)
			}
		})
	}
}

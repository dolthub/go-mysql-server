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

package plan

import (
	"reflect"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/internal/exprtest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSubqueryString(t *testing.T) {
	expr := NewSubquery(nil, "select 1")
	require.Equal(t, "(select 1)", expr.String())
	parsed := exprtest.RequireExpression(t, exprtest.ParseExpression(t, expr)).(*sqlparser.Subquery)
	assertParsedSubquery(t, parsed, expr)
}

func TestInSubqueryString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := NewInSubquery(ctx, expression.NewUnresolvedColumn("i"), NewSubquery(nil, "select j from t"))
	require.Equal(t, "(i IN (select j from t))", expr.String())
	assertParsedInSubquery(t, exprtest.RequireExpression(t, exprtest.ParseExpression(t, expr)), expr)
}

func TestNestedInSubqueryDescription(t *testing.T) {
	ctx := sql.NewEmptyContext()
	subquery := NewSubquery(NewUnresolvedTable("t", ""), "select j from t")
	inSubquery := NewInSubquery(ctx, expression.NewUnresolvedColumn("i"), subquery)
	expr := expression.NewAnd(expression.NewLiteral(true, types.Boolean), inSubquery)

	description := sql.Describe(ctx, expr, sql.DescribeOptions{Estimates: true})
	require.Contains(t, description, "InSubquery")
	require.Contains(t, description, "right: Subquery")
	require.Contains(t, description, "UnresolvedTable(t)")

	require.Equal(t, "(true AND (i IN (select j from t)))", expr.String())
	parsed := exprtest.RequireExpression(t, exprtest.ParseExpression(t, expr)).(*sqlparser.ParenExpr)
	parsedAnd := parsed.Expr.(*sqlparser.AndExpr)
	exprtest.AssertExpressionValue(t, parsedAnd.Left, expr.Children()[0])
	assertParsedInSubquery(t, parsedAnd.Right, inSubquery)
}

func assertParsedInSubquery(t *testing.T, parsed sqlparser.Expr, original *InSubquery) {
	t.Helper()
	parenthesized := parsed.(*sqlparser.ParenExpr)
	comparison := parenthesized.Expr.(*sqlparser.ComparisonExpr)
	require.Equal(t, sqlparser.InStr, comparison.Operator)
	exprtest.AssertExpressionValue(t, comparison.Left, original.Left())
	assertParsedSubquery(t, comparison.Right.(*sqlparser.Subquery), original.Right().(*Subquery))
}

func assertParsedSubquery(t *testing.T, parsed *sqlparser.Subquery, original *Subquery) {
	t.Helper()
	expectedStatement, err := sqlparser.Parse(original.QueryString)
	require.NoError(t, err)
	expected := expectedStatement.(*sqlparser.Select)
	actual := parsed.Select.(*sqlparser.Select)
	require.Len(t, actual.SelectExprs, len(expected.SelectExprs))
	for i := range expected.SelectExprs {
		expectedExpr := expected.SelectExprs[i].(*sqlparser.AliasedExpr).Expr
		actualExpr := actual.SelectExprs[i].(*sqlparser.AliasedExpr).Expr
		require.Equal(t, reflect.TypeOf(expectedExpr), reflect.TypeOf(actualExpr))
		switch expectedExpr := expectedExpr.(type) {
		case *sqlparser.SQLVal:
			actualValue := actualExpr.(*sqlparser.SQLVal)
			require.Equal(t, expectedExpr.Type, actualValue.Type)
			require.Equal(t, expectedExpr.Val, actualValue.Val)
		case *sqlparser.ColName:
			actualColumn := actualExpr.(*sqlparser.ColName)
			require.Equal(t, expectedExpr.Name.String(), actualColumn.Name.String())
			require.Equal(t, expectedExpr.Qualifier.Name.String(), actualColumn.Qualifier.Name.String())
		default:
			require.Failf(t, "unsupported subquery select expression", "%T", expectedExpr)
		}
	}
	require.Len(t, actual.From, len(expected.From))
	for i := range expected.From {
		expectedTable := expected.From[i].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
		actualTable := actual.From[i].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
		require.Equal(t, expectedTable.Name.String(), actualTable.Name.String())
		require.Equal(t, expectedTable.DbQualifier.String(), actualTable.DbQualifier.String())
	}
}

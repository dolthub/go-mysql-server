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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
)

func TestFiltersMerge(t *testing.T) {
	f1 := filtersByTable{
		"1": []sql.Expression{
			expression.NewLiteral("1", sql.LongText),
		},
		"2": []sql.Expression{
			expression.NewLiteral("2", sql.LongText),
		},
	}

	f2 := filtersByTable{
		"2": []sql.Expression{
			expression.NewLiteral("2.2", sql.LongText),
		},
		"3": []sql.Expression{
			expression.NewLiteral("3", sql.LongText),
		},
	}

	f1.merge(f2)

	require.Equal(t,
		filtersByTable{
			"1": []sql.Expression{
				expression.NewLiteral("1", sql.LongText),
			},
			"2": []sql.Expression{
				expression.NewLiteral("2", sql.LongText),
				expression.NewLiteral("2.2", sql.LongText),
			},
			"3": []sql.Expression{
				expression.NewLiteral("3", sql.LongText),
			},
		},
		f1,
	)
}

func TestSplitExpression(t *testing.T) {
	e := expression.NewAnd(
		expression.NewAnd(
			expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
			expression.NewNot(expression.NewUnresolvedColumn("foo")),
		),
		expression.NewAnd(
			expression.NewOr(
				expression.NewIsNull(expression.NewUnresolvedColumn("bar")),
				expression.NewNot(expression.NewUnresolvedColumn("bar")),
			),
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewLiteral("foo", sql.LongText),
			),
		),
	)

	expected := []sql.Expression{
		expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
		expression.NewNot(expression.NewUnresolvedColumn("foo")),
		expression.NewOr(
			expression.NewIsNull(expression.NewUnresolvedColumn("bar")),
			expression.NewNot(expression.NewUnresolvedColumn("bar")),
		),
		expression.NewEquals(
			expression.NewUnresolvedColumn("foo"),
			expression.NewLiteral("foo", sql.LongText),
		),
	}

	require.Equal(t,
		expected,
		splitConjunction(e),
	)
}

func TestSubtractExprSet(t *testing.T) {
	filters := []sql.Expression{
		expression.NewIsNull(nil),
		expression.NewNot(nil),
		expression.NewEquals(nil, nil),
		expression.NewGreaterThan(nil, nil),
	}

	handled := []sql.Expression{
		filters[1],
		filters[3],
	}

	unhandled := subtractExprSet(filters, handled)

	require.Equal(t,
		[]sql.Expression{filters[0], filters[2]},
		unhandled,
	)
}

func TestExprToTableFilters(t *testing.T) {
	expr := expression.NewAnd(
		expression.NewAnd(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3., sql.Float64),
				),
			),
			expression.NewIsNull(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable2", "i2", false),
			),
		),
		expression.NewOr(
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3.14, sql.Float64),
			),
			expression.NewGreaterThan(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3., sql.Float64),
			),
		),
	)

	expected := filtersByTable{
		"mytable": []sql.Expression{
			expression.NewEquals(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3.14, sql.Float64),
			),
			expression.NewGreaterThan(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
				expression.NewLiteral(3., sql.Float64),
			),
			expression.NewOr(
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3.14, sql.Float64),
				),
				expression.NewGreaterThan(
					expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
					expression.NewLiteral(3., sql.Float64),
				),
			),
		},
		"mytable2": []sql.Expression{
			expression.NewIsNull(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable2", "i2", false),
			),
		},
	}

	filters := exprToTableFilters(expr)
	assert.Equal(t, expected, filters)

	// Test various complex conditions -- anytime we can't neatly split the expressions into tables
	filters = exprToTableFilters(expression.NewAnd(
		lit(0),
		expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
	))
	expected = filtersByTable{
		"mytable": []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
		},
	}
	assert.Equal(t, expected, filters)

	filters = exprToTableFilters(expression.NewAnd(
		expression.NewLiteral(nil, sql.Null),
		expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
	))
	expected = filtersByTable{
		"mytable": []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
		},
	}
	assert.Equal(t, expected, filters)

	filters = exprToTableFilters(expression.NewAnd(
		expression.NewEquals(lit(1), mustExpr(function.NewRand(sql.NewEmptyContext()))),
		expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
	))
	expected = filtersByTable{
		"mytable": []sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
		},
	}
	assert.Equal(t, expected, filters)

	filters = exprToTableFilters(expression.NewOr(
		expression.NewLiteral(nil, sql.Null),
		expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
	))
	expected = filtersByTable{
		"mytable": []sql.Expression{
			expression.NewOr(
				expression.NewLiteral(nil, sql.Null),
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
			),
		},
	}
	assert.Equal(t, expected, filters)

	filters = exprToTableFilters(expression.NewAnd(
		eq(
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "a", false),
			lit(1),
		),
		eq(
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "f", false),
			expression.NewGetFieldWithTable(0, sql.Int64, "mytable2", "i", false),
		),
	))
	expected = filtersByTable{
		"mytable": []sql.Expression{
			eq(
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "a", false),
				lit(1),
			),
		},
	}
	assert.Equal(t, expected, filters)
}

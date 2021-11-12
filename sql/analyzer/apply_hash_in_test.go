package analyzer

import (
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestApplyHashIn(t *testing.T) {
	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Int64, Source: "foo"},
		{Name: "c", Type: sql.Int64, Source: "foo"},
		{Name: "d", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "foo"},
	})

	hitLiteral, _ := expression.NewHashInTuple(
		expression.NewGetField(0, sql.Int64, "foo", false),
		expression.NewTuple(
			expression.NewLiteral(int64(2), sql.Int64),
			expression.NewLiteral(int64(1), sql.Int64),
			expression.NewLiteral(int64(0), sql.Int64),
		),
	)

	hitTuple, _ := expression.NewHashInTuple(
		expression.NewTuple(
			expression.NewGetField(0, sql.Int64, "a", false),
			expression.NewGetField(1, sql.Int64, "b", false),
		),
		expression.NewTuple(
			expression.NewTuple(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
			expression.NewTuple(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
			expression.NewTuple(expression.NewLiteral(int64(0), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
		),
	)

	hitHeteroTuple, _ := expression.NewHashInTuple(
		expression.NewTuple(
			expression.NewGetField(0, sql.Int64, "a", false),
			expression.NewGetField(3, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), "d", false),
		),
		expression.NewTuple(
			expression.NewTuple(
				expression.NewLiteral(int64(2), sql.Int64),
				expression.NewLiteral("a", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral("b", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
			expression.NewTuple(
				expression.NewLiteral(int64(1), sql.Int64),
				expression.NewLiteral("c", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
		),
	)

	child := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
			expression.NewGetFieldWithTable(1, sql.Int64, "foo", "b", false),
			expression.NewGetFieldWithTable(2, sql.Int64, "foo", "c", false),
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	tests := []analyzerFnTestCase{
		{
			name: "filter with literals converted to hash in",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), sql.Int64),
						expression.NewLiteral(int64(1), sql.Int64),
						expression.NewLiteral(int64(0), sql.Int64),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				hitLiteral,
				child,
			),
		},
		{
			name: "hash in preserves sibling expressions",
			node: plan.NewFilter(
				expression.NewAnd(
					expression.NewInTuple(
						expression.NewGetField(0, sql.Int64, "foo", false),
						expression.NewTuple(
							expression.NewLiteral(int64(2), sql.Int64),
							expression.NewLiteral(int64(1), sql.Int64),
							expression.NewLiteral(int64(0), sql.Int64),
						),
					),
					expression.NewEquals(
						expression.NewBindVar("foo_id"),
						expression.NewLiteral(int8(2), sql.Int8),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewAnd(
					hitLiteral,
					expression.NewEquals(
						expression.NewBindVar("foo_id"),
						expression.NewLiteral(int8(2), sql.Int8),
					),
				),
				child,
			),
		},
		{
			name: "filter with tuple expression converted to hash in",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewTuple(
						expression.NewGetField(0, sql.Int64, "a", false),
						expression.NewGetField(1, sql.Int64, "b", false),
					),
					expression.NewTuple(
						expression.NewTuple(expression.NewLiteral(int64(2), sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(1), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(0), sql.Int64), expression.NewLiteral(int64(0), sql.Int64)),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				hitTuple,
				child,
			),
		},
		{
			name: "filter with hetero tuple converted to hash in",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewTuple(
						expression.NewGetField(0, sql.Int64, "a", false),
						expression.NewGetField(3, sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), "d", false),
					),
					expression.NewTuple(
						expression.NewTuple(
							expression.NewLiteral(int64(2), sql.Int64),
							expression.NewLiteral("a", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
						),
						expression.NewTuple(
							expression.NewLiteral(int64(1), sql.Int64),
							expression.NewLiteral("b", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
						),
						expression.NewTuple(
							expression.NewLiteral(int64(1), sql.Int64),
							expression.NewLiteral("c", sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
						),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				hitHeteroTuple,
				child,
			),
		},
		{
			name: "filter with nested tuple expression not selected",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewTuple(
						expression.NewTuple(
							expression.NewGetField(0, sql.Int64, "a", false),
							expression.NewGetField(1, sql.Int64, "b", false),
						),
						expression.NewGetField(1, sql.Int64, "b", false),
					),
					expression.NewTuple(
						expression.NewTuple(
							expression.NewTuple(
								expression.NewLiteral(int64(2), sql.Int64),
								expression.NewLiteral(int64(1), sql.Int64),
							),
							expression.NewLiteral(int64(1), sql.Int64),
						),
						expression.NewTuple(
							expression.NewTuple(
								expression.NewLiteral(int64(2), sql.Int64),
								expression.NewLiteral(int64(1), sql.Int64),
							),
							expression.NewLiteral(int64(0), sql.Int64),
						),
					),
				),
				child,
			),
			err: expression.ErrCantHashNestedExpression,
		},
		{
			name: "filter with binding expression not selected",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), sql.Int64),
						expression.NewLiteral(int64(1), sql.Int64),
						expression.NewBindVar("foo_id"),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, sql.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), sql.Int64),
						expression.NewLiteral(int64(1), sql.Int64),
						expression.NewBindVar("foo_id"),
					),
				),
				child,
			),
			err: expression.ErrUnsupportedHashInSubexpression,
		},
	}

	runTestCases(t, sql.NewEmptyContext(), tests, NewDefault(sql.NewDatabaseProvider()), getRule("apply_hash_in"))
}

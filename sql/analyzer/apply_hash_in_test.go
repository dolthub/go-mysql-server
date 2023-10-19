package analyzer

import (
	"context"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestApplyHashIn(t *testing.T) {
	ctx := &sql.Context{}
	db := memory.NewDatabase("mydb")
	pro := memory.NewDBProvider(db)

	table := memory.NewTable(db, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Type: types.Int64, Source: "foo"},
		{Name: "b", Type: types.Int64, Source: "foo"},
		{Name: "c", Type: types.Int64, Source: "foo"},
		{Name: "d", Type: types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Source: "foo"},
	}), nil)

	hitLiteral, _ := expression.NewHashInTuple(
		ctx,
		expression.NewGetField(0, types.Int64, "foo", false),
		expression.NewTuple(
			expression.NewLiteral(int64(2), types.Int64),
			expression.NewLiteral(int64(1), types.Int64),
			expression.NewLiteral(int64(0), types.Int64),
		),
	)

	hitTuple, _ := expression.NewHashInTuple(
		ctx,
		expression.NewTuple(
			expression.NewGetField(0, types.Int64, "a", false),
			expression.NewGetField(1, types.Int64, "b", false),
		),
		expression.NewTuple(
			expression.NewTuple(expression.NewLiteral(int64(2), types.Int64), expression.NewLiteral(int64(1), types.Int64)),
			expression.NewTuple(expression.NewLiteral(int64(1), types.Int64), expression.NewLiteral(int64(0), types.Int64)),
			expression.NewTuple(expression.NewLiteral(int64(0), types.Int64), expression.NewLiteral(int64(0), types.Int64)),
		),
	)

	hitHeteroTuple, _ := expression.NewHashInTuple(
		ctx,
		expression.NewTuple(
			expression.NewGetField(0, types.Int64, "a", false),
			expression.NewGetField(3, types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), "d", false),
		),
		expression.NewTuple(
			expression.NewTuple(
				expression.NewLiteral(int64(2), types.Int64),
				expression.NewLiteral("a", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
			expression.NewTuple(
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral("b", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
			expression.NewTuple(
				expression.NewLiteral(int64(1), types.Int64),
				expression.NewLiteral("c", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
			),
		),
	)

	child := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", false),
			expression.NewGetFieldWithTable(1, types.Int64, "db", "foo", "b", false),
			expression.NewGetFieldWithTable(2, types.Int64, "db", "foo", "c", false),
		},
		plan.NewResolvedTable(table, nil, nil),
	)

	tests := []analyzerFnTestCase{
		{
			name: "filter with literals converted to hash in",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, types.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), types.Int64),
						expression.NewLiteral(int64(1), types.Int64),
						expression.NewLiteral(int64(0), types.Int64),
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
						expression.NewGetField(0, types.Int64, "foo", false),
						expression.NewTuple(
							expression.NewLiteral(int64(2), types.Int64),
							expression.NewLiteral(int64(1), types.Int64),
							expression.NewLiteral(int64(0), types.Int64),
						),
					),
					expression.NewEquals(
						expression.NewBindVar("foo_id"),
						expression.NewLiteral(int8(2), types.Int8),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewAnd(
					hitLiteral,
					expression.NewEquals(
						expression.NewBindVar("foo_id"),
						expression.NewLiteral(int8(2), types.Int8),
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
						expression.NewGetField(0, types.Int64, "a", false),
						expression.NewGetField(1, types.Int64, "b", false),
					),
					expression.NewTuple(
						expression.NewTuple(expression.NewLiteral(int64(2), types.Int64), expression.NewLiteral(int64(1), types.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(1), types.Int64), expression.NewLiteral(int64(0), types.Int64)),
						expression.NewTuple(expression.NewLiteral(int64(0), types.Int64), expression.NewLiteral(int64(0), types.Int64)),
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
						expression.NewGetField(0, types.Int64, "a", false),
						expression.NewGetField(3, types.MustCreateStringWithDefaults(sqltypes.VarChar, 20), "d", false),
					),
					expression.NewTuple(
						expression.NewTuple(
							expression.NewLiteral(int64(2), types.Int64),
							expression.NewLiteral("a", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
						),
						expression.NewTuple(
							expression.NewLiteral(int64(1), types.Int64),
							expression.NewLiteral("b", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
						),
						expression.NewTuple(
							expression.NewLiteral(int64(1), types.Int64),
							expression.NewLiteral("c", types.MustCreateStringWithDefaults(sqltypes.VarChar, 20)),
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
							expression.NewGetField(0, types.Int64, "a", false),
							expression.NewGetField(1, types.Int64, "b", false),
						),
						expression.NewGetField(1, types.Int64, "b", false),
					),
					expression.NewTuple(
						expression.NewTuple(
							expression.NewTuple(
								expression.NewLiteral(int64(2), types.Int64),
								expression.NewLiteral(int64(1), types.Int64),
							),
							expression.NewLiteral(int64(1), types.Int64),
						),
						expression.NewTuple(
							expression.NewTuple(
								expression.NewLiteral(int64(2), types.Int64),
								expression.NewLiteral(int64(1), types.Int64),
							),
							expression.NewLiteral(int64(0), types.Int64),
						),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				mustNewHashInTuple(
					ctx,
					expression.NewTuple(
						expression.NewTuple(
							expression.NewGetField(0, types.Int64, "a", false),
							expression.NewGetField(1, types.Int64, "b", false),
						),
						expression.NewGetField(1, types.Int64, "b", false),
					),
					expression.NewTuple(
						expression.NewTuple(
							expression.NewTuple(
								expression.NewLiteral(int64(2), types.Int64),
								expression.NewLiteral(int64(1), types.Int64),
							),
							expression.NewLiteral(int64(1), types.Int64),
						),
						expression.NewTuple(
							expression.NewTuple(
								expression.NewLiteral(int64(2), types.Int64),
								expression.NewLiteral(int64(1), types.Int64),
							),
							expression.NewLiteral(int64(0), types.Int64),
						),
					),
				),
				child,
			),
		},
		{
			name: "skip filter with binding",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, types.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), types.Int64),
						expression.NewLiteral(int64(1), types.Int64),
						expression.NewBindVar("foo_id"),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewGetField(0, types.Int64, "foo", false),
					expression.NewTuple(
						expression.NewLiteral(int64(2), types.Int64),
						expression.NewLiteral(int64(1), types.Int64),
						expression.NewBindVar("foo_id"),
					),
				),
				child,
			),
		},
		{
			name: "filter with arithmetic on left",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewPlus(
						expression.NewLiteral(4, types.Int64),
						expression.NewGetField(0, types.Int64, "foo", false),
					),
					expression.NewTuple(
						expression.NewLiteral(6, types.Int64),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				mustNewHashInTuple(
					ctx,
					expression.NewPlus(
						expression.NewLiteral(4, types.Int64),
						expression.NewGetField(0, types.Int64, "foo", false),
					),
					expression.NewTuple(
						expression.NewLiteral(6, types.Int64),
					),
				),
				child,
			),
		},
		{
			name: "skip filter with arithmetic on right",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(6, types.Int64),
					expression.NewTuple(
						expression.NewPlus(
							expression.NewLiteral(4, types.Int64),
							expression.NewGetField(0, types.Int64, "foo", false),
						),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(6, types.Int64),
					expression.NewTuple(
						expression.NewPlus(
							expression.NewLiteral(4, types.Int64),
							expression.NewGetField(0, types.Int64, "foo", false),
						),
					),
				),
				child,
			),
		},
		{
			name: "function on left",
			node: plan.NewFilter(
				expression.NewInTuple(
					function.NewLower(
						expression.NewLiteral("hi", types.TinyText),
					),
					expression.NewTuple(
						expression.NewLiteral("hi", types.TinyText),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				mustNewHashInTuple(
					ctx,
					function.NewLower(
						expression.NewLiteral("hi", types.TinyText),
					),
					expression.NewTuple(
						expression.NewLiteral("hi", types.TinyText),
					),
				),
				child,
			),
		},
		{
			name: "skip filter with function on right",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral("hi", types.TinyText),
					expression.NewTuple(
						function.NewLower(
							expression.NewLiteral("hi", types.TinyText),
						),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral("hi", types.TinyText),
					expression.NewTuple(
						function.NewLower(
							expression.NewLiteral("hi", types.TinyText),
						),
					),
				),
				child,
			),
		},
		{
			name: "is null on left",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewIsNull(
						expression.NewLiteral(int64(0), types.Null),
					),
					expression.NewTuple(
						expression.NewLiteral(int64(0), types.Int64),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				mustNewHashInTuple(
					ctx,
					expression.NewIsNull(
						expression.NewLiteral(int64(0), types.Null),
					),
					expression.NewTuple(
						expression.NewLiteral(int64(0), types.Int64),
					),
				),
				child,
			),
		},
		{
			name: "skip filter with is null on right",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewIsNull(
							expression.NewLiteral(int64(0), types.Null),
						),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewIsNull(
							expression.NewLiteral(int64(0), types.Null),
						),
					),
				),
				child,
			),
		},
		{
			name: "is true on left",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewIsTrue(
						expression.NewLiteral(int64(0), types.Null),
					),
					expression.NewTuple(
						expression.NewLiteral(int64(0), types.Int64),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				mustNewHashInTuple(
					ctx,
					expression.NewIsTrue(
						expression.NewLiteral(int64(0), types.Null),
					),
					expression.NewTuple(
						expression.NewLiteral(int64(0), types.Int64),
					),
				),
				child,
			),
		},
		{
			name: "skip filter with is true on right",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewIsTrue(
							expression.NewLiteral(int64(0), types.Null),
						),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewIsTrue(
							expression.NewLiteral(int64(0), types.Null),
						),
					),
				),
				child,
			),
		},
		{
			name: "cast on left",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewConvert(
						expression.NewGetField(0, types.Int64, "foo", false),
						"char",
					),
					expression.NewTuple(
						expression.NewLiteral(int64(0), types.Int64),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				mustNewHashInTuple(
					ctx,
					expression.NewConvert(
						expression.NewGetField(0, types.Int64, "foo", false),
						"char",
					),
					expression.NewTuple(
						expression.NewLiteral(int64(0), types.Int64),
					),
				),
				child,
			),
		},
		{
			name: "skip filter with cast on right",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewConvert(
							expression.NewGetField(0, types.Int64, "foo", false),
							"char",
						),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewConvert(
							expression.NewGetField(0, types.Int64, "foo", false),
							"char",
						),
					),
				),
				child,
			),
		},
		{
			name: "skip filter with get field on right",
			node: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewGetField(0, types.Int64, "foo", false),
					),
				),
				child,
			),
			expected: plan.NewFilter(
				expression.NewInTuple(
					expression.NewLiteral(int64(0), types.Int64),
					expression.NewTuple(
						expression.NewGetField(0, types.Int64, "foo", false),
					),
				),
				child,
			),
		},
	}

	runTestCases(t, newContext(pro), tests, NewDefault(sql.NewDatabaseProvider()), getRule(applyHashInId))
}

func mustNewHashInTuple(ctx *sql.Context, left, right sql.Expression) *expression.HashInTuple {
	hin, err := expression.NewHashInTuple(ctx, left, right)
	if err != nil {
		panic(err)
	}
	return hin
}

func newContext(provider *memory.DbProvider) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))
}

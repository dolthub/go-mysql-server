package analyzer

import (
	"testing"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var nodes []struct {
	name string
	sql  string
	node sql.Node
}

func init() {
	db1 := memory.NewDatabase("a")
	xy := memory.NewTable("xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Source: "xy", Type: types.Int64},
		{Name: "y", Source: "xy", Type: types.Int64},
	}), nil)
	db1.AddTable("xy", xy)

	nodes = []struct {
		name string
		sql  string
		node sql.Node
	}{
		{
			name: "simple select",
			sql:  "select x from xy",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, types.Int64, "x", true),
				},
				plan.NewResolvedTable(xy.WithProjections([]string{"x"}), db1, nil),
			),
		},
		{
			name: "group by",
			sql:  "select x, count(x), y from xy group by x limit 1 offset 1",
			node: plan.NewLimit(
				expression.NewLiteral(1, types.Int64),
				plan.NewOffset(
					expression.NewLiteral(1, types.Int64),
					plan.NewProject(
						[]sql.Expression{},
						plan.NewGroupBy(
							[]sql.Expression{
								expression.NewGetField(0, types.Int64, "x", true),
								expression.NewAlias(
									"cnt",
									aggregation.NewCount(expression.NewGetField(0, types.Int64, "x", true)),
								),
								aggregation.NewCount(expression.NewGetField(1, types.Int64, "y", true)),
							},
							[]sql.Expression{
								expression.NewGetField(0, types.Int64, "x", true),
							},
							plan.NewResolvedTable(xy, db1, nil),
						),
					),
				),
			),
		},
		{
			name: "join tree",
			sql: `
select a1.x, a2.x, a3.x, a4.x
from a1 xy
join a2 on a1.x = a2.x
join a3 on a1.x = a3.x
join a4 on a1.x = a4.x`,
			node: plan.NewInnerJoin(
				plan.NewInnerJoin(
					plan.NewInnerJoin(
						plan.NewTableAlias(
							"a1",
							plan.NewResolvedTable(xy.WithProjections([]string{"x"}), db1, nil),
						),
						plan.NewTableAlias(
							"a2",
							plan.NewResolvedTable(xy.WithProjections([]string{"x"}), db1, nil),
						),
						expression.NewEquals(
							expression.NewGetFieldWithTable(0, types.Int64, "a1", "x", true),
							expression.NewGetFieldWithTable(1, types.Int64, "a2", "x", true),
						),
					),
					plan.NewTableAlias(
						"a3",
						plan.NewResolvedTable(xy.WithProjections([]string{"x"}), db1, nil),
					),
					expression.NewEquals(
						expression.NewGetFieldWithTable(0, types.Int64, "a1", "x", true),
						expression.NewGetFieldWithTable(2, types.Int64, "a3", "x", true),
					),
				),
				plan.NewTableAlias(
					"a4",
					plan.NewResolvedTable(xy.WithProjections([]string{"x"}), db1, nil),
				),
				expression.NewEquals(
					expression.NewGetFieldWithTable(0, types.Int64, "a1", "x", true),
					expression.NewGetFieldWithTable(3, types.Int64, "a4", "x", true),
				),
			),
		},
		{
			name: "complicated expression tree",
			sql:  "select (((x = 1 AND y > 1) OR (x < 5 AND -20 < y < 20)) AND (-10 < y < 10)) from xy",
			node: plan.NewProject(
				[]sql.Expression{
					expression.NewAnd(
						expression.NewOr(
							expression.NewAnd(
								expression.NewEquals(
									expression.NewGetFieldWithTable(0, types.Int64, "xy", "x", true),
									expression.NewLiteral(1, types.Int64),
								),
								expression.NewGreaterThan(
									expression.NewGetFieldWithTable(0, types.Int64, "xy", "y", true),
									expression.NewLiteral(1, types.Int64),
								),
							),
							expression.NewAnd(
								expression.NewLessThan(
									expression.NewGetFieldWithTable(0, types.Int64, "xy", "x", true),
									expression.NewLiteral(5, types.Int64),
								),
								expression.NewBetween(
									expression.NewLiteral(-20, types.Int64),
									expression.NewLiteral(20, types.Int64),
									expression.NewGetFieldWithTable(0, types.Int64, "xy", "y", true),
								),
							),
						),
						expression.NewBetween(
							expression.NewLiteral(-10, types.Int64),
							expression.NewLiteral(10, types.Int64),
							expression.NewGetFieldWithTable(0, types.Int64, "xy", "y", true),
						),
					),
				},
				plan.NewResolvedTable(xy, db1, nil),
			),
		},
	}
}

// TODO list the validation rules for each
// TODO handler to loop over running rules
var validationRules = []ValidatorFunc{
	validateLimitAndOffset,
	validateDeleteFrom,
	//validatePrivileges,
	validateCreateTable,
	validateExprSem,
	validateDropConstraint,
	validateReadOnlyDatabase,
	validateReadOnlyTransaction,
	validateDatabaseSet,
	validateIsResolved,
	validateOrderBy,
	validateGroupBy,
	validateSchemaSource,
	validateIndexCreation,
	validateIntervalUsage,
	validateOperands,
	validateSubqueryColumns,
	validateAggregations,
	validateUnionSchemasMatch,
}

var result1 error

func BenchmarkPanicValidation(b *testing.B) {
	ctx := sql.NewEmptyContext()
	for _, tt := range nodes {
		b.Run(tt.name, func(b *testing.B) {
			var r error
			for n := 0; n < b.N; n++ {
				r = validatePanic(ctx, tt.node)
			}
			result1 = r
		})
	}

}

func validatePanic(ctx *sql.Context, n sql.Node) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case semError:
				err = e.error
			default:
				panic(e)
			}
		}
	}()
	for _, v := range validationRules {
		v(ctx, nil, n, nil, nil)
	}
	return
}

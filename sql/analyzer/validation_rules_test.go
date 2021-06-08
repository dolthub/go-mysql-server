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

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/stretchr/testify/require"
)

func TestValidateResolved(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateResolvedRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil)
	require.NoError(err)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil)
	require.Error(err)
}

func TestValidateOrderBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateOrderByRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil)
	require.NoError(err)
	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil)
	require.NoError(err)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, plan.NewSort(
		[]sql.SortField{{Column: aggregation.NewCount(sql.NewEmptyContext(), nil), Order: sql.Descending}},
		nil,
	), nil)
	require.Error(err)
}

func TestValidateGroupBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateGroupByRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil)
	require.NoError(err)
	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil)
	require.NoError(err)

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
		{Name: "col2", Type: sql.Int64},
	}

	child := memory.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(2222)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("alias", expression.NewGetField(0, sql.Text, "col1", true)),
			expression.NewGetField(0, sql.Text, "col1", true),
			aggregation.NewCount(sql.NewEmptyContext(), expression.NewGetField(1, sql.Int64, "col2", true)),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
		},
		plan.NewResolvedTable(child, nil, nil),
	)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, p, nil)
	require.NoError(err)
}

func TestValidateGroupByErr(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateGroupByRule)

	_, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil)
	require.NoError(err)
	_, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil)
	require.NoError(err)

	childSchema := sql.Schema{
		{Name: "col1", Type: sql.Text},
		{Name: "col2", Type: sql.Int64},
	}

	child := memory.NewTable("test", childSchema)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(2222)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(sql.NewEmptyContext(), r))
	}

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
			expression.NewGetField(1, sql.Int64, "col2", true),
		},
		[]sql.Expression{
			expression.NewGetField(0, sql.Text, "col1", true),
		},
		plan.NewResolvedTable(child, nil, nil),
	)

	_, err = vr.Apply(sql.NewEmptyContext(), nil, p, nil)
	require.Error(err)
}

func TestValidateSchemaSource(t *testing.T) {
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"some random node",
			plan.NewProject(nil, nil),
			true,
		},
		{
			"table with valid schema",
			plan.NewResolvedTable(memory.NewTable(
				"mytable",
				sql.Schema{
					{Name: "foo", Source: "mytable"},
					{Name: "bar", Source: "mytable"},
				},
			), nil, nil),
			true,
		},
		{
			"table with invalid schema",
			plan.NewResolvedTable(memory.NewTable(
				"mytable",
				sql.Schema{
					{Name: "foo", Source: ""},
					{Name: "bar", Source: "something"},
				},
			), nil, nil),
			false,
		},
		{
			"table alias with table",
			plan.NewTableAlias("foo", plan.NewResolvedTable(memory.NewTable("mytable", sql.Schema{
				{Name: "foo", Source: "mytable"},
			}), nil, nil)),
			true,
		},
		{
			"table alias with subquery",
			plan.NewTableAlias(
				"foo",
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Text, "bar", false),
						expression.NewGetField(1, sql.Int64, "baz", false),
					},
					nil,
				),
			),
			true,
		},
	}

	rule := getValidationRule(validateSchemaSourceRule)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node, nil)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrValidationSchemaSource.Is(err))
			}
		})
	}
}

func TestValidateUnionSchemasMatch(t *testing.T) {
	table := plan.NewResolvedTable(memory.NewTable(
		"mytable",
		sql.Schema{
			{Name: "foo", Source: "mytable", Type: sql.Text},
			{Name: "bar", Source: "mytable", Type: sql.Int64},
			{Name: "rab", Source: "mytable", Type: sql.Text},
			{Name: "zab", Source: "mytable", Type: sql.Int64},
			{Name: "quuz", Source: "mytable", Type: sql.Boolean},
		},
	), nil, nil)
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"some random node",
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, sql.Text, "bar", false),
					expression.NewGetField(1, sql.Int64, "baz", false),
				},
				table,
			),
			true,
		},
		{
			"top-level union with matching schemas",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Text, "bar", false),
						expression.NewGetField(1, sql.Int64, "baz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, sql.Text, "rab", false),
						expression.NewGetField(3, sql.Int64, "zab", false),
					},
					table,
				),
			),
			true,
		},
		{
			"top-level union with longer left schema",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Text, "bar", false),
						expression.NewGetField(1, sql.Int64, "baz", false),
						expression.NewGetField(4, sql.Boolean, "quuz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, sql.Text, "rab", false),
						expression.NewGetField(3, sql.Int64, "zab", false),
					},
					table,
				),
			),
			false,
		},
		{
			"top-level union with longer right schema",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Text, "bar", false),
						expression.NewGetField(1, sql.Int64, "baz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, sql.Text, "rab", false),
						expression.NewGetField(3, sql.Int64, "zab", false),
						expression.NewGetField(4, sql.Boolean, "quuz", false),
					},
					table,
				),
			),
			false,
		},
		{
			"top-level union with mismatched type in schema",
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, sql.Text, "bar", false),
						expression.NewGetField(1, sql.Int64, "baz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, sql.Text, "rab", false),
						expression.NewGetField(3, sql.Boolean, "zab", false),
					},
					table,
				),
			),
			false,
		},
		{
			"subquery union",
			plan.NewSubqueryAlias(
				"aliased", "select bar, baz from mytable union select rab, zab from mytable",
				plan.NewUnion(
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetField(0, sql.Text, "bar", false),
							expression.NewGetField(1, sql.Int64, "baz", false),
						},
						table,
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetField(2, sql.Text, "rab", false),
							expression.NewGetField(3, sql.Boolean, "zab", false),
						},
						table,
					),
				),
			),
			false,
		},
	}

	rule := getValidationRule(validateUnionSchemasMatchRule)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node, nil)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrUnionSchemasMatch.Is(err))
			}
		})
	}
}

func TestValidateProjectTuples(t *testing.T) {
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"project with no tuple",
			plan.NewProject([]sql.Expression{
				expression.NewLiteral(1, sql.Int64),
			}, nil),
			true,
		},
		{
			"project with a 1 elem tuple",
			plan.NewProject([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
				),
			}, nil),
			true,
		},
		{
			"project with a 2 elem tuple",
			plan.NewProject([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
					expression.NewLiteral(2, sql.Int64),
				),
			}, nil),
			false,
		},
		{
			"distinct with a 2 elem tuple inside the project",
			plan.NewDistinct(
				plan.NewProject([]sql.Expression{
					expression.NewTuple(
						expression.NewLiteral(1, sql.Int64),
						expression.NewLiteral(2, sql.Int64),
					),
				}, nil)),
			false,
		},
		{
			"alias with a tuple",
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("foo", expression.NewTuple(
						expression.NewLiteral(1, sql.Int64),
						expression.NewLiteral(2, sql.Int64),
					)),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			false,
		},
		{
			"groupby with no tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewLiteral(1, sql.Int64),
			}, nil, nil),
			true,
		},
		{
			"groupby with a 1 elem tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
				),
			}, nil, nil),
			true,
		},
		{
			"groupby with a 2 elem tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, sql.Int64),
					expression.NewLiteral(1, sql.Int64),
				),
			}, nil, nil),
			false,
		},
	}

	rule := getValidationRule(validateProjectTuplesRule)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node, nil)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrProjectTuple.Is(err))
			}
		})
	}
}

func TestValidateIndexCreation(t *testing.T) {
	table := memory.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
	})

	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"columns from another table",
			plan.NewCreateIndex(
				"idx", plan.NewResolvedTable(table, nil, nil),
				[]sql.Expression{expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "bar", "b", false),
				)},
				"",
				make(map[string]string),
			),
			false,
		},
		{
			"columns that don't exist",
			plan.NewCreateIndex(
				"idx", plan.NewResolvedTable(table, nil, nil),
				[]sql.Expression{expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", false),
				)},
				"",
				make(map[string]string),
			),
			false,
		},
		{
			"columns only from table",
			plan.NewCreateIndex(
				"idx", plan.NewResolvedTable(table, nil, nil),
				[]sql.Expression{expression.NewEquals(
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "a", false),
					expression.NewGetFieldWithTable(0, sql.Int64, "foo", "b", false),
				)},
				"",
				make(map[string]string),
			),
			true,
		},
	}

	rule := getValidationRule(validateIndexCreationRule)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node, nil)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrUnknownIndexColumns.Is(err))
			}
		})
	}
}

func TestValidateCaseResultTypes(t *testing.T) {
	rule := getValidationRule(validateCaseResultTypesRule)

	testCases := []struct {
		name string
		expr *expression.Case
		ok   bool
	}{
		{
			"one of the branches does not match",
			expression.NewCase(
				expression.NewGetField(0, sql.Int64, "foo", false),
				[]expression.CaseBranch{
					{
						Cond:  expression.NewLiteral(int64(1), sql.Int64),
						Value: expression.NewLiteral("foo", sql.LongText),
					},
					{
						Cond:  expression.NewLiteral(int64(2), sql.Int64),
						Value: expression.NewLiteral(int64(1), sql.Int64),
					},
				},
				expression.NewLiteral("foo", sql.LongText),
			),
			false,
		},
		{
			"else is not exact but matches",
			expression.NewCase(
				expression.NewGetField(0, sql.Int64, "foo", false),
				[]expression.CaseBranch{
					{
						Cond:  expression.NewLiteral(int64(1), sql.Int64),
						Value: expression.NewLiteral(int64(2), sql.Int64),
					},
				},
				expression.NewLiteral(int64(3), sql.Int8),
			),
			true,
		},
		{
			"else does not match",
			expression.NewCase(
				expression.NewGetField(0, sql.Int64, "foo", false),
				[]expression.CaseBranch{
					{
						Cond:  expression.NewLiteral(int64(1), sql.Int64),
						Value: expression.NewLiteral("foo", sql.LongText),
					},
					{
						Cond:  expression.NewLiteral(int64(2), sql.Int64),
						Value: expression.NewLiteral("bar", sql.Text),
					},
				},
				expression.NewLiteral(int64(1), sql.Int64),
			),
			false,
		},
		{
			"all ok",
			expression.NewCase(
				expression.NewGetField(0, sql.Int64, "foo", false),
				[]expression.CaseBranch{
					{
						Cond:  expression.NewLiteral(int64(1), sql.Int64),
						Value: expression.NewLiteral("foo", sql.LongText),
					},
					{
						Cond:  expression.NewLiteral(int64(2), sql.Int64),
						Value: expression.NewLiteral("bar", sql.LongText),
					},
				},
				expression.NewLiteral("baz", sql.LongText),
			),
			true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, err := rule.Apply(sql.NewEmptyContext(), nil, plan.NewProject(
				[]sql.Expression{tt.expr},
				plan.NewResolvedTable(dualTable, nil, nil),
			), nil)

			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrCaseResultType.Is(err))
			}
		})
	}
}

func mustFunc(e sql.Expression, err error) sql.Expression {
	if err != nil {
		panic(err)
	}
	return e
}

func TestValidateIntervalUsage(t *testing.T) {
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"date add",
			plan.NewProject(
				[]sql.Expression{
					mustFunc(function.NewDateAdd(
						sql.NewEmptyContext(),
						expression.NewLiteral("2018-05-01", sql.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), sql.Int64),
							"DAY",
						),
					)),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			true,
		},
		{
			"date sub",
			plan.NewProject(
				[]sql.Expression{
					mustFunc(function.NewDateSub(
						sql.NewEmptyContext(),
						expression.NewLiteral("2018-05-01", sql.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), sql.Int64),
							"DAY",
						),
					)),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			true,
		},
		{
			"+ op",
			plan.NewProject(
				[]sql.Expression{
					expression.NewPlus(
						expression.NewLiteral("2018-05-01", sql.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), sql.Int64),
							"DAY",
						),
					),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			true,
		},
		{
			"- op",
			plan.NewProject(
				[]sql.Expression{
					expression.NewMinus(
						expression.NewLiteral("2018-05-01", sql.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), sql.Int64),
							"DAY",
						),
					),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			true,
		},
		{
			"invalid",
			plan.NewProject(
				[]sql.Expression{
					expression.NewInterval(
						expression.NewLiteral(int64(1), sql.Int64),
						"DAY",
					),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			false,
		},
		{
			"alias",
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("foo", expression.NewInterval(
						expression.NewLiteral(int64(1), sql.Int64),
						"DAY",
					)),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			_, err := validateIntervalUsage(sql.NewEmptyContext(), nil, tt.node, nil)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrIntervalInvalidUse.Is(err))
			}
		})
	}
}

func TestValidateExplodeUsage(t *testing.T) {
	ctx := sql.NewEmptyContext()
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"valid",
			plan.NewGenerate(
				plan.NewProject(
					[]sql.Expression{
						expression.NewAlias("foo", function.NewGenerate(
							ctx, expression.NewGetField(0, sql.CreateArray(sql.Int64), "f", false),
						)),
					},
					plan.NewUnresolvedTable("dual", ""),
				),
				expression.NewGetField(0, sql.CreateArray(sql.Int64), "foo", false),
			),
			true,
		},
		{
			"where",
			plan.NewFilter(
				function.NewArrayLength(
					ctx,
					function.NewExplode(
						ctx, expression.NewGetField(0, sql.CreateArray(sql.Int64), "foo", false),
					),
				),
				plan.NewGenerate(
					plan.NewProject(
						[]sql.Expression{
							expression.NewAlias("foo", function.NewGenerate(
								ctx, expression.NewGetField(0, sql.CreateArray(sql.Int64), "f", false),
							)),
						},
						plan.NewUnresolvedTable("dual", ""),
					),
					expression.NewGetField(0, sql.CreateArray(sql.Int64), "foo", false),
				),
			),
			false,
		},
		{
			"group by",
			plan.NewGenerate(
				plan.NewGroupBy(
					[]sql.Expression{
						expression.NewAlias("foo", function.NewExplode(
							ctx, expression.NewGetField(0, sql.CreateArray(sql.Int64), "f", false),
						)),
					},
					[]sql.Expression{
						expression.NewAlias("foo", function.NewExplode(
							ctx, expression.NewGetField(0, sql.CreateArray(sql.Int64), "f", false),
						)),
					},
					plan.NewUnresolvedTable("dual", ""),
				),
				expression.NewGetField(0, sql.CreateArray(sql.Int64), "foo", false),
			),
			false,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			_, err := validateExplodeUsage(sql.NewEmptyContext(), nil, tt.node, nil)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(ErrExplodeInvalidUse.Is(err))
			}
		})
	}
}

func TestValidateSubqueryColumns(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	node := plan.NewProject([]sql.Expression{
		plan.NewSubquery(plan.NewProject(
			[]sql.Expression{
				lit(1),
				lit(2),
			},
			dummyNode{true},
		), "select 1, 2"),
	}, dummyNode{true})

	_, err := validateSubqueryColumns(ctx, nil, node, nil)
	require.Error(err)
	require.True(sql.ErrSubqueryMultipleColumns.Is(err))

	table := memory.NewTable("test", sql.Schema{
		{Name: "foo", Type: sql.Text},
	})
	subTable := memory.NewTable("subtest", sql.Schema{
		{Name: "bar", Type: sql.Text},
	})

	node = plan.NewProject([]sql.Expression{
		plan.NewSubquery(plan.NewFilter(expression.NewGreaterThan(
			expression.NewGetField(0, sql.Boolean, "foo", false),
			lit(1),
		), plan.NewProject(
			[]sql.Expression{
				expression.NewGetField(1, sql.Boolean, "bar", false),
			},
			plan.NewResolvedTable(subTable, nil, nil),
		)), "select bar from subtest where foo > 1"),
	}, plan.NewResolvedTable(table, nil, nil))

	_, err = validateSubqueryColumns(ctx, nil, node, nil)
	require.NoError(err)

	node = plan.NewProject([]sql.Expression{
		plan.NewSubquery(plan.NewFilter(expression.NewGreaterThan(
			expression.NewGetField(1, sql.Boolean, "foo", false),
			lit(1),
		), plan.NewProject(
			[]sql.Expression{
				expression.NewGetField(2, sql.Boolean, "bar", false),
			},
			plan.NewResolvedTable(subTable, nil, nil),
		)), "select bar from subtest where foo > 1"),
	}, plan.NewResolvedTable(table, nil, nil))

	_, err = validateSubqueryColumns(ctx, nil, node, nil)
	require.NoError(err)

	node = plan.NewProject([]sql.Expression{
		plan.NewSubquery(plan.NewProject(
			[]sql.Expression{
				lit(1),
			},
			dummyNode{true},
		), "select 1"),
	}, dummyNode{true})

	_, err = validateSubqueryColumns(ctx, nil, node, nil)
	require.NoError(err)

}

type dummyNode struct{ resolved bool }

func (n dummyNode) String() string                                   { return "dummynode" }
func (n dummyNode) Resolved() bool                                   { return n.resolved }
func (dummyNode) Schema() sql.Schema                                 { return nil }
func (dummyNode) Children() []sql.Node                               { return nil }
func (dummyNode) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) { return nil, nil }
func (dummyNode) WithChildren(...sql.Node) (sql.Node, error)         { return nil, nil }

func getValidationRule(name string) Rule {
	for _, rule := range DefaultValidationRules {
		if rule.Name == name {
			return rule
		}
	}
	panic("missing rule")
}

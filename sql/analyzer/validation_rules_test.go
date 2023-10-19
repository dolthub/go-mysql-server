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

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestValidateResolved(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateResolvedId)

	_, _, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil, DefaultRuleSelector)
	require.NoError(err)

	_, _, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil, DefaultRuleSelector)
	require.Error(err)
}

func TestValidateOrderBy(t *testing.T) {
	require := require.New(t)

	vr := getValidationRule(validateOrderById)

	_, _, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil, DefaultRuleSelector)
	require.NoError(err)
	_, _, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil, DefaultRuleSelector)
	require.NoError(err)

	_, _, err = vr.Apply(sql.NewEmptyContext(), nil, plan.NewSort(
		[]sql.SortField{{Column: aggregation.NewCount(nil), Order: sql.Descending}},
		nil,
	), nil, DefaultRuleSelector)
	require.Error(err)
}

func TestValidateGroupBy(t *testing.T) {
	variables.InitSystemVariables()
	require := require.New(t)

	vr := getValidationRule(validateGroupById)

	_, _, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil, DefaultRuleSelector)
	require.NoError(err)
	_, _, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil, DefaultRuleSelector)
	require.NoError(err)

	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: types.Text},
		{Name: "col2", Type: types.Int64},
	})

	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	child := memory.NewTable(db, "test", childSchema, nil)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(2222)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(ctx, r))
	}

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("alias", expression.NewGetField(0, types.Text, "col1", true)),
			expression.NewGetField(0, types.Text, "col1", true),
			aggregation.NewCount(expression.NewGetField(1, types.Int64, "col2", true)),
		},
		[]sql.Expression{
			expression.NewGetField(0, types.Text, "col1", true),
		},
		plan.NewResolvedTable(child, nil, nil),
	)

	_, _, err = vr.Apply(sql.NewEmptyContext(), nil, p, nil, DefaultRuleSelector)
	require.NoError(err)
}

func TestValidateGroupByErr(t *testing.T) {
	require := require.New(t)
	vr := getValidationRule(validateGroupById)

	_, _, err := vr.Apply(sql.NewEmptyContext(), nil, dummyNode{true}, nil, DefaultRuleSelector)
	require.NoError(err)
	_, _, err = vr.Apply(sql.NewEmptyContext(), nil, dummyNode{false}, nil, DefaultRuleSelector)
	require.NoError(err)

	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "col1", Type: types.Text},
		{Name: "col2", Type: types.Int64},
	})

	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	child := memory.NewTable(db, "test", childSchema, nil)

	rows := []sql.Row{
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_1", int64(2222)),
		sql.NewRow("col1_2", int64(4444)),
		sql.NewRow("col1_1", int64(1111)),
		sql.NewRow("col1_2", int64(4444)),
	}

	for _, r := range rows {
		require.NoError(child.Insert(ctx, r))
	}

	p := plan.NewGroupBy(
		[]sql.Expression{
			expression.NewGetField(0, types.Text, "col1", true),
			expression.NewGetField(1, types.Int64, "col2", true),
		},
		[]sql.Expression{
			expression.NewGetField(0, types.Text, "col1", true),
		},
		plan.NewResolvedTable(child, nil, nil),
	)

	err = sql.SystemVariables.SetGlobal("sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY")
	require.NoError(err)
	_, _, err = vr.Apply(ctx, nil, p, nil, DefaultRuleSelector)
	require.Error(err)
}

func TestValidateSchemaSource(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

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
			plan.NewResolvedTable(memory.NewTable(db, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "foo", Source: "mytable"},
				{Name: "bar", Source: "mytable"},
			}), nil), nil, nil),
			true,
		},
		{
			"table with invalid schema",
			plan.NewResolvedTable(memory.NewTable(db, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "foo", Source: ""},
				{Name: "bar", Source: "something"},
			}), nil), nil, nil),
			false,
		},
		{
			"table alias with table",
			plan.NewTableAlias("foo", plan.NewResolvedTable(memory.NewTable(db, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
				{Name: "foo", Source: "mytable"},
			}), nil), nil, nil)),
			true,
		},
		{
			"table alias with subquery",
			plan.NewTableAlias(
				"foo",
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, types.Text, "bar", false),
						expression.NewGetField(1, types.Int64, "baz", false),
					},
					nil,
				),
			),
			true,
		},
	}

	rule := getValidationRule(validateSchemaSourceId)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, _, err := rule.Apply(ctx, nil, tt.node, nil, DefaultRuleSelector)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(analyzererrors.ErrValidationSchemaSource.Is(err))
			}
		})
	}
}

func TestValidateUnionSchemasMatch(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := plan.NewResolvedTable(memory.NewTable(db, "mytable", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "foo", Source: "mytable", Type: types.Text},
		{Name: "bar", Source: "mytable", Type: types.Int64},
		{Name: "rab", Source: "mytable", Type: types.Text},
		{Name: "zab", Source: "mytable", Type: types.Int64},
		{Name: "quuz", Source: "mytable", Type: types.Boolean},
	}), nil), nil, nil)
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"some random node",
			plan.NewProject(
				[]sql.Expression{
					expression.NewGetField(0, types.Text, "bar", false),
					expression.NewGetField(1, types.Int64, "baz", false),
				},
				table,
			),
			true,
		},
		{
			"top-level union with matching schemas",
			plan.NewSetOp(
				plan.UnionType,
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, types.Text, "bar", false),
						expression.NewGetField(1, types.Int64, "baz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, types.Text, "rab", false),
						expression.NewGetField(3, types.Int64, "zab", false),
					},
					table,
				),
				false, nil, nil, nil,
			),
			true,
		},
		{
			"top-level union with longer left schema",
			plan.NewSetOp(
				plan.UnionType,
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, types.Text, "bar", false),
						expression.NewGetField(1, types.Int64, "baz", false),
						expression.NewGetField(4, types.Boolean, "quuz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, types.Text, "rab", false),
						expression.NewGetField(3, types.Int64, "zab", false),
					},
					table,
				),
				false, nil, nil, nil,
			),
			false,
		},
		{
			"top-level union with longer right schema",
			plan.NewSetOp(
				plan.UnionType,
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, types.Text, "bar", false),
						expression.NewGetField(1, types.Int64, "baz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, types.Text, "rab", false),
						expression.NewGetField(3, types.Int64, "zab", false),
						expression.NewGetField(4, types.Boolean, "quuz", false),
					},
					table,
				),
				false, nil, nil, nil,
			),
			false,
		},
		{
			"top-level union with mismatched type in schema",
			plan.NewSetOp(
				plan.UnionType,
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(0, types.Text, "bar", false),
						expression.NewGetField(1, types.Int64, "baz", false),
					},
					table,
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetField(2, types.Text, "rab", false),
						expression.NewGetField(3, types.Boolean, "zab", false),
					},
					table,
				),
				false, nil, nil, nil,
			),
			false,
		},
		{
			"subquery union",
			plan.NewSubqueryAlias(
				"aliased", "select bar, baz from mytable union select rab, zab from mytable",
				plan.NewSetOp(
					plan.UnionType,
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetField(0, types.Text, "bar", false),
							expression.NewGetField(1, types.Int64, "baz", false),
						},
						table,
					),
					plan.NewProject(
						[]sql.Expression{
							expression.NewGetField(2, types.Text, "rab", false),
							expression.NewGetField(3, types.Boolean, "zab", false),
						},
						table,
					),
					false, nil, nil, nil),
			),
			false,
		},
	}

	rule := getValidationRule(validateUnionSchemasMatchId)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, _, err := rule.Apply(ctx, nil, tt.node, nil, DefaultRuleSelector)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(analyzererrors.ErrUnionSchemasMatch.Is(err))
			}
		})
	}
}

func TestValidateOperands(t *testing.T) {
	testCases := []struct {
		name string
		node sql.Node
		ok   bool
	}{
		{
			"project with no tuple",
			plan.NewProject([]sql.Expression{
				expression.NewLiteral(1, types.Int64),
			}, nil),
			true,
		},
		{
			"project with a 1 elem tuple",
			plan.NewProject([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, types.Int64),
				),
			}, nil),
			true,
		},
		{
			"project with a 2 elem tuple",
			plan.NewProject([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, types.Int64),
					expression.NewLiteral(2, types.Int64),
				),
			}, nil),
			false,
		},
		{
			"distinct with a 2 elem tuple inside the project",
			plan.NewDistinct(
				plan.NewProject([]sql.Expression{
					expression.NewTuple(
						expression.NewLiteral(1, types.Int64),
						expression.NewLiteral(2, types.Int64),
					),
				}, nil)),
			false,
		},
		{
			"alias with a tuple",
			plan.NewProject(
				[]sql.Expression{
					expression.NewAlias("foo", expression.NewTuple(
						expression.NewLiteral(1, types.Int64),
						expression.NewLiteral(2, types.Int64),
					)),
				},
				plan.NewUnresolvedTable("dual", ""),
			),
			false,
		},
		{
			"groupby with no tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewLiteral(1, types.Int64),
			}, nil, nil),
			true,
		},
		{
			"groupby with a 1 elem tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, types.Int64),
				),
			}, nil, nil),
			true,
		},
		{
			"groupby with a 2 elem tuple",
			plan.NewGroupBy([]sql.Expression{
				expression.NewTuple(
					expression.NewLiteral(1, types.Int64),
					expression.NewLiteral(1, types.Int64),
				),
			}, nil, nil),
			false,
		},
		{
			"validate subquery columns",
			plan.NewProject([]sql.Expression{
				plan.NewSubquery(plan.NewProject(
					[]sql.Expression{
						lit(1),
						lit(2),
					},
					dummyNode{true},
				), "select 1, 2"),
			}, dummyNode{true}),
			false,
		},
	}

	rule := getValidationRule(validateOperandsId)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, _, err := rule.Apply(sql.NewEmptyContext(), nil, tt.node, nil, DefaultRuleSelector)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(sql.ErrInvalidOperandColumns.Is(err))
			}
		})
	}
}

func TestValidateIndexCreation(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := memory.NewTable(db, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
	}), nil)

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
					expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", false),
					expression.NewGetFieldWithTable(0, types.Int64, "db", "bar", "b", false),
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
					expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", false),
					expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "c", false),
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
					expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "a", false),
					expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "b", false),
				)},
				"",
				make(map[string]string),
			),
			true,
		},
	}

	rule := getValidationRule(validateIndexCreationId)
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			_, _, err := rule.Apply(ctx, nil, tt.node, nil, DefaultRuleSelector)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(analyzererrors.ErrUnknownIndexColumns.Is(err))
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
						expression.NewLiteral("2018-05-01", types.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), types.Int64),
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
						expression.NewLiteral("2018-05-01", types.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), types.Int64),
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
						expression.NewLiteral("2018-05-01", types.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), types.Int64),
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
						expression.NewLiteral("2018-05-01", types.LongText),
						expression.NewInterval(
							expression.NewLiteral(int64(1), types.Int64),
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
						expression.NewLiteral(int64(1), types.Int64),
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
						expression.NewLiteral(int64(1), types.Int64),
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

			_, _, err := validateIntervalUsage(sql.NewEmptyContext(), nil, tt.node, nil, DefaultRuleSelector)
			if tt.ok {
				require.NoError(err)
			} else {
				require.Error(err)
				require.True(analyzererrors.ErrIntervalInvalidUse.Is(err))
			}
		})
	}
}

func TestValidateSubqueryColumns(t *testing.T) {
	t.Skip()

	require := require.New(t)

	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	table := memory.NewTable(db, "test", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "foo", Type: types.Text},
	}), nil)
	subTable := memory.NewTable(db, "subtest", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "bar", Type: types.Text},
	}), nil)

	var node sql.Node
	node = plan.NewProject([]sql.Expression{
		plan.NewSubquery(plan.NewFilter(expression.NewGreaterThan(
			expression.NewGetField(0, types.Boolean, "foo", false),
			lit(1),
		), plan.NewProject(
			[]sql.Expression{
				expression.NewGetField(1, types.Boolean, "bar", false),
			},
			plan.NewResolvedTable(subTable, nil, nil),
		)), "select bar from subtest where foo > 1"),
	}, plan.NewResolvedTable(table, nil, nil))

	_, _, err := validateSubqueryColumns(ctx, nil, node, nil, DefaultRuleSelector)
	require.NoError(err)

	node = plan.NewProject([]sql.Expression{
		plan.NewSubquery(plan.NewFilter(expression.NewGreaterThan(
			expression.NewGetField(1, types.Boolean, "foo", false),
			lit(1),
		), plan.NewProject(
			[]sql.Expression{
				expression.NewGetField(2, types.Boolean, "bar", false),
			},
			plan.NewResolvedTable(subTable, nil, nil),
		)), "select bar from subtest where foo > 1"),
	}, plan.NewResolvedTable(table, nil, nil))

	_, _, err = validateSubqueryColumns(ctx, nil, node, nil, DefaultRuleSelector)
	require.Error(err)
	require.True(analyzererrors.ErrSubqueryFieldIndex.Is(err))

	node = plan.NewProject([]sql.Expression{
		plan.NewSubquery(plan.NewProject(
			[]sql.Expression{
				lit(1),
			},
			dummyNode{true},
		), "select 1"),
	}, dummyNode{true})

	_, _, err = validateSubqueryColumns(ctx, nil, node, nil, DefaultRuleSelector)
	require.NoError(err)

}

type dummyNode struct{ resolved bool }

var _ sql.Node = dummyNode{}
var _ sql.CollationCoercible = dummyNode{}

func (n dummyNode) String() string                                   { return "dummynode" }
func (n dummyNode) Resolved() bool                                   { return n.resolved }
func (dummyNode) IsReadOnly() bool                                   { return true }
func (dummyNode) Schema() sql.Schema                                 { return nil }
func (dummyNode) Children() []sql.Node                               { return nil }
func (dummyNode) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) { return nil, nil }
func (dummyNode) WithChildren(...sql.Node) (sql.Node, error)         { return nil, nil }
func (dummyNode) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}
func (dummyNode) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

func getValidationRule(id RuleId) Rule {
	for _, rule := range DefaultValidationRules {
		if rule.Id == id {
			return rule
		}
	}
	panic("missing rule")
}

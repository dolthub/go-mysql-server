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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestEraseProjection(t *testing.T) {
	require := require.New(t)
	f := getRule(eraseProjectionId)

	db := memory.NewDatabase("db")

	table := memory.NewTable(db, "mytable", sql.NewPrimaryKeySchema(sql.Schema{{
		Name: "i", Source: "mytable", Type: types.Int64,
	}}), nil)

	expected := plan.NewSort(
		[]sql.SortField{{Column: expression.NewGetField(2, types.Int64, "foo", false)}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewGetFieldWithTable(0, 0, types.Int64, "db", "mytable", "i", false),
				expression.NewGetField(1, types.Int64, "bar", false),
				expression.NewAlias("foo", expression.NewLiteral(1, types.Int64)),
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewLiteral(1, types.Int64),
					expression.NewGetField(1, types.Int64, "bar", false),
				),
				plan.NewProject(
					[]sql.Expression{
						expression.NewGetFieldWithTable(0, 0, types.Int64, "db", "mytable", "i", false),
						expression.NewAlias("bar", expression.NewLiteral(2, types.Int64)),
					},
					plan.NewResolvedTable(table, nil, nil),
				),
			),
		),
	)

	node := plan.NewProject(
		[]sql.Expression{
			expression.NewGetFieldWithTable(0, 0, types.Int64, "db", "mytable", "i", false),
			expression.NewGetField(1, types.Int64, "bar", false),
			expression.NewGetField(2, types.Int64, "foo", false),
		},
		expected,
	)

	result, _, err := f.Apply(sql.NewEmptyContext(), NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(expected, result)

	result, _, err = f.Apply(sql.NewEmptyContext(), NewDefault(nil), expected, nil, DefaultRuleSelector)
	require.NoError(err)
	require.Equal(expected, result)
}

func TestEvalFilter(t *testing.T) {
	db := memory.NewDatabase("db")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	inner := memory.NewTable(db, "foo", sql.PrimaryKeySchema{}, nil)
	rule := getRule(simplifyFiltersId)

	testCases := []struct {
		filter   sql.Expression
		expected sql.Node
	}{
		{
			and(
				eq(lit(5), lit(5)),
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
			),
			plan.NewFilter(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			and(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				eq(lit(5), lit(5)),
			),
			plan.NewFilter(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			and(
				eq(lit(5), lit(4)),
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
			),
			plan.NewEmptyTableWithSchema(inner.Schema(ctx)),
		},
		{
			and(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.NewEmptyTableWithSchema(inner.Schema(ctx)),
		},
		{
			and(
				eq(lit(4), lit(4)),
				eq(lit(5), lit(5)),
			),
			plan.NewResolvedTable(inner, nil, nil),
		},
		{
			or(
				eq(lit(5), lit(4)),
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
			),
			plan.NewFilter(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			or(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				eq(lit(5), lit(4)),
			),
			plan.NewFilter(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				plan.NewResolvedTable(inner, nil, nil),
			),
		},
		{
			or(
				eq(lit(5), lit(5)),
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
			),
			plan.NewResolvedTable(inner, nil, nil),
		},
		{
			or(
				eq(
					expression.NewGetFieldWithTable(0, 0, types.Int64, "", "foo", "bar", false),
					lit(5)),
				eq(lit(5), lit(5)),
			),
			plan.NewResolvedTable(inner, nil, nil),
		},
		{
			or(
				eq(lit(5), lit(4)),
				eq(lit(5), lit(4)),
			),
			plan.NewEmptyTableWithSchema(inner.Schema(ctx)),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.filter.String(), func(t *testing.T) {
			require := require.New(t)
			node := plan.NewFilter(tt.filter, plan.NewResolvedTable(inner, nil, nil))
			result, _, err := rule.Apply(ctx, NewDefault(nil), node, nil, DefaultRuleSelector)
			require.NoError(err)
			require.Equal(tt.expected, result)
		})
	}
}

func TestPushNotFilters(t *testing.T) {
	tests := []struct {
		in  string
		exp string
	}{
		{
			in:  "NOT(NOT(x IS NULL))",
			exp: "xy.x IS NULL",
		},
		{
			in:  "NOT(x BETWEEN 0 AND 5)",
			exp: "((xy.x < 0) OR (xy.x > 5))",
		},
		{
			in:  "NOT(x <= 0)",
			exp: "(xy.x > 0)",
		},
		{
			in:  "NOT(x < 0)",
			exp: "(xy.x >= 0)",
		},
		{
			in:  "NOT(x > 0)",
			exp: "(xy.x <= 0)",
		},
		{
			in:  "NOT(x >= 0)",
			exp: "(xy.x < 0)",
		},
		// TODO this isn't correct for join filters
		//{
		//	in:  "NOT(y IS NULL)",
		//	exp: "((xy.x < NULL) OR (xy.x > NULL))",
		//},
		{
			in:  "NOT (x > 2 AND y > 2)",
			exp: "((xy.x <= 2) OR (xy.y <= 2))",
		},
		{
			in:  "NOT (x > 2 AND NOT(y > 2))",
			exp: "((xy.x <= 2) OR (xy.y > 2))",
		},
		{
			in:  "((NOT(x > 1 AND NOT((x > 0) OR (y < 2))) OR (y > 1)) OR NOT(y < 3))",
			exp: "((((xy.x <= 1) OR ((xy.x > 0) OR (xy.y < 2))) OR (xy.y > 1)) OR (xy.y >= 3))",
		},
	}

	// todo dummy catalog and table
	db := memory.NewDatabase("mydb")
	cat := newTestCatalog(db)
	pro := memory.NewDBProvider(db)
	sess := memory.NewSession(sql.NewBaseSession(), pro)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	ctx.SetCurrentDatabase("mydb")

	b := planbuilder.New(ctx, cat)

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			q := fmt.Sprintf("SELECT 1 from xy WHERE %s", tt.in)
			node, err := b.ParseOne(q)
			require.NoError(t, err)

			cmp, _, err := pushNotFilters(ctx, nil, node, nil, nil)
			require.NoError(t, err)

			cmpF := cmp.(*plan.Project).Child.(*plan.Filter).Expression
			cmpStr := cmpF.String()

			require.Equal(t, tt.exp, cmpStr, fmt.Sprintf("\nexpected: %s\nfound:%s\n", tt.exp, cmpStr))
		})
	}
}

func newTestCatalog(db *memory.Database) *sql.MapCatalog {
	cat := &sql.MapCatalog{
		Databases: make(map[string]sql.Database),
		Tables:    make(map[string]sql.Table),
	}

	cat.Tables["xy"] = memory.NewTable(db, "xy", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "x", Type: types.Int64},
		{Name: "y", Type: types.Int64},
		{Name: "z", Type: types.Int64},
	}, 0), nil)
	cat.Tables["uv"] = memory.NewTable(db, "uv", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "u", Type: types.Int64},
		{Name: "v", Type: types.Int64},
		{Name: "w", Type: types.Int64},
	}, 0), nil)

	db.AddTable("xy", cat.Tables["xy"].(memory.MemTable))
	db.AddTable("uv", cat.Tables["uv"].(memory.MemTable))
	cat.Databases["mydb"] = db
	cat.Funcs = function.NewRegistry()
	return cat
}

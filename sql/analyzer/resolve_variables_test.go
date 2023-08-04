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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
)

func TestResolveSetVariables(t *testing.T) {
	rule := getRuleFrom(OnceBeforeDefault_Exp, resolveSetVariablesId)

	var testCases = []analyzerFnTestCase{
		{
			name: "set defaults",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("@@auto_increment_increment"), expression.NewDefaultColumn("")),
					expression.NewSetField(uc("@@sql_select_limit"), expression.NewDefaultColumn("")),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.SystemVariableScope_Session), expression.NewLiteral(int64(1), types.Int64)),
					expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.SystemVariableScope_Session), expression.NewLiteral(int64(math.MaxInt32), types.Int64)),
				},
			),
		},
		{
			name: "set defaults with @@session",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("@@session.auto_increment_increment"), expression.NewDefaultColumn("")),
					expression.NewSetField(uc("@@session.sql_select_limit"), expression.NewDefaultColumn("")),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.SystemVariableScope_Session), expression.NewLiteral(int64(1), types.Int64)),
					expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.SystemVariableScope_Session), expression.NewLiteral(int64(math.MaxInt32), types.Int64)),
				},
			),
		},
		{
			name: "set defaults with @@session and mixed case",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("@@session.auto_increment_INCREMENT"), expression.NewDefaultColumn("")),
					expression.NewSetField(uc("@@sql_select_LIMIT"), expression.NewDefaultColumn("")),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(expression.NewSystemVar("auto_increment_INCREMENT", sql.SystemVariableScope_Session), expression.NewLiteral(int64(1), types.Int64)),
					expression.NewSetField(expression.NewSystemVar("sql_select_LIMIT", sql.SystemVariableScope_Session), expression.NewLiteral(int64(math.MaxInt32), types.Int64)),
				},
			),
		},
		{
			name: "set expression",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("@@auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(uc("@@sql_mode"), mustExpr(function.NewConcat(uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.SystemVariableScope_Session), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(expression.NewSystemVar("sql_mode", sql.SystemVariableScope_Session), mustExpr(function.NewConcat(uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
		},
		{
			name: "set expression with barewords",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(uc("@@sql_mode"), mustExpr(function.NewConcat(uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(expression.NewSystemVar("sql_mode", sql.SystemVariableScope_Session), mustExpr(function.NewConcat(uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
		},
		{
			name: "set all barewords",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(uc("sql_mode"), mustExpr(function.NewConcat(uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
		},
	}

	runTestCases(t, nil, testCases, nil, *rule)
}

func TestResolveBarewordSetVariables(t *testing.T) {
	rule := getRuleFrom(DefaultRules_Exp, resolveBarewordSetVariablesId)

	var testCases = []analyzerFnTestCase{
		{
			name: "var name and expression both barewords",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(&deferredColumn{uc("sql_mode")}, &deferredColumn{uc("hello")}),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(expression.NewSystemVar("sql_mode", sql.SystemVariableScope_Session), expression.NewLiteral("hello", types.LongText)),
				},
			),
		},
	}

	runTestCases(t, nil, testCases, nil, *rule)
}

func TestResolveColumnsSession(t *testing.T) {
	require := require.New(t)

	fooBarValue := int64(42)
	fooBarType := types.ApproximateTypeFromValue(fooBarValue)

	ctx := sql.NewContext(context.Background(), sql.WithSession(sql.NewBaseSession()))
	err := ctx.SetUserVariable(ctx, "foo_bar", fooBarValue, fooBarType)
	require.NoError(err)
	err = ctx.SetSessionVariable(ctx, "autocommit", true)
	require.NoError(err)

	node := plan.NewProject(
		[]sql.Expression{
			uc("@foo_bar"),
			uc("@bar_baz"),
			uc("@@autocommit"),
			uc("@myvar"),
		},
		plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
	)

	result, _, err := resolveVariables(ctx, NewDefault(nil), node, nil, DefaultRuleSelector)
	require.NoError(err)

	expected := plan.NewProject(
		[]sql.Expression{
			expression.NewUserVarWithType("foo_bar", fooBarType),
			expression.NewUserVarWithType("bar_baz", nil),
			expression.NewSystemVar("autocommit", sql.SystemVariableScope_Session),
			expression.NewUserVarWithType("myvar", nil),
		},
		plan.NewResolvedTable(plan.NewResolvedDualTable(), nil, nil),
	)

	require.Equal(expected, result)
}

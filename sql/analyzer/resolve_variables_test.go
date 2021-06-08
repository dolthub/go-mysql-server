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
	"math"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func TestResolveSetVariables(t *testing.T) {
	ctx := sql.NewEmptyContext()
	rule := getRuleFrom(OnceBeforeDefault, "resolve_set_variables")

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
					expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.SystemVariableScope_Session), expression.NewLiteral(int64(1), sql.Int64)),
					expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.SystemVariableScope_Session), expression.NewLiteral(int64(math.MaxInt32), sql.Int64)),
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
					expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.SystemVariableScope_Session), expression.NewLiteral(int64(1), sql.Int64)),
					expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.SystemVariableScope_Session), expression.NewLiteral(int64(math.MaxInt32), sql.Int64)),
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
					expression.NewSetField(expression.NewSystemVar("auto_increment_INCREMENT", sql.SystemVariableScope_Session), expression.NewLiteral(int64(1), sql.Int64)),
					expression.NewSetField(expression.NewSystemVar("sql_select_LIMIT", sql.SystemVariableScope_Session), expression.NewLiteral(int64(math.MaxInt32), sql.Int64)),
				},
			),
		},
		{
			name: "set expression",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("@@auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(uc("@@sql_mode"), mustExpr(function.NewConcat(ctx, uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.SystemVariableScope_Session), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(expression.NewSystemVar("sql_mode", sql.SystemVariableScope_Session), mustExpr(function.NewConcat(ctx, uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
		},
		{
			name: "set expression with barewords",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(uc("@@sql_mode"), mustExpr(function.NewConcat(ctx, uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
			expected: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(expression.NewSystemVar("sql_mode", sql.SystemVariableScope_Session), mustExpr(function.NewConcat(ctx, uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
		},
		{
			name: "set all barewords",
			node: plan.NewSet(
				[]sql.Expression{
					expression.NewSetField(uc("auto_increment_increment"), expression.NewArithmetic(lit(2), lit(3), "+")),
					expression.NewSetField(uc("sql_mode"), mustExpr(function.NewConcat(ctx, uc("@@sql_mode"), uc("@@sql_mode")))),
				},
			),
		},
	}

	runTestCases(t, nil, testCases, nil, *rule)
}

func TestResolveBarewordSetVariables(t *testing.T) {
	rule := getRuleFrom(DefaultRules, "resolve_bareword_set_variables")

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
					expression.NewSetField(expression.NewSystemVar("sql_mode", sql.SystemVariableScope_Session), expression.NewLiteral("hello", sql.LongText)),
				},
			),
		},
	}

	runTestCases(t, nil, testCases, nil, *rule)
}

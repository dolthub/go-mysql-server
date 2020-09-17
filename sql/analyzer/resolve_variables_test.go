// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
	"math"
	"testing"
)

func TestResolveSetVariables(t *testing.T) {
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
					expression.NewSetField(expression.NewSystemVar("auto_increment_increment", sql.Int64), expression.NewLiteral(int64(1), sql.Int64)),
					expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.Int32), expression.NewLiteral(math.MaxInt32, sql.Int32)),
				},
			),
		},
	}

	runTestCases(t, nil, testCases, nil, *rule)
}

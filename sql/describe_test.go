// Copyright 2026 Dolthub, Inc.
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

package sql_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type optionDescribingExpression struct{}

var _ sql.Expression = optionDescribingExpression{}
var _ sql.Describable = optionDescribingExpression{}

func (optionDescribingExpression) Resolved() bool { return true }
func (optionDescribingExpression) String() string { return "string" }
func (optionDescribingExpression) Type(*sql.Context) sql.Type {
	return types.Boolean
}
func (optionDescribingExpression) IsNullable(*sql.Context) bool { return false }
func (optionDescribingExpression) Eval(*sql.Context, sql.Row) (interface{}, error) {
	return true, nil
}
func (optionDescribingExpression) Children() []sql.Expression { return nil }
func (e optionDescribingExpression) WithChildren(*sql.Context, ...sql.Expression) (sql.Expression, error) {
	return e, nil
}
func (optionDescribingExpression) Describe(_ *sql.Context, options sql.DescribeOptions) string {
	return fmt.Sprintf("description(estimates=%t, analyze=%t)", options.Estimates, options.Analyze)
}

func TestDescribeOptionsPropagateThroughExpressions(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := expression.NewAnd(
		expression.NewLiteral(true, types.Boolean),
		optionDescribingExpression{},
	)

	description := sql.Describe(ctx, expr, sql.DescribeOptions{Estimates: true, Analyze: true})
	require.Equal(t, "(true AND description(estimates=true, analyze=true))", description)
}

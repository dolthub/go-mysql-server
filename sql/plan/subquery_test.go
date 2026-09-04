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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSubqueryString(t *testing.T) {
	expr := NewSubquery(nil, "select 1")
	require.Equal(t, "(select 1)", expr.String())
	_, err := sql.NewMysqlParser().ParseSimple("SELECT " + expr.String())
	require.NoError(t, err)
}

func TestInSubqueryString(t *testing.T) {
	ctx := sql.NewEmptyContext()
	expr := NewInSubquery(ctx, expression.NewUnresolvedColumn("i"), NewSubquery(nil, "select j from t"))
	require.Equal(t, "(i IN (select j from t))", expr.String())
	_, err := sql.NewMysqlParser().ParseSimple("SELECT " + expr.String())
	require.NoError(t, err)
}

func TestNestedInSubqueryDescription(t *testing.T) {
	ctx := sql.NewEmptyContext()
	subquery := NewSubquery(NewUnresolvedTable("t", ""), "select j from t")
	inSubquery := NewInSubquery(ctx, expression.NewUnresolvedColumn("i"), subquery)
	expr := expression.NewAnd(expression.NewLiteral(true, types.Boolean), inSubquery)

	description := sql.Describe(ctx, expr, sql.DescribeOptions{Estimates: true})
	require.Contains(t, description, "InSubquery")
	require.Contains(t, description, "right: Subquery")
	require.Contains(t, description, "UnresolvedTable(t)")

	require.Equal(t, "(true AND (i IN (select j from t)))", expr.String())
	_, err := sql.NewMysqlParser().ParseSimple("SELECT " + expr.String())
	require.NoError(t, err)
}

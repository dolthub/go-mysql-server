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

package dtablefunctions

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestTableFunctionWrapperEmptySet(t *testing.T) {
	ctx := sql.NewEmptyContext()
	fn := sql.Function1{
		Name: "empty_set",
		Fn: func(_ *sql.Context, _ sql.Expression) sql.Expression {
			return &emptySetExpression{Literal: expression.NewLiteral(nil, types.Int64)}
		},
	}
	wrapper := NewTableFunctionWrapper(fn)
	instance, err := wrapper.NewInstance(ctx, nil, []sql.Expression{expression.NewLiteral(nil, types.Int64)})
	require.NoError(t, err)
	tableFunc := instance.(*TableFunctionWrapper)

	iter, err := tableFunc.RowIter(ctx, nil)
	require.NoError(t, err)
	_, err = iter.Next(ctx)
	require.ErrorIs(t, err, io.EOF)
}

func TestTableFunctionWrapperScalarNull(t *testing.T) {
	ctx := sql.NewEmptyContext()
	fn := sql.Function1{
		Name: "scalar_null",
		Fn: func(_ *sql.Context, _ sql.Expression) sql.Expression {
			return expression.NewLiteral(nil, types.Int64)
		},
	}
	wrapper := NewTableFunctionWrapper(fn)
	instance, err := wrapper.NewInstance(ctx, nil, []sql.Expression{expression.NewLiteral(nil, types.Int64)})
	require.NoError(t, err)
	tableFunc := instance.(*TableFunctionWrapper)

	iter, err := tableFunc.RowIter(ctx, nil)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{nil}}, rows)
}

func TestTableFunctionWrapperPreservesMultiColumnRows(t *testing.T) {
	ctx := sql.NewEmptyContext()
	fn := sql.Function1{
		Name: "multi_column_set",
		Fn: func(_ *sql.Context, _ sql.Expression) sql.Expression {
			return &multiColumnSetExpression{Literal: expression.NewLiteral(nil, types.Int64)}
		},
	}
	wrapper := NewTableFunctionWrapper(fn)
	instance, err := wrapper.NewInstance(ctx, nil, []sql.Expression{expression.NewLiteral(1, types.Int64)})
	require.NoError(t, err)
	tableFunc := instance.(*TableFunctionWrapper)

	iter, err := tableFunc.RowIter(ctx, nil)
	require.NoError(t, err)
	rows, err := sql.RowIterToRows(ctx, iter)
	require.NoError(t, err)
	require.Equal(t, []sql.Row{{int64(1), int64(2)}}, rows)
}

type emptySetExpression struct {
	*expression.Literal
}

func (e *emptySetExpression) EvalRowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
	// Doltgres SRFs return nil, nil for an empty set. The table-function wrapper
	// converts that representation to an iterator whose first Next returns io.EOF.
	return nil, nil
}

func (e *emptySetExpression) ReturnsRowIter() bool {
	return true
}

type multiColumnSetExpression struct {
	*expression.Literal
}

func (e *multiColumnSetExpression) Eval(*sql.Context, sql.Row) (any, error) {
	return sql.RowsToRowIter(sql.Row{int64(1), int64(2)}), nil
}

func (e *multiColumnSetExpression) EvalRowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(sql.Row{sql.Row{int64(1), int64(2)}}), nil
}

func (e *multiColumnSetExpression) ReturnsRowIter() bool {
	return true
}

func (e *multiColumnSetExpression) OutParametersSchema() sql.Schema {
	return sql.Schema{
		{Name: "first", Type: types.Int64},
		{Name: "second", Type: types.Int64},
	}
}

func (e *multiColumnSetExpression) Unwrap(v any) sql.Row {
	return v.(sql.Row)
}

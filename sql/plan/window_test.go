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

package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation/window"
)

func TestWindow(t *testing.T) {
	t.Run("test buffer oversharing", func(t *testing.T) {
		w := sql.NewWindow(
			[]sql.Expression{
				expression.NewGetField(1, sql.TinyText, "b", false),
			},
			sql.SortFields{
				{
					Column: expression.NewGetField(0, sql.Int32, "a", false),
				},
			},
		)
		wIter := &windowIter{
			ctx: sql.NewEmptyContext(),
			selectExprs: []sql.Expression{
				mustExpr(window.NewRowNumber().(*window.RowNumber).WithWindow(w)),
				mustExpr(window.NewFirstValue(
					expression.NewGetField(1, sql.TinyText, "b", false),
				).(*window.FirstValue).WithWindow(w),
				),
			},
			childIter: newDummyIter(),
		}

		res := make([]sql.Row, 0)
		for {
			r, err := wIter.Next()
			if err == io.EOF {
				break
			}
			res = append(res, r)
		}

		require.Equal(t, res, []sql.Row{sql.NewRow(1, "a"), sql.NewRow(1, "b"), sql.NewRow(1, "c")})
	})
}

type dummyIter struct {
	rows []sql.Row
	pos  int
}

func newDummyIter() *dummyIter {
	rows := []sql.Row{
		newRowWithExtraCap(2, "a"),
		newRowWithExtraCap(1, "b"),
		newRowWithExtraCap(3, "c"),
	}
	return &dummyIter{
		rows: rows,
	}
}

func (i *dummyIter) Next() (sql.Row, error) {
	if i.pos >= len(i.rows) {
		return nil, io.EOF
	}
	row := i.rows[i.pos]
	i.pos++
	return row, nil
}

func (i *dummyIter) Close(ctx *sql.Context) error {
	return nil
}

func mustExpr(e sql.Expression, err error) sql.Expression {
	if err != nil {
		panic(err)
	}
	return e
}

// sql.NewRow, but the slice's underlying array is not filled
func newRowWithExtraCap(values ...interface{}) sql.Row {
	row := make([]interface{}, 0, len(values)+2)
	for _, v := range values {
		row = append(row, v)
	}
	return row
}

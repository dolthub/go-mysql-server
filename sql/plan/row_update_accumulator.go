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

package plan

import (
	"io"
	"sync"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// RowUpdateAccumulator wraps other nodes that update tables, and returns their results as OKResults with the appropriate
// fields set.
type RowUpdateAccumulator struct {
	UnaryNode
}

// NewRowUpdateResult returns a new RowUpdateResult with the given node to wrap.
func NewRowUpdateAccumulator(n sql.Node) *RowUpdateAccumulator {
	return &RowUpdateAccumulator{
		UnaryNode: UnaryNode{
			Child: n,
		},
	}
}

func (r RowUpdateAccumulator) Schema() sql.Schema {
	return sql.OkResultSchema
}

func (r RowUpdateAccumulator) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(r, 1, len(children))
	}
	return NewRowUpdateAccumulator(children[0]), nil
}

func (r RowUpdateAccumulator) String() string {
	return r.Child.String()
}

func (r RowUpdateAccumulator) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("RowUpdateAccumulator")
	_ = pr.WriteChildren(sql.DebugString(r.Child))
	return pr.String()
}

type accumulatorIter struct {
	iter sql.RowIter
	once sync.Once
}

func (a *accumulatorIter) Next() (sql.Row, error) {
	run := false
	a.once.Do(func() {
		run = true
	})

	if !run {
		return nil, io.EOF
	}

	var rowsAffected int
	for {
		_, err := a.iter.Next()
		if err == io.EOF {
			return sql.NewRow(sql.NewOkResult(rowsAffected)), nil
		}

		if err != nil {
			return nil, err
		}

		rowsAffected += 1 // TODO: sometimes this is greater than 1
	}
}

func (a *accumulatorIter) Close() error {
	return a.iter.Close()
}

func (r RowUpdateAccumulator) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	rowIter, err := r.Child.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &accumulatorIter{iter: rowIter}, nil
}

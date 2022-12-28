// Copyright 2022 Dolthub, Inc.
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
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// NewMergeJoin returns a node that performs a presorted merge join on
// two relations. We require 1) the join filter is an equality with disjoint
// join attributes, 2) the free attributes for a relation are a prefix for
// an index that will be used to return sorted rows.
func NewMergeJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeMerge, cond)
}

func NewLeftMergeJoin(left, right sql.Node, cond sql.Expression) *JoinNode {
	return NewJoin(left, right, JoinTypeLeftOuterMerge, cond)
}

func newMergeJoinIter(ctx *sql.Context, j *JoinNode, row sql.Row) (sql.RowIter, error) {
	l, err := j.left.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}
	r, err := j.right.RowIter(ctx, row)
	if err != nil {
		return nil, err
	}

	return &mergeJoinIter{
		parentRow: row,
		in:        l,
		out:       r,
		expr:      j.Filter.(expression.Comparer),
		typ:       j.Op,
		rowSize:   len(row) + len(j.left.Schema()) + len(j.right.Schema()),
		scopeLen:  j.ScopeLen,
	}, nil
}

type mergeJoinIter struct {
	expr      expression.Comparer
	in        sql.RowIter
	out       sql.RowIter
	parentRow sql.Row
	fullRow   sql.Row

	done       bool
	typ        JoinType
	rowSize    int
	scopeLen   int
	inRowSize  int
	outRowSize int
}

func (i *mergeJoinIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.fullRow == nil {
		inRow, err := i.in.Next(ctx)
		if err != nil {
			return nil, err
		}
		outRow, err := i.out.Next(ctx)
		if err != nil {
			return nil, err
		}
		i.fullRow = append(i.parentRow)
		i.fullRow = append(i.fullRow, inRow...)
		i.fullRow = append(i.fullRow, outRow...)
		i.inRowSize = len(inRow)
		i.outRowSize = len(outRow)
	}
	for {
		if i.done {
			return nil, io.EOF
		}

		res, err := i.expr.Compare(ctx, i.fullRow)
		if err != nil {
			return nil, err
		}

		switch {
		case res > 0:
			err = i.incInner(ctx)
		case res < 0:
			err = i.incOuter(ctx)
		case res == 0:
			ret := make(sql.Row, len(i.fullRow))
			copy(ret, i.fullRow)
			err = i.incOuter(ctx)
			if err != nil {
				return nil, err
			}
			return i.removeParentRow(ret), nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (i *mergeJoinIter) incInner(ctx *sql.Context) error {
	inOff := i.scopeLen + len(i.parentRow)
	newIn, err := i.in.Next(ctx)
	if errors.Is(err, io.EOF) {
		i.done = true
	} else if err != nil {
		return err
	}
	for j, v := range newIn {
		i.fullRow[inOff+j] = v
	}
	return nil
}

func (i *mergeJoinIter) incOuter(ctx *sql.Context) error {
	outOff := i.scopeLen + len(i.parentRow) + i.inRowSize
	newOut, err := i.out.Next(ctx)
	if errors.Is(err, io.EOF) {
		i.done = true
	} else if err != nil {
		return err
	}
	for j, v := range newOut {
		i.fullRow[outOff+j] = v
	}
	return nil
}

func (i *mergeJoinIter) removeParentRow(r sql.Row) sql.Row {
	copy(r[i.scopeLen:], r[len(i.parentRow):])
	r = r[:len(r)-len(i.parentRow)+i.scopeLen]
	return r
}

func (i *mergeJoinIter) Close(ctx *sql.Context) (err error) {
	if i.in != nil {
		err = i.in.Close(ctx)
	}

	if i.out != nil {
		if err == nil {
			err = i.out.Close(ctx)
		} else {
			i.out.Close(ctx)
		}
	}

	return err
}

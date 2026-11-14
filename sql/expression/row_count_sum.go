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

package expression

import (
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// RowCountSum is the internal sum of nonnegative LIMIT/OFFSET counts for TopN. Unlike SQL
// arithmetic, it saturates at MaxInt64: TopN cannot buffer more rows than the
// signed row count (or slice length) can represent. It must not be used for
// user-authored arithmetic expressions.
type RowCountSum struct {
	BinaryExpressionStub
}

func NewRowCountSum(limit, offset sql.Expression) *RowCountSum {
	return &RowCountSum{BinaryExpressionStub{LeftChild: limit, RightChild: offset}}
}

func (s *RowCountSum) Type(*sql.Context) sql.Type { return types.Int64 }
func (s *RowCountSum) String() string             { return fmt.Sprintf("(%s + %s)", s.LeftChild, s.RightChild) }
func (s *RowCountSum) CollationCoercibility(*sql.Context) (sql.CollationID, byte) {
	return sql.Collation_binary, 5
}
func (s *RowCountSum) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	counts := [2]int64{}
	for i, child := range s.Children() {
		value, err := child.Eval(ctx, row)
		if err != nil || value == nil {
			return nil, err
		}
		// Match the plan builder's literal row-count normalization for deferred
		// unsigned parameters as well. Conversion clamps out-of-range integers.
		value, _, err = types.Int64.Convert(ctx, value)
		if err != nil {
			return nil, err
		}
		counts[i] = value.(int64)
	}
	if counts[0] >= 0 && counts[1] > math.MaxInt64-counts[0] {
		return int64(math.MaxInt64), nil
	}
	return counts[0] + counts[1], nil
}
func (s *RowCountSum) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 2)
	}
	return NewRowCountSum(children[0], children[1]), nil
}

// Copyright 2025 Dolthub, Inc.
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

package spatial

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// InteriorRingN is a function that returns the Nth interior ring of a Polygon as a LineString.
// N is 1-based.
type InteriorRingN struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*InteriorRingN)(nil)
var _ sql.CollationCoercible = (*InteriorRingN)(nil)

// NewInteriorRingN creates a new InteriorRingN expression.
func NewInteriorRingN(ctx *sql.Context, g, n sql.Expression) sql.Expression {
	return &InteriorRingN{
		expression.BinaryExpressionStub{
			LeftChild:  g,
			RightChild: n,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (i *InteriorRingN) FunctionName() string {
	return "st_interiorringn"
}

// Description implements sql.FunctionExpression
func (i *InteriorRingN) Description() string {
	return "returns the Nth interior ring of the Polygon value as a LineString."
}

// IsNullable implements the sql.Expression interface.
func (i *InteriorRingN) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (i *InteriorRingN) Type(ctx *sql.Context) sql.Type {
	return types.LineStringType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*InteriorRingN) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (i *InteriorRingN) String(ctx *sql.Context) string {
	return fmt.Sprintf("%s(%s,%s)", i.FunctionName(), i.LeftChild.String(ctx), i.RightChild.String(ctx))
}

// WithChildren implements the Expression interface.
func (i *InteriorRingN) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 2)
	}
	return NewInteriorRingN(ctx, children[0], children[1]), nil
}

// Eval implements the sql.Expression interface.
func (i *InteriorRingN) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := i.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	nVal, err := i.RightChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if nVal == nil {
		return nil, nil
	}

	n, _, err := types.Int64.Convert(ctx, nVal)
	if err != nil {
		return nil, err
	}
	idx := int(n.(int64))

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(i.FunctionName())
	}

	p, ok := gv.(types.Polygon)
	if !ok {
		return nil, sql.ErrInvalidArgument.New(i.FunctionName())
	}

	// Interior rings start at index 1 in p.Lines (index 0 is exterior ring).
	// N is 1-based, so interior ring N is at p.Lines[N].
	if idx < 1 || idx >= len(p.Lines) {
		return nil, nil
	}

	ring := p.Lines[idx]
	ring.SRID = p.SRID
	return ring, nil
}

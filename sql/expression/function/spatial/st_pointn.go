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

// PointN is a function that returns the Nth point from a LineString.
// N is 1-based.
type PointN struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*PointN)(nil)
var _ sql.CollationCoercible = (*PointN)(nil)

// NewPointN creates a new PointN expression.
func NewPointN(ctx *sql.Context, g, n sql.Expression) sql.Expression {
	return &PointN{
		expression.BinaryExpressionStub{
			LeftChild:  g,
			RightChild: n,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (p *PointN) FunctionName() string {
	return "st_pointn"
}

// Description implements sql.FunctionExpression
func (p *PointN) Description() string {
	return "returns the Nth point in the LineString value."
}

// IsNullable implements the sql.Expression interface.
func (p *PointN) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (p *PointN) Type(ctx *sql.Context) sql.Type {
	return types.PointType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*PointN) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (p *PointN) String() string {
	return fmt.Sprintf("%s(%s,%s)", p.FunctionName(), p.LeftChild.String(), p.RightChild.String())
}

// WithChildren implements the Expression interface.
func (p *PointN) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}
	return NewPointN(ctx, children[0], children[1]), nil
}

// Eval implements the sql.Expression interface.
func (p *PointN) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := p.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	nVal, err := p.RightChild.Eval(ctx, row)
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
		return nil, sql.ErrInvalidGISData.New(p.FunctionName())
	}

	l, ok := gv.(types.LineString)
	if !ok {
		return nil, sql.ErrInvalidArgument.New(p.FunctionName())
	}

	// N is 1-based; out-of-range returns NULL
	if idx < 1 || idx > len(l.Points) {
		return nil, nil
	}

	return l.Points[idx-1], nil
}

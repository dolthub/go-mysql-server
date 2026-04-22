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

// NumInteriorRings is a function that returns the number of interior rings in a Polygon.
type NumInteriorRings struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*NumInteriorRings)(nil)
var _ sql.CollationCoercible = (*NumInteriorRings)(nil)

// NewNumInteriorRings creates a new NumInteriorRings expression.
func NewNumInteriorRings(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &NumInteriorRings{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (n *NumInteriorRings) FunctionName() string {
	return "st_numinteriorrings"
}

// Description implements sql.FunctionExpression
func (n *NumInteriorRings) Description() string {
	return "returns the number of interior rings in the Polygon value."
}

// IsNullable implements the sql.Expression interface.
func (n *NumInteriorRings) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (n *NumInteriorRings) Type(ctx *sql.Context) sql.Type {
	return types.Int32
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*NumInteriorRings) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (n *NumInteriorRings) String() string {
	return fmt.Sprintf("%s(%s)", n.FunctionName(), n.Child.String())
}

// WithChildren implements the Expression interface.
func (n *NumInteriorRings) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	return NewNumInteriorRings(ctx, children[0]), nil
}

// Eval implements the sql.Expression interface.
func (n *NumInteriorRings) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := n.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(n.FunctionName())
	}

	p, ok := gv.(types.Polygon)
	if !ok {
		return nil, sql.ErrInvalidArgument.New(n.FunctionName())
	}

	// The first ring is the exterior ring; all subsequent rings are interior
	if len(p.Lines) == 0 {
		return 0, nil
	}
	return len(p.Lines) - 1, nil
}

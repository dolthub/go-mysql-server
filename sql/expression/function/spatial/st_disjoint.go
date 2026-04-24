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

// Disjoint is a function that returns true if geometry g1 is spatially disjoint from g2.
// ST_Disjoint(g1, g2) is the inverse of ST_Intersects(g1, g2).
type Disjoint struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*Disjoint)(nil)
var _ sql.CollationCoercible = (*Disjoint)(nil)

// NewDisjoint creates a new Disjoint expression.
func NewDisjoint(ctx *sql.Context, g1, g2 sql.Expression) sql.Expression {
	return &Disjoint{
		expression.BinaryExpressionStub{
			LeftChild:  g1,
			RightChild: g2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (d *Disjoint) FunctionName() string {
	return "st_disjoint"
}

// Description implements sql.FunctionExpression
func (d *Disjoint) Description() string {
	return "returns 1 or 0 to indicate whether g1 is spatially disjoint from g2."
}

// IsNullable implements the sql.Expression interface.
func (d *Disjoint) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (d *Disjoint) Type(ctx *sql.Context) sql.Type {
	return types.Boolean
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Disjoint) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (d *Disjoint) String(ctx *sql.Context) string {
	return fmt.Sprintf("%s(%s,%s)", d.FunctionName(), d.LeftChild.String(ctx), d.RightChild.String(ctx))
}

// WithChildren implements the Expression interface.
func (d *Disjoint) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 2)
	}
	return NewDisjoint(ctx, children[0], children[1]), nil
}

// Eval implements the sql.Expression interface.
// ST_Disjoint is the inverse of ST_Intersects.
func (d *Disjoint) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	geom1, err := d.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	geom2, err := d.RightChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	g1, g2, err := validateGeomComp(ctx, geom1, geom2, d.FunctionName())
	if err != nil {
		return nil, err
	}
	if g1 == nil || g2 == nil {
		return nil, nil
	}

	intersects := isIntersects(g1, g2)
	return !intersects, nil
}

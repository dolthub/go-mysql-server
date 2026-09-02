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

// Contains is a function that returns true if geometry g1 spatially contains g2.
// ST_Contains(g1, g2) is equivalent to ST_Within(g2, g1).
type Contains struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*Contains)(nil)
var _ sql.CollationCoercible = (*Contains)(nil)

// NewContains creates a new Contains expression.
func NewContains(ctx *sql.Context, g1, g2 sql.Expression) sql.Expression {
	return &Contains{
		expression.BinaryExpressionStub{
			LeftChild:  g1,
			RightChild: g2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (c *Contains) FunctionName() string {
	return "st_contains"
}

// Description implements sql.FunctionExpression
func (c *Contains) Description() string {
	return "returns 1 or 0 to indicate whether g1 spatially contains g2."
}

// IsNullable implements the sql.Expression interface.
func (c *Contains) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (c *Contains) Type(ctx *sql.Context) sql.Type {
	return types.Boolean
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Contains) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (c *Contains) String() string {
	return fmt.Sprintf("%s(%s,%s)", c.FunctionName(), c.LeftChild.String(), c.RightChild.String())
}

// WithChildren implements the Expression interface.
func (c *Contains) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 2)
	}
	return NewContains(ctx, children[0], children[1]), nil
}

// Eval implements the sql.Expression interface.
// ST_Contains(g1, g2) is equivalent to ST_Within(g2, g1).
func (c *Contains) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	geom1, err := c.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	geom2, err := c.RightChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Note: arguments are swapped — Contains(g1,g2) == Within(g2,g1)
	g2, g1, err := validateGeomComp(ctx, geom2, geom1, c.FunctionName())
	if err != nil {
		return nil, err
	}
	if g1 == nil || g2 == nil {
		return nil, nil
	}

	// TODO (james): remove this switch block when the other comparisons are implemented
	switch g2.(type) {
	case types.LineString:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("LineString", c.FunctionName())
	case types.Polygon:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("Polygon", c.FunctionName())
	case types.MultiPoint:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("MultiPoint", c.FunctionName())
	case types.MultiLineString:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("MultiLineString", c.FunctionName())
	case types.MultiPolygon:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("MultiPolygon", c.FunctionName())
	case types.GeomColl:
		return nil, sql.ErrUnsupportedGISTypeForSpatialFunc.New("GeomColl", c.FunctionName())
	}

	return isWithin(g2, g1), nil
}

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

// GeometryN is a function that returns the Nth geometry from a GeometryCollection.
// For non-collection types, returns the geometry itself when N=1.
// N is 1-based.
type GeometryN struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*GeometryN)(nil)
var _ sql.CollationCoercible = (*GeometryN)(nil)

// NewGeometryN creates a new GeometryN expression.
func NewGeometryN(g, n sql.Expression) sql.Expression {
	return &GeometryN{
		expression.BinaryExpressionStub{
			LeftChild:  g,
			RightChild: n,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (g *GeometryN) FunctionName() string {
	return "st_geometryn"
}

// Description implements sql.FunctionExpression
func (g *GeometryN) Description() string {
	return "returns the Nth geometry from a geometry collection."
}

// IsNullable implements the sql.Expression interface.
func (g *GeometryN) IsNullable() bool {
	return true
}

// Type implements the sql.Expression interface.
func (g *GeometryN) Type() sql.Type {
	return types.GeometryType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*GeometryN) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (g *GeometryN) String() string {
	return fmt.Sprintf("%s(%s,%s)", g.FunctionName(), g.LeftChild.String(), g.RightChild.String())
}

// WithChildren implements the Expression interface.
func (g *GeometryN) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 2)
	}
	return NewGeometryN(children[0], children[1]), nil
}

// Eval implements the sql.Expression interface.
func (g *GeometryN) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := g.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	nVal, err := g.RightChild.Eval(ctx, row)
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
		return nil, sql.ErrInvalidGISData.New(g.FunctionName())
	}

	// N is 1-based; out-of-range returns NULL
	switch v := gv.(type) {
	case types.GeomColl:
		if idx < 1 || idx > len(v.Geoms) {
			return nil, nil
		}
		return v.Geoms[idx-1], nil
	case types.MultiPoint:
		if idx < 1 || idx > len(v.Points) {
			return nil, nil
		}
		return v.Points[idx-1], nil
	case types.MultiLineString:
		if idx < 1 || idx > len(v.Lines) {
			return nil, nil
		}
		return v.Lines[idx-1], nil
	case types.MultiPolygon:
		if idx < 1 || idx > len(v.Polygons) {
			return nil, nil
		}
		return v.Polygons[idx-1], nil
	default:
		// Non-collection types: return self for N=1, NULL otherwise
		if idx == 1 {
			return gv, nil
		}
		return nil, nil
	}
}

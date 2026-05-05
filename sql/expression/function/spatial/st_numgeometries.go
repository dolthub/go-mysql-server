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

// NumGeometries is a function that returns the number of geometries in a GeometryCollection,
// or 1 for non-collection geometry types.
type NumGeometries struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*NumGeometries)(nil)
var _ sql.CollationCoercible = (*NumGeometries)(nil)

// NewNumGeometries creates a new NumGeometries expression.
func NewNumGeometries(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &NumGeometries{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (n *NumGeometries) FunctionName() string {
	return "st_numgeometries"
}

// Description implements sql.FunctionExpression
func (n *NumGeometries) Description() string {
	return "returns the number of geometries in the geometry argument."
}

// IsNullable implements the sql.Expression interface.
func (n *NumGeometries) IsNullable(ctx *sql.Context) bool {
	return n.Child.IsNullable(ctx)
}

// Type implements the sql.Expression interface.
func (n *NumGeometries) Type(ctx *sql.Context) sql.Type {
	return types.Int32
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*NumGeometries) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (n *NumGeometries) String() string {
	return fmt.Sprintf("%s(%s)", n.FunctionName(), n.Child.String())
}

// WithChildren implements the Expression interface.
func (n *NumGeometries) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	return NewNumGeometries(ctx, children[0]), nil
}

// Eval implements the sql.Expression interface.
func (n *NumGeometries) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

	// For GeometryCollection, MultiPoint, MultiLineString, MultiPolygon: return component count
	// For simple geometry types (Point, LineString, Polygon): return NULL per MySQL behavior
	switch v := gv.(type) {
	case types.GeomColl:
		return len(v.Geoms), nil
	case types.MultiPoint:
		return len(v.Points), nil
	case types.MultiLineString:
		return len(v.Lines), nil
	case types.MultiPolygon:
		return len(v.Polygons), nil
	default:
		return nil, nil
	}
}

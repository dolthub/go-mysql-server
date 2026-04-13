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

// GeometryType is a function that returns the type of a geometry as a string.
type GeometryType struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*GeometryType)(nil)
var _ sql.CollationCoercible = (*GeometryType)(nil)

// NewGeometryType creates a new GeometryType expression.
func NewGeometryType(e sql.Expression) sql.Expression {
	return &GeometryType{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (g *GeometryType) FunctionName() string {
	return "st_geometrytype"
}

// Description implements sql.FunctionExpression
func (g *GeometryType) Description() string {
	return "returns the geometry type of the argument as a string."
}

// IsNullable implements the sql.Expression interface.
func (g *GeometryType) IsNullable() bool {
	return g.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (g *GeometryType) Type() sql.Type {
	return types.LongText
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*GeometryType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (g *GeometryType) String() string {
	return fmt.Sprintf("%s(%s)", g.FunctionName(), g.Child.String())
}

// WithChildren implements the Expression interface.
func (g *GeometryType) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(g, len(children), 1)
	}
	return NewGeometryType(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (g *GeometryType) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := g.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(g.FunctionName())
	}

	switch gv.(type) {
	case types.Point:
		return "POINT", nil
	case types.LineString:
		return "LINESTRING", nil
	case types.Polygon:
		return "POLYGON", nil
	case types.MultiPoint:
		return "MULTIPOINT", nil
	case types.MultiLineString:
		return "MULTILINESTRING", nil
	case types.MultiPolygon:
		return "MULTIPOLYGON", nil
	case types.GeomColl:
		return "GEOMCOLLECTION", nil
	default:
		return nil, sql.ErrInvalidGISData.New(g.FunctionName())
	}
}

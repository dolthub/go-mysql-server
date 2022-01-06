// Copyright 2020-2021 Dolthub, Inc.
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

package function

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// AsWKT is a function that converts a spatial type into WKT format (alias for AsText)
type AsWKT struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*AsWKT)(nil)

// NewAsWKT creates a new point expression.
func NewAsWKT(e sql.Expression) sql.Expression {
	return &AsWKT{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *AsWKT) FunctionName() string {
	return "st_aswkb"
}

// Description implements sql.FunctionExpression
func (p *AsWKT) Description() string {
	return "returns binary representation of given spatial type."
}

// IsNullable implements the sql.Expression interface.
func (p *AsWKT) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *AsWKT) Type() sql.Type {
	return p.Child.Type()
}

func (p *AsWKT) String() string {
	return fmt.Sprintf("ST_ASWKT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *AsWKT) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAsWKT(children[0]), nil
}

// PointToWKT converts a sql.Point to a byte array
func PointToWKT(p sql.Point) string {
	x := strconv.FormatFloat(p.X, 'g', -1, 64)
	y := strconv.FormatFloat(p.Y, 'g', -1, 64)
	return fmt.Sprintf("%s %s", x, y)
}

// LineToWKT converts a sql.Linestring to a byte array
func LineToWKT(l sql.Linestring) string {
	points := make([]string, len(l.Points))
	for i, p := range l.Points {
		points[i] = PointToWKT(p)
	}
	return strings.Join(points, ",")
}

// PolygonToWKT converts a sql.Polygon to a byte array
func PolygonToWKT(p sql.Polygon) string {
	lines := make([]string, len(p.Lines))
	for i, l := range p.Lines {
		lines[i] = "(" + LineToWKT(l) + ")"
	}
	return strings.Join(lines, ",")
}

// TODO: Could combine PointToWKT, LineToWKT and PolygonToWKT into recursive GeometryToBytes function?

// Eval implements the sql.Expression interface.
func (p *AsWKT) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	var geomType string
	var data string
	// Expect one of the geometry types
	switch v := val.(type) {
	case sql.Point:
		// Mark as point type
		geomType = "POINT"
		data = PointToWKT(v)
	case sql.Linestring:
		// Mark as linestring type
		geomType = "LINESTRING"
		data = LineToWKT(v)
	case sql.Polygon:
		// Mark as Polygon type
		geomType = "POLYGON"
		data = PolygonToWKT(v)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_AsWKT")
	}

	return fmt.Sprintf("%s(%s)", geomType, data), nil
}

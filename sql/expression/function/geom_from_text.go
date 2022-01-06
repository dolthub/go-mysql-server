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
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
)

// GeomFromText is a function that returns a point type from a WKT string
type GeomFromText struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*GeomFromText)(nil)

// NewGeomFromText creates a new point expression.
func NewGeomFromText(e sql.Expression) sql.Expression {
	return &GeomFromText{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *GeomFromText) FunctionName() string {
	return "st_geomfromtext"
}

// Description implements sql.FunctionExpression
func (p *GeomFromText) Description() string {
	return "returns a new point from a WKT string."
}

// IsNullable implements the sql.Expression interface.
func (p *GeomFromText) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *GeomFromText) Type() sql.Type {
	return p.Child.Type()
}

func (p *GeomFromText) String() string {
	return fmt.Sprintf("ST_GEOMFROMTEXT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *GeomFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewGeomFromText(children[0]), nil
}

// ParseWKTHeader should extract the type from the geometry string
func ParseWKTHeader(s string) (string, string, error) {
	// Read until first open parenthesis
	end := strings.Index(s, "(")

	// Bad if no parenthesis found
	if end == -1 {
		return "", "", sql.ErrInvalidGISData.New("ST_GeomFromText")
	}

	// Get Geometry Type
	geomType := s[:end]
	geomType = strings.TrimSpace(geomType)
	geomType = strings.ToLower(geomType)

	// Get data
	data := s[end:]
	data = strings.TrimSpace(data)

	// Check that data is surrounded by parentheses
	if data[0] != '(' || data[len(data)-1] != ')' {
		return "", "", sql.ErrInvalidGISData.New("ST_GeomFromText")
	}
	// Remove parentheses, and trim
	data = data[1 : len(data)-1]
	data = strings.TrimSpace(data)

	return geomType, data, nil
}

// Eval implements the sql.Expression interface.
func (p *GeomFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Expect a string, throw error otherwise
	s, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromText")
	}

	// Determine type, and get data
	geomType, data, err := ParseWKTHeader(s)
	if err != nil {
		return nil, err
	}

	// Parse accordingly
	// TODO: define consts instead of string comparison?
	switch geomType {
	case "point":
		return WKTToPoint(data)
	case "linestring":
		return WKTToLine(data)
	case "polygon":
		return WKTToPoly(data)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromText")
	}
}

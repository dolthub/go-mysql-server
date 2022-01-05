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

// PolyFromText is a function that returns a polygon type from a WKT string
type PolyFromText struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*PolyFromText)(nil)

// NewPolygonFromText creates a new polygon expression.
func NewPolygonFromText(e sql.Expression) sql.Expression {
	return &PolyFromText{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *PolyFromText) FunctionName() string {
	return "st_polyfromtext"
}

// Description implements sql.FunctionExpression
func (p *PolyFromText) Description() string {
	return "returns a new polygon from a WKT string."
}

// IsNullable implements the sql.Expression interface.
func (p *PolyFromText) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *PolyFromText) Type() sql.Type {
	return p.Child.Type()
}

func (p *PolyFromText) String() string {
	return fmt.Sprintf("ST_POLYFROMTEXT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *PolyFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewPolygonFromText(children[0]), nil
}

// WKTToPoly Expects a string like "(1 2, 3 4), (5 6, 7 8), ..."
func WKTToPoly(s string) (sql.Polygon, error) {
	var lines []sql.Linestring
	for {
		// Look for closing parentheses
		end := strings.Index(s, ")")
		if end == -1 {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
		}

		// Extract linestring string; does not include ")"
		lineStr := s[:end]

		// Must start with open parenthesis
		if len(lineStr) == 0 || lineStr[0] != '(' {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
		}

		// Remove leading "("
		lineStr = lineStr[1:]

		// Remove leading and trailing whitespace
		lineStr = strings.TrimSpace(lineStr)

		// Parse line
		if line, err := WKTToLine(lineStr); err == nil {
			// Check if line is linearring
			if isLinearRing(line) {
				lines = append(lines, line)
			} else {
				return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
			}
		} else {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
		}

		// Prepare next string
		s = s[end+1:]
		s = strings.TrimSpace(s)

		// Reached end
		if len(s) == 0 {
			break
		}

		// Linestrings must be comma-separated
		if s[0] != ',' {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromText")
		}

		// Drop leading comma
		s = s[1:]

		// Trim leading spaces
		s = strings.TrimSpace(s)
	}

	// Create Polygon object
	return sql.Polygon{Lines: lines}, nil
}

// Eval implements the sql.Expression interface.
func (p *PolyFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Expect a string, throw error otherwise
	if s, ok := val.(string); ok {
		// TODO: possible to use a regular expression? "*polygon *\( *[0-9][0-9]* *[0-9][0-9]* *\) *" /gi
		if geomType, data, err := ParseWKTHeader(s); err == nil && geomType == "polygon" {
			return WKTToPoly(data)
		}
	}

	return nil, sql.ErrInvalidGISData.New("ST_PolyFromText")
}

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

// PolygonFromText is a function that returns a polygon type from a WKT string
type PolygonFromText struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*PolygonFromText)(nil)

// NewPolygonFromText creates a new polygon expression.
func NewPolygonFromText(e sql.Expression) sql.Expression {
	return &PolygonFromText{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *PolygonFromText) FunctionName() string {
	return "st_polygonfromtext"
}

// Description implements sql.FunctionExpression
func (p *PolygonFromText) Description() string {
	return "returns a new polygon from a WKT string."
}

// IsNullable implements the sql.Expression interface.
func (p *PolygonFromText) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *PolygonFromText) Type() sql.Type {
	return p.Child.Type()
}

func (p *PolygonFromText) String() string {
	return fmt.Sprintf("ST_POLYGONFROMTEXT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *PolygonFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewPolygonFromText(children[0]), nil
}

// ParsePolygonString Expects a string like "(1 2, 3 4), (5 6, 7 8), ..."
func ParsePolygonString(s string) (interface{}, error) {
	var lines []sql.Linestring
	for {
		// Look for closing parentheses
		end := strings.Index(s, ")")
		if end == -1 {
			return nil, sql.ErrInvalidGISData.New("ST_PolygonFromText")
		}

		// Extract linestring string; does not include ")"
		linestringStr := s[:end]

		// Must start with open parenthesis
		if len(linestringStr) == 0 || linestringStr[0] != '(' {
			return nil, sql.ErrInvalidGISData.New("ST_PolygonFromText")
		}

		// Remove leading "("
		linestringStr = linestringStr[1:]

		// Remove leading and trailing whitespace
		linestringStr = strings.TrimSpace(linestringStr)

		// Parse line
		if line, err := ParseLinestringString(linestringStr); err != nil {
			lines = append(lines, line.(sql.Linestring))
		} else {
			return nil, sql.ErrInvalidGISData.New("ST_PolygonFromText")
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
			return nil, sql.ErrInvalidGISData.New("ST_PolygonFromText")
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
func (p *PolygonFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		if s, err = TrimTypePrefix(s, "polygon"); err == nil {
			return ParsePolygonString(s)
		}
	}

	return nil, sql.ErrInvalidGISData.New("ST_PolygonFromText")
}

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

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
)

// LinestringFromText is a function that returns a point type from a WKT string
type LinestringFromText struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*LinestringFromText)(nil)

// NewLinestringFromText creates a new point expression.
func NewLinestringFromText(e sql.Expression) sql.Expression {
	return &LinestringFromText{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *LinestringFromText) FunctionName() string {
	return "st_linestringfromtext"
}

// Description implements sql.FunctionExpression
func (p *LinestringFromText) Description() string {
	return "returns a new linestring from a WKT string."
}

// IsNullable implements the sql.Expression interface.
func (p *LinestringFromText) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *LinestringFromText) Type() sql.Type {
	return p.Child.Type()
}

func (p *LinestringFromText) String() string {
	return fmt.Sprintf("ST_LINESTRINGFROMTEXT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *LinestringFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewLinestringFromText(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (p *LinestringFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		const linestring = "linestring"
		// TODO: possible to use a regular expression? "*linestring *\( *[0-9][0-9]* *[0-9][0-9]* *\) *" /gi
		// Trim excess leading and trailing whitespace
		s = strings.TrimSpace(s)

		// Lowercase
		s = strings.ToLower(s)

		// "linestring" prefix must be first thing
		if s[:len(linestring)] != linestring {
			return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
		}

		// Remove "linestring" prefix
		s = s[len(linestring):]

		// Trim leading and trailing whitespace again
		s = strings.TrimSpace(s)

		// Must be surrounded in parentheses
		if s[0] != '(' || s[len(s)-1] != ')' {
			return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
		}

		// Remove parenthesis
		s = s[1:len(s)-1]

		// Separate by comma
		pointStrs := strings.Split(s, ",")

		// Parse each point string
		var points = make([]sql.Point, len(pointStrs))
		for i, p := range pointStrs {
			// Remove whitespace
			p = strings.TrimSpace(p)

			// Get everything between spaces
			coords := strings.Fields(p)

			// Check length
			if len(coords) != 2 {
				return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
			}

			// Parse x
			x, err := strconv.ParseFloat(coords[0], 64)
			if err != nil {
				return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
			}

			// Parse y
			y, err := strconv.ParseFloat(coords[1], 64)
			if err != nil {
				return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
			}

			points[i] = sql.Point{X: x, Y: y}
		}

		// Create point object
		return sql.Linestring{Points: points}, nil
	}

	return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
}

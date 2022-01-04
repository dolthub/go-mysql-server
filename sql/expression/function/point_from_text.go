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

// PointFromText is a function that returns a point type from a WKT string
type PointFromText struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*PointFromText)(nil)

// NewPointFromText creates a new point expression.
func NewPointFromText(e sql.Expression) sql.Expression {
	return &PointFromText{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *PointFromText) FunctionName() string {
	return "st_pointfromtext"
}

// Description implements sql.FunctionExpression
func (p *PointFromText) Description() string {
	return "returns a new point from a WKT string."
}

// IsNullable implements the sql.Expression interface.
func (p *PointFromText) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *PointFromText) Type() sql.Type {
	return p.Child.Type()
}

func (p *PointFromText) String() string {
	return fmt.Sprintf("ST_POINTFROMTEXT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *PointFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewPointFromText(children[0]), nil
}

func TrimTypePrefix(s, typeStr string) (string, error) {
	// Trim excess leading and trailing whitespace
	s = strings.TrimSpace(s)

	// Lowercase
	s = strings.ToLower(s)

	// typeStr prefix must be first thing
	if s[:len(typeStr)] != typeStr {
		return "", sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Remove typeStr prefix
	s = s[len(typeStr):]

	// Trim leading and trailing whitespace again
	s = strings.TrimSpace(s)

	// Must be surrounded in parentheses
	if s[0] != '(' || s[len(s)-1] != ')' {
		return "", sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Remove parenthesis
	s = s[1 : len(s)-1]

	// Trim leading and trailing whitespace again
	s = strings.TrimSpace(s)

	return s, nil
}

// ParsePointString expects a string like this "1.2 3.4"
func ParsePointString(s string) (interface{}, error) {
	// Empty string is wrong
	if len(s) == 0 {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Get everything between spaces
	args := strings.Fields(s)

	// Check length
	if len(args) != 2 {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Parse x
	x, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Parse y
	y, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
	}

	// Create point object
	return sql.Point{X: x, Y: y}, nil
}

// Eval implements the sql.Expression interface.
func (p *PointFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		// TODO: possible to use a regular expression? "*point *\( *[0-9][0-9]* *[0-9][0-9]* *\) *" /gi
		if s, err = TrimTypePrefix(s, "point"); err == nil {
			return ParsePointString(s)
		}
	}

	return nil, sql.ErrInvalidGISData.New("ST_PointFromText")
}
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

// LineFromText is a function that returns a point type from a WKT string
type LineFromText struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*LineFromText)(nil)

// NewLineFromText creates a new point expression.
func NewLineFromText(e sql.Expression) sql.Expression {
	return &LineFromText{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *LineFromText) FunctionName() string {
	return "st_linefromtext"
}

// Description implements sql.FunctionExpression
func (p *LineFromText) Description() string {
	return "returns a new line from a WKT string."
}

// IsNullable implements the sql.Expression interface.
func (p *LineFromText) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *LineFromText) Type() sql.Type {
	return p.Child.Type()
}

func (p *LineFromText) String() string {
	return fmt.Sprintf("ST_LINEFROMTEXT(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *LineFromText) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewLineFromText(children[0]), nil
}

// WKTToLine expects a string like "1.2 3.4, 5.6 7.8, ..."
func WKTToLine(s string) (sql.Linestring, error) {
	// Empty string is wrong
	if len(s) == 0 {
		return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromText")
	}

	// Separate by comma
	pointStrs := strings.Split(s, ",")

	// Parse each point string
	var points = make([]sql.Point, len(pointStrs))
	for i, ps := range pointStrs {
		// Remove leading and trailing whitespace
		ps = strings.TrimSpace(ps)

		// Parse point
		if p, err := WKTToPoint(ps); err == nil {
			points[i] = p
		} else {
			return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromText")
		}
	}

	// Create Linestring object
	return sql.Linestring{Points: points}, nil
}

// Eval implements the sql.Expression interface.
func (p *LineFromText) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		if geomType, data, err := ParseWKTHeader(s); err == nil && geomType == "linestring" {
			return WKTToLine(data)
		}
	}

	return nil, sql.ErrInvalidGISData.New("ST_LineFromText")
}

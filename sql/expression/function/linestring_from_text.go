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

// ParseLinestringString expects a string like "1.2 3.4, 5.6 7.8, ..."
func ParseLinestringString(s string) (interface{}, error) {
	// Empty string is wrong
	if len(s) == 0 {
		return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
	}

	// Separate by comma
	pointStrs := strings.Split(s, ",")

	// Parse each point string
	var points = make([]sql.Point, len(pointStrs))
	for i, ps := range pointStrs {
		// Remove leading and trailing whitespace
		ps = strings.TrimSpace(ps)

		// Parse point
		if p, err := ParsePointString(ps); err == nil {
			points[i] = p.(sql.Point)
		} else {
			return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
		}
	}

	// Create Linestring object
	return sql.Linestring{Points: points}, nil
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
		if s, err = TrimTypePrefix(s, "linestring"); err == nil {
			return ParseLinestringString(s)
		}
	}

	return nil, sql.ErrInvalidGISData.New("ST_LinestringFromText")
}
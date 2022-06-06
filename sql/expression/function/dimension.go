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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Dimension is a function that converts a spatial type into WKT format (alias for AsText)
type Dimension struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*Dimension)(nil)

// NewDimension creates a new point expression.
func NewDimension(e sql.Expression) sql.Expression {
	return &Dimension{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *Dimension) FunctionName() string {
	return "st_dimension"
}

// Description implements sql.FunctionExpression
func (p *Dimension) Description() string {
	return "returns the dimension of the geometry given."
}

// IsNullable implements the sql.Expression interface.
func (p *Dimension) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *Dimension) Type() sql.Type {
	return sql.Int32
}

func (p *Dimension) String() string {
	return fmt.Sprintf("ST_DIMENSION(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *Dimension) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewDimension(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (p *Dimension) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return nil if geometry is nil
	if val == nil {
		return nil, nil
	}

	// Expect one of the geometry types
	switch val.(type) {
	case sql.Point:
		return 0, nil
	case sql.LineString:
		return 1, nil
	case sql.Polygon:
		return 2, nil
	default:
		return nil, sql.ErrInvalidGISData.New("ST_DIMENSION")
	}
}

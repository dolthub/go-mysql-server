// Copyright 2020-2022 Dolthub, Inc.
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

// STLength is a function that returns the STLength of a LineString
type STLength struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*STLength)(nil)

// NewArea creates a new STX expression.
func NewSTLength(arg sql.Expression) sql.Expression {
	return &STLength{expression.UnaryExpression{Child: arg}}
}

// FunctionName implements sql.FunctionExpression
func (a *STLength) FunctionName() string {
	return "st_srid"
}

// Description implements sql.FunctionExpression
func (a *STLength) Description() string {
	return "returns the SRID value of given geometry object. If given a second argument, returns a new geometry object with second argument as SRID value."
}

// Type implements the sql.Expression interface.
func (a *STLength) Type() sql.Type {
	return sql.Float64
}

func (a *STLength) String() string {
	return fmt.Sprintf("ST_AREA(%a)", a.Child)
}

// WithChildren implements the Expression interface.
func (a *STLength) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), 1)
	}
	return NewArea(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (a *STLength) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate argument
	v, err := a.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return nil if argument is nil
	if v == nil {
		return nil, nil
	}

	p, ok := v.(sql.Polygon)
	if !ok {
		return nil, ErrInvalidAreaArgument.New(v)
	}

	var totalArea float64
	for i, l := range p.Lines {
		area := calculateArea(l)
		if i != 0 {
			area = -area
		}
		totalArea += area
	}
	return totalArea, nil
}

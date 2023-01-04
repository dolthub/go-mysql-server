// Copyright 2023 Dolthub, Inc.
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

package spatial

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// Within is a function that true if left is spatially within right
type Within struct {
	expression.BinaryExpression
}

var _ sql.FunctionExpression = (*Within)(nil)

// NewWithin creates a new Within expression.
func NewWithin(g1, g2 sql.Expression) sql.Expression {
	return &Within{
		expression.BinaryExpression{
			Left: g1,
			Right: g2,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (w *Within) FunctionName() string {
	return "st_within"
}

// Description implements sql.FunctionExpression
func (w *Within) Description() string {
	return "returns 1 or 0 to indicate whether g1 is spatially within g2. This tests the opposite relationship as st_contains()."
}

// Type implements the sql.Expression interface.
func (w *Within) Type() sql.Type {
	return sql.Boolean
}

func (w *Within) String() string {
	return fmt.Sprintf("%s(%s,%s)", w.FunctionName(), w.Left, w.Right)
}

// WithChildren implements the Expression interface.
func (w *Within) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(w, len(children), 2)
	}
	return NewWithin(children[0], children[1]), nil
}

// Eval implements the sql.Expression interface.
func (w *Within) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	g1, err := w.Left.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	g2, err := w.Right.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if g1 == nil || g2 == nil {
		return nil, nil
	}

	var geom1, geom2 sql.GeometryValue
	var ok bool
	geom1, ok = g1.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(w.FunctionName())
	}
	geom2, ok = g2.(sql.GeometryValue)
	if !ok {
		return nil, sql.ErrInvalidGISData.New(w.FunctionName())
	}

	if geom1.GetGeomType() != geom2.GetGeomType() {
		return 0, nil
	}

	if geom1.GetSRID() != geom2.GetSRID() {
		return nil, sql.ErrDiffSRIDs.New(s.FunctionName(), geom1.GetSRID(), geom2.GetSRID())
	}



	return nil, nil
}

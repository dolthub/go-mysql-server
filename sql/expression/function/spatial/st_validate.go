// Copyright 2025 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Validate is a function that validates a geometry according to OGC rules.
// If the geometry is valid, it is returned unchanged; otherwise NULL is returned.
type Validate struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Validate)(nil)
var _ sql.CollationCoercible = (*Validate)(nil)

// NewValidate creates a new Validate expression.
func NewValidate(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &Validate{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (v *Validate) FunctionName() string {
	return "st_validate"
}

// Description implements sql.FunctionExpression
func (v *Validate) Description() string {
	return "validates a geometry according to OGC rules. Returns the geometry if valid, NULL otherwise."
}

// IsNullable implements the sql.Expression interface.
func (v *Validate) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (v *Validate) Type(ctx *sql.Context) sql.Type {
	return types.GeometryType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Validate) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (v *Validate) String() string {
	return fmt.Sprintf("%s(%s)", v.FunctionName(), v.Child.String())
}

// WithChildren implements the Expression interface.
func (v *Validate) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 1)
	}
	return NewValidate(ctx, children[0]), nil
}

// isValidGeometry checks if a geometry is valid according to basic OGC rules.
func isValidGeometry(gv types.GeometryValue) bool {
	switch v := gv.(type) {
	case types.Point:
		return true

	case types.LineString:
		// A linestring must have at least 2 points
		return len(v.Points) >= 2

	case types.Polygon:
		if len(v.Lines) == 0 {
			return false
		}
		for _, ring := range v.Lines {
			// Each ring must have at least 4 points (3 distinct + closing)
			if len(ring.Points) < 4 {
				return false
			}
			// Ring must be closed
			first := ring.Points[0]
			last := ring.Points[len(ring.Points)-1]
			if first.X != last.X || first.Y != last.Y {
				return false
			}
		}
		return true

	case types.MultiPoint:
		return true

	case types.MultiLineString:
		for _, line := range v.Lines {
			if len(line.Points) < 2 {
				return false
			}
		}
		return true

	case types.MultiPolygon:
		for _, poly := range v.Polygons {
			if !isValidGeometry(poly) {
				return false
			}
		}
		return true

	case types.GeomColl:
		for _, geom := range v.Geoms {
			if !isValidGeometry(geom) {
				return false
			}
		}
		return true

	default:
		return false
	}
}

// Eval implements the sql.Expression interface.
func (v *Validate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := v.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(v.FunctionName())
	}

	if isValidGeometry(gv) {
		return gv, nil
	}
	return nil, nil
}

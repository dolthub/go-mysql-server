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

// Envelope is a function that returns the minimum bounding rectangle (MBR) for a geometry value.
// The result is a Polygon for 2D bounding boxes, a LineString for degenerate cases where the
// bounding box is a line, or a Point when it degenerates to a single point.
type Envelope struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*Envelope)(nil)
var _ sql.CollationCoercible = (*Envelope)(nil)

// NewEnvelope creates a new Envelope expression.
func NewEnvelope(e sql.Expression) sql.Expression {
	return &Envelope{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (e *Envelope) FunctionName() string {
	return "st_envelope"
}

// Description implements sql.FunctionExpression
func (e *Envelope) Description() string {
	return "returns the minimum bounding rectangle for the geometry value."
}

// IsNullable implements the sql.Expression interface.
func (e *Envelope) IsNullable() bool {
	return e.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (e *Envelope) Type() sql.Type {
	return types.GeometryType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Envelope) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (e *Envelope) String() string {
	return fmt.Sprintf("%s(%s)", e.FunctionName(), e.Child.String())
}

// WithChildren implements the Expression interface.
func (e *Envelope) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewEnvelope(children[0]), nil
}

// Eval implements the sql.Expression interface.
func (e *Envelope) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := e.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(e.FunctionName())
	}

	srid := gv.GetSRID()
	minX, minY, maxX, maxY := gv.BBox()

	// Point: bounding box degenerates to a point
	if minX == maxX && minY == maxY {
		return types.Point{SRID: srid, X: minX, Y: minY}, nil
	}

	// Horizontal or vertical line: bounding box degenerates to a linestring
	if minX == maxX || minY == maxY {
		return types.LineString{
			SRID: srid,
			Points: []types.Point{
				{SRID: srid, X: minX, Y: minY},
				{SRID: srid, X: maxX, Y: maxY},
			},
		}, nil
	}

	// General case: return a polygon representing the bounding box
	return types.Polygon{
		SRID: srid,
		Lines: []types.LineString{
			{
				SRID: srid,
				Points: []types.Point{
					{SRID: srid, X: minX, Y: minY},
					{SRID: srid, X: maxX, Y: minY},
					{SRID: srid, X: maxX, Y: maxY},
					{SRID: srid, X: minX, Y: maxY},
					{SRID: srid, X: minX, Y: minY},
				},
			},
		},
	}, nil
}

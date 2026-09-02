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

// ExteriorRing is a function that returns the exterior ring of a Polygon as a LineString.
type ExteriorRing struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*ExteriorRing)(nil)
var _ sql.CollationCoercible = (*ExteriorRing)(nil)

// NewExteriorRing creates a new ExteriorRing expression.
func NewExteriorRing(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &ExteriorRing{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (e *ExteriorRing) FunctionName() string {
	return "st_exteriorring"
}

// Description implements sql.FunctionExpression
func (e *ExteriorRing) Description() string {
	return "returns the exterior ring of the Polygon value as a LineString."
}

// IsNullable implements the sql.Expression interface.
func (e *ExteriorRing) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (e *ExteriorRing) Type(ctx *sql.Context) sql.Type {
	return types.LineStringType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ExteriorRing) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (e *ExteriorRing) String() string {
	return fmt.Sprintf("%s(%s)", e.FunctionName(), e.Child.String())
}

// WithChildren implements the Expression interface.
func (e *ExteriorRing) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}
	return NewExteriorRing(ctx, children[0]), nil
}

// Eval implements the sql.Expression interface.
func (e *ExteriorRing) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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

	p, ok := gv.(types.Polygon)
	if !ok {
		return nil, sql.ErrInvalidArgument.New(e.FunctionName())
	}

	if len(p.Lines) == 0 {
		return nil, nil
	}

	// The exterior ring is the first linestring in the polygon
	ring := p.Lines[0]
	ring.SRID = p.SRID
	return ring, nil
}

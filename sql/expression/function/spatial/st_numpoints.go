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

// NumPoints is a function that returns the number of points in a LineString.
type NumPoints struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*NumPoints)(nil)
var _ sql.CollationCoercible = (*NumPoints)(nil)

// NewNumPoints creates a new NumPoints expression.
func NewNumPoints(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &NumPoints{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (n *NumPoints) FunctionName() string {
	return "st_numpoints"
}

// Description implements sql.FunctionExpression
func (n *NumPoints) Description() string {
	return "returns the number of points in a LineString."
}

// IsNullable implements the sql.Expression interface.
func (n *NumPoints) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (n *NumPoints) Type(ctx *sql.Context) sql.Type {
	return types.Int32
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*NumPoints) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (n *NumPoints) String(ctx *sql.Context) string {
	return fmt.Sprintf("%s(%s)", n.FunctionName(), n.Child.String(ctx))
}

// WithChildren implements the Expression interface.
func (n *NumPoints) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	return NewNumPoints(ctx, children[0]), nil
}

// Eval implements the sql.Expression interface.
func (n *NumPoints) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := n.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(n.FunctionName())
	}

	// ST_NumPoints is only defined for LineString; returns NULL for other types
	l, ok := gv.(types.LineString)
	if !ok {
		return nil, nil
	}

	return len(l.Points), nil
}

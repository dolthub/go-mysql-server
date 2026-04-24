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

// IsEmpty is a function that returns whether a geometry is an empty geometry collection.
type IsEmpty struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*IsEmpty)(nil)
var _ sql.CollationCoercible = (*IsEmpty)(nil)

// NewIsEmpty creates a new IsEmpty expression.
func NewIsEmpty(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &IsEmpty{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (i *IsEmpty) FunctionName() string {
	return "st_isempty"
}

// Description implements sql.FunctionExpression
func (i *IsEmpty) Description() string {
	return "returns whether the geometry is an empty geometry collection."
}

// IsNullable implements the sql.Expression interface.
func (i *IsEmpty) IsNullable(ctx *sql.Context) bool {
	return i.Child.IsNullable(ctx)
}

// Type implements the sql.Expression interface.
func (i *IsEmpty) Type(ctx *sql.Context) sql.Type {
	return types.Boolean
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*IsEmpty) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (i *IsEmpty) String(ctx *sql.Context) string {
	return fmt.Sprintf("%s(%s)", i.FunctionName(), i.Child.String(ctx))
}

// WithChildren implements the Expression interface.
func (i *IsEmpty) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewIsEmpty(ctx, children[0]), nil
}

// Eval implements the sql.Expression interface.
func (i *IsEmpty) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := i.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	gv, err := types.UnwrapGeometry(ctx, val)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New(i.FunctionName())
	}

	// In MySQL, only an empty GeometryCollection is considered "empty".
	// All other geometry types (including multi-types) are non-empty.
	switch v := gv.(type) {
	case types.GeomColl:
		return len(v.Geoms) == 0, nil
	default:
		return false, nil
	}
}

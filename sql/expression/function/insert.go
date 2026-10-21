// Copyright 2020-2024 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Insert implements the SQL function INSERT() which inserts a substring at a specified position
type Insert struct {
	str    sql.Expression
	pos    sql.Expression
	length sql.Expression
	newStr sql.Expression
}

var _ sql.FunctionExpression = (*Insert)(nil)
var _ sql.CollationCoercible = (*Insert)(nil)

// NewInsert creates a new Insert expression
func NewInsert(ctx *sql.Context, str, pos, length, newStr sql.Expression) sql.Expression {
	return &Insert{str, pos, length, newStr}
}

// FunctionName implements sql.FunctionExpression
func (i *Insert) FunctionName() string {
	return "insert"
}

// Description implements sql.FunctionExpression
func (i *Insert) Description() string {
	return "returns the string str, with the substring beginning at position pos and len characters long replaced by the string newstr."
}

// Children implements the Expression interface
func (i *Insert) Children() []sql.Expression {
	return []sql.Expression{i.str, i.pos, i.length, i.newStr}
}

// Resolved implements the Expression interface
func (i *Insert) Resolved() bool {
	return i.str.Resolved() && i.pos.Resolved() && i.length.Resolved() && i.newStr.Resolved()
}

// IsNullable implements the Expression interface
func (i *Insert) IsNullable(ctx *sql.Context) bool {
	return i.str.IsNullable(ctx) || i.pos.IsNullable(ctx) || i.length.IsNullable(ctx) || i.newStr.IsNullable(ctx)
}

// Type implements the Expression interface
func (i *Insert) Type(ctx *sql.Context) sql.Type {
	return types.LongTextForType(i.str.Type(ctx))
}

// CollationCoercibility implements the interface sql.CollationCoercible
func (i *Insert) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.ResolveCoercibilityExpressions(ctx, i.str)
}

// String implements the Expression interface
func (i *Insert) String() string {
	return fmt.Sprintf("insert(%s, %s, %s, %s)", i.str, i.pos, i.length, i.newStr)
}

// WithChildren implements the Expression interface
func (i *Insert) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 4 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 4)
	}
	return NewInsert(ctx, children[0], children[1], children[2], children[3]), nil
}

// Eval implements the Expression interface
func (i *Insert) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	collation, _ := i.CollationCoercibility(ctx)

	s, ok, err := evalString(ctx, i.str, row, collation)
	if err != nil || !ok {
		return nil, err
	}

	p, ok, err := evalInt64(ctx, i.pos, row)
	if err != nil || !ok {
		return nil, err
	}

	l, ok, err := evalInt64(ctx, i.length, row)
	if err != nil || !ok {
		return nil, err
	}

	n, ok, err := evalString(ctx, i.newStr, row, collation)
	if err != nil || !ok {
		return nil, err
	}

	if p < 1 {
		return s, nil
	}

	handler := sql.NewCharSetHandler(collation)
	numChars, err := handler.NumChars(s)
	if err != nil {
		return nil, err
	}

	startIdx := p - 1
	if startIdx >= int64(numChars) {
		return s, nil
	}

	endIdx := int64(numChars)
	if l >= 0 && startIdx+l < endIdx {
		endIdx = startIdx + l
	}

	startByte, err := handler.CharPos(s, int(startIdx))
	if err != nil {
		return nil, err
	}
	endByte, err := handler.CharPos(s, int(endIdx))
	if err != nil {
		return nil, err
	}

	return s[:startByte] + n + s[endByte:], nil
}

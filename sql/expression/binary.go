// Copyright 2021 Dolthub, Inc.
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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// The BINARY operator converts the expression to a binary string (a string that has the binary character set and binary
// collation). A common use for BINARY is to force a character string comparison to be done byte by byte using numeric
// byte values rather than character by character. The BINARY operator also causes trailing spaces in comparisons to be
// significant.
//
//cc: https://dev.mysql.com/doc/refman/8.0/en/cast-functions.html#operator_binary
type Binary struct {
	UnaryExpression
}

func NewBinary(e sql.Expression) sql.Expression {
	return &Binary{UnaryExpression{Child: e}}
}

func (b *Binary) String() string {
	return fmt.Sprintf("BINARY(%s)", b.Child.String())
}

func (b *Binary) Type() sql.Type {
	return sql.LongBlob
}

func (b *Binary) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	expr, err := b.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	return convertValue(expr, ConvertToBinary)
}

func (b *Binary) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidArgumentNumber.New("BINARY", "1", len(children))
	}

	return NewBinary(children[0]), nil
}

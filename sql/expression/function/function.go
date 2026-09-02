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
	"strings"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type UnaryFunc struct {
	expression.UnaryExpressionStub
	// The type returned by the function
	RetType sql.Type
	// Name is the name of the function
	Name string
}

func NewUnaryFunc(arg sql.Expression, name string, returnType sql.Type) *UnaryFunc {
	return &UnaryFunc{
		UnaryExpressionStub: expression.UnaryExpressionStub{Child: arg},
		Name:                name,
		RetType:             returnType,
	}
}

// FunctionName implements sql.FunctionExpression
func (uf *UnaryFunc) FunctionName() string {
	return strings.ToLower(uf.Name)
}

// EvalChild is a convenience function for safely evaluating a child expression
func (uf *UnaryFunc) EvalChild(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if uf.Child == nil {
		return nil, nil
	}

	return uf.Child.Eval(ctx, row)
}

// String implements the fmt.Stringer interface.
func (uf *UnaryFunc) String() string {
	return fmt.Sprintf("%s(%s)", uf.FunctionName(), uf.Child.String())
}

// Type implements the Expression interface.
func (uf *UnaryFunc) Type(ctx *sql.Context) sql.Type {
	return uf.RetType
}

// evalInt64 evaluates |expr| against |row| and coerces the result
// to an int64, recording any conversion warnings on |ctx|. It
// returns (0, false, nil) if the evaluated value is SQL NULL.
func evalInt64(ctx *sql.Context, expr sql.Expression, row sql.Row) (int64, bool, error) {
	if expr == nil {
		return 0, false, nil
	}
	v, err := expr.Eval(ctx, row)
	if err != nil || v == nil {
		return 0, false, err
	}
	c, _, err := types.Int64.Convert(ctx, v)
	if err != nil {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
	}
	n, ok := c.(int64)
	return n, ok, nil
}

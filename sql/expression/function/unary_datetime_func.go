// Copyright 2026 Dolthub, Inc.
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

// UnaryDatetimeFunc is a sql.Function which takes a single datetime argument
type UnaryDatetimeFunc struct {
	expression.UnaryExpressionStub
	// SQLType is the return type of the function
	SQLType sql.Type
	// name is the name of the function
	name string
}

// NewUnaryDatetimeFunc returns a new UnaryDatetimeFunc over |arg| with the given function name and return type.
func NewUnaryDatetimeFunc(arg sql.Expression, name string, sqlType sql.Type) *UnaryDatetimeFunc {
	return &UnaryDatetimeFunc{
		UnaryExpressionStub: expression.UnaryExpressionStub{
			Child: arg,
		},
		name:    name,
		SQLType: sqlType,
	}
}

// Name implements sql.FunctionExpression
func (dtf *UnaryDatetimeFunc) Name() string {
	return dtf.name
}

// EvalChild evaluates the child expression and converts the result to a datetime value, returning a nil value if it cannot be converted.
func (dtf *UnaryDatetimeFunc) EvalChild(ctx *sql.Context, row sql.Row) (any, error) {
	val, err := dtf.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	val, _, err = types.DatetimeMaxPrecision.Convert(ctx, val)
	if err != nil {
		ctx.Warn(mysql.ERTruncatedWrongValue, "%s", err.Error())
		return nil, nil
	}
	return val, nil
}

// String implements the fmt.Stringer interface.
func (dtf *UnaryDatetimeFunc) String() string {
	return fmt.Sprintf("%s(%s)", strings.ToUpper(dtf.name), dtf.Child.String())
}

// IsNullable implements the Expression interface
func (dtf *UnaryDatetimeFunc) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the Expression interface.
func (dtf *UnaryDatetimeFunc) Type(ctx *sql.Context) sql.Type {
	return dtf.SQLType
}

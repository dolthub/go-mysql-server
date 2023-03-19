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
	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

type ConnectionID struct {
	NoArgFunc
}

func (c ConnectionID) IsNonDeterministic() bool {
	return true
}

func connIDFuncLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.ID(), nil
}

var _ sql.FunctionExpression = ConnectionID{}

func NewConnectionID() sql.Expression {
	return ConnectionID{
		NoArgFunc: NoArgFunc{"connection_id", types.Uint32},
	}
}

// FunctionName implements sql.FunctionExpression
func (c ConnectionID) FunctionName() string {
	return "connection_id"
}

// Description implements sql.FunctionExpression
func (c ConnectionID) Description() string {
	return "returns the current connection ID."
}

// Eval implements sql.Expression
func (c ConnectionID) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return connIDFuncLogic(ctx, row)
}

// WithChildren implements sql.Expression
func (c ConnectionID) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, children)
}

type User struct {
	NoArgFunc
}

func (c User) IsNonDeterministic() bool {
	return true
}

func userFuncLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	if ctx.Client().User == "" && ctx.Client().Address == "" {
		return "", nil
	}

	return ctx.Client().User + "@" + ctx.Client().Address, nil
}

var _ sql.FunctionExpression = User{}

// Description implements sql.FunctionExpression
func (c User) Description() string {
	return "returns the authenticated user name and host name."
}

func NewUser() sql.Expression {
	return User{
		NoArgFunc: NoArgFunc{"user", types.LongText},
	}
}

func NewCurrentUser() sql.Expression {
	return User{
		NoArgFunc: NoArgFunc{"current_user", types.LongText},
	}
}

// Eval implements sql.Expression
func (c User) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return userFuncLogic(ctx, row)
}

// WithChildren implements sql.Expression
func (c User) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, children)
}

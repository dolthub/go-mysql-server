// Copyright 2020 Liquidata, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

type NoArgFunc struct {
	Name    string
	SQLType sql.Type
}

// FunctionName implements sql.FunctionExpression
func (fn NoArgFunc) FunctionName() string {
	return fn.Name
}

// Type implements the Expression interface.
func (fn NoArgFunc) Type() sql.Type { return fn.SQLType }

func (fn NoArgFunc) String() string { return strings.ToUpper(fn.Name) + "()" }

// IsNullable implements the Expression interface.
func (fn NoArgFunc) IsNullable() bool { return false }

// Resolved implements the Expression interface.
func (fn NoArgFunc) Resolved() bool { return true }

// Children implements the Expression interface.
func (fn NoArgFunc) Children() []sql.Expression { return nil }

// WithChildren implements the Expression interface.
func NoArgFuncWithChildren(fn sql.Expression, children []sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(fn, len(children), 0)
	}
	return fn, nil
}

type CurrDate struct {
	NoArgFunc
}

var _ sql.FunctionExpression = CurrDate{}

func NewCurrDate() sql.Expression {
	return CurrDate{
		NoArgFunc: NoArgFunc{"curdate", sql.LongText},
	}
}

func NewCurrentDate() sql.Expression {
	return CurrDate{
		NoArgFunc: NoArgFunc{"current_date", sql.LongText},
	}
}

func (c CurrDate) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return currDateLogic(ctx, row)
}

func (c CurrDate) WithChildren(expressions ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, expressions)
}

type CurrTime struct {
	NoArgFunc
}

var _ sql.FunctionExpression = CurrTime{}

func NewCurrTime() sql.Expression {
	return CurrTime{
		NoArgFunc: NoArgFunc{"curtime", sql.LongText},
	}
}

func NewCurrentTime() sql.Expression {
	return CurrTime{
		NoArgFunc: NoArgFunc{"current_time", sql.LongText},
	}
}

func (c CurrTime) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return currTimeLogic(ctx, row)
}

func (c CurrTime) WithChildren(expressions ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, expressions)
}

type CurrTimestamp struct {
	NoArgFunc
}

var _ sql.FunctionExpression = CurrTimestamp{}

func NewCurrTimestamp() sql.Expression {
	return CurrTimestamp{
		NoArgFunc: NoArgFunc{"current_timestamp", sql.Datetime},
	}
}

func (c CurrTimestamp) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return currTimeLogic(ctx, row)
}

func (c CurrTimestamp) WithChildren(expressions ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, expressions)
}

type ConnectionID struct {
	NoArgFunc
}

func connIDFuncLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.ID(), nil
}

var _ sql.FunctionExpression = ConnectionID{}

func NewConnectionID() sql.Expression {
	return ConnectionID{
		NoArgFunc: NoArgFunc{"connection_id", sql.Uint32},
	}
}

func (c ConnectionID) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return connIDFuncLogic(ctx, row)
}

func (c ConnectionID) WithChildren(expressions ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, expressions)
}

type User struct {
	NoArgFunc
}

func userFuncLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	return ctx.Client().User, nil
}

var _ sql.FunctionExpression = User{}

func NewUser() sql.Expression {
	return User{
		NoArgFunc: NoArgFunc{"user", sql.LongText},
	}
}

func NewCurrentUser() sql.Expression {
	return User{
		NoArgFunc: NoArgFunc{"current_user", sql.LongText},
	}
}

func (c User) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return userFuncLogic(ctx, row)
}

func (c User) WithChildren(expressions ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, expressions)
}

type ReleaseAllLocks struct {
	NoArgFunc
	ls *sql.LockSubsystem
}

var _ sql.FunctionExpression = ReleaseAllLocks{}

func NewReleaseAllLocks(ls *sql.LockSubsystem) func() sql.Expression {
	return func() sql.Expression {
		return ReleaseAllLocks{
			NoArgFunc: NoArgFunc{"release_all_locks", sql.Int32},
			ls: ls,
		}
	}
}

func (c ReleaseAllLocks) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return c.ls.ReleaseAll(ctx)
}

func (c ReleaseAllLocks) WithChildren(expressions ...sql.Expression) (sql.Expression, error) {
	return NoArgFuncWithChildren(c, expressions)
}

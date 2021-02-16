// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, User 2.0 (the "License");
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
)

// User is a function that returns server user.
type User struct {
	NoArgFunc
}

var _ sql.FunctionExpression = (*User)(nil)

func userFuncLogic(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	user := ctx.Client().User
	if user == "" {
		return user, nil
	}
	return fmt.Sprintf("%s%s", user, "@%"), nil
}

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

// FunctionName implements sql.FunctionExpression
func (f User) FunctionName() string {
	return "user"
}

// Type implements the Expression interface.
func (f User) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (f User) IsNullable() bool {
	return false
}

func (f User) String() string {
	return "current_user"
}

// WithChildren implements the Expression interface.
func (f User) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return f, nil
}

// Resolved implements the Expression interface.
func (f User) Resolved() bool {
	return true
}

// Children implements the Expression interface.
func (f User) Children() []sql.Expression { return nil }

// Eval implements the Expression interface.
func (f User) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return userFuncLogic(ctx, row)
}

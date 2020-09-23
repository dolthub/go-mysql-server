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

package expression

import (
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

// GetSessionVar is an expression that returns the value of a session variable.
type GetSessionVar struct {
	name  string
	typ   sql.Type
}

// NewGetSessionVar creates a new GetSessionField expression.
func NewGetSessionVar(varname string, typ sql.Type) *GetSessionVar {
	return &GetSessionVar{varname, typ}
}

// Children implements the sql.Expression interface.
func (f *GetSessionVar) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (f *GetSessionVar) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	_, val := ctx.Get(f.name)
	return val, nil
}

// Type implements the sql.Expression interface.
func (f *GetSessionVar) Type() sql.Type { return f.typ }

// IsNullable implements the sql.Expression interface.
func (f *GetSessionVar) IsNullable() bool { return true }

// Resolved implements the sql.Expression interface.
func (f *GetSessionVar) Resolved() bool { return true }

// String implements the sql.Expression interface.
func (f *GetSessionVar) String() string { return "@@" + f.name }

func (f *GetSessionVar) DebugString() string {
	return fmt.Sprintf("@@%s, type=%s", f.name, f.typ)
}

// WithChildren implements the Expression interface.
func (f *GetSessionVar) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return f, nil
}

// SystemVar is an expression that returns the name of a system variable. It's used as the expression on the left hand
// side of a SET statement.
type SystemVar struct {
	Name string
	typ  sql.Type
}

// NewSystemVar creates a new SystemVar expression.
func NewSystemVar(name string, typ sql.Type) *SystemVar {
	return &SystemVar{name, typ}
}

// Children implements the sql.Expression interface.
func (v *SystemVar) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (v *SystemVar) Eval(*sql.Context, sql.Row) (interface{}, error) {
	return v.Name, nil
}

// Type implements the sql.Expression interface.
func (v *SystemVar) Type() sql.Type { return v.typ }

// IsNullable implements the sql.Expression interface.
func (v *SystemVar) IsNullable() bool { return false }

// Resolved implements the sql.Expression interface.
func (v *SystemVar) Resolved() bool { return true }

// String implements the sql.Expression interface.
func (v *SystemVar) String() string { return "@@" + v.Name }

func (v *SystemVar) DebugString() string {
	return fmt.Sprintf("@@%s (%s)", v.Name, v.typ)
}

// WithChildren implements the Expression interface.
func (v *SystemVar) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 0)
	}
	return v, nil
}

// UserVar is an expression that returns the name of a user variable. It's used as the expression on the left hand
// side of a SET statement.
type UserVar struct {
	Name string
}

// NewUserVar creates a new UserVar expression.
func NewUserVar(name string) *UserVar {
	return &UserVar{name}
}

// Children implements the sql.Expression interface.
func (v *UserVar) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (v *UserVar) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	_, val := ctx.Get(v.Name)
	return val, nil
}

// Type implements the sql.Expression interface.
// TODO: type checking based on type of user var
func (v *UserVar) Type() sql.Type { return sql.Boolean }

// IsNullable implements the sql.Expression interface.
func (v *UserVar) IsNullable() bool { return true }

// Resolved implements the sql.Expression interface.
func (v *UserVar) Resolved() bool { return true }

// String implements the sql.Expression interface.
func (v *UserVar) String() string { return "@" + v.Name }

func (v *UserVar) DebugString() string {
	return fmt.Sprintf("@%s", v.Name)
}

// WithChildren implements the Expression interface.
func (v *UserVar) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 0)
	}
	return v, nil
}

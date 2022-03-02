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

package expression

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// SystemVar is an expression that returns the value of a system variable. It's also used as the expression on the left
// hand side of a SET statement for a system variable.
type SystemVar struct {
	Name  string
	Scope sql.SystemVariableScope
}

// NewSystemVar creates a new SystemVar expression.
func NewSystemVar(name string, scope sql.SystemVariableScope) *SystemVar {
	return &SystemVar{name, scope}
}

// Children implements the sql.Expression interface.
func (v *SystemVar) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (v *SystemVar) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	switch v.Scope {
	case sql.SystemVariableScope_Session:
		val, err := ctx.GetSessionVariable(ctx, v.Name)
		if err != nil {
			return nil, err
		}
		return val, nil
	case sql.SystemVariableScope_Global:
		_, val, ok := sql.SystemVariables.GetGlobal(v.Name)
		if !ok {
			return nil, sql.ErrUnknownSystemVariable.New(v.Name)
		}
		return val, nil
	default: // should never happen
		return nil, fmt.Errorf("unknown scope `%v` on system variable `%s`", v.Scope, v.Name)
	}
}

// Type implements the sql.Expression interface.
func (v *SystemVar) Type() sql.Type {
	if sysVar, _, ok := sql.SystemVariables.GetGlobal(v.Name); ok {
		return sysVar.Type
	}
	return sql.Null
}

// IsNullable implements the sql.Expression interface.
func (v *SystemVar) IsNullable() bool { return false }

// Resolved implements the sql.Expression interface.
func (v *SystemVar) Resolved() bool { return true }

// String implements the sql.Expression interface.
func (v *SystemVar) String() string {
	switch v.Scope {
	case sql.SystemVariableScope_Session:
		return fmt.Sprintf("@@SESSION.%s", v.Name)
	case sql.SystemVariableScope_Global:
		return fmt.Sprintf("@@GLOBAL.%s", v.Name)
	default: // should never happen
		return fmt.Sprintf("@@UNKNOWN(%v).%s", v.Scope, v.Name)
	}
}

// WithChildren implements the Expression interface.
func (v *SystemVar) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 0)
	}
	return v, nil
}

// UserVar is an expression that returns the value of a user variable. It's also used as the expression on the left hand
// side of a SET statement for a user var.
type UserVar struct {
	Name     string
	exprType sql.Type
}

// NewUserVar creates a UserVar with a name, but no type information, for use as the left-hand value
// in a SetField assignment Expression. This method should not be used when the user variable is
// being used as a value, since the correct type information will not be available.
func NewUserVar(name string) *UserVar {
	return &UserVar{name, sql.Null}
}

// NewUserVarWithType creates a UserVar with its type resolved, so that it can be used as a value
// in other expressions.
func NewUserVarWithType(name string, t sql.Type) *UserVar {
	return &UserVar{name, t}
}

// Children implements the sql.Expression interface.
func (v *UserVar) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (v *UserVar) Eval(ctx *sql.Context, _ sql.Row) (interface{}, error) {
	_, val, err := ctx.GetUserVariable(ctx, v.Name)
	if err != nil {
		return nil, err
	}

	return val, nil
}

// Type implements the sql.Expression interface.
func (v *UserVar) Type() sql.Type {
	return v.exprType
}

// IsNullable implements the sql.Expression interface.
func (v *UserVar) IsNullable() bool { return true }

// Resolved implements the sql.Expression interface.
func (v *UserVar) Resolved() bool { return true }

// String implements the sql.Expression interface.
func (v *UserVar) String() string { return "@" + v.Name }

// WithChildren implements the Expression interface.
func (v *UserVar) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(v, len(children), 0)
	}
	return v, nil
}

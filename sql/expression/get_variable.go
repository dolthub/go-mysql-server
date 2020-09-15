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

// GetSessionField is an expression that returns the value of a session configuration.
type GetSessionField struct {
	name  string
	typ   sql.Type
	value interface{}
}

// NewGetSessionField creates a new GetSessionField expression.
func NewGetSessionField(name string, typ sql.Type, value interface{}) *GetSessionField {
	return &GetSessionField{name, typ, value}
}

// Children implements the sql.Expression interface.
func (f *GetSessionField) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (f *GetSessionField) Eval(*sql.Context, sql.Row) (interface{}, error) {
	// TODO: fill in from ctx, not value at analysis time
	return f.value, nil
}

// Type implements the sql.Expression interface.
func (f *GetSessionField) Type() sql.Type { return f.typ }

// IsNullable implements the sql.Expression interface.
func (f *GetSessionField) IsNullable() bool { return f.value == nil }

// Resolved implements the sql.Expression interface.
func (f *GetSessionField) Resolved() bool { return true }

// String implements the sql.Expression interface.
func (f *GetSessionField) String() string { return "@@" + f.name }

func (f *GetSessionField) DebugString() string {
	return fmt.Sprintf("@@%s, type=%s, val=%v", f.name, f.typ, f.value)
}

// WithChildren implements the Expression interface.
func (f *GetSessionField) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return f, nil
}
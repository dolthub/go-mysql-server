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
	"github.com/dolthub/go-mysql-server/sql"
)

// Database implements the DATABASE() function
type Database struct{}

var _ sql.FunctionExpression = (*Database)(nil)

// NewDatabase returns a new Database function
func NewDatabase() sql.Expression {
	return &Database{}
}

// FunctionName implements sql.FunctionExpression
func (db *Database) FunctionName() string {
	return "database"
}

// Type implements the sql.Expression (sql.LongText)
func (db *Database) Type() sql.Type { return sql.LongText }

// IsNullable implements the sql.Expression interface.
// The function returns always true
func (db *Database) IsNullable() bool {
	return true
}

func (*Database) String() string {
	return "DATABASE()"
}

// WithChildren implements the Expression interface.
func (d *Database) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 0)
	}
	return NewDatabase(), nil
}

// Resolved implements the sql.Expression interface.
func (db *Database) Resolved() bool {
	return true
}

// Children implements the sql.Expression interface.
func (db *Database) Children() []sql.Expression { return nil }

// Eval implements the sql.Expression interface.
func (db *Database) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if ctx.GetCurrentDatabase() == "" {
		return nil, nil
	}
	return ctx.GetCurrentDatabase(), nil
}

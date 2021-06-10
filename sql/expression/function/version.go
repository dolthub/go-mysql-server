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

	"github.com/dolthub/go-mysql-server/sql"
)

const mysqlVersion = "8.0.11"

// Version is a function that returns server version.
type Version string

var _ sql.FunctionExpression = (Version)("")

// NewVersion creates a new Version UDF.
func NewVersion(versionPostfix string) func(*sql.Context, ...sql.Expression) (sql.Expression, error) {
	return func(*sql.Context, ...sql.Expression) (sql.Expression, error) {
		return Version(versionPostfix), nil
	}
}

// FunctionName implements sql.FunctionExpression
func (f Version) FunctionName() string {
	return "version"
}

// Type implements the Expression interface.
func (f Version) Type() sql.Type { return sql.LongText }

// IsNullable implements the Expression interface.
func (f Version) IsNullable() bool {
	return false
}

func (f Version) String() string {
	return "VERSION()"
}

// WithChildren implements the Expression interface.
func (f Version) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return f, nil
}

// Resolved implements the Expression interface.
func (f Version) Resolved() bool {
	return true
}

// Children implements the Expression interface.
func (f Version) Children() []sql.Expression { return nil }

// Eval implements the Expression interface.
func (f Version) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	if f == "" {
		return mysqlVersion, nil
	}

	return fmt.Sprintf("%s-%s", mysqlVersion, string(f)), nil
}

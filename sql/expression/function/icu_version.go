// Copyright 2024 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/vitess/go/vt/proto/query"
)

const icuVersion = "73.1"

// ICUVersion is a function that returns the ICU library used to support regex operations.
type ICUVersion struct{}

var _ sql.FunctionExpression = (*ICUVersion)(nil)
var _ sql.CollationCoercible = (*ICUVersion)(nil)

// NewICUVersion creates a new Version UDF.
func NewICUVersion() sql.Expression {
	return &ICUVersion{}
}

// FunctionName implements sql.FunctionExpression
func (icu *ICUVersion) FunctionName() string {
	return "version"
}

// Description implements sql.FunctionExpression
func (icu *ICUVersion) Description() string {
	return "returns a string that indicates the SQL server version."
}

// Type implements the Expression interface.
func (icu *ICUVersion) Type() sql.Type {
	return types.MustCreateString(query.Type_VARCHAR, int64(len(icuVersion)), sql.Collation_Default)
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (icu *ICUVersion) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_utf8mb3_general_ci, 3
}

// IsNullable implements the Expression interface.
func (icu *ICUVersion) IsNullable() bool {
	return false
}

func (icu *ICUVersion) String() string {
	return fmt.Sprintf("%s()", icu.FunctionName())
}

// WithChildren implements the Expression interface.
func (icu *ICUVersion) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(icu, len(children), 0)
	}
	return icu, nil
}

// Resolved implements the Expression interface.
func (icu *ICUVersion) Resolved() bool {
	return true
}

// Children implements the Expression interface.
func (icu *ICUVersion) Children() []sql.Expression { return nil }

// Eval implements the Expression interface.
func (icu *ICUVersion) Eval(_ *sql.Context, _ sql.Row) (interface{}, error) {
	return icuVersion, nil
}

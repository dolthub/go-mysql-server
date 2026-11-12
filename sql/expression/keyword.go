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

package expression

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// keyword is a bare SQL keyword that a function takes as an argument. It
// evaluates to its upper-case text and prints without quotes, so a printed
// expression parses again.
type keyword struct {
	// val holds the upper-case name, boxed once so Eval does not allocate.
	val interface{}
}

var _ sql.Expression = (*keyword)(nil)

// NewKeyword returns a keyword for |name| to pass as a function argument.
func NewKeyword(name string) sql.Expression {
	return &keyword{val: strings.ToUpper(name)}
}

// Resolved returns true, since a keyword has no columns to bind.
func (k *keyword) Resolved() bool {
	return true
}

// IsNullable returns false, since a keyword is never NULL.
func (k *keyword) IsNullable(ctx *sql.Context) bool {
	return false
}

// Type returns LongText, so the planner types the keyword as text.
func (k *keyword) Type(ctx *sql.Context) sql.Type {
	return types.LongText
}

// Eval returns the keyword text, so a function can read it as a string.
func (k *keyword) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	return k.val, nil
}

// String returns the keyword bare, without quotes, so the output parses.
func (k *keyword) String() string {
	return k.val.(string)
}

// Children returns nil, because a keyword has no child expressions.
func (*keyword) Children() []sql.Expression {
	return nil
}

// WithChildren returns the keyword unchanged, so tree rewrites can reuse it.
func (k *keyword) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(k, len(children), 0)
	}
	return k, nil
}

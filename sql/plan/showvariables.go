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

package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ShowVariables is a node that shows the global and session variables
type ShowVariables struct {
	Filter sql.Expression
	Global bool
}

var _ sql.Node = (*ShowVariables)(nil)
var _ sql.Expressioner = (*ShowVariables)(nil)
var _ sql.CollationCoercible = (*ShowVariables)(nil)

// NewShowVariables returns a new ShowVariables reference.
func NewShowVariables(filter sql.Expression, isGlobal bool) *ShowVariables {
	return &ShowVariables{
		Filter: filter,
		Global: isGlobal,
	}
}

// Resolved implements sql.Node interface. The function always returns true.
func (sv *ShowVariables) Resolved() bool {
	return true
}

// WithChildren implements the Node interface.
func (sv *ShowVariables) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(sv, len(children), 0)
	}

	return sv, nil
}

// CheckPrivileges implements the interface sql.Node.
func (sv *ShowVariables) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ShowVariables) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// String implements the fmt.Stringer interface.
func (sv *ShowVariables) String() string {
	var f string
	if sv.Filter != nil {
		f = fmt.Sprintf(" WHERE %s", sv.Filter.String())
	}

	if sv.Global {
		return fmt.Sprintf("SHOW GLOBAL VARIABLES%s", f)
	}
	return fmt.Sprintf("SHOW VARIABLES%s", f)
}

// Schema returns a new Schema reference for "SHOW VARIABLES" query.
func (*ShowVariables) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Variable_name", Type: types.LongText, Nullable: false},
		&sql.Column{Name: "Value", Type: types.LongText, Nullable: true},
	}
}

// Children implements sql.Node interface. The function always returns nil.
func (*ShowVariables) Children() []sql.Node { return nil }

func (sv *ShowVariables) Expressions() []sql.Expression {
	return []sql.Expression{sv.Filter}
}

func (sv *ShowVariables) IsReadOnly() bool {
	return true
}

func (sv *ShowVariables) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(sv, len(exprs), 1)
	}
	ret := *sv
	ret.Filter = exprs[0]
	return &ret, nil
}

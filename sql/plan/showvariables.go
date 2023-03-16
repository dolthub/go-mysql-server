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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ShowVariables is a node that shows the global and session variables
type ShowVariables struct {
	filter sql.Expression
	global bool
}

// NewShowVariables returns a new ShowVariables reference.
func NewShowVariables(filter sql.Expression, isGlobal bool) *ShowVariables {
	return &ShowVariables{
		filter: filter,
		global: isGlobal,
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

// String implements the fmt.Stringer interface.
func (sv *ShowVariables) String() string {
	var f string
	if sv.filter != nil {
		f = fmt.Sprintf(" WHERE %s", sv.filter.String())
	}

	if sv.global {
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

// RowIter implements the sql.Node interface.
// The function returns an iterator for filtered variables (based on like pattern)
func (sv *ShowVariables) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	var sysVars map[string]interface{}

	if sv.global {
		sysVars = sql.SystemVariables.GetAllGlobalVariables()
	} else {
		sysVars = ctx.GetAllSessionVariables()
	}

	for k, v := range sysVars {
		if sv.filter != nil {
			res, err := sv.filter.Eval(ctx, sql.Row{k})
			if err != nil {
				return nil, err
			}
			if !res.(bool) {
				continue
			}
		}
		rows = append(rows, sql.NewRow(k, v))
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	return sql.RowsToRowIter(rows...), nil
}

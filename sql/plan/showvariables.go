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
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// ShowVariables is a node that shows the global and session variables
//TODO: implement the GLOBAL and SESSION distinction
type ShowVariables struct {
	pattern string
}

// NewShowVariables returns a new ShowVariables reference.
// like is a "like pattern". If like is an empty string it will return all variables.
func NewShowVariables(like string) *ShowVariables {
	return &ShowVariables{
		pattern: like,
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

// String implements the fmt.Stringer interface.
func (sv *ShowVariables) String() string {
	var like string
	if sv.pattern != "" {
		like = fmt.Sprintf(" LIKE '%s'", sv.pattern)
	}
	return fmt.Sprintf("SHOW VARIABLES%s", like)
}

// Schema returns a new Schema reference for "SHOW VARIABLES" query.
func (*ShowVariables) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Variable_name", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Value", Type: sql.LongText, Nullable: true},
	}
}

// Children implements sql.Node interface. The function always returns nil.
func (*ShowVariables) Children() []sql.Node { return nil }

// RowIter implements the sql.Node interface.
// The function returns an iterator for filtered variables (based on like pattern)
func (sv *ShowVariables) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var (
		rows []sql.Row
		like sql.Expression
	)
	if sv.pattern != "" {
		like = expression.NewLike(
			expression.NewGetField(0, sql.LongText, "", false),
			expression.NewGetField(1, sql.LongText, sv.pattern, false),
		)
	}

	for k, v := range ctx.GetAllSessionVariables() {
		if like != nil {
			b, err := like.Eval(ctx, sql.NewRow(k, sv.pattern))
			if err != nil {
				return nil, err
			}
			if !b.(bool) {
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

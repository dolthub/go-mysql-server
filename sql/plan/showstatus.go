// Copyright 2021 Dolthub, Inc.
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
)

type ShowStatus struct {
	pattern string
}

// NewShowStatus returns a new ShowStatus reference.
// like is a "like pattern". If like is an empty string it will return all variables.
func NewShowStatus(like string) *ShowStatus {
	return &ShowStatus{
		pattern: like,
	}
}

// Resolved implements sql.Node interface. The function always returns true.
func (ss *ShowStatus) Resolved() bool {
	return true
}

// WithChildren implements the Node interface.
func (ss *ShowStatus) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(ss, len(children), 0)
	}

	return ss, nil
}

func (ss *ShowStatus) String() string {
	var like string
	if ss.pattern != "" {
		like = fmt.Sprintf(" LIKE '%s'", ss.pattern)
	}
	return fmt.Sprintf("SHOW CHARSET%s", like)
}

func (ss *ShowStatus) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Variable_name", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "value", Type: sql.LongText, Nullable: false},
	}
}

func (ss *ShowStatus) Children() []sql.Node { return nil }

func (ss *ShowStatus) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var (
		rows []sql.Row
		//like sql.Expression
	)
	//if ss.pattern != "" {
	//	like = expression.NewLike(
	//		expression.NewGetField(0, sql.LongText, "", false),
	//		expression.NewGetField(1, sql.LongText, ss.pattern, false),
	//	)
	//}

	rows = append(rows, sql.Row{nil, nil})

	return sql.RowsToRowIter(rows...), nil
}

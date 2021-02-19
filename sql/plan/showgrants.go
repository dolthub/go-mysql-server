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
	"github.com/dolthub/go-mysql-server/sql"
)

// ShowGrants shows the columns details of a table.
type ShowGrants struct{}

var (
	showGrantsSchema = sql.Schema{
		{Name: "Grants for root@", Type: sql.LongText},
	}
)

// NewShowGrants creates a new ShowGrants node.
func NewShowGrants() *ShowGrants {
	return &ShowGrants{}
}

// Schema implements the sql.Node interface.
func (s *ShowGrants) Schema() sql.Schema {
	return sql.Schema{{
		Name: "Grants for root@%",
		Type: sql.LongText,
	}}
}

// RowIter creates a new ShowGrants node.
func (s *ShowGrants) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	span, _ := ctx.Span("plan.ShowGrants")

	rows := []sql.Row{
		sql.Row{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"},
	}

	return sql.NewSpanIter(span, sql.RowsToRowIter(rows...)), nil
}

// WithChildren implements the Node interface.
func (s *ShowGrants) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NewShowGrants(), nil
}

func (s *ShowGrants) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("ShowGrants")
	return p.String()
}

func (s *ShowGrants) Children() []sql.Node {
	return nil
}

func (s *ShowGrants) Resolved() bool {
	return true
}

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
	"sort"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// ShowTables is a node that shows the database tables.
type ShowTables struct {
	db   sql.Database
	Full bool
	AsOf sql.Expression
}

var showTablesSchema = sql.Schema{
	{Name: "Table", Type: sql.LongText},
}

var showTablesFullSchema = sql.Schema{
	{Name: "Table", Type: sql.LongText},
	{Name: "Table_type", Type: sql.LongText},
}

// NewShowTables creates a new show tables node given a database.
func NewShowTables(database sql.Database, full bool, asOf sql.Expression) *ShowTables {
	return &ShowTables{
		db:   database,
		Full: full,
		AsOf: asOf,
	}
}

var _ sql.Databaser = (*ShowTables)(nil)
var _ sql.Expressioner = (*ShowTables)(nil)

// Database implements the sql.Databaser interface.
func (p *ShowTables) Database() sql.Database {
	return p.db
}

// WithDatabase implements the sql.Databaser interface.
func (p *ShowTables) WithDatabase(db sql.Database) (sql.Node, error) {
	nc := *p
	nc.db = db
	return &nc, nil
}

// Resolved implements the Resolvable interface.
func (p *ShowTables) Resolved() bool {
	_, ok := p.db.(sql.UnresolvedDatabase)
	return !ok && expression.ExpressionsResolved(p.Expressions()...)
}

// Children implements the Node interface.
func (*ShowTables) Children() []sql.Node {
	return nil
}

// Schema implements the Node interface.
func (p *ShowTables) Schema() sql.Schema {
	if p.Full {
		return showTablesFullSchema
	}

	return showTablesSchema
}

// RowIter implements the Node interface.
func (p *ShowTables) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var tableNames []string

	// TODO: this entire analysis should really happen in the analyzer, as opposed to at execution time
	if p.AsOf != nil {
		if vdb, ok := p.db.(sql.VersionedDatabase); ok {
			asOf, err := p.AsOf.Eval(ctx, nil)
			if err != nil {
				return nil, err
			}

			tableNames, err = vdb.GetTableNamesAsOf(ctx, asOf)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, sql.ErrAsOfNotSupported.New(p.db.Name())
		}
	} else {
		var err error
		tableNames, err = p.db.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}
	}

	sort.Strings(tableNames)

	var rows []sql.Row
	for _, tableName := range tableNames {
		row := sql.Row{tableName}
		if p.Full {
			row = append(row, "BASE TABLE")
		}
		rows = append(rows, row)
	}

	// TODO: currently there is no way to see views AS OF a particular time
	if vdb, ok := p.db.(sql.ViewDatabase); ok {
		views, err := vdb.AllViews(ctx)
		if err != nil {
			return nil, err
		}
		for _, view := range views {
			row := sql.Row{view.Name}
			if p.Full {
				row = append(row, "VIEW")
			}
			rows = append(rows, row)
		}
	}

	for _, view := range ctx.GetViewRegistry().ViewsInDatabase(p.db.Name()) {
		row := sql.Row{view.Name()}
		if p.Full {
			row = append(row, "VIEW")
		}
		rows = append(rows, row)
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	return sql.RowsToRowIter(rows...), nil
}

// WithChildren implements the Node interface.
func (p *ShowTables) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}

	return p, nil
}

func (p ShowTables) String() string {
	return "ShowTables"
}

// Expressions implements sql.Expressioner
func (p *ShowTables) Expressions() []sql.Expression {
	if p.AsOf == nil {
		return nil
	}
	return []sql.Expression{p.AsOf}
}

// WithExpressions implements sql.Expressioner
func (p *ShowTables) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(exprs), 1)
	}

	np := *p
	np.AsOf = exprs[0]
	return &np, nil
}

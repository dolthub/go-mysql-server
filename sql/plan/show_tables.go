package plan

import (
	"io"
	"sort"

	"github.com/src-d/go-mysql-server/sql"
)

type ShowTables struct {
	database sql.Database
}

func NewShowTables(database sql.Database) *ShowTables {
	return &ShowTables{
		database: database,
	}
}

func (p *ShowTables) Resolved() bool {
	_, ok := p.database.(*sql.UnresolvedDatabase)
	return !ok
}

func (*ShowTables) Children() []sql.Node {
	return nil
}

func (*ShowTables) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "table",
		Type:     sql.Text,
		Nullable: false,
	}}
}

func (p *ShowTables) RowIter() (sql.RowIter, error) {
	tableNames := []string{}
	for key := range p.database.Tables() {
		tableNames = append(tableNames, key)
	}

	sort.Strings(tableNames)

	return &showTablesIter{tableNames: tableNames}, nil
}

func (p *ShowTables) TransformUp(f func(sql.Node) sql.Node) sql.Node {
	return f(NewShowTables(p.database))
}

func (p *ShowTables) TransformExpressionsUp(f func(sql.Expression) sql.Expression) sql.Node {
	return p
}

type showTablesIter struct {
	tableNames []string
	idx        int
}

func (i *showTablesIter) Next() (sql.Row, error) {
	if i.idx >= len(i.tableNames) {
		return nil, io.EOF
	}
	row := sql.NewRow(i.tableNames[i.idx])
	i.idx++

	return row, nil
}

func (i *showTablesIter) Close() error {
	i.tableNames = nil
	return nil
}

package plan

import (
	"sort"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ShowDatabases is a node that shows the databases.
type ShowDatabases struct {
	Catalog *sql.Catalog
}

// NewShowDatabases creates a new show databases node.
func NewShowDatabases() *ShowDatabases {
	return new(ShowDatabases)
}

// Resolved implements the Resolvable interface.
func (p *ShowDatabases) Resolved() bool {
	return true
}

// Children implements the Node interface.
func (*ShowDatabases) Children() []sql.Node {
	return nil
}

// Schema implements the Node interface.
func (*ShowDatabases) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "database",
		Type:     sql.Text,
		Nullable: false,
	}}
}

// RowIter implements the Node interface.
func (p *ShowDatabases) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	dbs := p.Catalog.AllDatabases()
	var rows = make([]sql.Row, len(dbs))
	for i, db := range dbs {
		rows[i] = sql.Row{db.Name()}
	}

	sort.Slice(rows, func(i, j int) bool {
		return strings.Compare(rows[i][0].(string), rows[j][0].(string)) < 0
	})

	return sql.RowsToRowIter(rows...), nil
}

// TransformUp implements the Transformable interface.
func (p *ShowDatabases) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	np := *p
	return f(&np)
}

// TransformExpressionsUp implements the Transformable interface.
func (p *ShowDatabases) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return p, nil
}
func (p ShowDatabases) String() string {
	return "ShowDatabases"
}

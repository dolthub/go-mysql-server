package plan

import (
	"sort"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
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
		Name:     "Database",
		Type:     sql.LongText,
		Nullable: false,
	}}
}

// RowIter implements the Node interface.
func (p *ShowDatabases) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	dbs := p.Catalog.AllDatabases()
	var rows = make([]sql.Row, 0, len(dbs))
	for _, db := range dbs {
		if sql.InformationSchemaDatabaseName != db.Name() {
			rows = append(rows, sql.Row{db.Name()})
		}
	}

	sort.Slice(rows, func(i, j int) bool {
		return strings.Compare(rows[i][0].(string), rows[j][0].(string)) < 0
	})

	return sql.RowsToRowIter(rows...), nil
}

// WithChildren implements the Node interface.
func (p *ShowDatabases) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 0)
	}

	return p, nil
}

func (p ShowDatabases) String() string {
	return "ShowDatabases"
}

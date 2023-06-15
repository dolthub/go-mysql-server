package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TableCountLookup short-circuits `select count(*) from table`
// using the sql.StatisticsTable interface.
type TableCountLookup struct {
	aliasName string
	table     sql.StatisticsTable
	cnt       uint64
}

func NewTableCount(aliasName string, table sql.StatisticsTable, cnt uint64) sql.Node {
	return &TableCountLookup{aliasName: aliasName, table: table, cnt: cnt}
}

var _ sql.Node = (*TableCountLookup)(nil)

func (t TableCountLookup) Count() uint64 {
	return t.cnt
}

func (t TableCountLookup) Resolved() bool {
	return true
}

func (t TableCountLookup) String() string {
	return fmt.Sprintf("table_count(%s) as %s", t.table.Name(), t.aliasName)
}

func (t TableCountLookup) Schema() sql.Schema {
	return sql.Schema{{
		Name:     t.aliasName,
		Type:     types.Int64,
		Nullable: false,
		Source:   t.table.Name(),
	}}
}

func (t TableCountLookup) Children() []sql.Node {
	return nil
}

func (t TableCountLookup) WithChildren(_ ...sql.Node) (sql.Node, error) {
	return t, nil
}

func (t TableCountLookup) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	return true
}

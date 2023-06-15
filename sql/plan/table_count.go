package plan

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TableCountLookup short-circuits `select count(*) from table`
// using the sql.StatisticsTable interface.
type TableCountLookup struct {
	table sql.StatisticsTable
	cnt   uint64
}

func NewTableCount(table sql.StatisticsTable, cnt uint64) sql.Node {
	return &TableCountLookup{table: table, cnt: cnt}
}

var _ sql.Node = (*TableCountLookup)(nil)

func (t TableCountLookup) Count() uint64 {
	return t.cnt
}

func (t TableCountLookup) Resolved() bool {
	return true
}

func (t TableCountLookup) String() string {
	return fmt.Sprintf("table_count(%s)", t.table.Name())
}

func (t TableCountLookup) Schema() sql.Schema {
	return sql.Schema{{
		Name:     "count(1)",
		Type:     types.Int64,
		Nullable: false,
		Source:   t.table.Name(),
	}}
}

func (t TableCountLookup) Children() []sql.Node {
	return nil
}

func (t TableCountLookup) WithChildren(children ...sql.Node) (sql.Node, error) {
	return t, nil
}

func (t TableCountLookup) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

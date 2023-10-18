package plan

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/stats"

	"github.com/dolthub/go-mysql-server/sql"
)

func NewUpdateHistogram(db, table string, cols []string, stats *stats.Statistic) *UpdateHistogram {
	return &UpdateHistogram{db: db, cols: cols, table: table, stats: stats}
}

type UpdateHistogram struct {
	db    string
	table string
	cols  []string
	stats *stats.Statistic
	prov  sql.StatsProvider
}

var _ sql.Node = (*UpdateHistogram)(nil)

func (u *UpdateHistogram) Db() string {
	return u.db
}

func (u *UpdateHistogram) Table() string {
	return u.table
}

func (u *UpdateHistogram) Cols() []string {
	return u.cols
}

func (u *UpdateHistogram) Stats() *stats.Statistic {
	return u.stats
}

func (u *UpdateHistogram) WithProvider(prov sql.StatsProvider) *UpdateHistogram {
	ret := *u
	ret.prov = prov
	return &ret
}

func (u *UpdateHistogram) StatsProvider() sql.StatsProvider {
	return u.prov
}

func (u *UpdateHistogram) Resolved() bool {
	return true
}

func (u *UpdateHistogram) String() string {
	statMap := u.stats.ToInterface()
	statBytes, _ := json.Marshal(statMap)
	return fmt.Sprintf("update histogram  %s.(%s) using %s", u.table, strings.Join(u.cols, ","), statBytes)
}

func (u *UpdateHistogram) Schema() sql.Schema {
	return analyzeSchema
}

func (u *UpdateHistogram) Children() []sql.Node {
	return nil
}

func (u *UpdateHistogram) WithChildren(children ...sql.Node) (sql.Node, error) {
	return u, nil
}

func (u *UpdateHistogram) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

func (u *UpdateHistogram) IsReadOnly() bool {
	return false
}

func NewDropHistogram(db, table string, cols []string) *DropHistogram {
	return &DropHistogram{db: db, cols: cols, table: table}
}

type DropHistogram struct {
	db    string
	table string
	cols  []string
	prov  sql.StatsProvider
}

var _ sql.Node = (*DropHistogram)(nil)

func (d *DropHistogram) StatsProvider() sql.StatsProvider {
	return d.prov
}

func (d *DropHistogram) WithProvider(prov sql.StatsProvider) *DropHistogram {
	ret := *d
	ret.prov = prov
	return &ret
}

func (d *DropHistogram) Db() string {
	return d.db
}

func (d *DropHistogram) Table() string {
	return d.table
}

func (d *DropHistogram) Cols() []string {
	return d.cols
}

func (d *DropHistogram) Resolved() bool {
	return true
}

func (d *DropHistogram) String() string {
	return fmt.Sprintf("drop histogram %s.(%s)", d.table, strings.Join(d.cols, ","))
}

func (d *DropHistogram) Schema() sql.Schema {
	return analyzeSchema
}

func (d *DropHistogram) Children() []sql.Node {
	return nil
}

func (d *DropHistogram) WithChildren(_ ...sql.Node) (sql.Node, error) {
	return d, nil
}

func (d *DropHistogram) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

func (d *DropHistogram) IsReadOnly() bool {
	return false
}

package plan

import (
	"fmt"
	"io"
	"strings"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

type AnalyzeTable struct {
	Db     string
	Stats  sql.StatsReadWriter
	Tables []sql.DbTable
}

var analyzeSchema = sql.Schema{
	{Name: "Table", Type: types.LongText},
	{Name: "Op", Type: types.LongText},
	{Name: "Msg_type", Type: types.LongText},
	{Name: "Msg_text", Type: types.LongText},
}

func NewAnalyze(names []sql.DbTable) *AnalyzeTable {
	return &AnalyzeTable{
		Tables: names,
	}
}

// Schema implements the interface sql.Node.
// TODO: should be |Tables|Op|Msg_type|Msg_text|
func (n *AnalyzeTable) Schema() sql.Schema {
	return analyzeSchema
}

func (n *AnalyzeTable) WithCatalog(cat sql.Catalog) *AnalyzeTable {
	ret := *n
	ret.Stats = ret.Stats.AssignCatalog(cat).(sql.StatsReadWriter)
	return &ret
}

func (n *AnalyzeTable) WithTables(tables []sql.DbTable) *AnalyzeTable {
	n.Tables = tables
	return n
}

func (n *AnalyzeTable) WithDb(db string) *AnalyzeTable {
	n.Db = db
	return n
}

func (n *AnalyzeTable) WithStats(stats sql.StatsReadWriter) *AnalyzeTable {
	n.Stats = stats
	return n
}

// String implements the interface sql.Node.
func (n *AnalyzeTable) String() string {
	tblNames := make([]string, len(n.Tables))
	for i, t := range n.Tables {
		tblNames[i] = t.String()
	}
	return fmt.Sprintf("AnalyzeTable table %s", strings.Join(tblNames, ", "))
}

// Resolved implements the Resolvable interface.
func (n *AnalyzeTable) Resolved() bool {
	return n.Stats != nil
}

// Children implements the interface sql.Node.
func (n *AnalyzeTable) Children() []sql.Node {
	return nil
}

// WithChildren implements the interface sql.Node.
func (n *AnalyzeTable) WithChildren(_ ...sql.Node) (sql.Node, error) {
	return n, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *AnalyzeTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// RowIter implements the interface sql.Node.
// TODO: support cross / multi db analyze
func (n *AnalyzeTable) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	return &analyzeTableIter{
		idx:    0,
		tables: n.Tables,
		stats:  n.Stats,
	}, nil
}

type analyzeTableIter struct {
	idx    int
	tables []sql.DbTable
	stats  sql.StatsReadWriter
}

var _ sql.RowIter = &analyzeTableIter{}

func (itr *analyzeTableIter) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.tables) {
		return nil, io.EOF
	}

	t := itr.tables[itr.idx]

	msgType := "status"
	msgText := "OK"
	err := itr.stats.Analyze(ctx, t.Db, t.Table)
	if err != nil {
		msgType = "Error"
		msgText = err.Error()
	}
	itr.idx++
	return sql.Row{t.Table, "analyze", msgType, msgText}, nil
}

func (itr *analyzeTableIter) Close(ctx *sql.Context) error {
	return nil
}

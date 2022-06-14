package plan

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/mysql_db"

	"github.com/dolthub/go-mysql-server/sql"
)

type Analyze struct {
	db   sql.Database
	tbls []sql.Node
}

func NewAnalyze(db sql.Database, tbls []sql.Node) *Analyze {
	return &Analyze{
		db:   db,
		tbls: tbls,
	}
}

// Schema implements the interface sql.Node.
// TODO: should be |Tables|Op|Msg_type|Msg_text|
func (n *Analyze) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *Analyze) String() string {
	tblNames := make([]string, len(n.tbls))
	for i, tbl := range n.tbls {
		switch resTbl := tbl.(type) {
		case *ResolvedTable:
			tblNames[i] = resTbl.Name()
		case *UnresolvedTable:
			tblNames[i] = resTbl.Name()
		case *Exchange:
			tblNames[i] = resTbl.Child.String()
		}
	}
	return fmt.Sprintf("Analyze table %s", strings.Join(tblNames, ", "))
}

// Database implements the interface sql.Databaser.
func (n *Analyze) Database() sql.Database {
	return n.db
}

// WithDatabase implements the interface sql.Databaser.
func (n *Analyze) WithDatabase(db sql.Database) (sql.Node, error) {
	nn := *n
	nn.db = db
	return &nn, nil
}

// Resolved implements the Resolvable interface.
func (n *Analyze) Resolved() bool {
	_, ok := n.db.(sql.UnresolvedDatabase)
	for _, tbl := range n.tbls {
		if !tbl.Resolved() {
			return false
		}
	}
	return !ok
}

// Children implements the interface sql.Node.
func (n *Analyze) Children() []sql.Node {
	return n.tbls
}

// WithChildren implements the interface sql.Node.
func (n *Analyze) WithChildren(children ...sql.Node) (sql.Node, error) {
	// Deep copy children
	newChildren := make([]sql.Node, len(children))
	copy(newChildren, children)

	nn := *n
	nn.tbls = newChildren
	return &nn, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *Analyze) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// RowIter implements the interface sql.Node.
func (n *Analyze) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	mysql, ok := n.db.(*mysql_db.MySQLDb)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}
	colStatsTableData := mysql.ColumnStatisticsTable().Data()

	for _, tbl := range n.tbls {
		var resTbl *ResolvedTable
		switch t := tbl.(type) {
		case *ResolvedTable:
			resTbl = t
		case *Exchange:
			resTbl = t.Child.(*ResolvedTable)
		case DeferredAsOfTable:
			resTbl = t.ResolvedTable
		default:
			return nil, sql.ErrTableNotFound.New(tbl.String())
		}

		var statsTbl sql.StatisticsTable
		if wrappedTbl, ok := resTbl.Table.(sql.TableWrapper); ok {
			statsTbl = wrappedTbl.Underlying().(sql.StatisticsTable)
		} else {
			statsTbl = resTbl.Table.(sql.StatisticsTable)
		}

		statsTbl.CalculateStatistics(ctx)

		// TODO: pushdown filters on indexed access to get better cost estimates
		// TODO: still need to get this information from table and put it in Column TableStatistics Table

		// Go through each column of table we want to analyze
		for _, col := range statsTbl.Schema() {
			// Create Primary Key for lookup
			colStatsPk := mysql_db.ColumnStatisticsPrimaryKey{
				SchemaName: database,
				TableName:  statsTbl.Name(),
				ColumnName: col.Name,
			}

			// Remove if existing
			existingRows := colStatsTableData.Get(colStatsPk)
			for _, row := range existingRows {
				colStatsTableData.Remove(ctx, colStatsPk, row)
			}

			stats, err := statsTbl.GetStatistics(ctx)
			if err != nil {
				return nil, err
			}

			colStats, err := stats.GetColumnStatistic(col.Name)
			if err != nil {
				return nil, err
			}

			// TODO: sort buckets?
			hist := colStats.GetHistogram()
			keys := make([]float64, 0)
			for k, _ := range hist {
				keys = append(keys, k)
			}
			sort.Float64s(keys)
			histString := ""
			for _, k := range keys {
				histString += fmt.Sprintf("[v: %g, f: %g] ", hist[k].GetValue(), hist[k].GetFrequency())
			}

			// Insert row entry
			colStatsTableData.Put(ctx, &mysql_db.ColumnStatistics{
				SchemaName: database,
				TableName:  statsTbl.Name(),
				ColumnName: col.Name,
				Count:      colStats.GetCount(),
				NullCount:  colStats.GetNullCount(),
				Mean:       colStats.GetMean(),
				Min:        colStats.GetMin(),
				Max:        colStats.GetMax(),
				Histogram:  histString,
			})
		}
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

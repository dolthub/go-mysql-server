package plan

import (
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
	"io"
)

// TODO: do i need additional database argument?
type Analyze struct {
	db  sql.Database
	tbl sql.Node
}

func NewAnalyze(db sql.Database, tbl sql.Node) *Analyze {
	return &Analyze{
		db:  db,
		tbl: tbl,
	}
}

// Schema implements the interface sql.Node.
// TODO: should be |Tables|Op|Msg_type|Msg_text|
func (n *Analyze) Schema() sql.Schema {
	return sql.OkResultSchema
}

// String implements the interface sql.Node.
func (n *Analyze) String() string {
	return fmt.Sprintf("Analyze table %s.%s", n.db.Name(), n.tbl.String())
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
	return !ok && n.tbl.Resolved()
}

// Children implements the interface sql.Node.
func (n *Analyze) Children() []sql.Node {
	return []sql.Node{n.tbl}
}

// WithChildren implements the interface sql.Node.
func (n *Analyze) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(n, len(children), 1)
	}
	nn := *n
	nn.tbl = children[0]
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

	// Access mysql db, which is called GrantTables for now
	mysql, ok := n.db.(*grant_tables.GrantTables)
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New("mysql")
	}

	// Get column statistics table
	colStatsTableData := mysql.ColumnStatisticsTable().Data()

	// Check if table was resolved
	var tbl *ResolvedTable
	switch v := n.tbl.(type) {
	case *ResolvedTable:
		tbl = v
	case *Exchange:
		tbl = v.Child.(*ResolvedTable)
	default:
		return nil, sql.ErrTableNotFound.New(n.tbl.String())
	}

	// Calculate stats
	tblIter, err := tbl.RowIter(ctx, row)
	if err != nil {
		return nil, sql.ErrTableNotFound.New("couldn't read from table")
	}
	defer func() {
		tblIter.Close(ctx)
	}()

	// TODO: helper method probably
	count := 0
	means := make([]float64, len(tbl.Schema()))
	mins := make([]float64, len(tbl.Schema()))
	maxs := make([]float64, len(tbl.Schema()))
	for i := 0; i < len(tbl.Schema()); i++ {
		mins[i] = math.MaxFloat64
		maxs[i] = math.SmallestNonzeroFloat64 // not sure if this is right
	}

	for {
		// Get row
		row, err := tblIter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// accumulate sum of every column
		// TODO: watch out for types
		// TODO: watch out for precision/overflow issues
		for i := 0; i < len(tbl.Schema()); i++ {
			num, err := sql.Float64.Convert(row[i])
			if err != nil {
				return nil, err
			}
			numFloat := num.(float64)
			means[i] += numFloat
			mins[i] = math.Min(numFloat, mins[i])
			maxs[i] = math.Max(numFloat, maxs[i])
		}

		// TODO: means, median, not null
		count++
	}

	// Go through each column of table we want to analyze
	for i, col := range tbl.Schema() {
		// Create Primary Key for lookup
		colStatsPk := grant_tables.ColStatsPrimaryKey{
			SchemaName: database,
			TableName:  tbl.String(),
			ColumnName: col.Name,
		}

		// Remove if existing
		existingRows := colStatsTableData.Get(colStatsPk)
		for _, row := range existingRows {
			colStatsTableData.Remove(ctx, colStatsPk, row)
		}

		// Insert new
		colStatsTableData.Put(ctx, &grant_tables.ColStats{
			SchemaName: database,
			TableName:  tbl.String(),
			ColumnName: col.Name,
			Count:      uint64(count),
			Mean:       means[i] / float64(count),
			Min:        mins[i],
			Max:        maxs[i],
		})
	}

	if err := mysql.Persist(ctx); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

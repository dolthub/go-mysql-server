package plan

import (
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
)

type AnalyzeTable struct {
	tbls []sql.Node
}

var analyzeSchema = sql.Schema{
	{Name: "Table", Type: sql.LongText},
	{Name: "Op", Type: sql.LongText},
	{Name: "Msg_type", Type: sql.LongText},
	{Name: "Msg_text", Type: sql.LongText},
}

func NewAnalyze(tbls []sql.Node) *AnalyzeTable {
	return &AnalyzeTable{
		tbls: tbls,
	}
}

// Schema implements the interface sql.Node.
// TODO: should be |Tables|Op|Msg_type|Msg_text|
func (n *AnalyzeTable) Schema() sql.Schema {
	return analyzeSchema
}

// String implements the interface sql.Node.
func (n *AnalyzeTable) String() string {
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
	return fmt.Sprintf("AnalyzeTable table %s", strings.Join(tblNames, ", "))
}

// Resolved implements the Resolvable interface.
func (n *AnalyzeTable) Resolved() bool {
	for _, tbl := range n.tbls {
		if !tbl.Resolved() {
			return false
		}
	}
	return true
}

// Children implements the interface sql.Node.
func (n *AnalyzeTable) Children() []sql.Node {
	return n.tbls
}

// WithChildren implements the interface sql.Node.
func (n *AnalyzeTable) WithChildren(children ...sql.Node) (sql.Node, error) {
	// Deep copy children
	newChildren := make([]sql.Node, len(children))
	copy(newChildren, children)

	nn := *n
	nn.tbls = newChildren
	return &nn, nil
}

// CheckPrivileges implements the interface sql.Node.
func (n *AnalyzeTable) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

type analyzeTableIter struct {
	idx  int
	tbls []sql.Node
}

var _ sql.RowIter = &analyzeTableIter{}

func (itr *analyzeTableIter) Next(ctx *sql.Context) (sql.Row, error) {
	if itr.idx >= len(itr.tbls) {
		return nil, io.EOF
	}

	// find resolved table
	var resTbl *ResolvedTable
	transform.Inspect(itr.tbls[itr.idx], func(n sql.Node) bool {
		if t, ok := n.(*ResolvedTable); ok {
			resTbl = t
			return false
		}
		return true
	})

	var statsTbl sql.StatisticsTable
	if wrappedTbl, ok := resTbl.Table.(sql.TableWrapper); ok {
		statsTbl = wrappedTbl.Underlying().(sql.StatisticsTable)
	} else {
		statsTbl = resTbl.Table.(sql.StatisticsTable)
	}

	msgType := "status"
	msgText := "OK"
	if err := statsTbl.AnalyzeTable(ctx); err != nil {
		msgType = "Error"
		msgText = err.Error()
	}
	itr.idx++
	return sql.Row{statsTbl.Name(), "analyze", msgType, msgText}, nil
}

func (itr *analyzeTableIter) Close(ctx *sql.Context) error {
	return nil
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
		idx:  0,
		tbls: n.tbls,
	}, nil
}

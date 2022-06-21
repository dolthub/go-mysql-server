package plan

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/transform"

	"github.com/dolthub/go-mysql-server/sql"
)

type Analyze struct {
	tbls []sql.Node
}

func NewAnalyze(tbls []sql.Node) *Analyze {
	return &Analyze{
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

// Resolved implements the Resolvable interface.
func (n *Analyze) Resolved() bool {
	for _, tbl := range n.tbls {
		if !tbl.Resolved() {
			return false
		}
	}
	return true
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
// TODO: support cross / multi db analyze
func (n *Analyze) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// Assume table is in current database
	database := ctx.GetCurrentDatabase()
	if database == "" {
		return nil, sql.ErrNoDatabaseSelected.New()
	}

	for _, tbl := range n.tbls {
		// find resolved table
		var resTbl *ResolvedTable
		transform.Inspect(tbl, func(n sql.Node) bool {
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

		if err := statsTbl.CalculateStatistics(ctx); err != nil {
			return nil, err
		}
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

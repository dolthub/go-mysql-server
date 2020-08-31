package plan

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

var ErrUpdateNotSupported = errors.NewKind("table doesn't support UPDATE")
var ErrUpdateUnexpectedSetResult = errors.NewKind("attempted to set field but expression returned %T")

// Update is a node for updating rows on tables.
type Update struct {
	sql.Node
	UpdateExprs []sql.Expression
}

// NewUpdate creates an Update node.
func NewUpdate(n sql.Node, updateExprs []sql.Expression) *Update {
	return &Update{n, updateExprs}
}

// Expressions implements the Expressioner interface.
func (p *Update) Expressions() []sql.Expression {
	return p.UpdateExprs
}

// Schema implements the Node interface.
func (p *Update) Schema() sql.Schema {
	return sql.OkResultSchema
}

// Resolved implements the Resolvable interface.
func (p *Update) Resolved() bool {
	if !p.Node.Resolved() {
		return false
	}
	for _, updateExpr := range p.UpdateExprs {
		if !updateExpr.Resolved() {
			return false
		}
	}
	return true
}

func (p *Update) Children() []sql.Node {
	return []sql.Node{p.Node}
}

func getUpdatable(node sql.Node) (sql.UpdatableTable, error) {
	switch node := node.(type) {
	case sql.UpdatableTable:
		return node, nil
	case *ResolvedTable:
		return getUpdatableTable(node.Table)
	}
	for _, child := range node.Children() {
		updater, _ := getUpdatable(child)
		if updater != nil {
			return updater, nil
		}
	}
	return nil, ErrUpdateNotSupported.New()
}

func getUpdatableTable(t sql.Table) (sql.UpdatableTable, error) {
	switch t := t.(type) {
	case sql.UpdatableTable:
		return t, nil
	case sql.TableWrapper:
		return getUpdatableTable(t.Underlying())
	default:
		return nil, ErrUpdateNotSupported.New()
	}
}

// Execute inserts the rows in the database.
func (p *Update) Execute(ctx *sql.Context) (int, int, error) {
	updatable, err := getUpdatable(p.Node)
	if err != nil {
		return 0, 0, err
	}
	schema := p.Node.Schema()

	iter, err := p.Node.RowIter(ctx, nil)
	if err != nil {
		return 0, 0, err
	}

	updater := updatable.Updater(ctx)

	rowsMatched := 0
	rowsUpdated := 0
	for {
		oldRow, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = iter.Close()
			return rowsMatched, rowsUpdated, err
		}
		rowsMatched++

		newRow, err := p.applyUpdates(ctx, oldRow)
		if err != nil {
			_ = iter.Close()
			return rowsMatched, rowsUpdated, err
		}
		if equals, err := oldRow.Equals(newRow, schema); err == nil {
			if !equals {
				err = updater.Update(ctx, oldRow, newRow)
				if err != nil {
					_ = iter.Close()
					return rowsMatched, rowsUpdated, err
				}
				rowsUpdated++
			}
		} else {
			_ = iter.Close()
			return rowsMatched, rowsUpdated, err
		}
	}

	if err := updater.Close(ctx); err != nil {
		return 0, 0, err
	}

	return rowsMatched, rowsUpdated, nil
}

// UpdateInfo is the Info for OKResults returned by Update nodes.
type UpdateInfo struct {
	Matched, Updated, Warnings int
}

// String implements fmt.Stringer
func (ui UpdateInfo) String() string {
	return fmt.Sprintf("Rows matched: %d  Changed: %d  Warnings: %d", ui.Matched, ui.Updated, ui.Warnings)
}

// RowIter implements the Node interface.
func (p *Update) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	matched, updated, err := p.Execute(ctx)
	if err != nil {
		return nil, err
	}

	info := UpdateInfo{matched, updated, 0}
	return sql.RowsToRowIter(sql.NewRow(sql.OkResult{
		RowsAffected: uint64(updated),
		InsertID:     0,
		Info:         info,
	})), nil
}

// WithChildren implements the Node interface.
func (p *Update) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewUpdate(children[0], p.UpdateExprs), nil
}

// WithExpressions implements the Expressioner interface.
func (p *Update) WithExpressions(newExprs ...sql.Expression) (sql.Node, error) {
	if len(newExprs) != len(p.UpdateExprs) {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(p.UpdateExprs), 1)
	}
	return NewUpdate(p.Node, newExprs), nil
}

func (p Update) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update")
	_ = pr.WriteChildren(p.Node.String())
	var children []string
	for _, updateExpr := range p.UpdateExprs {
		children = append(children, updateExpr.String())
	}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (p Update) DebugString() string {
	pr := sql.NewTreePrinter()
	var updateExprs []string
	for _, e := range p.UpdateExprs {
		updateExprs = append(updateExprs, sql.DebugString(e))
	}
	_ = pr.WriteNode(fmt.Sprintf("Update(%s)", strings.Join(updateExprs, ",")))
	_ = pr.WriteChildren(sql.DebugString(p.Node))
	var children []string
	for _, updateExpr := range p.UpdateExprs {
		children = append(children, sql.DebugString(updateExpr))
	}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (p *Update) applyUpdates(ctx *sql.Context, row sql.Row) (sql.Row, error) {
	var ok bool
	prev := row
	for _, updateExpr := range p.UpdateExprs {
		val, err := updateExpr.Eval(ctx, prev)
		if err != nil {
			return nil, err
		}
		prev, ok = val.(sql.Row)
		if !ok {
			return nil, ErrUpdateUnexpectedSetResult.New(val)
		}
	}
	return prev, nil
}

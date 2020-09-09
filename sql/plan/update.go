package plan

import (
	"fmt"
	"strings"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/go-mysql-server/sql"
)

var ErrUpdateNotSupported = errors.NewKind("table doesn't support UPDATE")
var ErrUpdateUnexpectedSetResult = errors.NewKind("attempted to set field but expression returned %T")

// Update is a node for updating rows on tables.
type Update struct {
	UnaryNode
	UpdateExprs []sql.Expression
}

// NewUpdate creates an Update node.
func NewUpdate(n sql.Node, updateExprs []sql.Expression) *Update {
	return &Update{UnaryNode{n}, updateExprs}
}

// Expressions implements the Expressioner interface.
func (p *Update) Expressions() []sql.Expression {
	return p.UpdateExprs
}

// Schema implements sql.Node. The schema of an update is a concatenation of the old and new rows.
func (p *Update) Schema() sql.Schema {
	return append(p.Child.Schema(), p.Child.Schema()...)
}

// Resolved implements the Resolvable interface.
func (p *Update) Resolved() bool {
	if !p.Child.Resolved() {
		return false
	}
	for _, updateExpr := range p.UpdateExprs {
		if !updateExpr.Resolved() {
			return false
		}
	}
	return true
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

// UpdateInfo is the Info for OKResults returned by Update nodes.
type UpdateInfo struct {
	Matched, Updated, Warnings int
}

// String implements fmt.Stringer
func (ui UpdateInfo) String() string {
	return fmt.Sprintf("Rows matched: %d  Changed: %d  Warnings: %d", ui.Matched, ui.Updated, ui.Warnings)
}

type updateIter struct {
	childIter   sql.RowIter
	updateExprs []sql.Expression
	schema      sql.Schema
	updater     sql.RowUpdater
	ctx         *sql.Context
}

func (u *updateIter) Next() (sql.Row, error) {
	oldRow, err := u.childIter.Next()
	if err != nil {
		return nil, err
	}

	newRow, err := u.applyUpdates(oldRow)

	// Reduce the row to the length of the schema. The length can differ when some update values come from an outer
	// scope, which will be the first N values in the row.
	// TODO: handle this in the analyzer instead?
	if len(u.schema) < len(newRow) {
		newRow = newRow[len(newRow)-len(u.schema):]
		oldRow = oldRow[len(oldRow)-len(u.schema):]
	}

	if equals, err := oldRow.Equals(newRow, u.schema); err == nil {
		if !equals {
			err = u.updater.Update(u.ctx, oldRow, newRow)
			if err != nil {
				return nil, err
			}
		}
	} else {
		return nil, err
	}

	return oldRow.Append(newRow), nil
}

func (u *updateIter) applyUpdates(row sql.Row) (sql.Row, error) {
	var ok bool
	prev := row
	for _, updateExpr := range u.updateExprs {
		val, err := updateExpr.Eval(u.ctx, prev)
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

func (u *updateIter) Close() error {
	if err := u.updater.Close(u.ctx); err != nil {
		return err
	}
	return u.childIter.Close()
}

func newUpdateIter(childIter sql.RowIter, updateExprs []sql.Expression, schema sql.Schema, updater sql.RowUpdater, ctx *sql.Context) *updateIter {
	return &updateIter{
		childIter:   childIter,
		updateExprs: updateExprs,
		updater:     updater,
		schema:      schema,
		ctx:         ctx,
	}
}

// RowIter implements the Node interface.
func (p *Update) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	updatable, err := getUpdatable(p.Child)
	if err != nil {
		return nil, err
	}
	schema := p.Child.Schema()

	iter, err := p.Child.RowIter(ctx, nil)
	if err != nil {
		return nil, err
	}

	updater := updatable.Updater(ctx)

	return newUpdateIter(iter, p.UpdateExprs, schema, updater, ctx), nil
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
	return NewUpdate(p.Child, newExprs), nil
}

func (p Update) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update")
	_ = pr.WriteChildren(p.Child.String())
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
	_ = pr.WriteChildren(sql.DebugString(p.Child))
	var children []string
	for _, updateExpr := range p.UpdateExprs {
		children = append(children, sql.DebugString(updateExpr))
	}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

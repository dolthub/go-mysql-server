package plan

import (
	"fmt"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// TableLock is a read or write lock on a table.
type TableLock struct {
	Table sql.Node
	// Write if it's true, read if it's false.
	Write bool
}

// LockTables will lock tables for the session in which it's executed.
type LockTables struct {
	Catalog *sql.Catalog
	Locks   []*TableLock
}

// NewLockTables creates a new LockTables node.
func NewLockTables(locks []*TableLock) *LockTables {
	return &LockTables{Locks: locks}
}

var _ sql.Node = (*LockTables)(nil)

// Children implements the sql.Node interface.
func (t *LockTables) Children() []sql.Node {
	var children = make([]sql.Node, len(t.Locks))
	for i, l := range t.Locks {
		children[i] = l.Table
	}
	return children
}

// Resolved implements the sql.Node interface.
func (t *LockTables) Resolved() bool {
	for _, l := range t.Locks {
		if !l.Table.Resolved() {
			return false
		}
	}
	return true
}

// Schema implements the sql.Node interface.
func (t *LockTables) Schema() sql.Schema { return nil }

// RowIter implements the sql.Node interface.
func (t *LockTables) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.LockTables")
	defer span.Finish()

	id := ctx.ID()
	for _, l := range t.Locks {
		lockable, err := getLockable(l.Table)
		if err != nil {
			// If a table is not lockable, just skip it
			ctx.Warn(0, err.Error())
			continue
		}

		if err := lockable.Lock(ctx, l.Write); err != nil {
			ctx.Error(0, "unable to lock table: %s", err)
		} else {
			t.Catalog.LockTable(id, lockable.Name())
		}
	}

	return sql.RowsToRowIter(), nil
}

func (t *LockTables) String() string {
	var children = make([]string, len(t.Locks))
	for i, l := range t.Locks {
		if l.Write {
			children[i] = fmt.Sprintf("[WRITE] %s", l.Table.String())
		} else {
			children[i] = fmt.Sprintf("[READ] %s", l.Table.String())
		}
	}

	p := sql.NewTreePrinter()
	_ = p.WriteNode("LockTables")
	_ = p.WriteChildren(children...)
	return p.String()
}

// TransformUp implements the sql.Node interface.
func (t *LockTables) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	var children = make([]*TableLock, len(t.Locks))
	for i, l := range t.Locks {
		node, err := l.Table.TransformUp(f)
		if err != nil {
			return nil, err
		}
		children[i] = &TableLock{node, l.Write}
	}

	nt := *t
	nt.Locks = children
	return f(&nt)
}

// TransformExpressionsUp implements the sql.Node interface.
func (t *LockTables) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

// ErrTableNotLockable is returned whenever a lockable table can't be found.
var ErrTableNotLockable = errors.NewKind("table %s is not lockable")

func getLockable(node sql.Node) (sql.Lockable, error) {
	switch node := node.(type) {
	case *ResolvedTable:
		return getLockableTable(node.Table)
	default:
		return nil, ErrTableNotLockable.New("unknown")
	}
}

func getLockableTable(table sql.Table) (sql.Lockable, error) {
	switch t := table.(type) {
	case sql.Lockable:
		return t, nil
	case sql.TableWrapper:
		return getLockableTable(t.Underlying())
	default:
		return nil, ErrTableNotLockable.New(t.Name())
	}
}

// UnlockTables will release all locks for the current session.
type UnlockTables struct {
	Catalog *sql.Catalog
}

// NewUnlockTables returns a new UnlockTables node.
func NewUnlockTables() *UnlockTables {
	return new(UnlockTables)
}

var _ sql.Node = (*UnlockTables)(nil)

// Children implements the sql.Node interface.
func (t *UnlockTables) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (t *UnlockTables) Resolved() bool { return true }

// Schema implements the sql.Node interface.
func (t *UnlockTables) Schema() sql.Schema { return nil }

// RowIter implements the sql.Node interface.
func (t *UnlockTables) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	span, ctx := ctx.Span("plan.UnlockTables")
	defer span.Finish()

	if err := t.Catalog.UnlockTables(ctx, ctx.ID()); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (t *UnlockTables) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("UnlockTables")
	return p.String()
}

// TransformUp implements the sql.Node interface.
func (t *UnlockTables) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(t)
}

// TransformExpressionsUp implements the sql.Node interface.
func (t *UnlockTables) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return t, nil
}

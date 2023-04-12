package rowexec

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"gopkg.in/src-d/go-errors.v1"
	"io"
)

type rowFunc func(ctx *sql.Context) (sql.Row, error)

type lazyRowIter struct {
	next rowFunc
}

func (i *lazyRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.next != nil {
		res, err := i.next(ctx)
		i.next = nil
		return res, err
	}
	return nil, io.EOF
}

func (i *lazyRowIter) Close(ctx *sql.Context) error {
	return nil
}

// ErrTableNotLockable is returned whenever a lockable table can't be found.
var ErrTableNotLockable = errors.NewKind("table %s is not lockable")

func getLockable(node sql.Node) (sql.Lockable, error) {
	switch node := node.(type) {
	case *plan.ResolvedTable:
		return getLockableTable(node.Table)
	case sql.TableWrapper:
		return getLockableTable(node.Underlying())
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

// transactionCommittingIter is a simple RowIter wrapper to allow the engine to conditionally commit a transaction
// during the Close() operation
type transactionCommittingIter struct {
	childIter           sql.RowIter
	childIter2          sql.RowIter2
	transactionDatabase string
}

func (t transactionCommittingIter) Next(ctx *sql.Context) (sql.Row, error) {
	return t.childIter.Next(ctx)
}

func (t transactionCommittingIter) Close(ctx *sql.Context) error {
	var err error
	if t.childIter != nil {
		err = t.childIter.Close(ctx)
	}
	if err != nil {
		return err
	}

	tx := ctx.GetTransaction()
	// TODO: In the future we should ensure that analyzer supports implicit commits instead of directly
	// accessing autocommit here.
	// cc. https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html
	autocommit, err := plan.IsSessionAutocommit(ctx)
	if err != nil {
		return err
	}

	commitTransaction := ((tx != nil) && !ctx.GetIgnoreAutoCommit()) && autocommit
	if commitTransaction {
		ts, ok := ctx.Session.(sql.TransactionSession)
		if !ok {
			return nil
		}

		ctx.GetLogger().Tracef("committing transaction %s", tx)
		if err := ts.CommitTransaction(ctx, tx); err != nil {
			return err
		}

		// Clearing out the current transaction will tell us to start a new one the next time this session queries
		ctx.SetTransaction(nil)
	}

	return nil
}

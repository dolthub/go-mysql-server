package rowexec

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"gopkg.in/src-d/go-errors.v1"
	"io"
	"os"
)

const (
	fakeReadCommittedEnvVar = "READ_COMMITTED_HACK"
)

var fakeReadCommitted bool

func init() {
	_, ok := os.LookupEnv(fakeReadCommittedEnvVar)
	if ok {
		fakeReadCommitted = true
	}
}

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

func (t transactionCommittingIter) Next2(ctx *sql.Context, frame *sql.RowFrame) error {
	return t.childIter2.Next2(ctx, frame)
}

func (t transactionCommittingIter) Close(ctx *sql.Context) error {
	var err error
	if t.childIter != nil {
		err = t.childIter.Close(ctx)
	} else if t.childIter2 != nil {
		err = t.childIter2.Close(ctx)
	}
	if err != nil {
		return err
	}

	tx := ctx.GetTransaction()
	// TODO: In the future we should ensure that analyzer supports implicit commits instead of directly
	// accessing autocommit here.
	// cc. https://dev.mysql.com/doc/refman/8.0/en/implicit-commit.html
	autocommit, err := IsSessionAutocommit(ctx)
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

// IsSessionAutocommit returns true if the current session is using implicit transaction management
// through autocommit.
func IsSessionAutocommit(ctx *sql.Context) (bool, error) {
	if ReadCommitted(ctx) {
		return true, nil
	}

	autoCommitSessionVar, err := ctx.GetSessionVariable(ctx, sql.AutoCommitSessionVar)
	if err != nil {
		return false, err
	}
	return types.ConvertToBool(autoCommitSessionVar)
}

func ReadCommitted(ctx *sql.Context) bool {
	if !fakeReadCommitted {
		return false
	}

	val, err := ctx.GetSessionVariable(ctx, "transaction_isolation")
	if err != nil {
		return false
	}

	valStr, ok := val.(string)
	if !ok {
		return false
	}

	return valStr == "READ-COMMITTED"
}

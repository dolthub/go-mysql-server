package plan

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"strings"
)

type DropTrigger struct {
	db          sql.Database
	IfExists    bool
	TriggerName string
}

var _ sql.Databaser = (*DropTrigger)(nil)
var _ sql.Node = (*DropTrigger)(nil)

// NewDropTrigger creates a new NewDropTrigger node for DROP TRIGGER statements.
func NewDropTrigger(db sql.Database, trigger string, ifExists bool) *DropTrigger {
	return &DropTrigger{
		db:          db,
		IfExists:    ifExists,
		TriggerName: strings.ToLower(trigger),
	}
}

// Resolved implements the sql.Node interface.
func (d *DropTrigger) Resolved() bool {
	_, ok := d.db.(sql.UnresolvedDatabase)
	return !ok
}

// String implements the sql.Node interface.
func (d *DropTrigger) String() string {
	ifExists := ""
	if d.IfExists {
		ifExists = "IF EXISTS "
	}
	return fmt.Sprintf("DROP TRIGGER %s%s", ifExists, d.TriggerName)
}

// Schema implements the sql.Node interface.
func (d *DropTrigger) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (d *DropTrigger) Children() []sql.Node {
	return nil
}

// RowIter implements the sql.Node interface.
func (d *DropTrigger) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	triggerDb, ok := d.db.(sql.TriggerDatabase)
	if !ok {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		} else {
			return nil, sql.ErrTriggerDoesNotExist.New(d.TriggerName)
		}
	}
	err := triggerDb.DropTrigger(ctx, d.TriggerName)
	if d.IfExists && sql.ErrTriggerDoesNotExist.Is(err) {
		return sql.RowsToRowIter(), nil
	} else if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the sql.Node interface.
func (d *DropTrigger) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

// Database implements the sql.Databaser interface.
func (d *DropTrigger) Database() sql.Database {
	return d.db
}

// WithDatabase implements the sql.Databaser interface.
func (d *DropTrigger) WithDatabase(db sql.Database) (sql.Node, error) {
	nd := *d
	nd.db = db
	return &nd, nil
}

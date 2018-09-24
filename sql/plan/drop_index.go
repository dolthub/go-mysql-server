package plan

import (
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var (
	// ErrIndexNotFound is returned when the index cannot be found.
	ErrIndexNotFound = errors.NewKind("unable to find index %q on table %q of database %q")
	// ErrTableNotValid is returned when the table is not valid
	ErrTableNotValid = errors.NewKind("table is not valid")
	// ErrTableNotNameable is returned when the table is not nameable.
	ErrTableNotNameable = errors.NewKind("can't get name from table")
	// ErrIndexNotAvailable is returned when trying to delete an index that is
	// still not ready for usage.
	ErrIndexNotAvailable = errors.NewKind("index %q is still not ready for usage and can't be deleted")
)

// DropIndex is a node to drop an index.
type DropIndex struct {
	Name            string
	Table           sql.Node
	Catalog         *sql.Catalog
	CurrentDatabase string
}

// NewDropIndex creates a new DropIndex node.
func NewDropIndex(name string, table sql.Node) *DropIndex {
	return &DropIndex{name, table, nil, ""}
}

// Resolved implements the Node interface.
func (d *DropIndex) Resolved() bool { return d.Table.Resolved() }

// Schema implements the Node interface.
func (d *DropIndex) Schema() sql.Schema { return nil }

// Children implements the Node interface.
func (d *DropIndex) Children() []sql.Node { return []sql.Node{d.Table} }

// RowIter implements the Node interface.
func (d *DropIndex) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	db, err := d.Catalog.Database(d.CurrentDatabase)
	if err != nil {
		return nil, err
	}

	n, ok := d.Table.(sql.Nameable)
	if !ok {
		return nil, ErrTableNotNameable.New()
	}

	table, ok := db.Tables()[n.Name()]
	if !ok {
		return nil, sql.ErrTableNotFound.New(n.Name())
	}

	index := d.Catalog.Index(db.Name(), d.Name)
	if index == nil {
		return nil, ErrIndexNotFound.New(d.Name, n.Name(), db.Name())
	}
	d.Catalog.ReleaseIndex(index)

	if !d.Catalog.CanUseIndex(index) {
		return nil, ErrIndexNotAvailable.New(d.Name)
	}

	done, err := d.Catalog.DeleteIndex(db.Name(), d.Name, true)
	if err != nil {
		return nil, err
	}

	driver := d.Catalog.IndexDriver(index.Driver())
	if driver == nil {
		return nil, ErrInvalidIndexDriver.New(index.Driver())
	}

	<-done

	partitions, err := table.Partitions(ctx)
	if err != nil {
		return nil, err
	}

	if err := driver.Delete(index, partitions); err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(), nil
}

func (d *DropIndex) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DropIndex(%s)", d.Name)
	_ = pr.WriteChildren(d.Table.String())
	return pr.String()
}

// TransformExpressionsUp implements the Node interface.
func (d *DropIndex) TransformExpressionsUp(fn sql.TransformExprFunc) (sql.Node, error) {
	t, err := d.Table.TransformExpressionsUp(fn)
	if err != nil {
		return nil, err
	}

	nc := *d
	nc.Table = t
	return &nc, nil
}

// TransformUp implements the Node interface.
func (d *DropIndex) TransformUp(fn sql.TransformNodeFunc) (sql.Node, error) {
	t, err := d.Table.TransformUp(fn)
	if err != nil {
		return nil, err
	}

	nc := *d
	nc.Table = t
	return fn(&nc)
}

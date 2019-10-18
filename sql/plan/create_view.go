package plan

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
)

type CreateView struct {
	UnaryNode
	Database sql.Database
	Name     string
	Columns  []string
	Catalog *sql.Catalog
}

func NewCreateView(
	database sql.Database,
	name string,
	columns []string,
	definition *SubqueryAlias,
) *CreateView {
	return &CreateView{
		UnaryNode{Child: definition},
		database,
		name,
		columns,
		nil,
	}
}

// Children implements the Node interface.
func (create *CreateView) Children() []sql.Node {
	return []sql.Node{create.Child}
}

// Resolved implements the Node interface.
func (create *CreateView) Resolved() bool {
	// TOOD: Check whether the database has been resolved
	// _, ok := create.Database.(sql.UnresolvedDatabase)
	return create.Child.Resolved()
}

// RowIter implements the Node interface.
func (create *CreateView) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	// TODO: add it to the register

	return sql.RowsToRowIter(), nil
}

// Schema implements the Node interface.
func (create *CreateView) Schema() sql.Schema { return nil }

func (create *CreateView) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("CreateView(%s)", create.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Columns (%s)", strings.Join(create.Columns, ", ")),
		fmt.Sprintf("As (%s)", create.Child.String()),
	)
	return pr.String()
}

// WithChildren implements the Node interface.
func (create *CreateView) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(create, len(children), 1)
	}

	newCreate := create
	newCreate.Child = children[0]
	return newCreate, nil
}

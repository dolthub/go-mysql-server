package plan

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
)

type CreateView struct {
	UnaryNode
	database  sql.Database
	Name      string
	Columns   []string
	Catalog   *sql.Catalog
	IsReplace bool
}

func NewCreateView(
	database sql.Database,
	name string,
	columns []string,
	definition *SubqueryAlias,
	isReplace bool,
) *CreateView {
	return &CreateView{
		UnaryNode{Child: definition},
		database,
		name,
		columns,
		nil,
		isReplace,
	}
}

// View returns the view that will be created by this node
func (create *CreateView) View() sql.View {
	return sql.NewView(create.Name, create.Child)
}

// Children implements the Node interface. It returns the Child of the
// CreateView node; i.e., the definition of the view that will be created.
func (create *CreateView) Children() []sql.Node {
	return []sql.Node{create.Child}
}

// Resolved implements the Node interface.
func (create *CreateView) Resolved() bool {
	_, ok := create.database.(sql.UnresolvedDatabase)
	return !ok && create.Child.Resolved()
}

// RowIter implements the Node interface.
func (create *CreateView) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	view := sql.NewView(create.Name, create.Child)

	if create.IsReplace {
		_ = create.Catalog.ViewRegistry.Delete(create.database.Name(), view.Name())
	}

	return sql.RowsToRowIter(), create.Catalog.ViewRegistry.Register(create.database.Name(), view)
}

// Schema implements the Node interface.
func (create *CreateView) Schema() sql.Schema { return nil }

func (create *CreateView) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("CreateView(%s)", create.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Columns (%s)", strings.Join(create.Columns, ", ")),
		create.Child.String(),
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

func (create *CreateView) Database() sql.Database {
	return create.database
}

func (create *CreateView) WithDatabase(database sql.Database) (sql.Node, error) {
	newCreate := *create
	newCreate.database = database
	return &newCreate, nil
}

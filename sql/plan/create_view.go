package plan

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
)

// CreateView is a node representing the creation (or replacement) of a view,
// which is defined by the Child node. The Columns member represent the
// explicit columns specified by the query, if any.
type CreateView struct {
	UnaryNode
	database  sql.Database
	Name      string
	Columns   []string
	Catalog   *sql.Catalog
	IsReplace bool
}

// NewCreateView creates a CreateView node with the specified parameters,
// setting its catalog to nil.
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

// View returns the view that will be created by this node.
func (create *CreateView) View() sql.View {
	return sql.NewView(create.Name, create.Child)
}

// Children implements the Node interface. It returns the Child of the
// CreateView node; i.e., the definition of the view that will be created.
func (create *CreateView) Children() []sql.Node {
	return []sql.Node{create.Child}
}

// Resolved implements the Node interface. This node is resolved if and only if
// the database and the Child are both resolved.
func (create *CreateView) Resolved() bool {
	_, ok := create.database.(sql.UnresolvedDatabase)
	return !ok && create.Child.Resolved()
}

// RowIter implements the Node interface. When executed, this function creates
// (or replaces) the view. It can error if the CraeteView's IsReplace member is
// set to false and the view already exists. The RowIter returned is always
// empty.
func (create *CreateView) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	view := sql.NewView(create.Name, create.Child)
	registry := create.Catalog.ViewRegistry

	if create.IsReplace {
		err := registry.Delete(create.database.Name(), view.Name())
		if err != nil && !sql.ErrNonExistingView.Is(err) {
			return sql.RowsToRowIter(), err
		}
	}

	return sql.RowsToRowIter(), registry.Register(create.database.Name(), view)
}

// Schema implements the Node interface. It always returns nil.
func (create *CreateView) Schema() sql.Schema { return nil }

// String implements the fmt.Stringer interface, using sql.TreePrinter to
// generate the string.
func (create *CreateView) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("CreateView(%s)", create.Name)
	_ = pr.WriteChildren(
		fmt.Sprintf("Columns (%s)", strings.Join(create.Columns, ", ")),
		create.Child.String(),
	)
	return pr.String()
}

// WithChildren implements the Node interface. It only succeeds if the length
// of the specified children equals 1.
func (create *CreateView) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(create, len(children), 1)
	}

	newCreate := create
	newCreate.Child = children[0]
	return newCreate, nil
}

// Database implements the Databaser interface, and it returns the database in
// which CreateView will create the view.
func (create *CreateView) Database() sql.Database {
	return create.database
}

// Database implements the Databaser interface, and it returns a copy of this
// node with the specified database.
func (create *CreateView) WithDatabase(database sql.Database) (sql.Node, error) {
	newCreate := *create
	newCreate.database = database
	return &newCreate, nil
}

package plan

import (
	"fmt"
	"strings"

	"github.com/src-d/go-mysql-server/sql"
)

type CreateView struct {
	UnaryNode
	database sql.Database
	Name     string
	Columns  []string
	Catalog  *sql.Catalog
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
	_, ok := create.database.(sql.UnresolvedDatabase)
	return !ok && create.Child.Resolved()
}

// RowIter implements the Node interface.
func (create *CreateView) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	view := sql.View{create.Name, create.Child}

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

package plan

import "github.com/dolthub/go-mysql-server/sql"

type UpdateJoin struct {
	editors map[string]*TableEditorIter
	UnaryNode
}

func NewUpdateJoin(editorMap map[string]*TableEditorIter, child sql.Node) *UpdateJoin {
	return &UpdateJoin{
		editors: editorMap,
		UnaryNode: UnaryNode{Child: child},
	}
}

var _ sql.RowUpdater = (*UpdateJoin)(nil)
var _ sql.Node = (*UpdateJoin)(nil)

func (u UpdateJoin) StatementBegin(ctx *sql.Context) {
	panic("implement me")
}

func (u UpdateJoin) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	panic("implement me")
}

func (u UpdateJoin) StatementComplete(ctx *sql.Context) error {
	panic("implement me")
}

func (u UpdateJoin) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	panic("implement me")
}

func (u UpdateJoin) Close(context *sql.Context) error {
	panic("implement me")
}

func (u UpdateJoin) Resolved() bool {
	panic("implement me")
}

func (u UpdateJoin) String() string {
	panic("implement me")
}

func (u UpdateJoin) Schema() sql.Schema {
	panic("implement me")
}

func (u UpdateJoin) Children() []sql.Node {
	panic("implement me")
}

func (u UpdateJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("implement me")
}

func (u UpdateJoin) WithChildren(node ...sql.Node) (sql.Node, error) {
	panic("implement me")
}
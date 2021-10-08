package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type UpdateJoin struct {
	editors map[string]sql.RowUpdater
	UnaryNode
}

func NewUpdateJoin(editorMap map[string]sql.RowUpdater, child sql.Node) *UpdateJoin {
	return &UpdateJoin{
		editors:   editorMap,
		UnaryNode: UnaryNode{Child: child},
	}
}

var _ sql.Node = (*UpdateJoin)(nil)

func (u *UpdateJoin) Resolved() bool {
	return true
}

func (u *UpdateJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("Update Join")
	_ = pr.WriteChildren(u.Child.String())
	return pr.String()

}

func (u *UpdateJoin) Schema() sql.Schema {
	return u.Child.Schema()
}

func (u *UpdateJoin) Children() []sql.Node {
	return []sql.Node{u.Child}
}

func (u *UpdateJoin) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return u.Child.RowIter(ctx, row)
}

func (u *UpdateJoin) GetUpdatable() sql.UpdatableTable {
	return &updatableJoinTable{
		editors:  u.editors,
		joinNode: u.Child.(*UpdateSource).Child,
	}
}

func (u *UpdateJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(u, len(children), 1)
	}

	return NewUpdateJoin(u.editors, children[0]), nil
}

// Manges the editing of a table
type updatableJoinTable struct {
	editors  map[string]sql.RowUpdater
	joinNode sql.Node
}

var _ sql.UpdatableTable = (*updatableJoinTable)(nil)

func (u updatableJoinTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	panic("this method should not be called")
}

func (u updatableJoinTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	panic("this method should not be called")
}

func (u updatableJoinTable) Name() string {
	panic("this method should not be called")
}

func (u updatableJoinTable) String() string {
	panic("this method should not be called")
}

func (u updatableJoinTable) Schema() sql.Schema {
	return u.joinNode.Schema() // just return the schema of the join row for now. This will cause for additional updates but could be better ig?
}

func (u updatableJoinTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &updatableJoinUpdater{
		initialEditorMap: u.editors,
		updatedEditorMap: u.editors,
		joinSchema:       u.joinNode.Schema(),
	}
}

type updatableJoinUpdater struct {
	initialEditorMap map[string]sql.RowUpdater
	updatedEditorMap map[string]sql.RowUpdater
	joinSchema       sql.Schema
}

var _ sql.RowUpdater = (*updatableJoinUpdater)(nil)

func (u updatableJoinUpdater) StatementBegin(ctx *sql.Context) {}

func (u updatableJoinUpdater) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	u.updatedEditorMap = u.initialEditorMap
	return nil
}

func (u updatableJoinUpdater) StatementComplete(ctx *sql.Context) error {
	u.initialEditorMap = u.updatedEditorMap
	return nil
}

func (u updatableJoinUpdater) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	tableToOldRowMap := splitRowIntoTableRowMap(old, u.joinSchema)
	tableToNewRowMap := splitRowIntoTableRowMap(new, u.joinSchema)

	for tableName, updater := range u.updatedEditorMap {
		oldRow := tableToOldRowMap[tableName]
		newRow := tableToNewRowMap[tableName]


		err := updater.Update(ctx, oldRow, newRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (u updatableJoinUpdater) Close(ctx *sql.Context) error {
	for _, updater := range u.updatedEditorMap {
		err := updater.Close(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// splitRowIntoTableRowMap takes a join table row and breaks into a map of tables and their respective row.
func splitRowIntoTableRowMap(row sql.Row, joinSchema sql.Schema) map[string]sql.Row {
	ret := make(map[string]sql.Row)

	if len(joinSchema) == 0 {
		return ret
	}

	currentTable := joinSchema[0].Source
	currentRow := sql.Row{row[0]}

	for i := 1; i < len(joinSchema); i++ {
		c := joinSchema[i]

		if c.Source != currentTable {
			ret[currentTable] = currentRow
			currentTable = c.Source
			currentRow = sql.Row{row[i]}
		} else {
			currentTable = c.Source
			currentRow = append(currentRow, row[i])
		}
	}

	ret[currentTable] = currentRow

	return ret
}

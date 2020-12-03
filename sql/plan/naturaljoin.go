package plan

import "github.com/dolthub/go-mysql-server/sql"

// NaturalJoin is a join that automatically joins by all the columns with the
// same name.
// NaturalJoin is a placeholder node, it should be transformed into an INNER
// JOIN during analysis.
type NaturalJoin struct {
	BinaryNode
}

// NewNaturalJoin returns a new NaturalJoin node.
func NewNaturalJoin(left, right sql.Node) *NaturalJoin {
	return &NaturalJoin{BinaryNode{left, right}}
}

// RowIter implements the Node interface.
func (NaturalJoin) RowIter(*sql.Context, sql.Row) (sql.RowIter, error) {
	panic("NaturalJoin is a placeholder, RowIter called")
}

// Schema implements the Node interface.
func (NaturalJoin) Schema() sql.Schema {
	panic("NaturalJoin is a placeholder, Schema called")
}

// Resolved implements the Node interface.
func (NaturalJoin) Resolved() bool { return false }

func (j NaturalJoin) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("NaturalJoin")
	_ = pr.WriteChildren(j.left.String(), j.right.String())
	return pr.String()
}

// WithChildren implements the Node interface.
func (j *NaturalJoin) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(j, len(children), 2)
	}

	return NewNaturalJoin(children[0], children[1]), nil
}

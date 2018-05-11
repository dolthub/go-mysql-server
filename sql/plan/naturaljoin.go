package plan

import "gopkg.in/src-d/go-mysql-server.v0/sql"

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
func (NaturalJoin) RowIter(*sql.Context) (sql.RowIter, error) {
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
	_ = pr.WriteChildren(j.Left.String(), j.Right.String())
	return pr.String()
}

// TransformUp implements the Node interface.
func (j *NaturalJoin) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	left, err := j.Left.TransformUp(f)
	if err != nil {
		return nil, err
	}

	right, err := j.Right.TransformUp(f)
	if err != nil {
		return nil, err
	}

	return f(NewNaturalJoin(left, right))
}

// TransformExpressionsUp implements the Node interface.
func (j *NaturalJoin) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	left, err := j.Left.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	right, err := j.Right.TransformExpressionsUp(f)
	if err != nil {
		return nil, err
	}

	return NewNaturalJoin(left, right), nil
}

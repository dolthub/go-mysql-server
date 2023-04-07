package memory

import (
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.TableFunction = (*IntSequenceTable)(nil)
var _ sql.CollationCoercible = (*IntSequenceTable)(nil)

// IntSequenceTable a simple table function that returns a sequence
// of integers.
type IntSequenceTable struct {
	name string
	len  int
}

func (s IntSequenceTable) NewInstance(_ *sql.Context, _ sql.Database, args []sql.Expression) (sql.Node, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("sequence table expects 2 arguments: (name, len)")
	}
	nameExp, ok := args[0].(*expression.Literal)
	if !ok {
		return nil, fmt.Errorf("sequence table expects arguments to be literal expressions")
	}
	name, ok := nameExp.Value().(string)
	if !ok {
		return nil, fmt.Errorf("sequence table expects 1st argument to be column name")
	}
	lenExp, ok := args[1].(*expression.Literal)
	if !ok {
		return nil, fmt.Errorf("sequence table expects arguments to be literal expressions")
	}
	length, _, err := types.Int64.Convert(lenExp.Value())
	if !ok {
		return nil, fmt.Errorf("%w; sequence table expects 2nd argument to be a sequence length integer", err)
	}
	return IntSequenceTable{name: name, len: int(length.(int64))}, nil
}

func (s IntSequenceTable) Resolved() bool {
	return true
}

func (s IntSequenceTable) String() string {
	return fmt.Sprintf("sequence(%s, %d)", s.name, s.len)
}

func (s IntSequenceTable) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("sequence")
	children := []string{
		fmt.Sprintf("name: %s", s.name),
		fmt.Sprintf("len: %d", s.len),
	}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (s IntSequenceTable) Schema() sql.Schema {
	schema := []*sql.Column{
		&sql.Column{
			Name: s.name,
			Type: types.Int64,
		},
	}

	return schema
}

func (s IntSequenceTable) Children() []sql.Node {
	return []sql.Node{}
}

func (s IntSequenceTable) RowIter(_ *sql.Context, _ sql.Row) (sql.RowIter, error) {
	rowIter := &SequenceTableFnRowIter{i: 0, n: s.len}
	return rowIter, nil
}

func (s IntSequenceTable) WithChildren(_ ...sql.Node) (sql.Node, error) {
	return s, nil
}

func (s IntSequenceTable) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (IntSequenceTable) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (s IntSequenceTable) Expressions() []sql.Expression {
	return []sql.Expression{}
}

func (s IntSequenceTable) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	return s, nil
}

func (s IntSequenceTable) Database() sql.Database {
	return nil
}

func (s IntSequenceTable) WithDatabase(_ sql.Database) (sql.Node, error) {
	return s, nil
}

func (s IntSequenceTable) Name() string {
	return "sequence_table"
}

func (s IntSequenceTable) Description() string {
	return "sequence"
}

var _ sql.RowIter = (*SequenceTableFnRowIter)(nil)

type SequenceTableFnRowIter struct {
	n int
	i int
}

func (i *SequenceTableFnRowIter) Next(_ *sql.Context) (sql.Row, error) {
	if i.i >= i.n {
		return nil, io.EOF
	}
	ret := sql.Row{i.i}
	i.i++
	return ret, nil
}

func (i *SequenceTableFnRowIter) Close(_ *sql.Context) error {
	return nil
}

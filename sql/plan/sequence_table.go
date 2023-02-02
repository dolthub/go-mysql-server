package plan

import (
	"fmt"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.TableFunction = (*SequenceTableFn)(nil)

// SequenceTableFn an extremely simple implementation of TableFunction for testing.
// When evaluated, returns a single row: {"foo", 123}
type SequenceTableFn struct {
	name string
	len  int
}

func (s SequenceTableFn) NewInstance(_ *sql.Context, _ sql.Database, args []sql.Expression) (sql.Node, error) {
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
	length, err := types.Int64.Convert(lenExp.Value())
	if !ok {
		return nil, fmt.Errorf("%w; sequence table expects 2nd argument to be a sequence length integer", err)
	}
	return SequenceTableFn{name: name, len: int(length.(int64))}, nil
}

func (s SequenceTableFn) Resolved() bool {
	return true
}

func (s SequenceTableFn) String() string {
	return fmt.Sprintf("sequence(%s, %d)", s.name, s.len)
}

func (s SequenceTableFn) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("sequence")
	children := []string{
		fmt.Sprintf("name: %s", s.name),
		fmt.Sprintf("len: %d", s.len),
	}
	_ = pr.WriteChildren(children...)
	return pr.String()
}

func (s SequenceTableFn) Schema() sql.Schema {
	schema := []*sql.Column{
		&sql.Column{
			Name: s.name,
			Type: types.Int64,
		},
	}

	return schema
}

func (s SequenceTableFn) Children() []sql.Node {
	return []sql.Node{}
}

func (s SequenceTableFn) RowIter(_ *sql.Context, _ sql.Row) (sql.RowIter, error) {
	rowIter := &SequenceTableFnRowIter{i: 0, n: s.len}
	return rowIter, nil
}

func (s SequenceTableFn) WithChildren(_ ...sql.Node) (sql.Node, error) {
	return s, nil
}

func (s SequenceTableFn) CheckPrivileges(_ *sql.Context, _ sql.PrivilegedOperationChecker) bool {
	return true
}

func (s SequenceTableFn) Expressions() []sql.Expression {
	return []sql.Expression{}
}

func (s SequenceTableFn) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	return s, nil
}

func (s SequenceTableFn) Database() sql.Database {
	return nil
}

func (s SequenceTableFn) WithDatabase(_ sql.Database) (sql.Node, error) {
	return s, nil
}

func (s SequenceTableFn) Name() string {
	return "sequence"
}

func (s SequenceTableFn) Description() string {
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

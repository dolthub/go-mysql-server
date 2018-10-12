package plan

import (
	"fmt"
	"io"
	"sync/atomic"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// ShowVariables is a node that shows the global or session variables
type ShowVariables struct {
	config  map[string]sql.TypedValue
	pattern string
}

func NewShowVariables(config map[string]sql.TypedValue, pattern string) *ShowVariables {
	return &ShowVariables{
		config:  config,
		pattern: pattern,
	}
}

func (sv *ShowVariables) Resolved() bool {
	return true
}

// TransformUp implements the Transformable interface.
func (sv *ShowVariables) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewShowVariables(sv.config, sv.pattern))
}

// TransformExpressionsUp implements the Transformable interface.
func (sv *ShowVariables) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return sv, nil
}

// String implements the Stringer interface.
func (sv *ShowVariables) String() string {
	var like string
	if sv.pattern != "" {
		like = fmt.Sprintf(" LIKE '%s'", sv.pattern)
	}
	return fmt.Sprintf("SHOW VARIABLES%s", like)
}

func (*ShowVariables) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Variable_name", Type: sql.Text, Nullable: false},
		&sql.Column{Name: "Value", Type: sql.Text, Nullable: true},
	}
}

func (*ShowVariables) Children() []sql.Node { return nil }

// RowIter implements the Node interface.
func (sv *ShowVariables) RowIter(ctx *sql.Context) (sql.RowIter, error) {

	it := &showVariablesIter{}
	like := expression.NewLike(
		expression.NewGetField(0, sql.Text, "", false),
		expression.NewGetField(1, sql.Text, sv.pattern, false),
	)

	for k, v := range sv.config {
		ok := (sv.pattern == "")
		if !ok {
			b, err := like.Eval(ctx, sql.NewRow(k, sv.pattern))
			if err != nil {
				return nil, err
			}
			ok = b.(bool)
		}

		if ok {
			it.names = append(it.names, k)
			it.values = append(it.values, v.Value)
			it.total++
		}
	}

	return it, nil
}

type showVariablesIter struct {
	names  []string
	values []interface{}
	offset uint32
	total  uint32
}

func (it *showVariablesIter) Next() (sql.Row, error) {
	i := atomic.LoadUint32(&it.offset)
	if i >= it.total {
		return nil, io.EOF
	}
	defer atomic.AddUint32(&it.offset, 1)

	return sql.NewRow(it.names[i], it.values[i]), nil
}

func (it *showVariablesIter) Close() error {
	atomic.StoreUint32(&it.total, 0)
	atomic.StoreUint32(&it.offset, 0)
	it.values = nil
	it.names = nil
	return nil
}

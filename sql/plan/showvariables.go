package plan

import (
	"fmt"
	"io"
	"sync/atomic"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

// ShowVariables is a node that shows the global and session variables
type ShowVariables struct {
	config  map[string]sql.TypedValue
	pattern string
}

// NewShowVariables returns a new ShowVariables reference.
// config is a variables lookup table
// like is a "like pattern". If like is an empty string it will return all variables.
func NewShowVariables(config map[string]sql.TypedValue, like string) *ShowVariables {
	return &ShowVariables{
		config:  config,
		pattern: like,
	}
}

// Resolved implements sql.Node interface. The function always returns true.
func (sv *ShowVariables) Resolved() bool {
	return true
}

// TransformUp implements the sq.Transformable interface.
func (sv *ShowVariables) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(NewShowVariables(sv.config, sv.pattern))
}

// TransformExpressionsUp implements the sql.Transformable interface.
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

// Schema returns a new Schema reference for "SHOW VARIABLES" query.
func (*ShowVariables) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Variable_name", Type: sql.Text, Nullable: false},
		&sql.Column{Name: "Value", Type: sql.Text, Nullable: true},
	}
}

// Children implements sql.Node interface. The function always returns nil.
func (*ShowVariables) Children() []sql.Node { return nil }

// RowIter implements the sql.Node interface.
// The function returns an iterator for filtered variables (based on like pattern)
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

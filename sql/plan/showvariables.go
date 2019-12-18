package plan

import (
	"fmt"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
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

// WithChildren implements the Node interface.
func (sv *ShowVariables) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(sv, len(children), 0)
	}

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
		&sql.Column{Name: "Variable_name", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Value", Type: sql.LongText, Nullable: true},
	}
}

// Children implements sql.Node interface. The function always returns nil.
func (*ShowVariables) Children() []sql.Node { return nil }

// RowIter implements the sql.Node interface.
// The function returns an iterator for filtered variables (based on like pattern)
func (sv *ShowVariables) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var (
		rows []sql.Row
		like sql.Expression
	)
	if sv.pattern != "" {
		like = expression.NewLike(
			expression.NewGetField(0, sql.LongText, "", false),
			expression.NewGetField(1, sql.LongText, sv.pattern, false),
		)
	}

	for k, v := range sv.config {
		if like != nil {
			b, err := like.Eval(ctx, sql.NewRow(k, sv.pattern))
			if err != nil {
				return nil, err
			}
			if !b.(bool) {
				continue
			}
		}

		rows = append(rows, sql.NewRow(k, v.Value))
	}

	return sql.RowsToRowIter(rows...), nil
}

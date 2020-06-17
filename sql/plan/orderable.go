package plan

import (
	"errors"
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

var ErrIterUnorderable = errors.New("row iter tree is not orderable")

type OrderableNode interface {
	sql.Node
	OrderableIter(ctx *sql.Context) (OrderableIter, error)
}

type OrderableIter interface {
	sql.RowIter
	RowOrder() []SortField
	LazyProjections() []sql.Expression
}

type orderableTableIter struct {
	*sql.TableRowIter
}

var _ OrderableIter = (*orderableTableIter)(nil)

func (i *orderableTableIter) RowOrder() []SortField {
	var fields []SortField
	for idx, col := range i.TableRowIter.Schema() {
		if !col.PrimaryKey {
			continue
		}
		fields = append(fields, SortField{
			Column: expression.NewGetField(idx, col.Type, col.Name, col.Nullable),
			Order: Ascending,
			NullOrdering: NullsFirst,
		})
	}
	return fields
}

func (i *orderableTableIter) LazyProjections() []sql.Expression {
	getFields := make([]sql.Expression, len(i.TableRowIter.Schema()))
	for i, col := range i.TableRowIter.Schema() {
		getFields[i] = expression.NewGetField(i, col.Type, col.Name, col.Nullable)
	}
	return getFields
}
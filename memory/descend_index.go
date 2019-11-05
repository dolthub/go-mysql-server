package memory

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

type DescendIndexLookup struct {
	id    string
	Gt    []interface{}
	Lte   []interface{}
	Index ExpressionsIndex
}

var _ memoryIndexLookup = (*DescendIndexLookup)(nil)

func (l *DescendIndexLookup) ID() string { return l.id }

func (l *DescendIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &dummyIndexValueIter{
		tbl:       l.Index.MemTable(),
		partition: p,
		matchExpressions: func() []sql.Expression {
			return []sql.Expression { l.EvalExpression() }
		}}, nil
}

func (l *DescendIndexLookup) EvalExpression() sql.Expression {
	if len(l.Index.ColumnExpressions()) > 1 {
		panic("Descend index unsupported for multi-column indexes")
	}

	gt, typ := getType(l.Gt[0])
	gtexpr := expression.NewGreaterThan(l.Index.ColumnExpressions()[0], expression.NewLiteral(gt, typ))
	if len(l.Gt) > 0 {
		lt, _ := getType(l.Gt[0])
		return and(
			gtexpr,
			expression.NewLessThanOrEqual(l.Index.ColumnExpressions()[0], expression.NewLiteral(lt, typ)),
		)
	}
	return gtexpr
}

func (l *DescendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *DescendIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *DescendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(l.Index, l, lookups...)
}

func (*DescendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Difference is not implemented")
}

func (*DescendIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Intersection is not implemented")
}


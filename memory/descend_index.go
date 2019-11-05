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
		tbl:             l.Index.MemTable(),
		partition:       p,
		matchExpression: l.EvalExpression(),
	}, nil
}

func (l *DescendIndexLookup) EvalExpression() sql.Expression {
	if len(l.Index.ColumnExpressions()) > 1 {
		panic("Descend index unsupported for multi-column indexes")
	}

	gt, typ := getType(l.Gt[0])
	gtexpr := expression.NewGreaterThan(l.Index.ColumnExpressions()[0], expression.NewLiteral(gt, typ))
	if len(l.Lte) > 0 {
		lte, _ := getType(l.Lte[0])
		return and(
			gtexpr,
			expression.NewLessThanOrEqual(l.Index.ColumnExpressions()[0], expression.NewLiteral(lte, typ)),
		)
	}
	return gtexpr
}

func (l *DescendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *DescendIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (l *DescendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(l.Index, l, lookups...)
}

func (*DescendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("descendIndexLookup.Difference is not implemented")
}

func (l *DescendIndexLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	return intersection(l.Index, l, lookups...)
}


package memory

import (
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
)

type AscendIndexLookup struct {
	id    string
	Gte   []interface{}
	Lt    []interface{}
	Index ExpressionsIndex
}

var _ memoryIndexLookup = (*AscendIndexLookup)(nil)

func (l *AscendIndexLookup) ID() string { return l.id }

func (AscendIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("unimplemented")
}

func (l *AscendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *AscendIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (l *AscendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(l.Index, l, lookups...)
}

func (l *AscendIndexLookup) EvalExpression() sql.Expression {
	if len(l.Index.ColumnExpressions()) > 1 {
		panic("Ascend index unsupported for multi-column indexes")
	}

	gt, typ := getType(l.Gte[0])
	gte := expression.NewGreaterThanOrEqual(l.Index.ColumnExpressions()[0], expression.NewLiteral(gt, typ))
	if l.Lt != nil {
		lt, _ := getType(l.Lt[0])
		return and(
			gte,
			expression.NewLessThan(l.Index.ColumnExpressions()[0], expression.NewLiteral(lt, typ)),
		)
	}
	return gte
}

func (AscendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("ascendIndexLookup.Difference is not implemented")
}

func (AscendIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("ascendIndexLookup.Intersection is not implemented")
}

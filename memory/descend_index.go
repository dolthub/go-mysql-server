package memory

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

type DescendIndexLookup struct {
	id    string
	Gt    []interface{}
	Lte   []interface{}
	Index ExpressionsIndex
}


var _ memoryIndexLookup = (*DescendIndexLookup)(nil)
var _ sql.IndexLookup = (*DescendIndexLookup)(nil)

func (l *DescendIndexLookup) ID() string { return l.id }
func (l *DescendIndexLookup) String() string { return l.id }

func (l *DescendIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &indexValIter{
		tbl:             l.Index.MemTable(),
		partition:       p,
		matchExpression: l.EvalExpression(),
	}, nil
}

func (l *DescendIndexLookup) EvalExpression() sql.Expression {
	if len(l.Index.ColumnExpressions()) > 1 {
		panic("Descend index unsupported for multi-column indexes")
	}

	var ltExpr, gtExpr sql.Expression
	hasLt := len(l.Lte) > 0
	hasGte := len(l.Gt) > 0

	if hasLt {
		lt, typ := getType(l.Lte[0])
		ltExpr = expression.NewLessThanOrEqual(l.Index.ColumnExpressions()[0], expression.NewLiteral(lt, typ))
	}
	if hasGte {
		gte, typ := getType(l.Gt[0])
		gtExpr = expression.NewGreaterThan(l.Index.ColumnExpressions()[0], expression.NewLiteral(gte, typ))
	}

	switch {
	case hasLt && hasGte:
		return and(ltExpr, gtExpr)
	case hasLt:
		return ltExpr
	case hasGte:
		return gtExpr
	default:
		panic("Either Lte or Gt must be set")
	}
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


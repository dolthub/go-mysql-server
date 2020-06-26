package memory

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
)

type AscendIndexLookup struct {
	id    string
	Gte   []interface{}
	Lt    []interface{}
	Index ExpressionsIndex
}

var _ memoryIndexLookup = (*AscendIndexLookup)(nil)

func (l *AscendIndexLookup) ID() string     { return l.id }
func (l *AscendIndexLookup) String() string { return l.id }

func (l *AscendIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &indexValIter{
		tbl:             l.Index.MemTable(),
		partition:       p,
		matchExpression: l.EvalExpression(),
	}, nil
}

func (l *AscendIndexLookup) Indexes() []string {
	return []string{l.id}
}

func (l *AscendIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (l *AscendIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(l.Index, l, lookups...)
}

func (l *AscendIndexLookup) EvalExpression() sql.Expression {
	if len(l.Index.ColumnExpressions()) > 1 {
		panic("Ascend index unsupported for multi-column indexes")
	}

	var ltExpr, gtExpr sql.Expression
	hasLt := len(l.Lt) > 0
	hasGte := len(l.Gte) > 0

	if hasLt {
		lt, typ := getType(l.Lt[0])
		ltExpr = expression.NewLessThan(l.Index.ColumnExpressions()[0], expression.NewLiteral(lt, typ))
	}
	if hasGte {
		gte, typ := getType(l.Gte[0])
		gtExpr = expression.NewGreaterThanOrEqual(l.Index.ColumnExpressions()[0], expression.NewLiteral(gte, typ))
	}

	switch {
	case hasLt && hasGte:
		return and(ltExpr, gtExpr)
	case hasLt:
		return ltExpr
	case hasGte:
		return gtExpr
	default:
		panic("Either Lt or Gte must be set")
	}
}

func (*AscendIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("ascendIndexLookup.Difference is not implemented")
}

func (l *AscendIndexLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	return intersection(l.Index, l, lookups...)
}

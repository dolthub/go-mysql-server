package memory

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type DescendIndexLookup struct {
	id    string
	Gt    []interface{}
	Lte   []interface{}
	Index ExpressionsIndex
}

var _ memoryIndexLookup = (*DescendIndexLookup)(nil)
var _ sql.IndexLookup = (*DescendIndexLookup)(nil)

func (l *DescendIndexLookup) ID() string     { return l.id }
func (l *DescendIndexLookup) String() string { return l.id }

func (l *DescendIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &indexValIter{
		tbl:             l.Index.MemTable(),
		partition:       p,
		matchExpression: l.EvalExpression(),
	}, nil
}

func (l *DescendIndexLookup) EvalExpression() sql.Expression {
	var columnExprs []sql.Expression
	for i, indexExpr := range l.Index.ColumnExpressions() {

		var ltExpr, gtExpr sql.Expression
		hasLt := len(l.Lte) > 0
		hasGte := len(l.Gt) > 0

		if hasLt {
			lt, typ := getType(l.Lte[i])
			ltExpr = expression.NewLessThanOrEqual(indexExpr, expression.NewLiteral(lt, typ))
		}
		if hasGte {
			gte, typ := getType(l.Gt[i])
			gtExpr = expression.NewGreaterThan(indexExpr, expression.NewLiteral(gte, typ))
		}

		switch {
		case hasLt && hasGte:
			columnExprs = append(columnExprs, ltExpr, gtExpr)
		case hasLt:
			columnExprs = append(columnExprs, ltExpr)
		case hasGte:
			columnExprs = append(columnExprs, gtExpr)
		default:
			panic("Either Lte or Gt must be set")
		}
	}

	return and(columnExprs...)
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

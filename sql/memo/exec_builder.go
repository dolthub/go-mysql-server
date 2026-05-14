package memo

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type ExecBuilder struct{}

func NewExecBuilder() *ExecBuilder {
	return &ExecBuilder{}
}

func (b *ExecBuilder) buildRel(ctx *sql.Context, r RelExpr, children ...sql.Node) (sql.Node, error) {
	n, err := buildRelExpr(ctx, b, r, children...)
	if err != nil {
		return nil, err
	}

	// TODO: distinctOp doesn't seem to be propagated through all the time
	return b.wrapInDistinct(ctx, n, r.Distinct(), r.DistinctOn())
}

func (b *ExecBuilder) buildInnerJoin(ctx *sql.Context, j *InnerJoin, children ...sql.Node) (sql.Node, error) {
	if len(j.Filter) == 0 {
		return plan.NewCrossJoin(ctx, children[0], children[1]), nil
	}
	filters := b.buildFilterConjunction(ctx, j.Filter...)

	return plan.NewInnerJoin(ctx, children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildCrossJoin(ctx *sql.Context, j *CrossJoin, children ...sql.Node) (sql.Node, error) {
	return plan.NewCrossJoin(ctx, children[0], children[1]), nil
}

// TODO: buildLeftJoin, buildSemiJoin, and buildAntiJoin are all identical. Condense into single function
func (b *ExecBuilder) buildLeftJoin(ctx *sql.Context, j *LeftJoin, children ...sql.Node) (sql.Node, error) {
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	return plan.NewJoin(ctx, children[0], children[1], j.Op, filters), nil
}

func (b *ExecBuilder) buildFullOuterJoin(ctx *sql.Context, j *FullOuterJoin, children ...sql.Node) (sql.Node, error) {
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	return plan.NewFullOuterJoin(ctx, children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildSemiJoin(ctx *sql.Context, j *SemiJoin, children ...sql.Node) (sql.Node, error) {
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	left := children[0]
	return plan.NewJoin(ctx, left, children[1], j.Op, filters), nil
}

func (b *ExecBuilder) buildAntiJoin(ctx *sql.Context, j *AntiJoin, children ...sql.Node) (sql.Node, error) {
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	return plan.NewJoin(ctx, children[0], children[1], j.Op, filters), nil
}

func (b *ExecBuilder) buildLookupJoin(ctx *sql.Context, j *LookupJoin, children ...sql.Node) (sql.Node, error) {
	left := children[0]
	right, err := b.buildIndexScan(ctx, j.Lookup, children[1])
	if err != nil {
		return nil, err
	}
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	return plan.NewJoin(ctx, left, right, j.Op, filters), nil
}

func (b *ExecBuilder) buildRangeHeap(ctx *sql.Context, sr *RangeHeap, children ...sql.Node) (ret sql.Node, err error) {
	switch n := children[0].(type) {
	case *plan.Distinct:
		ret, err = b.buildRangeHeap(ctx, sr, n.Child)
		ret = plan.NewDistinct(ret, n.DistinctOn()...)
	case *plan.OrderedDistinct:
		ret, err = b.buildRangeHeap(ctx, sr, n.Child)
		ret = plan.NewOrderedDistinct(ret)
	case *plan.Filter:
		ret, err = b.buildRangeHeap(ctx, sr, n.Child)
		ret = plan.NewFilter(ctx, n.Expression, ret)
	case *plan.Project:
		ret, err = b.buildRangeHeap(ctx, sr, n.Child)
		ret = plan.NewProject(ctx, n.Projections, ret)
	case *plan.Limit:
		ret, err = b.buildRangeHeap(ctx, sr, n.Child)
		ret = plan.NewLimit(n.Limit, ret)
	case *plan.Sort:
		ret, err = b.buildRangeHeap(ctx, sr, n.Child)
		ret = plan.NewSort(n.SortFields, ret)
	default:
		var childNode sql.Node
		if sr.MinIndex != nil {
			childNode, err = b.buildIndexScan(ctx, sr.MinIndex, children[0])
		} else {
			sortExpr := sr.MinExpr
			if err != nil {
				return nil, err
			}
			sf := []sql.SortField{{
				Column:       sortExpr,
				Order:        sql.Ascending,
				NullOrdering: sql.NullsFirst,
			}}
			childNode = plan.NewSort(sf, n)
		}

		if err != nil {
			return nil, err
		}
		ret, err = plan.NewRangeHeap(
			childNode,
			sr.ValueCol,
			sr.MinColRef,
			sr.MaxColRef,
			sr.RangeClosedOnLowerBound,
			sr.RangeClosedOnUpperBound)
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *ExecBuilder) buildRangeHeapJoin(ctx *sql.Context, j *RangeHeapJoin, children ...sql.Node) (sql.Node, error) {
	var left sql.Node
	var err error
	if j.RangeHeap.ValueIndex != nil {
		left, err = b.buildIndexScan(ctx, j.RangeHeap.ValueIndex)
		if err != nil {
			return nil, err
		}
	} else {
		sortExpr := j.RangeHeap.ValueExpr
		sf := []sql.SortField{{
			Column:       sortExpr,
			Order:        sql.Ascending,
			NullOrdering: sql.NullsFirst,
		}}
		left = plan.NewSort(sf, children[0])
	}

	right, err := b.buildRangeHeap(ctx, j.RangeHeap, children[1])
	if err != nil {
		return nil, err
	}
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	return plan.NewJoin(ctx, left, right, j.Op, filters), nil
}

func (b *ExecBuilder) buildConcatJoin(ctx *sql.Context, j *ConcatJoin, children ...sql.Node) (sql.Node, error) {
	var alias string
	var name string
	rightC := children[1]
	switch n := rightC.(type) {
	case *plan.TableAlias:
		alias = n.Name()
		name = n.Child.(sql.Nameable).Name()
		rightC = n.Child
	case *plan.ResolvedTable:
		name = n.Name()
	}

	right, err := b.buildIndexScan(ctx, j.Concat[0], children[1])
	if err != nil {
		return nil, err
	}
	for _, look := range j.Concat[1:] {
		l, err := b.buildIndexScan(ctx, look, children[1])
		if err != nil {
			return nil, err
		}
		right = plan.NewTransformedNamedNode(plan.NewConcat(l, right), name)
	}

	if alias != "" {
		// restore alias
		right = plan.NewTableAlias(alias, right)
	}

	filters := b.buildFilterConjunction(ctx, j.Filter...)

	return plan.NewJoin(ctx, children[0], right, j.Op, filters), nil
}

func (b *ExecBuilder) buildHashJoin(ctx *sql.Context, j *HashJoin, children ...sql.Node) (sql.Node, error) {
	leftProbeFilters := make([]sql.Expression, len(j.LeftAttrs))
	for i := range j.LeftAttrs {
		leftProbeFilters[i] = j.LeftAttrs[i]
	}
	leftProbeKey := expression.Tuple(leftProbeFilters)

	tmpScope := j.g.m.scope
	if tmpScope != nil {
		tmpScope = tmpScope.NewScopeNoJoin()
	}

	rightEntryFilters := make([]sql.Expression, len(j.RightAttrs))
	for i := range j.RightAttrs {
		rightEntryFilters[i] = j.RightAttrs[i]
	}
	rightEntryKey := expression.Tuple(rightEntryFilters)

	filters := b.buildFilterConjunction(ctx, j.Filter...)

	outer := plan.NewHashLookup(ctx, children[1], rightEntryKey, leftProbeKey, j.Op)
	inner := children[0]
	return plan.NewJoin(ctx, inner, outer, j.Op, filters), nil
}

func (b *ExecBuilder) buildIndexScan(ctx *sql.Context, i *IndexScan, children ...sql.Node) (sql.Node, error) {
	// need keyExprs for whole range for every dimension

	if len(children) == 0 {
		if i.Alias != "" {
			return plan.NewTableAlias(i.Alias, i.Table), nil
		}
		return i.Table, nil
	}
	var ret sql.Node
	var err error
	switch n := children[0].(type) {
	case sql.TableNode:
		if i.Alias != "" {
			ret = plan.NewTableAlias(i.Alias, i.Table)
		} else {
			ret = i.Table
		}
	case *plan.TableAlias:
		ret = plan.NewTableAlias(n.Name(), i.Table)
	case *plan.IndexedTableAccess:
		ret = i.Table
	case *plan.Distinct:
		ret, err = b.buildIndexScan(ctx, i, n.Child)
		ret = plan.NewDistinct(ret, n.DistinctOn()...)
	case *plan.OrderedDistinct:
		ret, err = b.buildIndexScan(ctx, i, n.Child)
		ret = plan.NewOrderedDistinct(ret)
	case *plan.Project:
		ret, err = b.buildIndexScan(ctx, i, n.Child)
		ret = plan.NewProject(ctx, n.Projections, ret)
	case *plan.Filter:
		ret, err = b.buildIndexScan(ctx, i, n.Child)
		ret = plan.NewFilter(ctx, n.Expression, ret)
	case *plan.Limit:
		ret, err = b.buildIndexScan(ctx, i, n.Child)
		ret = plan.NewLimit(n.Limit, ret)
	case *plan.Sort:
		ret, err = b.buildIndexScan(ctx, i, n.Child)
		ret = plan.NewSort(n.SortFields, ret)
	default:
		return nil, fmt.Errorf("unexpected *indexScan child: %T", n)
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func checkIndexTypeMismatch(ctx *sql.Context, idx sql.Index, rang sql.Range) bool {
	mysqlRange, ok := rang.(sql.MySQLRange)
	if !ok {
		return false
	}
	for i, typ := range idx.ColumnExpressionTypes(ctx) {
		if !types.Null.Equals(mysqlRange[i].Typ) && !typ.Type.Equals(mysqlRange[i].Typ) {
			return true
		}
	}
	return false
}

func (b *ExecBuilder) buildMergeJoin(ctx *sql.Context, j *MergeJoin, children ...sql.Node) (sql.Node, error) {
	inner, err := b.buildIndexScan(ctx, j.InnerScan, children[0])
	if err != nil {
		return nil, err
	}
	outer, err := b.buildIndexScan(ctx, j.OuterScan, children[1])
	if err != nil {
		return nil, err
	}

	if j.SwapCmp {
		switch cmp := j.Filter[0].(type) {
		case *expression.Equals:
			j.Filter[0] = expression.NewEquals(cmp.Right(), cmp.Left())
		case *expression.LessThan:
			j.Filter[0] = expression.NewGreaterThan(cmp.Right(), cmp.Left())
		case *expression.LessThanOrEqual:
			j.Filter[0] = expression.NewGreaterThanOrEqual(cmp.Right(), cmp.Left())
		default:
			return nil, fmt.Errorf("unexpected non-comparison condition in merge join, %T", cmp)
		}
	}
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	return plan.NewJoin(ctx, inner, outer, j.Op, filters), nil
}

func (b *ExecBuilder) buildLateralJoin(ctx *sql.Context, j *LateralJoin, children ...sql.Node) (sql.Node, error) {
	if len(j.Filter) == 0 {
		return plan.NewLateralCrossJoin(ctx, children[0], children[1]), nil
	}
	filters := b.buildFilterConjunction(ctx, j.Filter...)
	return plan.NewJoin(ctx, children[0], children[1], j.Op.AsLateral(), filters), nil
}

func (b *ExecBuilder) buildSubqueryAlias(ctx *sql.Context, r *SubqueryAlias, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildMax1Row(ctx *sql.Context, r *Max1Row, children ...sql.Node) (sql.Node, error) {
	return plan.NewMax1Row(children[0], ""), nil
}

func (b *ExecBuilder) buildTableFunc(ctx *sql.Context, r *TableFunc, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildRecursiveCte(ctx *sql.Context, r *RecursiveCte, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildValues(ctx *sql.Context, r *Values, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildRecursiveTable(ctx *sql.Context, r *RecursiveTable, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildJSONTable(ctx *sql.Context, n *JSONTable, _ ...sql.Node) (sql.Node, error) {
	return n.Table, nil
}

func (b *ExecBuilder) buildTableAlias(ctx *sql.Context, r *TableAlias, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildTableScan(ctx *sql.Context, r *TableScan, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildEmptyTable(ctx *sql.Context, r *EmptyTable, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildSetOp(ctx *sql.Context, r *SetOp, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildProject(ctx *sql.Context, r *Project, children ...sql.Node) (sql.Node, error) {
	proj := make([]sql.Expression, len(r.Projections))
	for i := range r.Projections {
		proj[i] = r.Projections[i]
	}
	return plan.NewProject(ctx, proj, children[0]), nil
}

func (b *ExecBuilder) buildDistinct(ctx *sql.Context, r *Distinct, children ...sql.Node) (sql.Node, error) {
	return plan.NewDistinct(children[0], r.distinctOn...), nil
}

func (b *ExecBuilder) buildFilter(ctx *sql.Context, r *Filter, children ...sql.Node) (sql.Node, error) {
	ret := plan.NewFilter(ctx, expression.JoinAnd(r.Filters...), children[0])
	return ret, nil
}

func (b *ExecBuilder) wrapInDistinct(ctx *sql.Context, n sql.Node, d distinctOp, distinctOn []sql.Expression) (sql.Node, error) {
	switch d {
	case HashDistinctOp:
		return plan.NewDistinct(n, distinctOn...), nil
	case SortedDistinctOp:
		return plan.NewOrderedDistinct(n), nil
	case NoDistinctOp:
		return n, nil
	default:
		return nil, fmt.Errorf("unexpected distinct operator: %d", d)
	}
}

func (b *ExecBuilder) buildFilterConjunction(ctx *sql.Context, filters ...sql.Expression) sql.Expression {
	if len(filters) == 0 {
		return expression.NewLiteral(true, types.Boolean)
	}
	return expression.JoinAnd(filters...)
}

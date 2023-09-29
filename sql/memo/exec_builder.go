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

func (b *ExecBuilder) buildRel(r RelExpr, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	n, err := buildRelExpr(b, r, input, children...)
	if err != nil {
		return nil, err
	}

	return b.buildDistinct(n, r.Distinct())
}

func (b *ExecBuilder) buildInnerJoin(j *InnerJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	if len(j.Filter) == 0 {
		return plan.NewCrossJoin(children[0], children[1]), nil
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewInnerJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildCrossJoin(j *CrossJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return plan.NewCrossJoin(children[0], children[1]), nil
}

func (b *ExecBuilder) buildLeftJoin(j *LeftJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewLeftOuterJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildFullOuterJoin(j *FullOuterJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewFullOuterJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildSemiJoin(j *SemiJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	left := children[0]
	return plan.NewJoin(left, children[1], j.Op, filters), nil
}

func (b *ExecBuilder) buildAntiJoin(j *AntiJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(children[0], children[1], j.Op, filters), nil
}

func (b *ExecBuilder) buildLookup(l *Lookup, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	var ret sql.Node
	var err error

	// the key lookup only has visibility into the left half of the join,
	// so we hide the right input cols
	sch := input[:len(input)-len(l.Parent.Right.RelProps.OutputCols())]

	keyExprs := make([]sql.Expression, len(l.KeyExprs))
	for i := range l.KeyExprs {
		keyExprs[i], err = b.buildScalar(l.KeyExprs[i], sch)
		if err != nil {
			return nil, err
		}

	}

	if err != nil {
		return nil, err
	}
	switch n := children[0].(type) {
	case sql.TableNode:
		ret, err = plan.NewIndexedAccessForTableNode(n, plan.NewLookupBuilder(l.Index.SqlIdx(), keyExprs, l.Nullmask))
	case *plan.TableAlias:
		ret, err = plan.NewIndexedAccessForTableNode(n.Child.(sql.TableNode), plan.NewLookupBuilder(l.Index.SqlIdx(), keyExprs, l.Nullmask))
		ret = plan.NewTableAlias(n.Name(), ret)
	case *plan.Distinct:
		ret, err = b.buildLookup(l, input, n.Child)
		ret = plan.NewDistinct(ret)
	case *plan.Filter:
		ret, err = b.buildLookup(l, input, n.Child)
		ret = plan.NewFilter(n.Expression, ret)
	case *plan.Project:
		ret, err = b.buildLookup(l, input, n.Child)
		ret = plan.NewProject(n.Projections, ret)
	case *plan.Limit:
		ret, err = b.buildLookup(l, input, n.Child)
		ret = plan.NewLimit(n.Limit, ret)
	default:
		panic(fmt.Sprintf("unexpected lookup child %T", n))
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *ExecBuilder) buildLookupJoin(j *LookupJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	left := children[0]
	right, err := b.buildLookup(j.Lookup, input, children[1])
	if err != nil {
		return nil, err
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(left, right, j.Op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildRangeHeap(sr *RangeHeap, leftSch, rightSch sql.Schema, children ...sql.Node) (ret sql.Node, err error) {
	switch n := children[0].(type) {
	case *plan.Distinct:
		ret, err = b.buildRangeHeap(sr, leftSch, rightSch, n.Child)
		ret = plan.NewDistinct(ret)
	case *plan.Filter:
		ret, err = b.buildRangeHeap(sr, leftSch, rightSch, n.Child)
		ret = plan.NewFilter(n.Expression, ret)
	case *plan.Project:
		ret, err = b.buildRangeHeap(sr, leftSch, rightSch, n.Child)
		ret = plan.NewProject(n.Projections, ret)
	case *plan.Limit:
		ret, err = b.buildRangeHeap(sr, leftSch, rightSch, n.Child)
		ret = plan.NewLimit(n.Limit, ret)
	default:
		var childNode sql.Node
		if sr.MinIndex != nil {
			childNode, err = b.buildIndexScan(sr.MinIndex, rightSch, n)
		} else {
			sortExpr, err := b.buildScalar(*sr.MinExpr, rightSch)
			if err != nil {
				return nil, err
			}
			sf := []sql.SortField{{
				Column:       sortExpr,
				Order:        sql.Ascending,
				NullOrdering: sql.NullsLast, // Due to https://github.com/dolthub/go-mysql-server/issues/1903
			}}
			childNode = plan.NewSort(sf, n)
		}

		if err != nil {
			return nil, err
		}
		ret, err = plan.NewRangeHeap(
			childNode,
			leftSch,
			rightSch,
			sr.ValueCol.Gf,
			sr.MinColRef.Gf,
			sr.MaxColRef.Gf,
			sr.RangeClosedOnLowerBound,
			sr.RangeClosedOnUpperBound)
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *ExecBuilder) buildRangeHeapJoin(j *RangeHeapJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	leftSch := input[:len(input)-len(j.Right.RelProps.OutputCols())]
	rightSch := input[len(j.Left.RelProps.OutputCols()):]

	var left sql.Node
	var err error
	if j.RangeHeap.ValueIndex != nil {
		left, err = b.buildIndexScan(j.RangeHeap.ValueIndex, input, children[0])
		if err != nil {
			return nil, err
		}
	} else {
		sortExpr, err := b.buildScalar(*j.RangeHeap.ValueExpr, leftSch)
		if err != nil {
			return nil, err
		}
		sf := []sql.SortField{{
			Column:       sortExpr,
			Order:        sql.Ascending,
			NullOrdering: sql.NullsLast, // Due to https://github.com/dolthub/go-mysql-server/issues/1903
		}}
		left = plan.NewSort(sf, children[0])
	}

	right, err := b.buildRangeHeap(j.RangeHeap, leftSch, rightSch, children[1])
	if err != nil {
		return nil, err
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(left, right, j.Op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildConcatJoin(j *ConcatJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
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

	right, err := b.buildLookup(j.Concat[0], input, rightC)
	if err != nil {
		return nil, err
	}
	for _, look := range j.Concat[1:] {
		l, err := b.buildLookup(look, input, rightC)
		if err != nil {
			return nil, err
		}
		right = plan.NewTransformedNamedNode(plan.NewConcat(l, right), name)
	}

	if alias != "" {
		// restore alias
		right = plan.NewTableAlias(alias, right)
	}

	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}

	return plan.NewJoin(children[0], right, j.Op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildHashJoin(j *HashJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	leftProbeFilters := make([]sql.Expression, len(j.LeftAttrs))
	var err error
	for i := range j.LeftAttrs {
		leftProbeFilters[i], err = b.buildScalar(j.LeftAttrs[i].Scalar, input)
		if err != nil {
			return nil, err
		}
	}
	leftProbeKey := expression.Tuple(leftProbeFilters)

	tmpScope := j.g.m.scope
	if tmpScope != nil {
		tmpScope = tmpScope.NewScopeNoJoin()
	}

	rightEntryFilters := make([]sql.Expression, len(j.RightAttrs))
	for i := range j.RightAttrs {
		rightEntryFilters[i], err = b.buildScalar(j.RightAttrs[i].Scalar, j.Right.RelProps.OutputCols())
		if err != nil {
			return nil, err
		}
	}
	rightEntryKey := expression.Tuple(rightEntryFilters)

	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}

	outer := plan.NewHashLookup(children[1], rightEntryKey, leftProbeKey, j.Op)
	inner := children[0]
	return plan.NewJoin(inner, outer, j.Op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildIndexScan(i *IndexScan, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	// need keyExprs for whole range for every dimension
	l := sql.IndexLookup{Index: i.Idx.SqlIdx(), Ranges: sql.RangeCollection{i.Range}}

	var ret sql.Node
	var err error
	switch n := children[0].(type) {
	case sql.TableNode:
		ret, err = plan.NewStaticIndexedAccessForTableNode(n, l)
	case *plan.TableAlias:
		ret, err = plan.NewStaticIndexedAccessForTableNode(n.Child.(sql.TableNode), l)
		ret = plan.NewTableAlias(n.Name(), ret)
	case *plan.Distinct:
		ret, err = b.buildIndexScan(i, input, n.Child)
		ret = plan.NewDistinct(ret)
	case *plan.OrderedDistinct:
		ret, err = b.buildIndexScan(i, input, n.Child)
		ret = plan.NewOrderedDistinct(ret)
	case *plan.Project:
		ret, err = b.buildIndexScan(i, input, n.Child)
		ret = plan.NewProject(n.Projections, ret)
	case *plan.Filter:
		ret, err = b.buildIndexScan(i, input, n.Child)
		ret = plan.NewFilter(n.Expression, ret)
	default:
		return nil, fmt.Errorf("unexpected *indexScan child: %T", n)
	}
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func checkIndexTypeMismatch(idx sql.Index, rang sql.Range) bool {
	for i, typ := range idx.ColumnExpressionTypes() {
		if !types.Null.Equals(rang[i].Typ) && !typ.Type.Equals(rang[i].Typ) {
			return true
		}
	}
	return false
}

func (b *ExecBuilder) buildMergeJoin(j *MergeJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	inner, err := b.buildIndexScan(j.InnerScan, input, children[0])
	if err != nil {
		return nil, err
	}
	if checkIndexTypeMismatch(j.InnerScan.Idx.SqlIdx(), j.InnerScan.Range) {
		return nil, fmt.Errorf("index scan type mismatch")
	}

	outer, err := b.buildIndexScan(j.OuterScan, input, children[1])
	if err != nil {
		return nil, err
	}
	if checkIndexTypeMismatch(j.OuterScan.Idx.SqlIdx(), j.OuterScan.Range) {
		return nil, fmt.Errorf("index scan type mismatch")
	}
	if j.SwapCmp {
		switch cmp := j.Filter[0].(type) {
		case *Equal:
			j.Filter[0] = &Equal{Left: cmp.Right, Right: cmp.Left}
		case *Lt:
			j.Filter[0] = &Gt{Left: cmp.Right, Right: cmp.Left}
		case *Leq:
			j.Filter[0] = &Geq{Left: cmp.Right, Right: cmp.Left}
		default:
			return nil, fmt.Errorf("unexpected non-comparison condition in merge join, %T", cmp)
		}
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(inner, outer, j.Op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildLateralJoin(j *LateralJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	if len(j.Filter) == 0 {
		return plan.NewCrossJoin(children[0], children[1]), nil
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(children[0], children[1], j.Op.AsLateral(), filters), nil
}

func (b *ExecBuilder) buildSubqueryAlias(r *SubqueryAlias, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildMax1Row(r *Max1Row, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return plan.NewMax1Row(children[0], ""), nil
}

func (b *ExecBuilder) buildTableFunc(r *TableFunc, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildRecursiveCte(r *RecursiveCte, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildValues(r *Values, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildRecursiveTable(r *RecursiveTable, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildJSONTable(n *JSONTable, input sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return n.Table, nil
}

func (b *ExecBuilder) buildTableAlias(r *TableAlias, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildTableScan(r *TableScan, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildEmptyTable(r *EmptyTable, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildSetOp(r *SetOp, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildProject(r *Project, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	projInput := input[len(input)-len(children[0].Schema()):]
	proj := make([]sql.Expression, len(r.Projections))
	var err error
	for i := range r.Projections {
		proj[i], err = b.buildScalar(r.Projections[i].Scalar, projInput)
		if err != nil {
			return nil, err
		}
	}
	return plan.NewProject(proj, children[0]), nil
}

func (b *ExecBuilder) buildFilter(r *Filter, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	projInput := input[len(input)-len(children[0].Schema()):]
	filters := make([]sql.Expression, len(r.Filters))
	var err error
	for i := range r.Filters {
		filters[i], err = b.buildScalar(r.Filters[i].Scalar, projInput)
		if err != nil {
			return nil, err
		}
	}
	ret := plan.NewFilter(expression.JoinAnd(filters...), children[0])
	return ret, nil
}

func (b *ExecBuilder) buildDistinct(n sql.Node, d distinctOp) (sql.Node, error) {
	switch d {
	case HashDistinctOp:
		return plan.NewDistinct(n), nil
	case SortedDistinctOp:
		return plan.NewOrderedDistinct(n), nil
	case noDistinctOp:
		return n, nil
	default:
		return nil, fmt.Errorf("unexpected distinct operator: %d", d)
	}
}

// scalar expressions

func (b *ExecBuilder) buildScalar(e ScalarExpr, sch sql.Schema) (sql.Expression, error) {
	return buildScalarExpr(b, e, sch)
}

func (b *ExecBuilder) buildFilterConjunction(scope *plan.Scope, s sql.Schema, filters ...ScalarExpr) (sql.Expression, error) {
	var ret sql.Expression
	if len(filters) == 0 {
		return expression.NewLiteral(true, types.Boolean), nil
	}
	for i := range filters {
		filter, err := b.buildScalar(filters[i], s)
		if err != nil {
			return nil, err
		}
		if ret == nil {
			ret = filter
		} else {
			ret = expression.NewAnd(ret, filter)
		}
	}
	return ret, nil
}

func (b *ExecBuilder) buildEqual(e *Equal, sch sql.Schema) (sql.Expression, error) {
	l, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	r, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewEquals(l, r), nil
}

func (b *ExecBuilder) buildLiteral(e *Literal, sch sql.Schema) (sql.Expression, error) {
	return expression.NewLiteral(e.Val, e.Typ), nil
}

func (b *ExecBuilder) buildColRef(e *ColRef, sch sql.Schema) (sql.Expression, error) {
	return e.Gf, nil
}

func (b *ExecBuilder) buildOr(e *Or, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewOr(left, right), nil
}

func (b *ExecBuilder) buildAnd(e *And, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewAnd(left, right), nil
}

func (b *ExecBuilder) buildRegexp(e *Regexp, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewRegexp(left, right), nil
}

func (b *ExecBuilder) buildLeq(e *Leq, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewLessThanOrEqual(left, right), nil
}

func (b *ExecBuilder) buildLt(e *Lt, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewLessThan(left, right), nil
}

func (b *ExecBuilder) buildGt(e *Gt, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewGreaterThan(left, right), nil
}

func (b *ExecBuilder) buildGeq(e *Geq, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewGreaterThanOrEqual(left, right), nil
}

func (b *ExecBuilder) buildInTuple(e *InTuple, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewInTuple(left, right), nil
}

func (b *ExecBuilder) buildNullSafeEq(e *NullSafeEq, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewNullSafeEquals(left, right), nil
}

func (b *ExecBuilder) buildNot(e *Not, sch sql.Schema) (sql.Expression, error) {
	child, err := b.buildScalar(e.Child.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewNot(child), nil
}

func (b *ExecBuilder) buildArithmetic(e *Arithmetic, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.Left.Scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.Right.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewArithmetic(left, right, e.Op.String()), nil
}

func (b *ExecBuilder) buildBindvar(e *Bindvar, sch sql.Schema) (sql.Expression, error) {
	return &expression.BindVar{Name: e.Name, Typ: e.Typ}, nil
}

func (b *ExecBuilder) buildIsNull(e *IsNull, sch sql.Schema) (sql.Expression, error) {
	child, err := b.buildScalar(e.Child.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewIsNull(child), nil
}

func (b *ExecBuilder) buildTuple(e *Tuple, sch sql.Schema) (sql.Expression, error) {
	values := make([]sql.Expression, len(e.Values))
	var err error
	for i := range values {
		values[i], err = b.buildScalar(e.Values[i].Scalar, sch)
		if err != nil {
			return nil, err
		}
	}
	return expression.NewTuple(values...), nil
}

func (b *ExecBuilder) buildBetween(e *Between, sch sql.Schema) (sql.Expression, error) {
	value, err := b.buildScalar(e.Value.Scalar, sch)
	if err != nil {
		return nil, err
	}
	min, err := b.buildScalar(e.Min.Scalar, sch)
	if err != nil {
		return nil, err
	}
	max, err := b.buildScalar(e.Max.Scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewBetween(value, min, max), nil
}

func (b *ExecBuilder) buildHidden(e *Hidden, sch sql.Schema) (sql.Expression, error) {
	return e.E, nil
}

package memo

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/fixidx"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
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
	case *plan.ResolvedTable:
		ret, err = plan.NewIndexedAccessForResolvedTable(n, plan.NewLookupBuilder(l.Index, keyExprs, l.Nullmask))
	case *plan.TableAlias:
		ret, err = plan.NewIndexedAccessForResolvedTable(n.Child.(*plan.ResolvedTable), plan.NewLookupBuilder(l.Index, keyExprs, l.Nullmask))
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

	cr := plan.NewCachedResults(children[1])
	outer := plan.NewHashLookup(cr, rightEntryKey, leftProbeKey)
	inner := children[0]
	return plan.NewJoin(inner, outer, j.Op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildIndexScan(i *IndexScan, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	// need keyExprs for whole range for every dimension
	cets := i.Idx.ColumnExpressionTypes()
	ranges := make(sql.Range, len(cets))
	for i, cet := range cets {
		ranges[i] = sql.AllRangeColumnExpr(cet.Type)
	}

	l := sql.IndexLookup{Index: i.Idx, Ranges: sql.RangeCollection{ranges}}

	var ret sql.Node
	var err error
	switch n := children[0].(type) {
	case *plan.ResolvedTable:
		ret, err = plan.NewStaticIndexedAccessForResolvedTable(n, l)
	case *plan.TableAlias:
		ret, err = plan.NewStaticIndexedAccessForResolvedTable(n.Child.(*plan.ResolvedTable), l)
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

func (b *ExecBuilder) buildMergeJoin(j *MergeJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	inner, err := b.buildIndexScan(j.InnerScan, input, children[0])
	if err != nil {
		return nil, err
	}
	outer, err := b.buildIndexScan(j.OuterScan, input, children[1])
	if err != nil {
		return nil, err
	}
	if j.SwapCmp {
		cmp, ok := j.Filter[0].(*Equal)
		if !ok {
			return nil, fmt.Errorf("unexpected non-equals comparison in merge join")
		}
		j.Filter[0] = &Equal{Left: cmp.Right, Right: cmp.Left}
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.Filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(inner, outer, j.Op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildSubqueryAlias(r *SubqueryAlias, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildMax1Row(r *Max1Row, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.Table, nil
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
func (b *ExecBuilder) buildTableAlias(r *TableAlias, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildTableScan(r *TableScan, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.Table, nil
}

func (b *ExecBuilder) buildEmptyTable(r *EmptyTable, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
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
	gf, _, err := fixidx.FixFieldIndexes(e.Group().m.scope, nil, sch, e.Gf)
	return gf, err
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

func (b *ExecBuilder) buildHidden(e *Hidden, sch sql.Schema) (sql.Expression, error) {
	ret, _, err := fixidx.FixFieldIndexes(e.g.m.scope, nil, sch, e.E)
	return ret, err
}

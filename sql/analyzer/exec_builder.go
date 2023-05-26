package analyzer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type ExecBuilder struct{}

func NewExecBuilder() *ExecBuilder {
	return &ExecBuilder{}
}

func (b *ExecBuilder) buildRel(r relExpr, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	n, err := buildRelExpr(b, r, input, children...)
	if err != nil {
		return nil, err
	}

	return b.buildDistinct(n, r.distinct())
}

func (b *ExecBuilder) buildInnerJoin(j *innerJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	if len(j.filter) == 0 {
		return plan.NewCrossJoin(children[0], children[1]), nil
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewInnerJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildCrossJoin(j *crossJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return plan.NewCrossJoin(children[0], children[1]), nil
}

func (b *ExecBuilder) buildLeftJoin(j *leftJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewLeftOuterJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildFullOuterJoin(j *fullOuterJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewFullOuterJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildSemiJoin(j *semiJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	left := children[0]
	return plan.NewJoin(left, children[1], j.op, filters), nil
}

func (b *ExecBuilder) buildAntiJoin(j *antiJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(children[0], children[1], j.op, filters), nil
}

func (b *ExecBuilder) buildLookup(l *lookup, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	var ret sql.Node
	var err error

	// the key lookup only has visibility into the left half of the join,
	// so we hide the right input cols
	sch := input[:len(input)-len(l.parent.right.relProps.OutputCols())]
	keyExprs, _, err := FixFieldIndexesOnExpressions(l.parent.g.m.scope, nil, sch, l.keyExprs...)

	if err != nil {
		return nil, err
	}
	switch n := children[0].(type) {
	case *plan.ResolvedTable:
		ret, err = plan.NewIndexedAccessForResolvedTable(n, plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
	case *plan.TableAlias:
		ret, err = plan.NewIndexedAccessForResolvedTable(n.Child.(*plan.ResolvedTable), plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
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

func (b *ExecBuilder) buildLookupJoin(j *lookupJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	left := children[0]
	right, err := b.buildLookup(j.lookup, input, children[1])
	if err != nil {
		return nil, err
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(left, right, j.op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildConcatJoin(j *concatJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
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

	right, err := b.buildLookup(j.concat[0], input, rightC)
	if err != nil {
		return nil, err
	}
	for _, look := range j.concat[1:] {
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

	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}

	return plan.NewJoin(children[0], right, j.op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildHashJoin(j *hashJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	innerFilters := make([]sql.Expression, len(j.innerAttrs))
	var err error
	for i := range j.innerAttrs {
		innerFilters[i], err = b.buildScalar(j.innerAttrs[i].scalar, input)
		if err != nil {
			return nil, err
		}
	}
	innerAttrs := expression.Tuple(innerFilters)

	if err != nil {
		return nil, err
	}
	tmpScope := j.g.m.scope
	if tmpScope != nil {
		tmpScope = tmpScope.newScopeNoJoin()
	}

	outerFilters := make([]sql.Expression, len(j.outerAttrs))
	for i := range j.outerAttrs {
		outerFilters[i], err = b.buildScalar(j.outerAttrs[i].scalar, input)
		if err != nil {
			return nil, err
		}

	}
	outerAttrs := expression.Tuple(outerFilters)

	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}

	cr := plan.NewCachedResults(children[1])
	outer := plan.NewHashLookup(cr, outerAttrs, innerAttrs)
	inner := children[0]
	return plan.NewJoin(inner, outer, j.op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildIndexScan(i *indexScan, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	// need keyExprs for whole range for every dimension
	cets := i.idx.ColumnExpressionTypes()
	ranges := make(sql.Range, len(cets))
	for i, cet := range cets {
		ranges[i] = sql.AllRangeColumnExpr(cet.Type)
	}

	l := sql.IndexLookup{Index: i.idx, Ranges: sql.RangeCollection{ranges}}

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

func (b *ExecBuilder) buildMergeJoin(j *mergeJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	inner, err := b.buildIndexScan(j.innerScan, input, children[0])
	if err != nil {
		return nil, err
	}
	outer, err := b.buildIndexScan(j.outerScan, input, children[1])
	if err != nil {
		return nil, err
	}
	filters, err := b.buildFilterConjunction(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewJoin(inner, outer, j.op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildSubqueryAlias(r *subqueryAlias, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildMax1Row(r *max1Row, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildTableFunc(r *tableFunc, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildRecursiveCte(r *recursiveCte, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildValues(r *values, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildRecursiveTable(r *recursiveTable, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.table, nil
}
func (b *ExecBuilder) buildTableAlias(r *tableAlias, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildTableScan(r *tableScan, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildEmptyTable(r *emptyTable, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildProject(r *project, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	projInput := input[len(input)-len(children[0].Schema()):]
	proj := make([]sql.Expression, len(r.projections))
	var err error
	for i := range r.projections {
		proj[i], err = b.buildScalar(r.projections[i].scalar, projInput)
		if err != nil {
			return nil, err
		}
	}
	return plan.NewProject(proj, children[0]), nil
}

func (b *ExecBuilder) buildFilter(r *filter, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return nil, nil
}

func (b *ExecBuilder) buildDistinct(n sql.Node, d distinctOp) (sql.Node, error) {
	switch d {
	case hashDistinctOp:
		return plan.NewDistinct(n), nil
	case sortedDistinctOp:
		return plan.NewOrderedDistinct(n), nil
	case noDistinctOp:
		return n, nil
	default:
		return nil, fmt.Errorf("unexpected distinct operator: %d", d)
	}
}

// scalar expressions

func (b *ExecBuilder) buildScalar(e scalarExpr, sch sql.Schema) (sql.Expression, error) {
	return buildScalarExpr(b, e, sch)
}

func (b *ExecBuilder) buildFilterConjunction(scope *Scope, s sql.Schema, filters ...scalarExpr) (sql.Expression, error) {
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

func (b *ExecBuilder) buildEqual(e *equal, sch sql.Schema) (sql.Expression, error) {
	l, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	r, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewEquals(l, r), nil
}

func (b *ExecBuilder) buildLiteral(e *literal, sch sql.Schema) (sql.Expression, error) {
	return expression.NewLiteral(e.val, e.typ), nil
}

func (b *ExecBuilder) buildColRef(e *colRef, sch sql.Schema) (sql.Expression, error) {
	gf, _, err := FixFieldIndexes(e.g.m.scope, nil, sch, e.gf)
	return gf, err
}

func (b *ExecBuilder) buildOr(e *or, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewOr(left, right), nil
}

func (b *ExecBuilder) buildAnd(e *and, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewAnd(left, right), nil
}

func (b *ExecBuilder) buildRegexp(e *regexp, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewRegexp(left, right), nil
}

func (b *ExecBuilder) buildLeq(e *leq, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewLessThanOrEqual(left, right), nil
}

func (b *ExecBuilder) buildLt(e *lt, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewLessThan(left, right), nil
}

func (b *ExecBuilder) buildGt(e *gt, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewGreaterThan(left, right), nil
}

func (b *ExecBuilder) buildGeq(e *geq, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewGreaterThanOrEqual(left, right), nil
}

func (b *ExecBuilder) buildInTuple(e *inTuple, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewInTuple(left, right), nil
}

func (b *ExecBuilder) buildNullSafeEq(e *nullSafeEq, sch sql.Schema) (sql.Expression, error) {
	left, err := b.buildScalar(e.left.scalar, sch)
	if err != nil {
		return nil, err
	}
	right, err := b.buildScalar(e.right.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewNullSafeEquals(left, right), nil
}

func (b *ExecBuilder) buildNot(e *not, sch sql.Schema) (sql.Expression, error) {
	child, err := b.buildScalar(e.child.scalar, sch)
	if err != nil {
		return nil, err
	}
	return expression.NewNot(child), nil
}

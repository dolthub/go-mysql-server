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

func (b *ExecBuilder) buildFilters(scope *Scope, s sql.Schema, filters ...sql.Expression) (sql.Expression, error) {
	f, _, err := FixFieldIndexesOnExpressions(scope, nil, s, filters...)
	if err != nil {
		return nil, err
	}
	return expression.JoinAnd(f...), nil
}

func (b *ExecBuilder) buildInnerJoin(j *innerJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	if len(j.filter) == 0 {
		return plan.NewCrossJoin(children[0], children[1]), nil
	}
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewInnerJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildCrossJoin(j *crossJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return plan.NewCrossJoin(children[0], children[1]), nil
}

func (b *ExecBuilder) buildLeftJoin(j *leftJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewLeftOuterJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildFullOuterJoin(j *fullOuterJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	return plan.NewFullOuterJoin(children[0], children[1], filters), nil
}

func (b *ExecBuilder) buildSemiJoin(j *semiJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}
	left := children[0]
	return plan.NewJoin(left, children[1], j.op, filters), nil
}

func (b *ExecBuilder) buildAntiJoin(j *antiJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
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
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
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

	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
	if err != nil {
		return nil, err
	}

	return plan.NewJoin(children[0], right, j.op, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildHashJoin(j *hashJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	innerAttrs, err := b.buildFilters(j.g.m.scope, input, expression.Tuple(j.innerAttrs))
	if err != nil {
		return nil, err
	}
	tmpScope := j.g.m.scope
	if tmpScope != nil {
		tmpScope = tmpScope.newScopeNoJoin()
	}
	outerAttrs, err := b.buildFilters(tmpScope, j.right.relProps.OutputCols(), expression.Tuple(j.outerAttrs))
	if err != nil {
		return nil, err
	}
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
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
	filters, err := b.buildFilters(j.g.m.scope, input, j.filter...)
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
	p, _, err := FixFieldIndexesOnExpressions(r.g.m.scope, nil, projInput, r.projections...)
	if err != nil {
		return nil, err
	}
	return plan.NewProject(p, children[0]), nil
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

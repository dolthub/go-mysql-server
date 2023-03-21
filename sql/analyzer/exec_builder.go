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
	return buildRelExpr(b, r, input, children...)
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
	case *plan.SelectSingleRel:
		switch n := n.Rel.(type) {
		case *plan.ResolvedTable:
			ret, err = plan.NewIndexedAccessForResolvedTable(n, plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
		case *plan.TableAlias:
			ret, err = plan.NewIndexedAccessForResolvedTable(n.Child.(*plan.ResolvedTable), plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
			ret = plan.NewTableAlias(n.Name(), ret)
		}
		ret = plan.NewSelectSingleRel(n.Select, ret.(sql.NameableNode))
	case *plan.Distinct:
		switch n := n.Child.(type) {
		case *plan.ResolvedTable:
			ret, err = plan.NewIndexedAccessForResolvedTable(n, plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
		case *plan.TableAlias:
			ret, err = plan.NewIndexedAccessForResolvedTable(n.Child.(*plan.ResolvedTable), plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
			ret = plan.NewTableAlias(n.Name(), ret)
		}
		ret = plan.NewDistinct(ret)
	case *plan.Filter:
		switch n := n.Child.(type) {
		case *plan.ResolvedTable:
			ret, err = plan.NewIndexedAccessForResolvedTable(n, plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
		case *plan.TableAlias:
			ret, err = plan.NewIndexedAccessForResolvedTable(n.Child.(*plan.ResolvedTable), plan.NewLookupBuilder(l.index, keyExprs, l.nullmask))
			ret = plan.NewTableAlias(n.Name(), ret)
		}
		ret = plan.NewFilter(n.Expression, ret)
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

	var newOp plan.JoinType
	switch j.op {
	case plan.JoinTypeInner:
		newOp = plan.JoinTypeLookup
	case plan.JoinTypeLeftOuter:
		newOp = plan.JoinTypeLeftOuterLookup
	case plan.JoinTypeSemi:
		newOp = plan.JoinTypeSemiLookup
	case plan.JoinTypeRightSemi:
		if _, ok := left.(*plan.Max1Row); !ok {
			left = plan.NewDistinct(left)
		}
		newOp = plan.JoinTypeRightSemiLookup
	case plan.JoinTypeAnti:
		newOp = plan.JoinTypeAntiLookup
	default:
		panic(fmt.Sprintf("can only apply lookup to InnerJoin or LeftOuterJoin, found %s", j.op))
	}
	return plan.NewJoin(left, right, newOp, filters).WithScopeLen(j.g.m.scopeLen), nil
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

	var newOp plan.JoinType
	switch j.op {
	case plan.JoinTypeInner:
		newOp = plan.JoinTypeLookup
	case plan.JoinTypeLeftOuter:
		newOp = plan.JoinTypeLeftOuterLookup
	default:
		panic("can only apply lookup to InnerJoin or LeftOuterJoin")
	}
	return plan.NewJoin(children[0], right, newOp, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildHashJoin(j *hashJoin, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	innerAttrs, err := b.buildFilters(j.g.m.scope, input, expression.Tuple(j.innerAttrs))
	if err != nil {
		return nil, err
	}
	outerAttrs, err := b.buildFilters(j.g.m.scope, j.right.relProps.OutputCols(), expression.Tuple(j.outerAttrs))
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

	var newOp plan.JoinType
	switch j.op {
	case plan.JoinTypeInner:
		newOp = plan.JoinTypeHash
	case plan.JoinTypeLeftOuter:
		newOp = plan.JoinTypeLeftOuterHash
	case plan.JoinTypeSemi:
		newOp = plan.JoinTypeSemiHash
	case plan.JoinTypeAnti:
		newOp = plan.JoinTypeAntiHash
	default:
		return nil, fmt.Errorf("can only apply hash join to InnerJoin, LeftOuterJoin, SemiJoin, or AntiJoin")
	}
	return plan.NewJoin(inner, outer, newOp, filters).WithScopeLen(j.g.m.scopeLen), nil
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
		switch n := n.Child.(type) {
		case *plan.ResolvedTable:
			ret, err = plan.NewStaticIndexedAccessForResolvedTable(n, l)
		case *plan.TableAlias:
			ret, err = plan.NewStaticIndexedAccessForResolvedTable(n.Child.(*plan.ResolvedTable), l)
			ret = plan.NewTableAlias(n.Name(), ret)
		default:
			return nil, fmt.Errorf("unexpected *indexScan child: %T", n)
		}
		ret = plan.NewDistinct(ret)
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

	var newOp plan.JoinType
	switch j.op {
	case plan.JoinTypeInner:
		newOp = plan.JoinTypeMerge
	case plan.JoinTypeLeftOuter:
		newOp = plan.JoinTypeLeftOuterMerge
	case plan.JoinTypeSemi:
		newOp = plan.JoinTypeSemiMerge
	case plan.JoinTypeAnti:
		newOp = plan.JoinTypeAntiMerge
	default:
		return nil, fmt.Errorf("can only apply merge join to InnerJoin, LeftOuterJoin, SemiJoin, or AntiJoin")
	}

	return plan.NewJoin(inner, outer, newOp, filters).WithScopeLen(j.g.m.scopeLen), nil
}

func (b *ExecBuilder) buildSubqueryAlias(r *subqueryAlias, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildMax1Row(r *max1Row, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	return plan.NewMax1Row(r.table), nil
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

func (b *ExecBuilder) buildSelectSingleRel(r *selectSingleRel, _ sql.Schema, _ ...sql.Node) (sql.Node, error) {
	return r.table, nil
}

func (b *ExecBuilder) buildProject(r *project, input sql.Schema, children ...sql.Node) (sql.Node, error) {
	p, _, err := FixFieldIndexesOnExpressions(r.g.m.scope, nil, input, r.projections...)
	if err != nil {
		return nil, err
	}
	return plan.NewProject(p, children[0]), nil
}

func (b *ExecBuilder) buildDistinct(r *distinct, _ sql.Schema, children ...sql.Node) (sql.Node, error) {
	return plan.NewDistinct(children[0]), nil
}

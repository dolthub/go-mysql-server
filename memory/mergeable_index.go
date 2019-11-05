package memory

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"strings"
)

type MergeableDummyIndex struct {
	DB         string // required for engine tests with driver
	DriverName string // required for engine tests with driver
	Tbl        *Table // required for engine tests with driver
	TableName  string
	Exprs      []sql.Expression
}

func (i MergeableDummyIndex) Database() string { return i.DB }
func (i MergeableDummyIndex) Driver() string   { return i.DriverName }
func (i MergeableDummyIndex) MemTable() *Table   { return i.Tbl }
func (i MergeableDummyIndex) ColumnExpressions() []sql.Expression   { return i.Exprs }

func (i MergeableDummyIndex) Expressions() []string {
	var exprs []string
	for _, e := range i.Exprs {
		exprs = append(exprs, e.String())
	}
	return exprs
}

func (i MergeableDummyIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	return &AscendIndexLookup{Gte: keys}, nil
}

func (i MergeableDummyIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	return &AscendIndexLookup{Lt: keys}, nil
}

func (i MergeableDummyIndex) AscendRange(greaterOrEqual, lessThan []interface{}) (sql.IndexLookup, error) {
	return &AscendIndexLookup{Gte: greaterOrEqual, Lt: lessThan}, nil
}

func (i MergeableDummyIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	return &DescendIndexLookup{Gt: keys}, nil
}

func (i MergeableDummyIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	return &DescendIndexLookup{Lte: keys}, nil
}

func (i MergeableDummyIndex) DescendRange(lessOrEqual, greaterThan []interface{}) (sql.IndexLookup, error) {
	return &DescendIndexLookup{Gt: greaterThan, Lte: lessOrEqual}, nil
}

func (i MergeableDummyIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	lookup, err := i.Get(keys...)
	if err != nil {
		return nil, err
	}

	mergeable, _ := lookup.(*MergeableIndexLookup)
	return &NegateIndexLookup{Lookup: mergeable, Index: mergeable.Index}, nil
}

func (i MergeableDummyIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	return &MergeableIndexLookup{Key: key, Index: i}, nil
}

func (i MergeableDummyIndex) Has(sql.Partition, ...interface{}) (bool, error) {
	panic("not implemented")
}

func (i MergeableDummyIndex) ID() string {
	if len(i.Exprs) == 1 {
		return i.Exprs[0].String()
	}
	var parts = make([]string, len(i.Exprs))
	for i, e := range i.Exprs {
		parts[i] = e.String()
	}

	return "(" + strings.Join(parts, ", ") + ")"
}

func (i MergeableDummyIndex) Table() string { return i.TableName }

type MergeableLookup interface {
	ID() string
}

// ExpressionsIndex is an index made out of one or more expressions (usually field expressions)
type ExpressionsIndex interface {
	MemTable() 					*Table
	ColumnExpressions() []sql.Expression
}

type MergeableIndexLookup struct {
	Key           []interface{}
	Index         ExpressionsIndex
}

var _ sql.Mergeable = (*MergeableIndexLookup)(nil)
var _ sql.SetOperations = (*MergeableIndexLookup)(nil)
var _ memoryIndexLookup = (*MergeableIndexLookup)(nil)

func (i *MergeableIndexLookup) ID() string 													{ return strings.Join(i.Indexes(), ",") }

func (i *MergeableIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (i *MergeableIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &dummyIndexValueIter{
		tbl:       i.Index.MemTable(),
		partition: p,
		matchExpressions: func() []sql.Expression {
			var exprs []sql.Expression
			for exprI, expr := range i.Index.ColumnExpressions() {
				lit, typ := getType(i.Key[exprI])
				exprs = append(exprs, expression.NewEquals(expr, expression.NewLiteral(lit, typ)))
			}
			return exprs
		}}, nil
}

func (i *MergeableIndexLookup) EvalExpression() sql.Expression {
	var exprs []sql.Expression
	for exprI, expr := range i.Index.ColumnExpressions() {
		lit, typ := getType(i.Key[exprI])
		exprs = append(exprs, expression.NewEquals(expr, expression.NewLiteral(lit, typ)))
	}
	return and(exprs...)
}

func (i *MergeableIndexLookup) Indexes() []string {
	var idxes = make([]string, len(i.Key))
	for i, e := range i.Key {
		idxes[i] = fmt.Sprint(e)
	}
	return idxes
}

func (i *MergeableIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("not implemented")
}

func (i *MergeableIndexLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	return intersection(i.Index, i, lookups...)
}

// Intersects the lookups given together, collapsing redundant layers of intersections for lookups that have previously
// been merged. E.g. merging a MergeableIndexLookup with a MergedIndexLookup that has 2 intersections will return a
// MergedIndexLookup with 3 lookups intersected: the left param and the two intersected lookups from the
// MergedIndexLookup.
func intersection(idx ExpressionsIndex, left sql.IndexLookup, lookups ...sql.IndexLookup) sql.IndexLookup {
	var merged []sql.IndexLookup
	var allLookups []sql.IndexLookup
	allLookups = append(allLookups, left)
	allLookups = append(allLookups, lookups...)
	for _, lookup := range allLookups {
		if mil, ok := lookup.(*MergedIndexLookup); ok && len(mil.Intersections) > 0 {
			merged = append(merged, mil.Intersections...)
		} else {
			merged = append(merged, lookup)
		}
	}

	return &MergedIndexLookup{
		Intersections: merged,
		idx: idx,
	}
}

func (i *MergeableIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(i.Index, i, lookups...)
}

// Unions the lookups given together, collapsing redundant layers of unions for lookups that have previously been
// merged. E.g. merging a MergeableIndexLookup with a MergedIndexLookup that has 2 unions will return a
// MergedIndexLookup with 3 lookups unioned: the left param and the two unioned lookups from the MergedIndexLookup.
func union(idx ExpressionsIndex, left sql.IndexLookup, lookups ...sql.IndexLookup) sql.IndexLookup {
	var merged []sql.IndexLookup
	var allLookups []sql.IndexLookup
	allLookups = append(allLookups, left)
	allLookups = append(allLookups, lookups...)
	for _, lookup := range allLookups {
		if mil, ok := lookup.(*MergedIndexLookup); ok && len(mil.Unions) > 0 {
			merged = append(merged, mil.Unions...)
		} else {
			merged = append(merged, lookup)
		}
	}

	return &MergedIndexLookup{
		Unions: merged,
		idx: idx,
	}
}

// An index lookup that has been merged with another.
// Exactly one of the Unions or Intersections fields should be set.
type MergedIndexLookup struct {
	Unions        []sql.IndexLookup
	Intersections []sql.IndexLookup
	idx           ExpressionsIndex
}

var _ sql.Mergeable = (*MergedIndexLookup)(nil)
var _ sql.SetOperations = (*MergedIndexLookup)(nil)
var _ memoryIndexLookup = (*MergedIndexLookup)(nil)

func (m *MergedIndexLookup) EvalExpression() sql.Expression {
	var exprs []sql.Expression
	if m.Intersections != nil {
		for _, lookup := range m.Intersections {
			exprs = append(exprs, lookup.(memoryIndexLookup).EvalExpression())
		}
		return and(exprs...)
	}
	if m.Unions != nil {
		for _, lookup := range m.Unions {
			exprs = append(exprs, lookup.(memoryIndexLookup).EvalExpression())
		}
		return or(exprs...)
	}
	panic("either Unions or Intersections must be non-nil")
}

func (m *MergedIndexLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	return intersection(m.idx, m, lookups...)
}

func (m *MergedIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(m.idx, m, lookups...)
}

func (m *MergedIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("not implemented")
}

func (m *MergedIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (m *MergedIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &dummyIndexValueIter{
		tbl:       m.idx.MemTable(),
		partition: p,
		matchExpressions: func() []sql.Expression {
			return []sql.Expression { m.EvalExpression() }
		}}, nil
}

func or(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	return expression.NewOr(expressions[0], or(expressions[1:]...))
}

func and(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	return expression.NewAnd(expressions[0], and(expressions[1:]...))
}

func (m *MergedIndexLookup) Indexes() []string {
	panic("implement me")
}

func (m *MergedIndexLookup) ID() string {
	panic("implement me")
}
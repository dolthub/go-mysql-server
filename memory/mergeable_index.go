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
	return &NegateIndexLookup{Lookup: mergeable}, nil
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
	// GetUnions() []MergeableLookup
	// GetIntersections() []MergeableLookup
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

func (i *MergeableIndexLookup) ID() string 													{ return strings.Join(i.Indexes(), ",") }
// func (i *MergeableIndexLookup) GetUnions() []MergeableLookup        { return nil }
// func (i *MergeableIndexLookup) GetIntersections() []MergeableLookup { return nil }
func (i *MergeableIndexLookup) ClearUnions() { }
func (i *MergeableIndexLookup) ClearIntersections() { }

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
	return intersection(i, lookups...)
}

func intersection(left sql.IndexLookup, lookups ...sql.IndexLookup) sql.IndexLookup {
	return &MergedIndexLookup{
		Intersections: merge(left, lookups),
	}
}

func (i *MergeableIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(i, lookups...)
}

func union(left sql.IndexLookup, lookups ...sql.IndexLookup) sql.IndexLookup {
	return &MergedIndexLookup{
		Unions: merge(left, lookups),
	}
}

func merge(left sql.IndexLookup, lookups []sql.IndexLookup) []sql.IndexLookup {
	var merged []sql.IndexLookup
	var allLookups []sql.IndexLookup
	allLookups = append(allLookups, left)
	allLookups = append(allLookups, lookups...)
	for _, lookup := range allLookups {
		if mil, ok := lookup.(*MergedIndexLookup); ok {
			merged = append(merged, mil.Intersections...)
			merged = append(merged, mil.Unions...)
		} else {
			merged = append(merged, lookup)
		}
	}

	return merged
}

type MergedIndexLookup struct {
	Unions        []sql.IndexLookup
	Intersections []sql.IndexLookup
}

var _ sql.Mergeable = (*MergedIndexLookup)(nil)
var _ sql.SetOperations = (*MergedIndexLookup)(nil)

func (m *MergedIndexLookup) ClearUnions() {
	m.Unions = nil
}

func (m *MergedIndexLookup) ClearIntersections() {
	m.Intersections = nil
}

func (m *MergedIndexLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	return intersection(m, lookups...)
}

func (m *MergedIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return union(m, lookups...)
}

func (m *MergedIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("not implemented")
}

func (m *MergedIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (m *MergedIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	panic("implement me")
}

func (m *MergedIndexLookup) Indexes() []string {
	panic("implement me")
}

func (m *MergedIndexLookup) ID() string {
	panic("implement me")
}
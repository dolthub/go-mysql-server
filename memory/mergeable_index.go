package memory

import (
	"fmt"
	"github.com/src-d/go-mysql-server/sql"
	"strings"
)

type MergeableLookup interface {
	ID() string
	GetUnions() []string
	GetIntersections() []string
}

// ExpressionsIndex is an index made out of one or more expressions (usually field expressions)
type ExpressionsIndex interface {
	ColumnExpressions() []sql.Expression
}

type MergeableIndexLookup struct {
	Key           []interface{}
	Index         ExpressionsIndex
	Unions        []string
	Intersections []string
}

var _ sql.Mergeable = (*MergeableIndexLookup)(nil)
var _ sql.SetOperations = (*MergeableIndexLookup)(nil)

func (i *MergeableIndexLookup) ID() string              { return strings.Join(i.Indexes(), ",") }
func (i *MergeableIndexLookup) GetUnions() []string        { return i.Unions }
func (i *MergeableIndexLookup) GetIntersections() []string { return i.Intersections }

func (i *MergeableIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (i *MergeableIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
}

func (i *MergeableIndexLookup) Indexes() []string {
	var idxes = make([]string, len(i.Key))
	for i, e := range i.Key {
		idxes[i] = fmt.Sprint(e)
	}
	return idxes
}

func (i *MergeableIndexLookup) Difference(indexes ...sql.IndexLookup) sql.IndexLookup {
	panic("not implemented")
}

func (i *MergeableIndexLookup) Intersection(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections, unions []string
	for _, idx := range indexes {
		intersections = append(intersections, idx.(MergeableLookup).ID())
		intersections = append(intersections, idx.(MergeableLookup).GetIntersections()...)
		unions = append(unions, idx.(MergeableLookup).GetUnions()...)
	}

	// TODO: fix logic
	return &MergeableIndexLookup{
		Key:    i.Key,
		Index:  i.Index,
		Unions:  append(i.Unions, unions...),
		Intersections: append(i.Intersections, intersections...),
	}
}

func (i *MergeableIndexLookup) Union(indexes ...sql.IndexLookup) sql.IndexLookup {
	var intersections, unions []string
	for _, idx := range indexes {
		unions = append(unions, idx.(*MergeableIndexLookup).ID())
		unions = append(unions, idx.(*MergeableIndexLookup).Unions...)
		intersections = append(intersections, idx.(*MergeableIndexLookup).Intersections...)
	}

	// TODO: fix logic
	return &MergeableIndexLookup{
		Key:    i.Key,
		Index:  i.Index,
		Unions:  append(i.Unions, unions...),
		Intersections: append(i.Intersections, intersections...),
	}
}

type MergedIndexLookup struct {
	Children []sql.IndexLookup
}

func (i *MergedIndexLookup) Values(sql.Partition) (sql.IndexValueIter, error) {
	return nil, nil
//	return &dummyIndexValueIter{}, nil
}

func (i *MergedIndexLookup) Indexes() []string {
	var indexes []string
	for _, c := range i.Children {
		indexes = append(indexes, c.Indexes()...)
	}
	return indexes
}

func (i *MergedIndexLookup) IsMergeable(sql.IndexLookup) bool {
	return true
}

func (i *MergedIndexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	return &MergedIndexLookup{append(i.Children, lookups...)}
}

func (MergedIndexLookup) Difference(...sql.IndexLookup) sql.IndexLookup {
	panic("mergedIndexLookup.Difference is not implemented")
}

func (MergedIndexLookup) Intersection(...sql.IndexLookup) sql.IndexLookup {
	panic("mergedIndexLookup.Intersection is not implemented")
}

type MergeableDummyIndex struct {
	DB         string // required for engine tests with driver
	DriverName string // required for engine tests with driver
	Tbl        *Table // required for engine tests with driver
	TableName  string
	Exprs      []sql.Expression
}

func (i MergeableDummyIndex) Database() string { return i.DB }
func (i MergeableDummyIndex) Driver() string   { return i.DriverName }
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
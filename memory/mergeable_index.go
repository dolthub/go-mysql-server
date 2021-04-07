// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type MergeableIndex struct {
	DB         string // required for engine tests with driver
	DriverName string // required for engine tests with driver
	Tbl        *Table // required for engine tests with driver
	TableName  string
	Exprs      []sql.Expression
	Name       string
	Unique     bool
	CommentStr string
}

var _ sql.Index = (*MergeableIndex)(nil)
var _ sql.AscendIndex = (*MergeableIndex)(nil)
var _ sql.DescendIndex = (*MergeableIndex)(nil)
var _ sql.NegateIndex = (*MergeableIndex)(nil)

func (i *MergeableIndex) Database() string                    { return i.DB }
func (i *MergeableIndex) Driver() string                      { return i.DriverName }
func (i *MergeableIndex) MemTable() *Table                    { return i.Tbl }
func (i *MergeableIndex) ColumnExpressions() []sql.Expression { return i.Exprs }
func (i *MergeableIndex) IsGenerated() bool                   { return false }

func (i *MergeableIndex) Expressions() []string {
	var exprs []string
	for _, e := range i.Exprs {
		exprs = append(exprs, e.String())
	}
	return exprs
}

func (i *MergeableIndex) IsUnique() bool {
	return i.Unique
}

func (i *MergeableIndex) Comment() string {
	return i.CommentStr
}

func (i *MergeableIndex) IndexType() string {
	if len(i.DriverName) > 0 {
		return i.DriverName
	}
	return "BTREE" // fake but so are you
}

func (i *MergeableIndex) AscendGreaterOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	return &AscendIndexLookup{Gte: keys, Index: i}, nil
}

func (i *MergeableIndex) AscendLessThan(keys ...interface{}) (sql.IndexLookup, error) {
	return &AscendIndexLookup{Lt: keys, Index: i}, nil
}

func (i *MergeableIndex) AscendRange(greaterOrEqual, lessThan []interface{}) (sql.IndexLookup, error) {
	return &AscendIndexLookup{Gte: greaterOrEqual, Lt: lessThan, Index: i}, nil
}

func (i *MergeableIndex) DescendGreater(keys ...interface{}) (sql.IndexLookup, error) {
	return &DescendIndexLookup{Gt: keys, Index: i}, nil
}

func (i *MergeableIndex) DescendLessOrEqual(keys ...interface{}) (sql.IndexLookup, error) {
	return &DescendIndexLookup{Lte: keys, Index: i}, nil
}

func (i *MergeableIndex) DescendRange(lessOrEqual, greaterThan []interface{}) (sql.IndexLookup, error) {
	return &DescendIndexLookup{Gt: greaterThan, Lte: lessOrEqual, Index: i}, nil
}

func (i *MergeableIndex) Not(keys ...interface{}) (sql.IndexLookup, error) {
	lookup, err := i.Get(keys...)
	if err != nil {
		return nil, err
	}

	mergeable, _ := lookup.(*MergeableIndexLookup)
	return &NegateIndexLookup{Lookup: mergeable, Index: mergeable.Index}, nil
}

func (i *MergeableIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	return &MergeableIndexLookup{Key: key, Index: i}, nil
}

func (i *MergeableIndex) Has(sql.Partition, ...interface{}) (bool, error) {
	panic("not implemented")
}

func (i *MergeableIndex) ID() string {
	if len(i.Name) > 0 {
		return i.Name
	}

	if len(i.Exprs) == 1 {
		return i.Exprs[0].String()
	}
	var parts = make([]string, len(i.Exprs))
	for i, e := range i.Exprs {
		parts[i] = e.String()
	}

	return "(" + strings.Join(parts, ", ") + ")"
}

func (i *MergeableIndex) Table() string { return i.TableName }

// All lookups in this package, except for UnmergeableLookup, are MergeableLookups. The IDs are mostly for testing /
// verification purposes.
type MergeableLookup interface {
	ID() string
}

// ExpressionsIndex is an index made out of one or more expressions (usually field expressions), linked to a Table.
type ExpressionsIndex interface {
	MemTable() *Table
	ColumnExpressions() []sql.Expression
}

// MergeableIndexLookup is a lookup linked to an ExpressionsIndex. It can be merged with any other MergeableIndexLookup.  All lookups in this package are Merge
type MergeableIndexLookup struct {
	Key   []interface{}
	Index ExpressionsIndex
}

// memoryIndexLookup is a lookup that defines an expression to evaluate which rows are part of the index values
type memoryIndexLookup interface {
	EvalExpression() sql.Expression
}

var _ sql.MergeableIndexLookup = (*MergeableIndexLookup)(nil)
var _ memoryIndexLookup = (*MergeableIndexLookup)(nil)

func (i *MergeableIndexLookup) ID() string     { return strings.Join(i.Indexes(), ",") }
func (i *MergeableIndexLookup) String() string { return strings.Join(i.Indexes(), ",") }

func (i *MergeableIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (i *MergeableIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	var exprs []sql.Expression
	for exprI, expr := range i.Index.ColumnExpressions() {
		lit, typ := getType(i.Key[exprI])
		if typ == sql.Null {
			exprs = append(exprs, expression.NewIsNull(expr))
		} else {
			exprs = append(exprs, expression.NewEquals(expr, expression.NewLiteral(lit, typ)))
		}
	}

	return &indexValIter{
		tbl:             i.Index.MemTable(),
		partition:       p,
		matchExpression: and(exprs...),
	}, nil
}

func (i *MergeableIndexLookup) EvalExpression() sql.Expression {
	var exprs []sql.Expression
	for exprI, expr := range i.Index.ColumnExpressions() {
		lit, typ := getType(i.Key[exprI])
		if typ == sql.Null {
			exprs = append(exprs, expression.NewIsNull(expr))
		} else {
			exprs = append(exprs, expression.NewEquals(expr, expression.NewLiteral(lit, typ)))
		}
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

func (i *MergeableIndexLookup) Intersection(lookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	return intersection(i.Index, i, lookups...), nil
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
		Index:         idx,
	}
}

func (i *MergeableIndexLookup) Union(lookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	return union(i.Index, i, lookups...), nil
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
		Index:  idx,
	}
}

// MergedIndexLookup is an index lookup that has been merged with another.
// Exactly one of the Unions or Intersections fields should be set, and correspond to a logical AND or OR operation,
// respectively.
type MergedIndexLookup struct {
	Unions        []sql.IndexLookup
	Intersections []sql.IndexLookup
	Index         ExpressionsIndex
}

var _ sql.MergeableIndexLookup = (*MergedIndexLookup)(nil)
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

func (m *MergedIndexLookup) Intersection(lookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	return intersection(m.Index, m, lookups...), nil
}

func (m *MergedIndexLookup) Union(lookups ...sql.IndexLookup) (sql.IndexLookup, error) {
	return union(m.Index, m, lookups...), nil
}

func (m *MergedIndexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	_, ok := lookup.(MergeableLookup)
	return ok
}

func (m *MergedIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return &indexValIter{
		tbl:             m.Index.MemTable(),
		partition:       p,
		matchExpression: m.EvalExpression(),
	}, nil
}

func (m *MergedIndexLookup) Indexes() []string {
	panic("not implemented")
}

func (m *MergedIndexLookup) String() string {
	return "mergedIndexLookup"
}

func (m *MergedIndexLookup) ID() string {
	return "mergedIndexLookup"
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

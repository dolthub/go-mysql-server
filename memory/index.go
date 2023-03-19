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
	"time"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

const CommentPreventingIndexBuilding = "__FOR TESTING: I cannot be built__"

type Index struct {
	DB         string // required for engine tests with driver
	DriverName string // required for engine tests with driver
	Tbl        *Table // required for engine tests with driver
	TableName  string
	Exprs      []sql.Expression
	Name       string
	Unique     bool
	Spatial    bool
	CommentStr string
	PrefixLens []uint16
}

var _ sql.Index = (*Index)(nil)
var _ sql.FilteredIndex = (*Index)(nil)
var _ sql.OrderedIndex = (*Index)(nil)

func (idx *Index) Database() string                    { return idx.DB }
func (idx *Index) Driver() string                      { return idx.DriverName }
func (idx *Index) MemTable() *Table                    { return idx.Tbl }
func (idx *Index) ColumnExpressions() []sql.Expression { return idx.Exprs }
func (idx *Index) IsGenerated() bool                   { return false }

func (idx *Index) Expressions() []string {
	var exprs []string
	for _, e := range idx.Exprs {
		exprs = append(exprs, e.String())
	}
	return exprs
}

func (idx *Index) CanSupport(...sql.Range) bool {
	return true
}

func (idx *Index) IsUnique() bool {
	return idx.Unique
}

func (idx *Index) IsSpatial() bool {
	return idx.Spatial
}

func (idx *Index) Comment() string {
	return idx.CommentStr
}

func (idx *Index) PrefixLengths() []uint16 {
	return idx.PrefixLens
}

func (idx *Index) IndexType() string {
	if len(idx.DriverName) > 0 {
		return idx.DriverName
	}
	return "BTREE" // fake but so are you
}

// NewLookup implements the interface sql.Index.
func (idx *Index) rangeFilterExpr(ranges ...sql.Range) (sql.Expression, error) {
	if idx.CommentStr == CommentPreventingIndexBuilding {
		return nil, nil
	}
	if len(ranges) == 0 {
		return nil, nil
	}
	if len(ranges[0]) != len(idx.Exprs) {
		return nil, fmt.Errorf("expected different key count: %s=>%d/%d", idx.Name, len(idx.Exprs), len(ranges[0]))
	}

	var rangeCollectionExpr sql.Expression
	for _, rang := range ranges {
		var rangeExpr sql.Expression
		for i, rce := range rang {
			var rangeColumnExpr sql.Expression
			switch rce.Type() {
			// Both Empty and All may seem like strange inclusions, but if only one range is given we need some
			// expression to evaluate, otherwise our expression would be a nil expression which would panic.
			case sql.RangeType_Empty:
				rangeColumnExpr = expression.NewEquals(expression.NewLiteral(1, types.Int8), expression.NewLiteral(2, types.Int8))
			case sql.RangeType_All:
				rangeColumnExpr = expression.NewEquals(expression.NewLiteral(1, types.Int8), expression.NewLiteral(1, types.Int8))
			case sql.RangeType_EqualNull:
				rangeColumnExpr = expression.NewIsNull(idx.Exprs[i])
			case sql.RangeType_GreaterThan:
				if sql.RangeCutIsBinding(rce.LowerBound) {
					rangeColumnExpr = expression.NewGreaterThan(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.LowerBound), rce.Typ.Promote()))
				} else {
					rangeColumnExpr = expression.NewNot(expression.NewIsNull(idx.Exprs[i]))
				}
			case sql.RangeType_GreaterOrEqual:
				rangeColumnExpr = expression.NewGreaterThanOrEqual(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.LowerBound), rce.Typ.Promote()))
			case sql.RangeType_LessThanOrNull:
				rangeColumnExpr = or(
					expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote())),
					expression.NewIsNull(idx.Exprs[i]),
				)
			case sql.RangeType_LessOrEqualOrNull:
				rangeColumnExpr = or(
					expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote())),
					expression.NewIsNull(idx.Exprs[i]),
				)
			case sql.RangeType_ClosedClosed:
				rangeColumnExpr = and(
					expression.NewGreaterThanOrEqual(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.LowerBound), rce.Typ.Promote())),
					expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote())),
				)
			case sql.RangeType_OpenOpen:
				if sql.RangeCutIsBinding(rce.LowerBound) {
					rangeColumnExpr = and(
						expression.NewGreaterThan(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.LowerBound), rce.Typ.Promote())),
						expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote())),
					)
				} else {
					// Lower bound is (NULL, ...)
					rangeColumnExpr = expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote()))
				}
			case sql.RangeType_OpenClosed:
				if sql.RangeCutIsBinding(rce.LowerBound) {
					rangeColumnExpr = and(
						expression.NewGreaterThan(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.LowerBound), rce.Typ.Promote())),
						expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote())),
					)
				} else {
					// Lower bound is (NULL, ...]
					rangeColumnExpr = expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote()))
				}
			case sql.RangeType_ClosedOpen:
				rangeColumnExpr = and(
					expression.NewGreaterThanOrEqual(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.LowerBound), rce.Typ.Promote())),
					expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(sql.GetRangeCutKey(rce.UpperBound), rce.Typ.Promote())),
				)
			}
			rangeExpr = and(rangeExpr, rangeColumnExpr)
		}
		rangeCollectionExpr = or(rangeCollectionExpr, rangeExpr)
	}
	return rangeCollectionExpr, nil
}

// ColumnExpressionTypes implements the interface sql.Index.
func (idx *Index) ColumnExpressionTypes() []sql.ColumnExpressionType {
	cets := make([]sql.ColumnExpressionType, len(idx.Exprs))
	for i, expr := range idx.Exprs {
		cets[i] = sql.ColumnExpressionType{
			Expression: expr.String(),
			Type:       expr.Type(),
		}
	}
	return cets
}

func (idx *Index) ID() string {
	if len(idx.Name) > 0 {
		return idx.Name
	}

	if len(idx.Exprs) == 1 {
		return idx.Exprs[0].String()
	}
	var parts = make([]string, len(idx.Exprs))
	for i, e := range idx.Exprs {
		parts[i] = e.String()
	}

	return "(" + strings.Join(parts, ", ") + ")"
}

func (idx *Index) Table() string { return idx.TableName }

func (idx *Index) HandledFilters(filters []sql.Expression) []sql.Expression {
	var handled []sql.Expression
	if idx.Spatial {
		return handled
	}
	for _, expr := range filters {
		if expression.ContainsImpreciseComparison(expr) {
			continue
		}
		handled = append(handled, expr)
	}
	return handled
}

// validateIndexType returns the best comparison type between the two given types, as it takes into consideration
// whether the types contain collations.
func (idx *Index) validateIndexType(valType sql.Type, rangeType sql.Type) sql.Type {
	if _, ok := rangeType.(sql.TypeWithCollation); ok {
		return rangeType.Promote()
	}
	return valType
}

// ExpressionsIndex is an index made out of one or more expressions (usually field expressions), linked to a Table.
type ExpressionsIndex interface {
	sql.Index
	MemTable() *Table
	ColumnExpressions() []sql.Expression
}

func getType(val interface{}) (interface{}, sql.Type) {
	switch val := val.(type) {
	case int:
		return int64(val), types.Int64
	case uint:
		return int64(val), types.Int64
	case int8:
		return int64(val), types.Int64
	case uint8:
		return int64(val), types.Int64
	case int16:
		return int64(val), types.Int64
	case uint16:
		return int64(val), types.Int64
	case int32:
		return int64(val), types.Int64
	case uint32:
		return int64(val), types.Int64
	case int64:
		return int64(val), types.Int64
	case uint64:
		return int64(val), types.Int64
	case float32:
		return float64(val), types.Float64
	case float64:
		return float64(val), types.Float64
	case string:
		return val, types.LongText
	case nil:
		return nil, types.Null
	case time.Time:
		return val, types.Datetime
	default:
		panic(fmt.Sprintf("Unsupported type for %v of type %T", val, val))
	}
}

func (idx *Index) Order() sql.IndexOrder {
	return sql.IndexOrderAsc
}

func or(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	if expressions[0] == nil {
		return or(expressions[1:]...)
	}
	return expression.NewOr(expressions[0], or(expressions[1:]...))
}

func and(expressions ...sql.Expression) sql.Expression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	if expressions[0] == nil {
		return and(expressions[1:]...)
	}
	return expression.NewAnd(expressions[0], and(expressions[1:]...))
}

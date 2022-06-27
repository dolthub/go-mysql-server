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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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
	CommentStr string
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

func (idx *Index) IsUnique() bool {
	return idx.Unique
}

func (idx *Index) Comment() string {
	return idx.CommentStr
}

func (idx *Index) IndexType() string {
	if len(idx.DriverName) > 0 {
		return idx.DriverName
	}
	return "BTREE" // fake but so are you
}

// NewLookup implements the interface sql.Index.
func (idx *Index) NewLookup(ctx *sql.Context, ranges ...sql.Range) (sql.IndexLookup, error) {
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
				rangeColumnExpr = expression.NewEquals(expression.NewLiteral(1, sql.Int8), expression.NewLiteral(2, sql.Int8))
			case sql.RangeType_All:
				rangeColumnExpr = expression.NewEquals(expression.NewLiteral(1, sql.Int8), expression.NewLiteral(1, sql.Int8))
			case sql.RangeType_EqualNull:
				rangeColumnExpr = expression.NewIsNull(idx.Exprs[i])
			case sql.RangeType_GreaterThan:
				if sql.RangeCutIsBinding(rce.LowerBound) {
					lit, typ := getType(sql.GetRangeCutKey(rce.LowerBound))
					rangeColumnExpr = expression.NewGreaterThan(idx.Exprs[i], expression.NewLiteral(lit, typ))
				} else {
					rangeColumnExpr = expression.NewNot(expression.NewIsNull(idx.Exprs[i]))
				}
			case sql.RangeType_GreaterOrEqual:
				lit, typ := getType(sql.GetRangeCutKey(rce.LowerBound))
				rangeColumnExpr = expression.NewGreaterThanOrEqual(idx.Exprs[i], expression.NewLiteral(lit, typ))
			case sql.RangeType_LessThanOrNull:
				lit, typ := getType(sql.GetRangeCutKey(rce.UpperBound))
				rangeColumnExpr = or(
					expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(lit, typ)),
					expression.NewIsNull(idx.Exprs[i]),
				)
			case sql.RangeType_LessOrEqualOrNull:
				lit, typ := getType(sql.GetRangeCutKey(rce.UpperBound))
				rangeColumnExpr = or(
					expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(lit, typ)),
					expression.NewIsNull(idx.Exprs[i]),
				)
			case sql.RangeType_ClosedClosed:
				lowLit, lowTyp := getType(sql.GetRangeCutKey(rce.LowerBound))
				upLit, upTyp := getType(sql.GetRangeCutKey(rce.UpperBound))
				rangeColumnExpr = and(
					expression.NewGreaterThanOrEqual(idx.Exprs[i], expression.NewLiteral(lowLit, lowTyp)),
					expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(upLit, upTyp)),
				)
			case sql.RangeType_OpenOpen:
				upLit, upTyp := getType(sql.GetRangeCutKey(rce.UpperBound))
				if sql.RangeCutIsBinding(rce.LowerBound) {
					lowLit, lowTyp := getType(sql.GetRangeCutKey(rce.LowerBound))
					rangeColumnExpr = and(
						expression.NewGreaterThan(idx.Exprs[i], expression.NewLiteral(lowLit, lowTyp)),
						expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(upLit, upTyp)),
					)
				} else {
					// Lower bound is (NULL, ...)
					rangeColumnExpr = expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(upLit, upTyp))
				}
			case sql.RangeType_OpenClosed:
				upLit, upTyp := getType(sql.GetRangeCutKey(rce.UpperBound))
				if sql.RangeCutIsBinding(rce.LowerBound) {
					lowLit, lowTyp := getType(sql.GetRangeCutKey(rce.LowerBound))
					rangeColumnExpr = and(
						expression.NewGreaterThan(idx.Exprs[i], expression.NewLiteral(lowLit, lowTyp)),
						expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(upLit, upTyp)),
					)
				} else {
					// Lower bound is (NULL, ...]
					rangeColumnExpr = expression.NewLessThanOrEqual(idx.Exprs[i], expression.NewLiteral(upLit, upTyp))
				}
			case sql.RangeType_ClosedOpen:
				lowLit, lowTyp := getType(sql.GetRangeCutKey(rce.LowerBound))
				upLit, upTyp := getType(sql.GetRangeCutKey(rce.UpperBound))
				rangeColumnExpr = and(
					expression.NewGreaterThanOrEqual(idx.Exprs[i], expression.NewLiteral(lowLit, lowTyp)),
					expression.NewLessThan(idx.Exprs[i], expression.NewLiteral(upLit, upTyp)),
				)
			}
			rangeExpr = and(rangeExpr, rangeColumnExpr)
		}
		rangeCollectionExpr = or(rangeCollectionExpr, rangeExpr)
	}

	return NewIndexLookup(ctx, idx, rangeCollectionExpr, ranges...), nil
}

// ColumnExpressionTypes implements the interface sql.Index.
func (idx *Index) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType {
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
	return filters
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
		return int64(val), sql.Int64
	case uint:
		return int64(val), sql.Int64
	case int8:
		return int64(val), sql.Int64
	case uint8:
		return int64(val), sql.Int64
	case int16:
		return int64(val), sql.Int64
	case uint16:
		return int64(val), sql.Int64
	case int32:
		return int64(val), sql.Int64
	case uint32:
		return int64(val), sql.Int64
	case int64:
		return int64(val), sql.Int64
	case uint64:
		return int64(val), sql.Int64
	case float32:
		return float64(val), sql.Float64
	case float64:
		return float64(val), sql.Float64
	case string:
		return val, sql.LongText
	case nil:
		return nil, sql.Null
	case time.Time:
		return val, sql.Datetime
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

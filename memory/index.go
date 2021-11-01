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

func (i *Index) Database() string                    { return i.DB }
func (i *Index) Driver() string                      { return i.DriverName }
func (i *Index) MemTable() *Table                    { return i.Tbl }
func (i *Index) ColumnExpressions() []sql.Expression { return i.Exprs }
func (i *Index) IsGenerated() bool                   { return false }

func (i *Index) Expressions() []string {
	var exprs []string
	for _, e := range i.Exprs {
		exprs = append(exprs, e.String())
	}
	return exprs
}

func (i *Index) IsUnique() bool {
	return i.Unique
}

func (i *Index) Comment() string {
	return i.CommentStr
}

func (i *Index) IndexType() string {
	if len(i.DriverName) > 0 {
		return i.DriverName
	}
	return "BTREE" // fake but so are you
}

// NewLookup implements the interface sql.Index.
func (i *Index) NewLookup(ctx *sql.Context, ranges ...sql.Range) (sql.IndexLookup, error) {
	if i.CommentStr == CommentPreventingIndexBuilding {
		return nil, nil
	}
	if len(ranges) == 0 {
		return nil, nil
	}
	exprs := i.ColumnExpressions()
	if len(ranges[0]) > len(exprs) {
		return nil, fmt.Errorf("too many keys given: %s=>%d", i.Name, len(ranges[0]))
	}
	mergeableIndex := i
	if len(ranges[0]) < len(exprs) {
		mergeableIndex = i.Partial(len(ranges[0]))
		exprs = mergeableIndex.ColumnExpressions()
	}

	var completeExpr sql.Expression
	for _, rang := range ranges {
		var filterExpr sql.Expression
		for i, rangeColumn := range rang {
			var rangeExpr sql.Expression
			for _, rangeColumnExpr := range rangeColumn {
				switch rangeColumnExpr.Type() {
				// Both Empty and All may seem like strange inclusions, but if only one range is given we need some
				// expression to evaluate, otherwise our expression would be a nil expression which would panic.
				case sql.RangeType_Empty:
					rangeExpr = or(rangeExpr, expression.NewEquals(expression.NewLiteral(1, sql.Int8), expression.NewLiteral(2, sql.Int8)))
				case sql.RangeType_All:
					rangeExpr = or(rangeExpr, expression.NewEquals(expression.NewLiteral(1, sql.Int8), expression.NewLiteral(1, sql.Int8)))
				case sql.RangeType_GreaterThan:
					lit, typ := getType(sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
					rangeExpr = or(rangeExpr, expression.NewNullSafeGreaterThan(exprs[i], expression.NewLiteral(lit, typ)))
				case sql.RangeType_GreaterOrEqual:
					lit, typ := getType(sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
					rangeExpr = or(rangeExpr, expression.NewNullSafeGreaterThanOrEqual(exprs[i], expression.NewLiteral(lit, typ)))
				case sql.RangeType_LessThan:
					lit, typ := getType(sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
					rangeExpr = or(rangeExpr, expression.NewNullSafeLessThan(exprs[i], expression.NewLiteral(lit, typ)))
				case sql.RangeType_LessOrEqual:
					lit, typ := getType(sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
					rangeExpr = or(rangeExpr, expression.NewNullSafeLessThanOrEqual(exprs[i], expression.NewLiteral(lit, typ)))
				case sql.RangeType_ClosedClosed:
					if ok, err := rangeColumnExpr.RepresentsEquals(); err != nil {
						return nil, err
					} else if ok {
						lit, typ := getType(sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
						if typ == sql.Null {
							rangeExpr = or(rangeExpr, expression.NewIsNull(exprs[i]))
						} else {
							rangeExpr = or(rangeExpr, expression.NewNullSafeEquals(exprs[i], expression.NewLiteral(lit, typ)))
						}
					} else {
						lowLit, lowTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
						upLit, upTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
						rangeExpr = or(rangeExpr,
							and(
								expression.NewNullSafeGreaterThanOrEqual(exprs[i], expression.NewLiteral(lowLit, lowTyp)),
								expression.NewNullSafeLessThanOrEqual(exprs[i], expression.NewLiteral(upLit, upTyp)),
							),
						)
					}
				case sql.RangeType_OpenOpen:
					lowLit, lowTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
					upLit, upTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
					rangeExpr = or(rangeExpr,
						and(
							expression.NewNullSafeGreaterThan(exprs[i], expression.NewLiteral(lowLit, lowTyp)),
							expression.NewNullSafeLessThan(exprs[i], expression.NewLiteral(upLit, upTyp)),
						),
					)
				case sql.RangeType_OpenClosed:
					lowLit, lowTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
					upLit, upTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
					rangeExpr = or(rangeExpr,
						and(
							expression.NewNullSafeGreaterThan(exprs[i], expression.NewLiteral(lowLit, lowTyp)),
							expression.NewNullSafeLessThanOrEqual(exprs[i], expression.NewLiteral(upLit, upTyp)),
						),
					)
				case sql.RangeType_ClosedOpen:
					lowLit, lowTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.LowerBound))
					upLit, upTyp := getType(sql.GetRangeCutKey(rangeColumnExpr.UpperBound))
					rangeExpr = or(rangeExpr,
						and(
							expression.NewNullSafeGreaterThanOrEqual(exprs[i], expression.NewLiteral(lowLit, lowTyp)),
							expression.NewNullSafeLessThan(exprs[i], expression.NewLiteral(upLit, upTyp)),
						),
					)
				}
			}
			if rangeExpr == nil {
				continue
			}
			filterExpr = and(filterExpr, rangeExpr)
		}
		completeExpr = or(completeExpr, filterExpr)
	}

	return NewIndexLookup(ctx, mergeableIndex, completeExpr, ranges...), nil
}

// ColumnExpressionTypes implements the interface sql.Index.
func (i *Index) ColumnExpressionTypes(*sql.Context) []sql.ColumnExpressionType {
	cets := make([]sql.ColumnExpressionType, len(i.Exprs))
	for i, expr := range i.Exprs {
		cets[i] = sql.ColumnExpressionType{
			Expression: expr.String(),
			Type:       expr.Type(),
		}
	}
	return cets
}

func (i *Index) ID() string {
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

func (i *Index) Table() string { return i.TableName }

func (i *Index) Partial(len int) *Index {
	mi := *i
	mi.Exprs = i.Exprs[:len]
	return &mi
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

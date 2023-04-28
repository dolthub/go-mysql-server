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
	"github.com/dolthub/go-mysql-server/sql/types"
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

func (idx *Index) rangeFilterExpr(ranges ...sql.Range) (sql.Expression, error) {
	if idx.CommentStr == CommentPreventingIndexBuilding {
		return nil, nil
	}
	return expression.NewRangeFilterExpr(idx.Exprs, ranges)
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

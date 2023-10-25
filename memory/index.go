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

	"github.com/dolthub/go-mysql-server/sql/fulltext"

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
	Spatial    bool
	Fulltext   bool
	CommentStr string
	PrefixLens []uint16
	fulltextInfo
}

type fulltextInfo struct {
	PositionTableName    string
	DocCountTableName    string
	GlobalCountTableName string
	RowCountTableName    string
	fulltext.KeyColumns
}

var _ sql.Index = (*Index)(nil)
var _ sql.FilteredIndex = (*Index)(nil)
var _ sql.OrderedIndex = (*Index)(nil)
var _ sql.ExtendedIndex = (*Index)(nil)
var _ fulltext.Index = (*Index)(nil)

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

func (idx *Index) ExtendedExpressions() []string {
	var exprs []string
	foundCols := make(map[string]struct{})
	for _, e := range idx.Exprs {
		foundCols[strings.ToLower(e.(*expression.GetField).Name())] = struct{}{}
		exprs = append(exprs, e.String())
	}
	for _, ord := range idx.Tbl.data.schema.PkOrdinals {
		col := idx.Tbl.data.schema.Schema[ord]
		if _, ok := foundCols[strings.ToLower(col.Name)]; !ok {
			exprs = append(exprs, fmt.Sprintf("%s.%s", idx.Tbl.name, col.Name))
		}
	}
	return exprs
}

// ExtendedExprs returns the same information as ExtendedExpressions, but in sql.Expression form.
func (idx *Index) ExtendedExprs() []sql.Expression {
	var exprs []sql.Expression
	foundCols := make(map[string]struct{})
	for _, e := range idx.Exprs {
		foundCols[strings.ToLower(e.(*expression.GetField).Name())] = struct{}{}
		exprs = append(exprs, e)
	}
	for _, ord := range idx.Tbl.data.schema.PkOrdinals {
		col := idx.Tbl.data.schema.Schema[ord]
		if _, ok := foundCols[strings.ToLower(col.Name)]; !ok {
			exprs = append(exprs, expression.NewGetFieldWithTable(ord, col.Type, idx.Tbl.name, col.Name, col.Nullable))
		}
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

func (idx *Index) IsFullText() bool {
	return idx.Fulltext
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

func (idx *Index) rowToIndexStorage(row sql.Row, partitionName string, rowIdx int) (sql.Row, error) {
	if idx.Name == "PRIMARY" {
		return row, nil
	}

	exprs := idx.ExtendedExprs()
	newRow := make(sql.Row, len(exprs)+1)
	for i, expr := range exprs {
		var err error
		newRow[i], err = expr.Eval(nil, row)
		if err != nil {
			return nil, err
		}
	}
	newRow[len(exprs)] = primaryRowLocation{
		partition: partitionName,
		idx:       rowIdx,
	}

	return newRow, nil
}

func (idx *Index) rangeFilterExpr(ctx *sql.Context, ranges ...sql.Range) (sql.Expression, error) {
	if idx.CommentStr == CommentPreventingIndexBuilding {
		return nil, nil
	}

	return expression.NewRangeFilterExpr(idx.ExtendedExprs(), ranges)
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

func (idx *Index) ExtendedColumnExpressionTypes() []sql.ColumnExpressionType {
	cets := make([]sql.ColumnExpressionType, 0, len(idx.Tbl.data.schema.Schema))
	cetsInExprs := make(map[string]struct{})
	for _, expr := range idx.Exprs {
		cetsInExprs[strings.ToLower(expr.(*expression.GetField).Name())] = struct{}{}
		cets = append(cets, sql.ColumnExpressionType{
			Expression: expr.String(),
			Type:       expr.Type(),
		})
	}
	for _, ord := range idx.Tbl.data.schema.PkOrdinals {
		col := idx.Tbl.data.schema.Schema[ord]
		if _, ok := cetsInExprs[strings.ToLower(col.Name)]; !ok {
			cets = append(cets, sql.ColumnExpressionType{
				Expression: fmt.Sprintf("%s.%s", idx.Tbl.name, col.Name),
				Type:       col.Type,
			})
		}
	}
	return cets
}

func (idx *Index) FullTextTableNames(ctx *sql.Context) (fulltext.IndexTableNames, error) {
	return fulltext.IndexTableNames{
		Config:      idx.Tbl.data.fullTextConfigTableName,
		Position:    idx.fulltextInfo.PositionTableName,
		DocCount:    idx.fulltextInfo.DocCountTableName,
		GlobalCount: idx.fulltextInfo.GlobalCountTableName,
		RowCount:    idx.fulltextInfo.RowCountTableName,
	}, nil
}

func (idx *Index) FullTextKeyColumns(ctx *sql.Context) (fulltext.KeyColumns, error) {
	return idx.fulltextInfo.KeyColumns, nil
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

func (idx *Index) Order() sql.IndexOrder {
	return sql.IndexOrderAsc
}

func (idx *Index) Reversible() bool {
	return true
}

func (idx Index) copy() *Index {
	return &idx
}

// columnIndexes returns the indexes in the given schema for the fields in this index
func (idx *Index) columnIndexes(schema sql.Schema) []int {
	indexes := make([]int, len(idx.Exprs))
	for i, expr := range idx.Exprs {
		gf, ok := expr.(*expression.GetField)
		if !ok {
			panic(fmt.Sprintf("expected GetField expression, got %T", expr))
		}
		indexes[i] = schema.IndexOfColName(gf.Name())
	}
	return indexes
}

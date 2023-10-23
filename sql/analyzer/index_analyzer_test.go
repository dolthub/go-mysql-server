// Copyright 2021 Dolthub, Inc.
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

package analyzer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestMatchingIndexes(t *testing.T) {
	ctx := sql.NewEmptyContext()
	const testDb = "mydb"
	const testTable = "test"

	v1 := expression.NewLiteral(1, types.Int64)
	v2 := expression.NewLiteral(2, types.Int64)
	v3 := expression.NewLiteral(3, types.Int64)

	dummy1 := &dummyIdx{
		id:       "dummy1",
		expr:     []sql.Expression{v1},
		database: testDb,
		table:    testTable,
	}
	dummy2 := &dummyIdx{
		id:       "dummy2",
		expr:     []sql.Expression{v1, v2, v3},
		database: testDb,
		table:    testTable,
	}
	dummy3 := &dummyIdx{
		id:       "dummy3",
		expr:     []sql.Expression{v1, v2},
		database: testDb,
		table:    testTable,
	}
	dummy4 := &dummyIdx{
		id:       "dummy4",
		expr:     []sql.Expression{v3, v2, v1},
		database: testDb,
		table:    testTable,
	}

	ia := &indexAnalyzer{
		indexesByTable: map[string][]sql.Index{testTable: {dummy1, dummy2, dummy3, dummy4}},
		indexRegistry:  nil,
		registryIdxes:  nil,
	}

	tableId := sql.TableID{
		DatabaseName: testDb,
		TableName:    testTable,
	}
	require.Equal(t, []sql.Index{dummy1, dummy2, dummy3}, ia.MatchingIndexes(ctx, tableId, v1))
	require.Equal(t, []sql.Index{dummy3, dummy2}, ia.MatchingIndexes(ctx, tableId, v2, v1))
	require.Equal(t, []sql.Index{dummy2, dummy4}, ia.MatchingIndexes(ctx, tableId, v3, v1))
	require.Equal(t, []sql.Index{dummy2, dummy4}, ia.MatchingIndexes(ctx, tableId, v2, v3, v1))
	require.Equal(t, []sql.Index{dummy4}, ia.MatchingIndexes(ctx, tableId, v3))
	require.Equal(t, []sql.Index{dummy4}, ia.MatchingIndexes(ctx, tableId, v2, v3))
	require.Equal(t, dummy1, ia.MatchingIndex(ctx, tableId, v1))
	require.Equal(t, dummy3, ia.MatchingIndex(ctx, tableId, v2, v1))
	require.Equal(t, dummy2, ia.MatchingIndex(ctx, tableId, v3, v1))
	require.Equal(t, dummy2, ia.MatchingIndex(ctx, tableId, v2, v3, v1))
	require.Equal(t, dummy4, ia.MatchingIndex(ctx, tableId, v3))
	require.Equal(t, dummy4, ia.MatchingIndex(ctx, tableId, v2, v3))
}

func TestExpressionsWithIndexesPartialMatching(t *testing.T) {
	const testDb = "mydb"
	const testTable = "test"

	v1 := expression.NewLiteral(1, types.Int64)
	v2 := expression.NewLiteral(2, types.Int64)
	v3 := expression.NewLiteral(3, types.Int64)
	v4 := expression.NewLiteral(4, types.Int64)

	gf1 := expression.NewGetField(0, types.Int64, "1", false)
	gf2 := expression.NewGetField(1, types.Int64, "2", false)
	//gf3 := expression.NewGetField(2, sql.Int64, "3", false)
	gf4 := expression.NewGetField(3, types.Int64, "4", false)

	dummy1 := &dummyIdx{
		id:       "dummy",
		expr:     []sql.Expression{v1, v2, v3},
		database: testDb,
		table:    testTable,
	}
	dummy2 := &dummyIdx{
		id:       "dummy",
		expr:     []sql.Expression{v2, v4, v1, v3},
		database: testDb,
		table:    testTable,
	}

	ia := &indexAnalyzer{
		indexesByTable: map[string][]sql.Index{testTable: {dummy1}},
		indexRegistry:  nil,
		registryIdxes:  nil,
	}
	exprList := ia.ExpressionsWithIndexes(testDb, gf1, gf2)
	require.Equal(t, [][]sql.Expression{{gf1, gf2}}, exprList)

	ia = &indexAnalyzer{
		indexesByTable: map[string][]sql.Index{testTable: {dummy1, dummy2}},
		indexRegistry:  nil,
		registryIdxes:  nil,
	}
	exprList = ia.ExpressionsWithIndexes(testDb, gf2, gf4, gf1)
	require.Equal(t, [][]sql.Expression{{gf2, gf4, gf1}, {gf1, gf2}}, exprList)
}

type dummyIdx struct {
	id       string
	expr     []sql.Expression
	database string
	table    string
}

var _ sql.Index = (*dummyIdx)(nil)

func (i dummyIdx) CanSupport(r ...sql.Range) bool {
	return false
}

func (i dummyIdx) Expressions() []string {
	var exprs []string
	for _, e := range i.expr {
		exprs = append(exprs, e.String())
	}
	return exprs
}
func (i *dummyIdx) ID() string              { return i.id }
func (i *dummyIdx) Database() string        { return i.database }
func (i *dummyIdx) Table() string           { return i.table }
func (i *dummyIdx) IsUnique() bool          { return false }
func (i *dummyIdx) IsSpatial() bool         { return false }
func (i *dummyIdx) IsFullText() bool        { return false }
func (i *dummyIdx) Comment() string         { return "" }
func (i *dummyIdx) IsGenerated() bool       { return false }
func (i *dummyIdx) IndexType() string       { return "BTREE" }
func (i *dummyIdx) PrefixLengths() []uint16 { return nil }

func (i *dummyIdx) NewLookup(*sql.Context, ...sql.Range) (sql.IndexLookup, error) {
	panic("not implemented")
}
func (i *dummyIdx) ColumnExpressionTypes() []sql.ColumnExpressionType {
	panic("not implemented")
}

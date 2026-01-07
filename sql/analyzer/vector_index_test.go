// Copyright 2024 Dolthub, Inc.
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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/types/jsontests"
)

var database = "mydb"

func jsonExpression(t *testing.T, val interface{}) sql.Expression {
	return expression.NewLiteral(jsontests.ConvertToJson(t, val), types.JSON)
}

type vectorIndexTestCase struct {
	name            string
	usesVectorIndex bool
	inputPlan       sql.Node
	expectedPlan    string
	expectedRows    []sql.Row
}

func vectorIndexTestCases(t *testing.T, db *memory.Database, table sql.IndexedTable) []vectorIndexTestCase {
	return []vectorIndexTestCase{
		{
			name: "without limit",
			inputPlan: plan.NewSort(
				sql.SortFields{
					{Column: vector.NewDistance(vector.DistanceL2Squared{}, jsonExpression(t, "[0.0, 0.0]"), expression.NewGetFieldWithTable(2, 1, types.JSON, "", "test-table", "v", false)), Order: sql.Ascending},
				}, plan.NewResolvedTable(table, db, nil)),
			usesVectorIndex: false,
			expectedRows: []sql.Row{
				sql.NewRow(int64(3), jsontests.ConvertToJson(t, "[1.0, 1.0]")),
				sql.NewRow(int64(2), jsontests.ConvertToJson(t, "[2.0, 2.0]")),
				sql.NewRow(int64(1), jsontests.ConvertToJson(t, "[3.0, 4.0]")),
			},
		},
		{
			name: "with limit",
			inputPlan: plan.NewTopN(
				sql.SortFields{
					{Column: vector.NewDistance(vector.DistanceL2Squared{}, jsonExpression(t, "[0.0, 0.0]"), expression.NewGetFieldWithTable(2, 1, types.JSON, "", "test-table", "v", false)), Order: sql.Ascending},
				}, expression.NewLiteral(1, types.Int64), plan.NewResolvedTable(table, db, nil)),
			usesVectorIndex: true,
			expectedPlan: `
IndexedTableAccess(test)
 ├─ index: [test-table.v]
 └─ order: VEC_DISTANCE_L2_SQUARED([0, 0], test-table.v) LIMIT 1 (bigint)
`,
			expectedRows: []sql.Row{
				sql.NewRow(int64(3), jsontests.ConvertToJson(t, "[1.0, 1.0]")),
			},
		},
	}
}

func TestVectorIndex(t *testing.T) {

	db := memory.NewDatabase("db")
	provider := memory.NewDBProvider(db)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(sql.NewBaseSession(), provider)))

	childSchema := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "pk", Type: types.Int64, Nullable: false},
		{Name: "v", Type: types.JSON, Nullable: false},
	})
	child := memory.NewTable(db.BaseDatabase, "test", childSchema, nil)

	rows := []sql.Row{
		sql.NewRow(int64(1), jsontests.ConvertToJson(t, "[3.0, 4.0]")),
		sql.NewRow(int64(2), jsontests.ConvertToJson(t, "[2.0, 2.0]")),
		sql.NewRow(int64(3), jsontests.ConvertToJson(t, "[1.0, 1.0]")),
	}

	for _, r := range rows {
		require.NoError(t, child.Insert(ctx, r))
	}

	indexLookup := sql.IndexLookup{
		Index:           &vectorIndex,
		Ranges:          nil,
		IsPointLookup:   false,
		IsEmptyRange:    false,
		IsSpatialLookup: false,
		IsReverse:       false,
	}

	vectorIndexTable := vectorIndexTable{child.IndexedAccess(ctx, indexLookup)}

	for _, testCase := range vectorIndexTestCases(t, db, vectorIndexTable) {
		t.Run(testCase.name, func(t *testing.T) {
			res, same, err := replaceIdxOrderByDistanceHelper(nil, nil, testCase.inputPlan, nil, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.usesVectorIndex, !bool(same))
			res = offsetAssignIndexes(res)
			if testCase.usesVectorIndex {
				require.Equal(t,
					strings.TrimSpace(testCase.expectedPlan),
					strings.TrimSpace(res.String()),
					"expected:\n%s,\nfound:\n%s\n", testCase.expectedPlan, res.String())
			}

			iter, err := rowexec.NewBuilder(nil, sql.EngineOverrides{}).Build(ctx, res, nil)
			require.NoError(t, err)
			rows, err = sql.RowIterToRows(ctx, iter)
			require.NoError(t, err)

			require.Equal(t, rows, testCase.expectedRows)
		})
	}
}

func TestShowCreateTableWithVectorIndex(t *testing.T) {
	var require = require.New(t)

	db := memory.NewDatabase("test")
	pro := memory.NewDBProvider(db)
	ctx := newContext(pro)

	schema := sql.Schema{
		&sql.Column{Name: "pk", Source: "test-table", Type: types.Int32, Nullable: true, PrimaryKey: true},
		&sql.Column{Name: "v", Source: "test-table", Type: types.JSON, Default: nil, Nullable: true},
	}

	table := memory.NewTable(db.BaseDatabase, "test-table", sql.NewPrimaryKeySchema(schema), &memory.ForeignKeyCollection{})

	showCreateTable, err := plan.NewShowCreateTable(plan.NewResolvedTable(table, nil, nil), false).WithTargetSchema(schema)
	require.NoError(err)

	// This mimics what happens during analysis (indexes get filled in for the table)
	showCreateTable.(*plan.ShowCreateTable).Indexes = []sql.Index{
		&vectorIndex,
	}

	rowIter, _ := rowexec.NewBuilder(nil, sql.EngineOverrides{}).Build(ctx, showCreateTable, nil)

	row, err := rowIter.Next(ctx)

	require.NoError(err)

	expected := sql.NewRow(
		table.Name(),
		"CREATE TABLE `test-table` (\n  `pk` int,\n"+
			"  `v` json,\n"+
			"  PRIMARY KEY (`pk`),\n"+
			"  VECTOR KEY `test` (`v`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
	)

	require.Equal(expected, row)
}

type vectorIndexTable struct {
	underlying sql.IndexedTable
}

var _ sql.IndexAddressable = (*indexSearchableTable)(nil)
var _ sql.IndexedTable = (*indexSearchableTable)(nil)

func (i vectorIndexTable) Name() string {
	return i.underlying.Name()
}

func (i vectorIndexTable) String() string {
	return i.underlying.String()
}

func (i vectorIndexTable) Schema() sql.Schema {
	return i.underlying.Schema()
}

func (i vectorIndexTable) Collation() sql.CollationID {
	return i.underlying.Collation()
}

func (i vectorIndexTable) Comment() string {
	if ct, ok := i.underlying.(sql.CommentedTable); ok {
		return ct.Comment()
	}
	return ""
}

func (i vectorIndexTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	return i.underlying.Partitions(context)
}

func (i vectorIndexTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	return i.underlying.PartitionRows(context, partition)
}

func (i vectorIndexTable) SkipIndexCosting() bool {
	return true
}

func (i vectorIndexTable) IndexWithPrefix(ctx *sql.Context, expressions []string) (sql.Index, error) {
	panic("implement me")
}

func (i vectorIndexTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return i
}

var vectorIndex = memory.Index{
	DB:         database,
	DriverName: "",
	Tbl:        nil,
	TableName:  "test-table",
	Exprs: []sql.Expression{
		expression.NewGetFieldWithTable(1, 1, types.JSON, "", "test-table", "v", false),
	},
	Name:                    "test",
	Unique:                  false,
	Spatial:                 false,
	Fulltext:                false,
	SupportedVectorFunction: vector.DistanceL2Squared{},
	CommentStr:              "",
	PrefixLens:              nil,
}

func (i vectorIndexTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return []sql.Index{&vectorIndex}, nil
}

func (i vectorIndexTable) PreciseMatch() bool {
	return false
}
func (i vectorIndexTable) LookupPartitions(context *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	return i.underlying.LookupPartitions(context, lookup)
}

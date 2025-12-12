// Copyright 2023-2024 Dolthub, Inc.
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

package sqle

import (
	"context"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/sql/variables"
)

func TestBindingsToExprs(t *testing.T) {
	type tc struct {
		Name     string
		Bindings map[string]*query.BindVariable
		Result   map[string]sql.Expression
		Err      bool
	}

	cases := []tc{
		{
			"Empty",
			map[string]*query.BindVariable{},
			map[string]sql.Expression{},
			false,
		},
		{
			"BadInt",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_INT8, Value: []byte("axqut")},
			},
			nil,
			true,
		},
		{
			"BadUint",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_UINT8, Value: []byte("-12")},
			},
			nil,
			true,
		},
		{
			"BadDecimal",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_DECIMAL, Value: []byte("axqut")},
			},
			nil,
			true,
		},
		{
			"BadBit",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_BIT, Value: []byte{byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0)}},
			},
			nil,
			true,
		},
		{
			"BadDate",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_DATE, Value: []byte("00000000")},
			},
			nil,
			true,
		},
		{
			"BadYear",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_YEAR, Value: []byte("asdf")},
			},
			nil,
			true,
		},
		{
			"BadDatetime",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_DATETIME, Value: []byte("0000")},
			},
			nil,
			true,
		},
		{
			"BadTimestamp",
			map[string]*query.BindVariable{
				"v1": &query.BindVariable{Type: query.Type_TIMESTAMP, Value: []byte("0000")},
			},
			nil,
			true,
		},
		{
			"SomeTypes",
			map[string]*query.BindVariable{
				"i8":        &query.BindVariable{Type: query.Type_INT8, Value: []byte("12")},
				"u64":       &query.BindVariable{Type: query.Type_UINT64, Value: []byte("4096")},
				"bin":       &query.BindVariable{Type: query.Type_VARBINARY, Value: []byte{byte(0xC0), byte(0x00), byte(0x10)}},
				"text":      &query.BindVariable{Type: query.Type_TEXT, Value: []byte("four score and seven years ago...")},
				"bit":       &query.BindVariable{Type: query.Type_BIT, Value: []byte{byte(0x0f)}},
				"date":      &query.BindVariable{Type: query.Type_DATE, Value: []byte("2020-10-20")},
				"year":      &query.BindVariable{Type: query.Type_YEAR, Value: []byte("2020")},
				"datetime":  &query.BindVariable{Type: query.Type_DATETIME, Value: []byte("2020-10-20T12:00:00Z")},
				"timestamp": &query.BindVariable{Type: query.Type_TIMESTAMP, Value: []byte("2020-10-20T12:00:00Z")},
			},
			map[string]sql.Expression{
				"i8":        expression.NewLiteral(int64(12), types.Int64),
				"u64":       expression.NewLiteral(uint64(4096), types.Uint64),
				"bin":       expression.NewLiteral([]byte{byte(0xC0), byte(0x00), byte(0x10)}, types.MustCreateBinary(query.Type_VARBINARY, int64(3))),
				"text":      expression.NewLiteral("four score and seven years ago...", types.MustCreateStringWithDefaults(query.Type_TEXT, 33)),
				"bit":       expression.NewLiteral(uint64(0x0f), types.MustCreateBitType(types.BitTypeMaxBits)),
				"date":      expression.NewLiteral(time.Date(2020, time.Month(10), 20, 0, 0, 0, 0, time.UTC), types.Date),
				"year":      expression.NewLiteral(int16(2020), types.Year),
				"datetime":  expression.NewLiteral(time.Date(2020, time.Month(10), 20, 12, 0, 0, 0, time.UTC), types.MustCreateDatetimeType(query.Type_DATETIME, 6)),
				"timestamp": expression.NewLiteral(time.Date(2020, time.Month(10), 20, 12, 0, 0, 0, time.UTC), types.MustCreateDatetimeType(query.Type_TIMESTAMP, 6)),
			},
			false,
		},
	}

	ctx := sql.NewEmptyContext()
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			res, err := bindingsToExprs(ctx, c.Bindings)
			if !c.Err {
				require.NoError(t, err)
				require.Equal(t, c.Result, res)
			} else {
				require.Error(t, err, "%v", res)
			}
		})
	}
}

// wrapper around sql.Table to make it not indexable
type nonIndexableTable struct {
	*memory.Table
}

var _ memory.MemTable = (*nonIndexableTable)(nil)

func (t *nonIndexableTable) IgnoreSessionData() bool {
	return true
}

func getRuleFrom(rules []analyzer.Rule, id analyzer.RuleId) *analyzer.Rule {
	for _, rule := range rules {
		if rule.Id == id {
			return &rule
		}
	}

	return nil
}

// TODO: this was an analyzer test, but we don't have a mock process list for it to use, so it has to be here
func TestTrackProcess(t *testing.T) {
	require := require.New(t)
	variables.InitStatusVariables()
	db := memory.NewDatabase("db")
	provider := memory.NewDBProvider(db)
	a := analyzer.NewDefault(provider)
	sess := memory.NewSession(sql.NewBaseSession(), provider)

	node := plan.NewInnerJoin(
		plan.NewResolvedTable(&nonIndexableTable{memory.NewPartitionedTable(db.BaseDatabase, "foo", sql.PrimaryKeySchema{}, nil, 2)}, nil, nil),
		plan.NewResolvedTable(memory.NewPartitionedTable(db.BaseDatabase, "bar", sql.PrimaryKeySchema{}, nil, 4), nil, nil),
		expression.NewLiteral(int64(1), types.Int64),
	)

	pl := NewProcessList()

	ctx := sql.NewContext(context.Background(), sql.WithPid(1), sql.WithProcessList(pl), sql.WithSession(sess))
	pl.AddConnection(ctx.Session.ID(), "localhost")
	pl.ConnectionReady(ctx.Session)
	ctx, err := ctx.ProcessList.BeginQuery(ctx, "SELECT foo")
	require.NoError(err)

	rule := getRuleFrom(analyzer.OnceAfterAll, analyzer.TrackProcessId)
	result, _, err := rule.Apply(ctx, a, node, nil, analyzer.DefaultRuleSelector, nil)
	require.NoError(err)

	processes := ctx.ProcessList.Processes()
	require.Len(processes, 1)
	require.Equal("SELECT foo", processes[0].Query)
	require.Equal(
		map[string]sql.TableProgress{
			"foo": {
				Progress:           sql.Progress{Name: "foo", Done: 0, Total: 2},
				PartitionsProgress: map[string]sql.PartitionProgress{},
			},
			"bar": {
				Progress:           sql.Progress{Name: "bar", Done: 0, Total: 4},
				PartitionsProgress: map[string]sql.PartitionProgress{},
			},
		},
		processes[0].Progress)

	join, ok := result.(*plan.JoinNode)
	require.True(ok)
	require.Equal(plan.JoinTypeInner, join.JoinType())

	lhs, ok := join.Left().(*plan.ResolvedTable)
	require.True(ok)
	_, ok = lhs.Table.(*plan.ProcessTable)
	require.True(ok)

	rhs, ok := join.Right().(*plan.ResolvedTable)
	require.True(ok)
	_, ok = rhs.Table.(*plan.ProcessTable)
	require.True(ok)

	iter, err := rowexec.NewBuilder(nil, sql.EngineOverrides{}).Build(ctx, result, nil)
	require.NoError(err)
	iter, _, err = rowexec.FinalizeIters(ctx, result, nil, iter)
	require.NoError(err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(err)

	processes = ctx.ProcessList.Processes()
	require.Len(processes, 1)
	require.Equal(sql.ProcessCommandSleep, processes[0].Command)
	require.Error(ctx.Err())
}

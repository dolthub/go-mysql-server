package enginetest

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"testing"
)

// list of types, want t1->t2 for all types

func TestJoinTypes(t *testing.T, harness Harness) {
	var queries = []string{
		"select /*+ JOIN_ORDER(ab,xy) */ * from ab join xy on b = y",
		"select /*+ JOIN_ORDER(xy,ab) */ * from ab join xy on b = y",
	}
	for _, t1 := range joinTypes {
		for _, t2 := range joinTypes {
			if t1.typ.Equals(t2.typ) {
				continue
			}
			t.Run(fmt.Sprintf("%s->%s", t1.typ, t2.typ), func(t *testing.T) {
				e := mustNewEngine(t, harness)
				defer e.Close()

				ctx := NewContext(harness)
				RunQueryWithContext(t, e, harness, ctx, fmt.Sprintf("create table ab (a int primary key, b %s, key (b))", t1.typ))
				for i, val := range t1.sample {
					RunQueryWithContext(t, e, harness, ctx, fmt.Sprintf("insert into ab values (%d, %s)", i, val))
				}
				RunQueryWithContext(t, e, harness, ctx, fmt.Sprintf("create table xy (x int primary key, y %s, key (y))", t2.typ))
				for i, val := range t2.sample {
					RunQueryWithContext(t, e, harness, ctx, fmt.Sprintf("insert into xy values (%d, %s)", i, val))
				}

				for k, c := range biasedCosters {
					for _, q := range queries {
						e.EngineAnalyzer().Coster = c
						evalJoinCorrectness(t, harness, e, fmt.Sprintf("%s join: %s", k, q), q, nil, false)
					}
				}
			})
		}
	}
}

var joinTypes = []struct {
	typ    sql.Type
	sample []string
}{
	{
		typ:    types.Int8,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Int24,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Int32,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Int64,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Uint8,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Uint16,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Uint24,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Uint32,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Uint64,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Default),
		sample: []string{"'0'", "'1'", "'2'"},
	},
	{
		typ:    types.Text,
		sample: []string{"'0'", "'1'", "'2'"},
	},
	{
		typ:    types.Blob,
		sample: []string{"'0'", "'1'", "'2'"},
	},
	{
		typ:    types.JSON,
		sample: []string{"'0'", "'1'", "'2'"},
	},
	{
		typ:    types.InternalDecimalType,
		sample: []string{"'0'", "'1'", "'2'"},
	},
	{
		typ:    types.Float32,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Float64,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Timestamp,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Time,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Datetime,
		sample: []string{"0", "1", "2"},
	},
	{
		typ:    types.Year,
		sample: []string{"2000", "2001", "2002"},
	},
}

var JoinTypeTests = []joinOpTest{
	{
		name: "type tests",
		setup: [][]string{
			setup.MydbData[0],
			{
				"create table xy_int8 (x int primary key, y int, z int, key(y), key(z))",
				"create table xy_int16 (x int primary key, y int, z int, key(y), key(z))",
				"create table xy_int32 (x int primary key, y int, z int, key(y), key(z))",
				"create table xy_int64 (x int primary key, y int, z int, key(y), key(z))",
				"create table xy_uint8 (x int primary key, y int, z int, key(y), key(z))",
				"create table xy_uint8 (x int primary key, y int, z int, key(y), key(z))",
				"create table uv (u int primary key, v int, unique key(u,v))",
				"insert into xyz values (0,0,0),(1,1,1),(2,1,null),(3,2,null)",
				"insert into uv values (0,0),(1,1),(2,null),(3,null)",
			},
		},
		tests: []JoinOpTests{
			{
				Query:    "select x,u,z from xyz join uv on z = u where y = 1 order by 1,2",
				Expected: []sql.Row{{1, 1, 1}},
			},
		},
	},
}

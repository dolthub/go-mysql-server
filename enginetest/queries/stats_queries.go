// Copyright 2023 DoltHub, Inc.
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

package queries

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var StatisticsQueries = []ScriptTest{
	{
		Name: "analyze single int column",
		SetUpScript: []string{
			"CREATE TABLE t (i bigint primary key)",
			"INSERT INTO t VALUES (1), (2), (3)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(3, 3, 0, 24, time.Now(), sql.NewStatQualifier("mydb", "t", "primary"), []string{"i"}, []sql.Type{types.Int64}, []*stats.Bucket{
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(1)}, nil, nil),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(2)}, nil, nil),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(3)}, nil, nil),
					}, sql.IndexClassDefault),
					},
				},
			},
		},
	},
	{
		Name: "analyze update/drop",
		SetUpScript: []string{
			"CREATE TABLE t (i bigint primary key, j bigint, key(j))",
			"INSERT INTO t VALUES (1, 4), (2, 5), (3, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "analyze table t update histogram on (i) using data '{\"row_count\": 40, \"distinct_count\": 40, \"null_count\": 1, \"buckets\": [{\"row_count\": 20, \"distinct_count\": 20, \"upper_bound\": [50], \"bound_count\": 1}, {\"row_count\": 20, \"distinct_count\": 20, \"upper_bound\": [80], \"bound_count\": 1}]}'",
				Expected: []sql.Row{{"t", "histogram", "status", "OK"}},
			},
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(40, 40, 1, 0, time.Now(), sql.NewStatQualifier("mydb", "t", "primary"), []string{"i"}, []sql.Type{types.Int64}, []*stats.Bucket{
						stats.NewHistogramBucket(20, 20, 0, 1, sql.Row{float64(50)}, nil, nil),
						stats.NewHistogramBucket(20, 20, 0, 1, sql.Row{float64(80)}, nil, nil),
					}, sql.IndexClassDefault),
					},
				},
			},
			{
				Query:    "analyze table t drop histogram on (i)",
				Expected: []sql.Row{{"t", "histogram", "status", "OK"}},
			},
			{
				Query:    "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "analyze two int columns",
		SetUpScript: []string{
			"CREATE TABLE t (i bigint primary key, j bigint, key(j))",
			"INSERT INTO t VALUES (1, 4), (2, 5), (3, 6)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(3, 3, 0, 48, time.Now(), sql.NewStatQualifier("mydb", "t", "primary"), []string{"i"}, []sql.Type{types.Int64}, []*stats.Bucket{
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(1)}, nil, []sql.Row{}),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(2)}, nil, []sql.Row{}),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(3)}, nil, []sql.Row{}),
					}, sql.IndexClassDefault),
					},
					{"mydb", "t", "j", stats.NewStatistic(3, 3, 0, 48, time.Now(), sql.NewStatQualifier("mydb", "t", "j"), []string{"j"}, []sql.Type{types.Int64}, []*stats.Bucket{
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(4)}, nil, []sql.Row{}),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(5)}, nil, []sql.Row{}),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(6)}, nil, []sql.Row{}),
					}, sql.IndexClassDefault),
					},
				},
			},
		},
	},
	{
		Name: "analyze float columns",
		SetUpScript: []string{
			"CREATE TABLE t (i double primary key)",
			"INSERT INTO t VALUES (1.25), (45.25), (7.5), (10.5)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(4, 4, 0, 32, time.Now(), sql.NewStatQualifier("mydb", "t", "primary"), []string{"i"}, []sql.Type{types.Float64}, []*stats.Bucket{
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(1.25)}, nil, []sql.Row{}),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(7.5)}, nil, []sql.Row{}),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(10.5)}, nil, []sql.Row{}),
						stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(45.25)}, nil, []sql.Row{}),
					}, sql.IndexClassDefault),
					},
				},
			},
		},
	},
	{
		Name: "analyze empty table creates stats with 0s",
		SetUpScript: []string{
			"CREATE TABLE t (i float)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "analyze columns that can't be converted to float throws error",
		SetUpScript: []string{
			"CREATE TABLE t (t longtext)",
			"INSERT INTO t VALUES ('not a number')",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Query: `
		SELECT
			COLUMN_NAME,
			JSON_EXTRACT(HISTOGRAM, '$."number-of-buckets-specified"')
		FROM information_schema.COLUMN_STATISTICS
		WHERE SCHEMA_NAME = 'mydb'
		AND TABLE_NAME = 'mytable'
		`,
		Expected: nil,
	},
}

type StatsPlanTest struct {
	Name        string
	SetUpScript []string
	Query       string
	Expected    []sql.Row
	IndexName   string
}

var StatsIndexTests = []ScriptTest{
	{
		Name: "choose range over full prefix match",
		SetUpScript: []string{
			"create table xy (x int, y int, z varchar(36) default(uuid()), w varchar(10), key (z), key (y,w), key(x,y))",
			"insert into xy (x,y,w) values (1, 1, 'a'), (2,1,'a'), (3,1,'b'),(4,2,'b'),(5,2,'c')",
			`
analyze table xy update histogram on (x,y) using data '{
  "statistic": {
    "qual": {
      "database": "mydb",
      "tab": "xy",
      "idx": "xy"
    },
    "types:":["bigint","bigint"],
    "columns":["x", "y"],
    "buckets": [
      {"upper_bound": [1,1], "row_count": 1},
      {"upper_bound": [2,1], "row_count": 1},
      {"upper_bound": [3,1], "row_count": 1},
      {"upper_bound": [4,2], "row_count": 1},
      {"upper_bound": [5,2], "row_count": 1}
    ]
  }
}'`,
			`
analyze table xy update histogram on (y,w) using data '{
  "statistic": {
    "qual": {
      "database": "mydb",
      "tab": "xy",
      "idx": "yw"
    },    "types:":["bigint","varchar(10)"],
    "columns":["y", "w"],
    "buckets":[
      {"upper_bound": [1,"a"], "row_count": 2},
      {"upper_bound": [1,"b"], "row_count": 1},
      {"upper_bound": [2,"b"], "row_count": 1},
      {"upper_bound": [2,"c"], "row_count": 1}
    ]
  }
}'`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:              "select * from xy where x > 4 and y = 1 and w = 'a'",
				Expected:           []sql.Row{},
				CheckIndexedAccess: true,
				IndexName:          "xy",
			},
		},
	},
}

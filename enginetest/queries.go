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

package enginetest

import (
	"time"

	"github.com/dolthub/vitess/go/sqltypes"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type QueryTest struct {
	Query           string
	Expected        []sql.Row
	ExpectedColumns sql.Schema // only Name and Type matter here, because that's what we send on the wire
	Bindings        map[string]sql.Expression
}

var QueryTests = []QueryTest{
	{
		Query: "SELECT * FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT mytable.* FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `mytable`.* FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `i`, `s` FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT * FROM mytable ORDER BY i DESC;",
		Expected: []sql.Row{
			{int64(3), "third row"},
			{int64(2), "second row"},
			{int64(1), "first row"},
		},
	},
	{
		Query: "SELECT * FROM mytable GROUP BY i,s;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(14)},
			{int64(1), float64(54)},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) as sum FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(14)},
			{int64(1), float64(54)},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) + sum(c3) as sum FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(28)},
			{int64(1), float64(108)},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(c3) + min(c3) as sum_and_min FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(16)},
			{int64(1), float64(76)},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "pk DIV 2",
				Type: sql.Int64,
			},
			{
				Name: "sum_and_min",
				Type: sql.Float64,
			},
		},
	},
	{
		Query: "SELECT pk DIV 2, SUM(`c3`) +    min( c3 ) FROM one_pk GROUP BY 1 ORDER BY 1",
		Expected: []sql.Row{
			{int64(0), float64(16)},
			{int64(1), float64(76)},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "pk DIV 2",
				Type: sql.Int64,
			},
			{
				Name: "SUM(`c3`) +    min( c3 )",
				Type: sql.Float64,
			},
		},
	},
	{
		Query: "SELECT pk1, SUM(c1) FROM two_pk GROUP BY pk1 ORDER BY pk1;",
		Expected: []sql.Row{
			{0, 10.0},
			{1, 50.0},
		},
	},
	{
		Query:    "select max(pk),c2 from one_pk group by c1 order by 1",
		Expected: []sql.Row{{0, 1}, {1, 11}, {2, 21}, {3, 31}},
	},
	{
		Query:    "SELECT pk1, SUM(c1) FROM two_pk WHERE pk1 = 0",
		Expected: []sql.Row{{0, 10.0}},
	},
	{
		Query:    "SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i AS x FROM mytable ORDER BY i DESC",
		Expected: []sql.Row{{3}, {2}, {1}},
	},
	{
		Query: "SELECT i AS s, mt.s FROM mytable mt ORDER BY i DESC",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "s",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT i AS s, s FROM mytable mt ORDER BY i DESC",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
	},
	{
		Query:    "SELECT i AS x FROM mytable ORDER BY x DESC",
		Expected: []sql.Row{{3}, {2}, {1}},
	},
	{
		Query:    "SELECT i FROM mytable AS mt;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query: "SELECT s,i FROM mytable;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mytable.s,i FROM mytable;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT t.s,i FROM mytable AS t;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT s,i FROM (select i,s FROM mytable) mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "SELECT a,b FROM (select i,s FROM mytable) mt (a,b) order by 1;",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: "SELECT a,b FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
	},
	{
		Query: "SELECT a,b FROM (select i,s FROM mytable order by i desc) mt (a,b);",
		Expected: []sql.Row{
			{3, "third row"},
			{2, "second row"},
			{1, "first row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "a",
				Type: sql.Int64,
			},
			{
				Name: "b",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT a FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		Expected: []sql.Row{
			{3},
			{2},
			{1},
		},
	},
	{
		Query: `SELECT * FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a order by 1`,
		Expected: []sql.Row{
			{1.0, "ab"},
			{2, 4},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "column_0",
				Type: sql.Int64,
			},
			{
				Name: "column_1",
				Type: sql.Int64,
			},
		},
	},
	{
		Query: `SELECT * FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a (c,d) order by 1`,
		Expected: []sql.Row{
			{1.0, "ab"},
			{2, 4},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "c",
				Type: sql.Int64,
			},
			{
				Name: "d",
				Type: sql.Int64,
			},
		},
	},
	{
		Query: `SELECT column_0 FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a order by 1`,
		Expected: []sql.Row{
			{1.0},
			{2},
		},
	},
	{
		Query: `SELECT column_0, sum(column_1) FROM 
			(values row(1,1), row(1,3), row(2,2), row(2,5), row(3,9)) a 
			group by 1 order by 1`,
		Expected: []sql.Row{
			{1, 4.0},
			{2, 7.0},
			{3, 9.0},
		},
	},
	{
		Query: `SELECT B, sum(C) FROM 
			(values row(1,1), row(1,3), row(2,2), row(2,5), row(3,9)) a (b,c) 
			group by 1 order by 1`,
		Expected: []sql.Row{
			{1, 4.0},
			{2, 7.0},
			{3, 9.0},
		},
	},
	{
		Query: `SELECT i, sum(i) FROM mytable group by 1 having avg(i) > 1 order by 1`,
		Expected: []sql.Row{
			{2, 2.0},
			{3, 3.0},
		},
	},
	{
		Query: `SELECT a.column_0, b.column_1 FROM (values row(1+1,2+2), row(floor(1.5),concat("a","b"))) a
			join (values row(2,4), row(1.0,"ab")) b on a.column_0 = b.column_0 and a.column_0 = b.column_0
			order by 1`,
		Expected: []sql.Row{
			{1.0, "ab"},
			{2, 4},
		},
	},
	{
		Query: `SELECT a.column_0, mt.s from (values row(1,"1"), row(2,"2"), row(4,"4")) a
			left join mytable mt on column_0 = mt.i
			order by 1`,
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{4, nil},
		},
	},
	{
		Query: `SELECT * FROM (select * from mytable) a
			join (select * from mytable) b on a.i = b.i
			order by 1`,
		Expected: []sql.Row{
			{1, "first row", 1, "first row"},
			{2, "second row", 2, "second row"},
			{3, "third row", 3, "third row"},
		},
	},
	{
		Query:    "select * from mytable t1 join mytable t2 on t2.i = t1.i where t2.i > 10",
		Expected: []sql.Row{},
	},
	{
		Query:    "select * from mytable t1 join mytable T2 on t2.i = t1.i where T2.i > 10",
		Expected: []sql.Row{},
	},
	{
		Query:    "select * from tabletest t1 join tabletest t2 on t2.s = t1.s where t2.i > 10",
		Expected: []sql.Row{},
	},
	{
		Query: `select mt.i, 
			((
				select count(*) from mytable
            	where i in (
               		select mt2.i from mytable mt2 where mt2.i > mt.i
            	)
			)) as greater_count
			from mytable mt order by 1`,
		Expected: []sql.Row{{1, 2}, {2, 1}, {3, 0}},
	},
	{
		Query: `select mt.i, 
			((
				select count(*) from mytable
            	where i in (
               		select mt2.i from mytable mt2 where mt2.i = mt.i
            	)
			)) as eq_count
			from mytable mt order by 1`,
		Expected: []sql.Row{{1, 1}, {2, 1}, {3, 1}},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT a.s,b.i FROM mt a join mt b on a.i = b.i order by 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i, s from mt1)
			SELECT mt1.i, concat(mt2.s, '!') FROM mt1 join mt2 on mt1.i = mt2.i + 1 order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable order by i limit 2), mt2 as (select i, s from mt1)
			SELECT mt1.i, concat(mt2.s, '!') FROM mt1 join mt2 on mt1.i = mt2.i + 1 order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i+1 as i, concat(s, '!') as s from mt1)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i+1 as i, concat(s, '!') as s from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 (i,s) as (select i+1, concat(s, '!') from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select concat(s, '!') as s, i+1 as i from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i order by 1;`,
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i,s FROM mytable) SELECT s,i FROM mt;",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i+1, concat(s,'!') FROM mytable) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
			{4, "third row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i+1 as x, concat(s,'!') as y FROM mytable) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{2, "first row!"},
			{3, "second row!"},
			{4, "third row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i+1, concat(s,'!') FROM mytable order by 1 limit 1) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{2, "first row!"},
		},
	},
	{
		Query: "WITH mt (s,i) as (select char_length(s), sum(i) FROM mytable group by 1) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{9, 4.0},
			{10, 2.0},
		},
	},
	{
		Query: "WITH mt (s,i) as (select i, row_number() over (order by i desc) FROM mytable) SELECT s,i FROM mt order by 1",
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable)
			SELECT mtouter.i, (select s from mt1 where s = mtouter.s) FROM mt1 as mtouter where mtouter.i > 1 order by 1`,
		Expected: []sql.Row{
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		// TODO: ORDER BY should apply to the union. The parser is wrong.
		Query: `SELECT s2, i2, i
			FROM (SELECT * FROM mytable) mytable
			RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
			ON i2 = i`,
		Expected: []sql.Row{
			{"third", 1, 1},
			{"second", 2, 2},
			{"first", 3, 3},
			{"not found", 4, nil},
		},
	},
	{
		Query: `SELECT
			"testing" AS s,
			(SELECT max(i)
			 FROM (SELECT * FROM mytable) mytable
			 RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
				ON i2 = i) AS rj
			FROM DUAL`,
		Expected: []sql.Row{
			{"testing", 3},
		},
	},
	{
		Query: `SELECT
			"testing" AS s,
			(SELECT max(i2)
			 FROM (SELECT * FROM mytable) mytable
			 RIGHT JOIN
				((SELECT i2, s2 FROM othertable ORDER BY i2 ASC)
				 UNION ALL
				 SELECT CAST(4 AS SIGNED) AS i2, "not found" AS s2 FROM DUAL) othertable
				ON i2 = i) AS rj
			FROM DUAL`,
		Expected: []sql.Row{
			{"testing", 4},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable)
			SELECT mtouter.i, (select s from mt1 where i = mtouter.i+1) FROM mt1 as mtouter where mtouter.i > 1 order by 1`,
		Expected: []sql.Row{
			{2, "third row"},
			{3, nil},
		},
	},
	{
		Query: `WITH mt1 as (select i,s FROM mytable)
			SELECT mtouter.i, 
				(with mt2 as (select i,s FROM mt1) select s from mt2 where i = mtouter.i+1) 
			FROM mt1 as mtouter where mtouter.i > 1 order by 1`,
		Expected: []sql.Row{
			{2, "third row"},
			{3, nil},
		},
	},
	{
		Query: `WITH common_table AS (SELECT cec.id, cec.strength FROM (SELECT 1 as id, 12 as strength) cec) SELECT strength FROM common_table cte`,
		Expected: []sql.Row{
			{12},
		},
	},
	{
		Query: `WITH common_table AS (SELECT cec.id id, cec.strength FROM (SELECT 1 as id, 12 as strength) cec) SELECT strength FROM common_table cte`,
		Expected: []sql.Row{
			{12},
		},
	},
	{
		Query: `WITH common_table AS (SELECT cec.id AS id, cec.strength FROM (SELECT 1 as id, 12 as strength) cec) SELECT strength FROM common_table cte`,
		Expected: []sql.Row{
			{12},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION SELECT s, i FROM mt UNION SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION ALL SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt UNION ALL SELECT s, i FROM mt UNION ALL SELECT s, i FROM mt;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)},
		},
	},
	{
		Query: "SELECT s, (select i from mytable mt where sub.i = mt.i) as subi FROM (select i,s,'hello' FROM mytable where s = 'first row') as sub;",
		Expected: []sql.Row{
			{"first row", int64(1)},
		},
	},
	{
		Query: "SELECT (select s from mytable mt where sub.i = mt.i) as subi FROM (select i,s,'hello' FROM mytable where i = 1) as sub;",
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: "SELECT (select s from mytable mt where sub.i = mt.i) as subi FROM (select s,i,'hello' FROM mytable where i = 1) as sub;",
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: "SELECT s, (select i from mytable mt where sub.i = mt.i) as subi FROM (select 'hello',i,s FROM mytable where s = 'first row') as sub;",
		Expected: []sql.Row{
			{"first row", int64(1)},
		},
	},
	{
		Query: "SELECT (select s from mytable mt where sub.i = mt.i) as subi FROM (select 'hello',i,s FROM mytable where i = 1) as sub;",
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: "SELECT mytable.s FROM mytable WHERE mytable.i IN (SELECT othertable.i2 FROM othertable) ORDER BY mytable.i ASC",
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		Query: "SELECT mytable.s FROM mytable WHERE mytable.i = (SELECT othertable.i2 FROM othertable WHERE othertable.s2 = 'second')",
		Expected: []sql.Row{
			{"second row"},
		},
	},
	{
		Query: "SELECT mytable.s FROM mytable WHERE mytable.i IN (SELECT othertable.i2 FROM othertable WHERE CONCAT(othertable.s2, ' row') = mytable.s)",
		Expected: []sql.Row{
			{"second row"},
		},
	},
	{
		Query: "SELECT mytable.i, selfjoined.s FROM mytable LEFT JOIN (SELECT * FROM mytable) selfjoined ON mytable.i = selfjoined.i",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: "SELECT s,i FROM MyTable ORDER BY 2",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT S,I FROM MyTable ORDER BY 2",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mt.s,mt.i FROM MyTable MT ORDER BY 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mT.S,Mt.I FROM MyTable MT ORDER BY 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT mt.* FROM MyTable MT ORDER BY mT.I;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"}},
	},
	{
		Query: "SELECT MyTABLE.s,myTable.i FROM MyTable ORDER BY 2;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: `SELECT "Hello!", CONcat(s, "!") FROM MyTable`,
		Expected: []sql.Row{
			{"Hello!", "first row!"},
			{"Hello!", "second row!"},
			{"Hello!", "third row!"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "Hello!",
				Type: sql.LongText,
			},
			{
				Name: "CONcat(s, \"!\")",
				Type: sql.LongText,
			},
		},
	},
	{
		Query: `SELECT "1" + '1'`,
		Expected: []sql.Row{
			{2.0},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: `"1" + '1'`,
				Type: sql.Float64,
			},
		},
	},
	{
		Query: "SELECT myTable.* FROM MYTABLE ORDER BY myTable.i;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"}},
	},
	{
		Query: "SELECT MyTABLE.S,myTable.I FROM MyTable ORDER BY mytable.i;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT MyTABLE.S as S, myTable.I as I FROM MyTable ORDER BY mytable.i;",
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		Query: "SELECT i, 1 AS foo, 2 AS bar FROM MyTable WHERE bar = 2 ORDER BY foo, i;",
		Expected: []sql.Row{
			{1, 1, 2},
			{2, 1, 2},
			{3, 1, 2}},
	},
	{
		Query: "SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = 2) AS a ORDER BY foo, i",
		Expected: []sql.Row{
			{2, 1, 2}},
	},
	{
		Query: "SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = ?) AS a ORDER BY foo, i",
		Expected: []sql.Row{
			{2, 1, 2}},
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(int64(2), sql.Int64),
		},
	},
	{
		Query: "SELECT i, 1 AS foo, 2 AS bar FROM (SELECT i FROM mYtABLE WHERE i = :var) AS a WHERE bar = :var ORDER BY foo, i",
		Expected: []sql.Row{
			{2, 1, 2}},
		Bindings: map[string]sql.Expression{
			"var": expression.NewLiteral(int64(2), sql.Int64),
		},
	},
	{
		Query:    "SELECT i, 1 AS foo, 2 AS bar FROM MyTable WHERE bar = 1 ORDER BY foo, i;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i, 1 AS foo, 2 AS bar FROM MyTable WHERE bar = ? ORDER BY foo, i;",
		Expected: []sql.Row{},
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(int64(1), sql.Int64),
		},
	},
	{
		Query:    "SELECT i, 1 AS foo, 2 AS bar FROM MyTable WHERE bar = :bar AND foo = :foo ORDER BY foo, i;",
		Expected: []sql.Row{},
		Bindings: map[string]sql.Expression{
			"bar": expression.NewLiteral(int64(1), sql.Int64),
			"foo": expression.NewLiteral(int64(1), sql.Int64),
		},
	},
	{
		Query: "SELECT :foo * 2",
		Expected: []sql.Row{
			{2},
		},
		Bindings: map[string]sql.Expression{
			"foo": expression.NewLiteral(int64(1), sql.Int64),
		},
	},
	{
		Query: "SELECT i from mytable where i in (:foo, :bar) order by 1",
		Expected: []sql.Row{
			{1},
			{2},
		},
		Bindings: map[string]sql.Expression{
			"foo": expression.NewLiteral(int64(1), sql.Int64),
			"bar": expression.NewLiteral(int64(2), sql.Int64),
		},
	},
	{
		Query: "SELECT i from mytable where i = :foo * 2",
		Expected: []sql.Row{
			{2},
		},
		Bindings: map[string]sql.Expression{
			"foo": expression.NewLiteral(int64(1), sql.Int64),
		},
	},
	{
		Query: "SELECT i from mytable where 4 = :foo * 2 order by 1",
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
		Bindings: map[string]sql.Expression{
			"foo": expression.NewLiteral(int64(2), sql.Int64),
		},
	},
	{
		Query:    "SELECT timestamp FROM reservedWordsTable;",
		Expected: []sql.Row{{"1"}},
	},
	{
		Query:    "SELECT RW.TIMESTAMP FROM reservedWordsTable rw;",
		Expected: []sql.Row{{"1"}},
	},
	{
		Query:    "SELECT `AND`, RW.`Or`, `SEleCT` FROM reservedWordsTable rw;",
		Expected: []sql.Row{{"1.1", "aaa", "create"}},
	},
	{
		Query:    "SELECT reservedWordsTable.AND, reservedWordsTABLE.Or, reservedwordstable.SEleCT FROM reservedWordsTable;",
		Expected: []sql.Row{{"1.1", "aaa", "create"}},
	},
	{
		Query:    "SELECT i + 1 FROM mytable;",
		Expected: []sql.Row{{int64(2)}, {int64(3)}, {int64(4)}},
	},
	{
		Query:    "SELECT i div 2 FROM mytable order by 1;",
		Expected: []sql.Row{{int64(0)}, {int64(1)}, {int64(1)}},
	},
	{
		Query:    "SELECT i DIV 2 FROM mytable order by 1;",
		Expected: []sql.Row{{int64(0)}, {int64(1)}, {int64(1)}},
	},
	{
		Query:    "SELECT -i FROM mytable;",
		Expected: []sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		Query:    "SELECT +i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT + - i FROM mytable;",
		Expected: []sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE -i = -2;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE -i <=> -2;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i = 2;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 = i;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 <=> i;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i > 2;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 < i;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i < 2;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 > i;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i <> 2;",
		Expected: []sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		Query:    "SELECT NULL IN (SELECT i FROM emptytable)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT NULL NOT IN (SELECT i FROM emptytable)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT NULL IN (SELECT i FROM mytable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL NOT IN (SELECT i FROM mytable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL NOT IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 2 IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT 2 NOT IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 100 IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 100 NOT IN (SELECT i2 FROM niltable)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1 IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1 IN (2,3,4,null,1)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT 1 IN (1,2,3)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT 1 IN (2,3,4)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT NULL IN (2,3,4)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT 'a' IN ('b','c',null,'d')`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT 'a' IN ('a','b','c','d')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT 'a' IN ('b','c','d')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 1 NOT IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT 1 NOT IN (2,3,4,null,1)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 1 NOT IN (1,2,3)",
		Expected: []sql.Row{{false}},
	},
	{
		Query:    "SELECT 1 NOT IN (2,3,4)",
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT NULL NOT IN (2,3,4)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT NULL NOT IN (2,3,4,null)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT 'a' NOT IN ('b','c',null,'d')`,
		Expected: []sql.Row{{nil}},
		ExpectedColumns: sql.Schema{
			{
				Name: "'a' NOT IN ('b','c',null,'d')",
				Type: sql.Boolean,
			},
		},
	},
	{
		Query:    `SELECT 'a' NOT IN ('a','b','c','d')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT 'a' NOT IN ('b','c','d')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i IN (1, 3)",
		Expected: []sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i = 1 OR i = 3",
		Expected: []sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		Query:    "SELECT * FROM mytable WHERE i = 1 AND i = 2",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE i >= 2 ORDER BY 1",
		Expected: []sql.Row{{int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 <= i ORDER BY 1",
		Expected: []sql.Row{{int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i <= 2 ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 2 >= i ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i > 2",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i+1 > 3",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i < 2",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i >= 2 OR i = 1 ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 = 2.0;",
		Expected: []sql.Row{{float32(2.0)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 < 2.0;",
		Expected: []sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 > 2.0;",
		Expected: []sql.Row{{float32(2.5)}},
	},
	{
		Query:    "SELECT f32 FROM floattable WHERE f64 <> 2.0;",
		Expected: []sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}, {float32(2.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 = 2.0;",
		Expected: []sql.Row{{float64(2.0)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 = -1.5;",
		Expected: []sql.Row{{float64(-1.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE -f32 = -2.0;",
		Expected: []sql.Row{{float64(2.0)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 < 2.0;",
		Expected: []sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 > 2.0;",
		Expected: []sql.Row{{float64(2.5)}},
	},
	{
		Query:    "SELECT f64 FROM floattable WHERE f32 <> 2.0;",
		Expected: []sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}, {float64(2.5)}},
	},
	{
		Query:    "SELECT f32 FROM floattable ORDER BY f64;",
		Expected: []sql.Row{{float32(-1.5)}, {float32(-1.0)}, {float32(1.0)}, {float32(1.5)}, {float32(2.0)}, {float32(2.5)}},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i DESC;",
		Expected: []sql.Row{{int64(3)}, {int64(2)}, {int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE 'hello';",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT 'hello';",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT * FROM mytable WHERE i = 2 AND s = 'third row'",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 1;",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 0;",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 1 OFFSET 1;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query: "SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT ?;",
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(1, sql.Int8),
		},
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query: "SELECT i FROM mytable ORDER BY i LIMIT ? OFFSET 2;",
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(1, sql.Int8),
			"v2": expression.NewLiteral(1, sql.Int8),
		},
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM (SELECT * FROM (SELECT i as i, s as s FROM mytable) f) s)",
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT i FROM (SELECT 1 AS i FROM DUAL UNION SELECT 2 AS i FROM DUAL) some_is WHERE i NOT IN (SELECT i FROM (SELECT 1 as i FROM DUAL) different_is);",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 1,1;",
		Expected: []sql.Row{{int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 3,1;",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 2,100;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b IS NULL",
		Expected: []sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b <=> NULL",
		Expected: []sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE NULL <=> b",
		Expected: []sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		Query: "SELECT i FROM niltable WHERE b IS NOT NULL",
		Expected: []sql.Row{
			{int64(2)}, {int64(3)},
			{int64(5)}, {int64(6)},
		},
	},
	{
		Query: "SELECT i FROM niltable WHERE b",
		Expected: []sql.Row{
			{int64(2)},
			{int64(5)},
		},
	},
	{
		Query: "SELECT i FROM niltable WHERE NOT b",
		Expected: []sql.Row{
			{int64(3)},
			{int64(6)},
		},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b IS TRUE",
		Expected: []sql.Row{{int64(2)}, {int64(5)}},
	},
	{
		Query: "SELECT i FROM niltable WHERE b IS NOT TRUE",
		Expected: []sql.Row{
			{int64(1)}, {int64(3)},
			{int64(4)}, {int64(6)},
		},
	},
	{
		Query:    "SELECT f FROM niltable WHERE b IS FALSE",
		Expected: []sql.Row{{nil}, {6.0}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE f < 5",
		Expected: []sql.Row{{int64(4)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE f > 5",
		Expected: []sql.Row{{int64(6)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE b IS NOT FALSE",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(4)}, {int64(5)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE i2 IS NULL ORDER BY 1",
		Expected: []sql.Row{{int64(1)}, {int64(3)}, {int64(5)}},
	},
	{
		Query:    "SELECT i FROM niltable WHERE i2 IS NOT NULL ORDER BY 1",
		Expected: []sql.Row{{int64(2)}, {int64(4)}, {int64(6)}},
	},
	{
		Query:    "select i from datetime_table where date_col = date('2019-12-31T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019-12-31T00:00:00'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019-12-31T00:00:01'",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019-12-31'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col = '2019/12/31'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where date_col > '2019-12-31' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where date_col >= '2019-12-31' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where date_col > '2019/12/31' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where date_col > '2019-12-31T00:00:01' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col = date('2020-01-01T12:00:00')",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where datetime_col = '2020-01-01T12:00:00'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where datetime_col = datetime('2020-01-01T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where datetime_col = '2020-01-01T12:00:01'",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where datetime_col > '2020-01-01T12:00:00' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col > '2020-01-01' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col >= '2020-01-01' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col > '2020/01/01' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where datetime_col > datetime('2020-01-01T12:00:00') order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = date('2020-01-02T12:00:00')",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = '2020-01-02T12:00:00'",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = datetime('2020-01-02T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = timestamp('2020-01-02T12:00:00')",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col = '2020-01-02T12:00:01'",
		Expected: []sql.Row{},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > '2020-01-02T12:00:00' order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > '2020-01-02' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col >= '2020-01-02' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > '2020/01/02' order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select i from datetime_table where timestamp_col > datetime('2020-01-02T12:00:00') order by 1",
		Expected: []sql.Row{{2}, {3}},
	},
	{
		Query:    "SELECT dt1.i FROM datetime_table dt1 join datetime_table dt2 on dt1.timestamp_col = dt2.timestamp_col order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "SELECT dt1.i FROM datetime_table dt1 join datetime_table dt2 on dt1.date_col = date(date_sub(dt2.timestamp_col, interval 2 day)) order by 1",
		Expected: []sql.Row{{1}, {2}},
	},
	{
		Query:    "SELECT COUNT(*) FROM mytable;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT COUNT(*) FROM mytable LIMIT 1;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT COUNT(*) AS c FROM mytable;",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT substring(s, 2, 3) FROM mytable",
		Expected: []sql.Row{{"irs"}, {"eco"}, {"hir"}},
	},
	{
		Query:    `SELECT substring("foo", 2, 2)`,
		Expected: []sql.Row{{"oo"}},
	},
	{
		Query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', 2)`,
		Expected: []sql.Row{
			{"a.b"},
		},
	},
	{
		Query: `SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', -2)`,
		Expected: []sql.Row{
			{"e.f"},
		},
	},
	{
		Query: `SELECT SUBSTRING_INDEX(SUBSTRING_INDEX('source{d}', '{d}', 1), 'r', -1)`,
		Expected: []sql.Row{
			{"ce"},
		},
	},
	{
		Query:    `SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY 1 HAVING s = 'secon'`,
		Expected: []sql.Row{{"secon"}},
	},
	{
		Query:    "SELECT YEAR('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(2007)}, {int32(2007)}, {int32(2007)}},
	},
	{
		Query:    "SELECT MONTH('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(12)}, {int32(12)}, {int32(12)}},
	},
	{
		Query:    "SELECT DAY('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(11)}, {int32(11)}, {int32(11)}},
	},
	{
		Query:    "SELECT HOUR('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(20)}, {int32(20)}, {int32(20)}},
	},
	{
		Query:    "SELECT MINUTE('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(21)}, {int32(21)}, {int32(21)}},
	},
	{
		Query:    "SELECT SECOND('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('2007-12-11 20:21:22') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT SECOND('2007-12-11T20:21:22Z') FROM mytable",
		Expected: []sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('2007-12-11') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT DAYOFYEAR('20071211') FROM mytable",
		Expected: []sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		Query:    "SELECT YEARWEEK('0000-01-01')",
		Expected: []sql.Row{{int32(1)}},
	},
	{
		Query:    "SELECT YEARWEEK('9999-12-31')",
		Expected: []sql.Row{{int32(999952)}},
	},
	{
		Query:    "SELECT YEARWEEK('2008-02-20', 1)",
		Expected: []sql.Row{{int32(200808)}},
	},
	{
		Query:    "SELECT YEARWEEK('1987-01-01')",
		Expected: []sql.Row{{int32(198652)}},
	},
	{
		Query:    "SELECT YEARWEEK('1987-01-01', 20), YEARWEEK('1987-01-01', 1), YEARWEEK('1987-01-01', 2), YEARWEEK('1987-01-01', 3), YEARWEEK('1987-01-01', 4), YEARWEEK('1987-01-01', 5), YEARWEEK('1987-01-01', 6), YEARWEEK('1987-01-01', 7)",
		Expected: []sql.Row{{int32(198653), int32(198701), int32(198652), int32(198701), int32(198653), int32(198652), int32(198653), int32(198652)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i BETWEEN 1 AND 2",
		Expected: []sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		Query:    "SELECT i FROM mytable WHERE i NOT BETWEEN 1 AND 2",
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti > '2019-12-31'",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE da = '2019-12-31'",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti < '2019-12-31'",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da < '2019-12-31'",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti > date_add('2019-12-30', INTERVAL 1 day)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE da > date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da >= date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti < date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da < date_add('2019-12-30', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti > date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE da > date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da >= date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    "SELECT id FROM typestable WHERE ti < date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query:    "SELECT id FROM typestable WHERE da < date_sub('2020-01-01', INTERVAL 1 DAY)",
		Expected: nil,
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable) othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: []sql.Row{
			{"first", int64(3)},
		},
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable WHERE s2 = 'first') othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: []sql.Row{
			{"first", int64(3)},
		},
	},
	{
		Query: `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable WHERE i2 = 3) othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: []sql.Row{
			{"first", int64(3)},
		},
	},
	{
		Query:    `SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM othertable WHERE s2 = 'second') othertable_one) othertable_two) othertable_three WHERE s2 = 'first'`,
		Expected: nil,
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i",
		Expected: []sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i AND i",
		Expected: []sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i OR i",
		Expected: []sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT i",
		Expected: []sql.Row{{int64(0), "0"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT i AND NOT i",
		Expected: []sql.Row{{int64(0), "0"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE NOT i OR NOT i",
		Expected: []sql.Row{{int64(0), "0"}},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE i OR NOT i",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE v",
		Expected: []sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE v AND v",
		Expected: []sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		Query:    "SELECT i,v from stringandtable WHERE v OR v",
		Expected: []sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE NOT v AND NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE NOT v OR NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		Query: "SELECT i,v from stringandtable WHERE v OR NOT v",
		Expected: []sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{nil, "2"},
		},
	},
	{
		Query: "SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1",
		Expected: []sql.Row{
			{"third"},
			{"secon"},
			{"first"},
		},
	},
	{
		Query: "SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "SELECT /*+ JOIN_ORDER(t1,t2) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "SELECT /*+ JOIN_ORDER(t2,t1) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "SELECT /*+ JOIN_ORDER(t1) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "SELECT /*+ JOIN_ORDER(t1, mytable) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "SELECT /*+ JOIN_ORDER(t1, not_exist) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "SELECT /*+ NOTHING(abc) */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "SELECT /*+ JOIN_ORDER( */ t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 1",
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: "select mytable.i as i2, othertable.i2 as i from mytable join othertable on i = i2 order by 1",
		Expected: []sql.Row{
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR s = s2 order by 1",
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{2, "second row", 2, "second"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 order by 1, 3",
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{1, "first row", 3, "first"},
			{2, "second row", 2, "second"},
			{3, "third row", 1, "third"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR SUBSTRING_INDEX(s, ' ', 2) = s2 order by 1, 3",
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{1, "first row", 3, "first"},
			{2, "second row", 2, "second"},
			{3, "third row", 1, "third"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 OR SUBSTRING_INDEX(s, ' ', 2) = s2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 order by 1, 3",
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{1, "first row", 3, "first"},
			{2, "second row", 2, "second"},
			{3, "third row", 1, "third"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: "SELECT i, s, i2, s2 FROM mytable INNER JOIN othertable ON SUBSTRING_INDEX(s, ' ', 2) = s2 OR SUBSTRING_INDEX(s, ' ', 1) = s2 OR i = i2 order by 1, 3",
		Expected: []sql.Row{
			{1, "first row", 1, "third"},
			{1, "first row", 3, "first"},
			{2, "second row", 2, "second"},
			{3, "third row", 1, "third"},
			{3, "third row", 3, "first"},
		},
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2
				from mytable join othertable on i = i2 order by 1`,
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1},
		},
	},
	{
		Query: `select row_number() over (order by i desc), mytable.i as i2
				from mytable join othertable on i = i2
				where mytable.i = 3 order by 1`,
		Expected: []sql.Row{
			{1, 3},
		},
	},
	{
		Query:    "SELECT t1.i FROM mytable t1 JOIN mytable t2 on t1.i = t2.i + 1 where t1.i = 2 and t2.i = 3",
		Expected: []sql.Row{},
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
		Expected: []sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable as OTHERTABLE INNER JOIN othertable as MYTABLE ON i = i2 ORDER BY i",
		Expected: []sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE s2 >= "first" AND i2 >= 2 ORDER BY 1`,
		Expected: []sql.Row{
			{"first", int64(3)},
			{"second", int64(2)},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE "first" <= s2 AND 2 <= i2 ORDER BY 1`,
		Expected: []sql.Row{
			{"first", int64(3)},
			{"second", int64(2)},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE s2 <= "second" AND i2 <= 2 ORDER BY 1`,
		Expected: []sql.Row{
			{"second", int64(2)},
		},
	},
	{
		Query: `SELECT s2, i2 FROM othertable WHERE "second" >= s2 AND 2 >= i2 ORDER BY 1`,
		Expected: []sql.Row{
			{"second", int64(2)},
		},
	},
	{
		Query: "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
		Expected: []sql.Row{
			{"third", int64(1), int64(1)},
			{"second", int64(2), int64(2)},
			{"first", int64(3), int64(3)},
		},
	},
	{
		Query: "SELECT i, i2, s2 FROM othertable JOIN mytable  ON i = i2 ORDER BY i",
		Expected: []sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	{
		Query: "SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 ORDER BY i",
		Expected: []sql.Row{
			{"third", int64(1), int64(1)},
			{"second", int64(2), int64(2)},
			{"first", int64(3), int64(3)},
		},
	},
	{
		Query: "SELECT substring(s2, 1), substring(s2, 2), substring(s2, 3) FROM othertable ORDER BY i2",
		Expected: []sql.Row{
			{"third", "hird", "ird"},
			{"second", "econd", "cond"},
			{"first", "irst", "rst"},
		},
	},
	{
		Query: `SELECT substring("first", 1), substring("second", 2), substring("third", 3)`,
		Expected: []sql.Row{
			{"first", "econd", "ird"},
		},
	},
	{
		Query: "SELECT substring(s2, -1), substring(s2, -2), substring(s2, -3) FROM othertable ORDER BY i2",
		Expected: []sql.Row{
			{"d", "rd", "ird"},
			{"d", "nd", "ond"},
			{"t", "st", "rst"},
		},
	},
	{
		Query: `SELECT substring("first", -1), substring("second", -2), substring("third", -3)`,
		Expected: []sql.Row{
			{"t", "nd", "ird"},
		},
	},
	{
		Query: "SELECT s FROM mytable INNER JOIN othertable " +
			"ON substring(s2, 1, 2) != '' AND i = i2 ORDER BY 1",
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		Query: `SELECT i FROM mytable NATURAL JOIN tabletest`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		Query: `SELECT i FROM mytable AS t NATURAL JOIN tabletest AS test`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		Query: `SELECT t.i, test.s FROM mytable AS t NATURAL JOIN tabletest AS test`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT fi, COUNT(*) FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC, fi`,
		Expected: []sql.Row{
			{"first row", int64(1)},
			{"second row", int64(1)},
			{"third row", int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*), fi  FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC, fi`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY 2`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY fi`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: `SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY 2`,
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		Query: "SELECT CAST(-3 AS UNSIGNED) FROM mytable",
		Expected: []sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		Query: "SELECT CONVERT(-3, UNSIGNED) FROM mytable",
		Expected: []sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		Query: "SELECT '3' > 2 FROM tabletest",
		Expected: []sql.Row{
			{true},
			{true},
			{true},
		},
	},
	{
		Query: "SELECT s > 2 FROM tabletest",
		Expected: []sql.Row{
			{false},
			{false},
			{false},
		},
	},
	{
		Query:    "SELECT * FROM tabletest WHERE s > 0",
		Expected: nil,
	},
	{
		Query: "SELECT * FROM tabletest WHERE s = 0",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		Query: "SELECT * FROM tabletest WHERE s = 'first row'",
		Expected: []sql.Row{
			{int64(1), "first row"},
		},
	},
	{
		Query: "SELECT s FROM mytable WHERE i IN (1, 2, 5)",
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
		},
	},
	{
		Query: "SELECT s FROM mytable WHERE i NOT IN (1, 2, 5)",
		Expected: []sql.Row{
			{"third row"},
		},
	},
	{
		Query: "SELECT 1 + 2",
		Expected: []sql.Row{
			{int64(3)},
		},
	},
	{
		Query:    `SELECT i AS foo FROM mytable WHERE foo NOT IN (1, 2, 5)`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query: `SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		Expected: []sql.Row{
			{int64(1), "first row", int64(1), "first row", "third", int64(1)},
			{int64(1), "first row", int64(2), "second row", "second", int64(2)},
			{int64(1), "first row", int64(3), "third row", "first", int64(3)},
			{int64(2), "second row", int64(1), "first row", "third", int64(1)},
			{int64(2), "second row", int64(2), "second row", "second", int64(2)},
			{int64(2), "second row", int64(3), "third row", "first", int64(3)},
			{int64(3), "third row", int64(1), "first row", "third", int64(1)},
			{int64(3), "third row", int64(2), "second row", "second", int64(2)},
			{int64(3), "third row", int64(3), "third row", "first", int64(3)},
		},
	},
	{
		Query: `SELECT * FROM tabletest join mytable mt INNER JOIN othertable ot ON tabletest.i = ot.i2 order by 1,3,6`,
		Expected: []sql.Row{
			{int64(1), "first row", int64(1), "first row", "third", int64(1)},
			{int64(1), "first row", int64(2), "second row", "third", int64(1)},
			{int64(1), "first row", int64(3), "third row", "third", int64(1)},
			{int64(2), "second row", int64(1), "first row", "second", int64(2)},
			{int64(2), "second row", int64(2), "second row", "second", int64(2)},
			{int64(2), "second row", int64(3), "third row", "second", int64(2)},
			{int64(3), "third row", int64(1), "first row", "first", int64(3)},
			{int64(3), "third row", int64(2), "second row", "first", int64(3)},
			{int64(3), "third row", int64(3), "third row", "first", int64(3)},
		},
	},
	{
		Query: `SELECT split(s," ") FROM mytable`,
		Expected: []sql.Row{
			sql.NewRow([]interface{}{"first", "row"}),
			sql.NewRow([]interface{}{"second", "row"}),
			sql.NewRow([]interface{}{"third", "row"}),
		},
	},
	{
		Query: `SELECT split(s,"s") FROM mytable`,
		Expected: []sql.Row{
			sql.NewRow([]interface{}{"fir", "t row"}),
			sql.NewRow([]interface{}{"", "econd row"}),
			sql.NewRow([]interface{}{"third row"}),
		},
	},
	{
		Query:    `SELECT SUM(i) FROM mytable`,
		Expected: []sql.Row{{float64(6)}},
	},
	{
		Query:    `SELECT GET_LOCK("test", 0)`,
		Expected: []sql.Row{{int8(1)}},
	},
	{
		Query:    `SELECT IS_FREE_LOCK("test")`,
		Expected: []sql.Row{{int8(0)}},
	},
	{
		Query:    `SELECT RELEASE_LOCK("test")`,
		Expected: []sql.Row{{int8(1)}},
	},
	{
		Query:    `SELECT RELEASE_ALL_LOCKS()`,
		Expected: []sql.Row{{int32(0)}},
	},
	{
		Query: `SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		Expected: []sql.Row{
			{int64(3), "third row", "first", int64(3)},
		},
	},
	{
		Query: `SELECT * FROM othertable ot INNER JOIN mytable mt ON mt.i = ot.i2 AND mt.i > 2`,
		Expected: []sql.Row{
			{"first", int64(3), int64(3), "third row"},
		},
	},
	{
		Query: `SELECT i AS foo FROM mytable ORDER BY i DESC`,
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY i DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY 2 DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY foo DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY foo DESC`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT COUNT(*) c, i AS i FROM mytable GROUP BY 2`,
		Expected: []sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		Query: `SELECT i AS i FROM mytable GROUP BY 1`,
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query: `SELECT CONCAT("a", "b", "c")`,
		Expected: []sql.Row{
			{string("abc")},
		},
	},
	{
		Query: `SELECT COALESCE(NULL, NULL, NULL, 'example', NULL, 1234567890)`,
		Expected: []sql.Row{
			{string("example")},
		},
	},
	{
		Query: `SELECT COALESCE(NULL, NULL, NULL, COALESCE(NULL, 1234567890))`,
		Expected: []sql.Row{
			{int32(1234567890)},
		},
	},
	{
		Query: "SELECT concat(s, i) FROM mytable",
		Expected: []sql.Row{
			{string("first row1")},
			{string("second row2")},
			{string("third row3")},
		},
	},
	{
		Query: "SELECT version()",
		Expected: []sql.Row{
			{string("8.0.11")},
		},
	},
	{
		Query: `SELECT RAND(100)`,
		Expected: []sql.Row{
			{float64(0.8165026937796166)},
		},
	},
	{
		Query:    `SELECT RAND(i) from mytable order by i`,
		Expected: []sql.Row{{0.6046602879796196}, {0.16729663442585624}, {0.7199826688373036}},
	},
	{
		Query: `SELECT RAND(100) = RAND(100)`,
		Expected: []sql.Row{
			{true},
		},
	},
	{
		Query: `SELECT RAND() = RAND()`,
		Expected: []sql.Row{
			{false},
		},
	},
	{
		Query: "SELECT SIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.8414709848078965},
		},
	},
	{
		Query: "SELECT COS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.5403023058681398},
		},
	},
	{
		Query: "SELECT TAN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1.557407724654902},
		},
	},
	{
		Query: "SELECT ASIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1.5707963267948966},
		},
	},
	{
		Query: "SELECT ACOS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.0},
		},
	},
	{
		Query: "SELECT ATAN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.7853981633974483},
		},
	},
	{
		Query: "SELECT COT(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.6420926159343308},
		},
	},
	{
		Query: "SELECT DEGREES(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{57.29577951308232},
		},
	},
	{
		Query: "SELECT RADIANS(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{0.017453292519943295},
		},
	},
	{
		Query: "SELECT CRC32(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{uint64(0x83dcefb7)},
		},
	},
	{
		Query: "SELECT SIGN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "SELECT ASCII(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{uint64(0x66)},
		},
	},
	{
		Query: "SELECT HEX(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"666972737420726F77"},
		},
	},
	{
		Query: "SELECT UNHEX(s) from mytable order by i limit 1",
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: "SELECT BIN(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT BIT_LENGTH(i) from mytable order by i limit 1",
		Expected: []sql.Row{
			{64},
		},
	},
	{
		Query: "select date_format(datetime_col, '%D') from datetime_table order by 1",
		Expected: []sql.Row{
			{"1st"},
			{"4th"},
			{"7th"},
		},
	},
	{
		Query: "select from_unixtime(i) from mytable order by 1",
		Expected: []sql.Row{
			{time.Unix(1, 0)},
			{time.Unix(2, 0)},
			{time.Unix(3, 0)},
		},
	},
	// TODO: add additional tests for other functions. Every function needs an engine test to ensure it works correctly
	//  with the analyzer.
	{
		Query:    "SELECT * FROM mytable WHERE 1 > 5",
		Expected: nil,
	},
	{
		Query: "SELECT SUM(i) + 1, i FROM mytable GROUP BY i ORDER BY i",
		Expected: []sql.Row{
			{float64(2), int64(1)},
			{float64(3), int64(2)},
			{float64(4), int64(3)},
		},
	},
	{
		Query: "SELECT SUM(i), i FROM mytable GROUP BY i ORDER BY 1+SUM(i) ASC",
		Expected: []sql.Row{
			{float64(1), int64(1)},
			{float64(2), int64(2)},
			{float64(3), int64(3)},
		},
	},
	{
		Query: "SELECT SUM(i) as sum, i FROM mytable GROUP BY i ORDER BY 1+SUM(i) ASC",
		Expected: []sql.Row{
			{float64(1), int64(1)},
			{float64(2), int64(2)},
			{float64(3), int64(3)},
		},
	},
	{
		Query: "SELECT SUM(i) as sum, i FROM mytable GROUP BY i ORDER BY sum ASC",
		Expected: []sql.Row{
			{float64(1), int64(1)},
			{float64(2), int64(2)},
			{float64(3), int64(3)},
		},
	},
	{
		Query: "SELECT i, SUM(i) FROM mytable GROUP BY i ORDER BY sum(i) DESC",
		Expected: []sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		Query: "SELECT i, SUM(i) as b FROM mytable GROUP BY i ORDER BY b DESC",
		Expected: []sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		Query: "SELECT i, SUM(i) as `sum(i)` FROM mytable GROUP BY i ORDER BY sum(i) DESC",
		Expected: []sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		Query:    "SELECT i FROM mytable UNION SELECT i+10 FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION DISTINCT SELECT i+10 FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION ALL SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION DISTINCT SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION ALL SELECT i FROM mytable UNION DISTINCT SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT i FROM mytable UNION SELECT i FROM mytable UNION ALL SELECT i FROM mytable;",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query: "SELECT i FROM mytable UNION SELECT s FROM mytable;",
		Expected: []sql.Row{
			{"1"},
			{"2"},
			{"3"},
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		Query:    "",
		Expected: []sql.Row{},
	},
	{
		Query: "/*!40101 SET NAMES " +
			sql.Collation_Default.CharacterSet().String() +
			" */",
		Expected: []sql.Row{
			{},
		},
	},
	{
		Query:    `SHOW DATABASES`,
		Expected: []sql.Row{{"mydb"}, {"foo"}, {"information_schema"}},
	},
	{
		Query:    `SHOW SCHEMAS`,
		Expected: []sql.Row{{"mydb"}, {"foo"}, {"information_schema"}},
	},
	{
		Query:    `SHOW GRANTS`,
		Expected: []sql.Row{{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION"}},
	},
	{
		Query: `SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA`,
		Expected: []sql.Row{
			{"information_schema", "utf8mb4", "utf8mb4_0900_ai_ci"},
			{"mydb", "utf8mb4", "utf8mb4_0900_ai_ci"},
			{"foo", "utf8mb4", "utf8mb4_0900_ai_ci"},
		},
	},
	{
		Query: `SELECT s FROM mytable WHERE s LIKE '%d row'`,
		Expected: []sql.Row{
			{"second row"},
			{"third row"},
		},
	},
	{
		Query: `SELECT s FROM mytable WHERE s LIKE '%D ROW'`,
		Expected: []sql.Row{
			{"second row"},
			{"third row"},
		},
	},
	{
		Query: `SELECT SUBSTRING(s, -3, 3) AS s FROM mytable WHERE s LIKE '%d row' GROUP BY 1`,
		Expected: []sql.Row{
			{"row"},
		},
	},
	{
		Query: `SELECT s FROM mytable WHERE s NOT LIKE '%d row'`,
		Expected: []sql.Row{
			{"first row"},
		},
	},
	{
		Query: `SELECT * FROM foo.other_table`,
		Expected: []sql.Row{
			{"a", int32(4)},
			{"b", int32(2)},
			{"c", int32(0)},
		},
	},
	{
		Query: `SELECT AVG(23.222000)`,
		Expected: []sql.Row{
			{float64(23.222)},
		},
	},
	{
		Query: `SELECT DATABASE()`,
		Expected: []sql.Row{
			{"mydb"},
		},
	},
	{
		Query: `SELECT USER()`,
		Expected: []sql.Row{
			{"user@client"},
		},
	},
	{
		Query: `SELECT CURRENT_USER()`,
		Expected: []sql.Row{
			{"user@client"},
		},
	},
	{
		Query: `SELECT CURRENT_USER`,
		Expected: []sql.Row{
			{"user@client"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "CURRENT_USER",
				Type: sql.LongText,
			},
		},
	},
	{
		Query: `SELECT CURRENT_user`,
		Expected: []sql.Row{
			{"user@client"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "CURRENT_user",
				Type: sql.LongText,
			},
		},
	},
	{
		Query: `SHOW VARIABLES LIKE 'gtid_mode`,
		Expected: []sql.Row{
			{"gtid_mode", "OFF"},
		},
	},
	{
		Query: `SHOW VARIABLES LIKE 'gtid%`,
		Expected: []sql.Row{
			{"gtid_executed", ""},
			{"gtid_executed_compression_period", int64(0)},
			{"gtid_mode", "OFF"},
			{"gtid_next", "AUTOMATIC"},
			{"gtid_owned", ""},
			{"gtid_purged", ""},
		},
	},
	{
		Query: `SHOW GLOBAL VARIABLES LIKE '%mode`,
		Expected: []sql.Row{
			{"block_encryption_mode", "aes-128-ecb"},
			{"gtid_mode", "OFF"},
			{"offline_mode", int64(0)},
			{"pseudo_slave_mode", int64(0)},
			{"rbr_exec_mode", "STRICT"},
			{"sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"},
			{"ssl_fips_mode", "OFF"},
		},
	},
	{
		Query:    `SELECT JSON_EXTRACT('"foo"', "$")`,
		Expected: []sql.Row{{sql.MustJSON(`"foo"`)}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"foo"')`,
		Expected: []sql.Row{{"foo"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('[1, 2, 3]')`,
		Expected: []sql.Row{{"[1, 2, 3]"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"\\t\\u0032"')`,
		Expected: []sql.Row{{"\t2"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE('"\t\\u0032"')`,
		Expected: []sql.Row{{"\t2"}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE(JSON_EXTRACT('{"xid":"hello"}', '$.xid')) = "hello"`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('{"xid":"hello"}', '$.xid') = "hello"`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('{"xid":"hello"}', '$.xid') = '"hello"'`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT JSON_UNQUOTE(JSON_EXTRACT('{"xid":null}', '$.xid'))`,
		Expected: []sql.Row{{"null"}},
	},
	{
		Query:    `select JSON_EXTRACT('{"id":234}', '$.id')-1;`,
		Expected: []sql.Row{{233.0}},
	},
	{
		Query:    `select JSON_EXTRACT('{"id":234}', '$.id') = 234;`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT CONNECTION_ID()`,
		Expected: []sql.Row{{uint32(1)}},
	},
	{
		Query: `SHOW CREATE DATABASE mydb`,
		Expected: []sql.Row{{
			"mydb",
			"CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */",
		}},
	},
	{
		Query: `SHOW CREATE TABLE two_pk`,
		Expected: []sql.Row{{
			"two_pk",
			"CREATE TABLE `two_pk` (\n" +
				"  `pk1` tinyint NOT NULL,\n" +
				"  `pk2` tinyint NOT NULL,\n" +
				"  `c1` tinyint NOT NULL,\n" +
				"  `c2` tinyint NOT NULL,\n" +
				"  `c3` tinyint NOT NULL,\n" +
				"  `c4` tinyint NOT NULL,\n" +
				"  `c5` tinyint NOT NULL,\n" +
				"  PRIMARY KEY (`pk1`,`pk2`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		}},
	},
	{
		Query: `SHOW CREATE TABLE myview`,
		Expected: []sql.Row{{
			"myview",
			"CREATE VIEW `myview` AS SELECT * FROM mytable",
		}},
	},
	{
		Query: `SHOW CREATE VIEW myview`,
		Expected: []sql.Row{{
			"myview",
			"CREATE VIEW `myview` AS SELECT * FROM mytable",
		}},
	},
	{
		Query:    `SELECT -1`,
		Expected: []sql.Row{{int8(-1)}},
	},
	{
		Query: `
		SHOW WARNINGS
		`,
		Expected: nil,
	},
	{
		Query:    `SHOW WARNINGS LIMIT 0`,
		Expected: nil,
	},
	{
		Query: `SELECT NULL`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT nullif('abc', NULL)`,
		Expected: []sql.Row{
			{"abc"},
		},
	},
	{
		Query: `SELECT nullif(NULL, NULL)`,
		Expected: []sql.Row{
			{sql.Null},
		},
	},
	{
		Query: `SELECT nullif(NULL, 123)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT nullif(123, 123)`,
		Expected: []sql.Row{
			{sql.Null},
		},
	},
	{
		Query: `SELECT nullif(123, 321)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(123, NULL)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(NULL, NULL)`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT ifnull(NULL, 123)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(123, 123)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT ifnull(123, 321)`,
		Expected: []sql.Row{
			{int8(123)},
		},
	},
	{
		Query: `SELECT if(123 = 123, "a", "b")`,
		Expected: []sql.Row{
			{"a"},
		},
	},
	{
		Query: `SELECT if(123 = 123, NULL, "b")`,
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query: `SELECT if(123 > 123, "a", "b")`,
		Expected: []sql.Row{
			{"b"},
		},
	},
	{
		Query: `SELECT if(NULL, "a", "b")`,
		Expected: []sql.Row{
			{"b"},
		},
	},
	{
		Query: `SELECT if("a", "a", "b")`,
		Expected: []sql.Row{
			{"b"},
		},
	},
	{
		Query: `SELECT i, if(s = "first row", "first", "not first") from mytable order by i`,
		Expected: []sql.Row{
			{1, "first"},
			{2, "not first"},
			{3, "not first"},
		},
	},
	{
		Query:    "SELECT i FROM mytable WHERE NULL > 10;",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NULL IN (10);",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NULL IN (NULL, NULL);",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT NULL NOT IN (NULL);",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT (NULL) <> 10;",
		Expected: nil,
	},
	{
		Query:    "SELECT i FROM mytable WHERE NOT NULL <> NULL;",
		Expected: nil,
	},
	{
		Query: `SELECT round(15728640/1024/1024)`,
		Expected: []sql.Row{
			{int64(15)},
		},
	},
	{
		Query: `SELECT round(15, 1)`,
		Expected: []sql.Row{
			{int8(15)},
		},
	},
	{
		Query: `SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM mytable`,
		Expected: []sql.Row{
			{"one"},
			{"two"},
			{"other"},
		},
	},
	{
		Query: `SELECT CASE WHEN i > 2 THEN 'more than two' WHEN i < 2 THEN 'less than two' ELSE 'two' END FROM mytable`,
		Expected: []sql.Row{
			{"less than two"},
			{"two"},
			{"more than two"},
		},
	},
	{
		Query: `SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM mytable`,
		Expected: []sql.Row{
			{"one"},
			{"two"},
			{nil},
		},
	},
	{
		Query: `SHOW COLLATION`,
		Expected: []sql.Row{
			{
				sql.Collation_binary.String(),
				"binary",
				sql.CollationToMySQLVals[sql.Collation_binary.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].PadSpace,
			},
			{
				sql.Collation_utf8_general_ci.String(),
				"utf8",
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].PadSpace,
			},
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].PadSpace,
			},
		},
	},
	{
		Query:    `SHOW COLLATION LIKE 'foo'`,
		Expected: nil,
	},
	{
		Query: `SHOW COLLATION LIKE 'utf8%'`,
		Expected: []sql.Row{
			{
				sql.Collation_utf8_general_ci.String(),
				"utf8",
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].PadSpace,
			},
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].PadSpace,
			},
		},
	},
	{
		Query:    `SHOW COLLATION WHERE charset = 'foo'`,
		Expected: nil,
	},
	{
		Query: "SHOW COLLATION WHERE `Default` = 'Yes'",
		Expected: []sql.Row{
			{
				sql.Collation_binary.String(),
				"binary",
				sql.CollationToMySQLVals[sql.Collation_binary.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_binary.Name].PadSpace,
			},
			{
				sql.Collation_utf8_general_ci.String(),
				"utf8",
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci.Name].PadSpace,
			},
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci.Name].PadSpace,
			},
		},
	},
	{
		Query: "SHOW CHARSET",
		Expected: []sql.Row{
			{
				sql.CharacterSet_utf8mb4.String(),
				sql.CharacterSet_utf8mb4.Description(),
				sql.CharacterSet_utf8mb4.DefaultCollation().String(),
				sql.CharacterSet_utf8mb4.MaxLength(),
			},
		},
	},
	{
		Query: "SHOW CHARACTER SET",
		Expected: []sql.Row{
			{
				sql.CharacterSet_utf8mb4.String(),
				sql.CharacterSet_utf8mb4.Description(),
				sql.CharacterSet_utf8mb4.DefaultCollation().String(),
				sql.CharacterSet_utf8mb4.MaxLength(),
			},
		},
	},
	{
		Query: "SHOW CHARSET LIKE 'utf8%'",
		Expected: []sql.Row{
			{
				sql.CharacterSet_utf8mb4.String(),
				sql.CharacterSet_utf8mb4.Description(),
				sql.CharacterSet_utf8mb4.DefaultCollation().String(),
				sql.CharacterSet_utf8mb4.MaxLength(),
			},
		},
	},
	{
		Query:    "show charset where charset='binary'",
		Expected: nil,
	},
	{
		Query:    `SHOW CHARSET WHERE Charset = 'foo'`,
		Expected: nil,
	},
	{
		Query:    "ROLLBACK",
		Expected: nil,
	},
	{
		Query:    "SELECT substring(s, 1, 1) FROM mytable ORDER BY substring(s, 1, 1)",
		Expected: []sql.Row{{"f"}, {"s"}, {"t"}},
	},
	{
		Query:    "SELECT substring(s, 1, 1), count(*) FROM mytable GROUP BY substring(s, 1, 1)",
		Expected: []sql.Row{{"f", int64(1)}, {"s", int64(1)}, {"t", int64(1)}},
	},
	{
		Query:    "SELECT substring(s, 1, 1) as x, count(*) FROM mytable GROUP BY X",
		Expected: []sql.Row{{"f", int64(1)}, {"s", int64(1)}, {"t", int64(1)}},
	},
	{
		Query:    "SELECT left(s, 1) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{"f"}, {"s"}, {"t"}},
	},
	{
		Query:    "SELECT left(s, 2) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{"fi"}, {"se"}, {"th"}},
	},
	{
		Query:    "SELECT left(s, 0) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{""}, {""}, {""}},
	},
	{
		Query:    "SELECT left(s, NULL) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{nil}, {nil}, {nil}},
	},
	{
		Query:    "SELECT left(s, 100) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{"first row"}, {"second row"}, {"third row"}},
	},
	{
		Query:    "SELECT instr(s, 'row') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(7)}, {int64(8)}, {int64(7)}},
	},
	{
		Query:    "SELECT instr(s, 'first') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(1)}, {int64(0)}, {int64(0)}},
	},
	{
		Query:    "SELECT instr(s, 'o') as l FROM mytable ORDER BY i",
		Expected: []sql.Row{{int64(8)}, {int64(4)}, {int64(8)}},
	},
	{
		Query:    "SELECT instr(s, NULL) as l FROM mytable ORDER BY l",
		Expected: []sql.Row{{nil}, {nil}, {nil}},
	},
	{
		Query:    "SELECT SLEEP(0.5)",
		Expected: []sql.Row{{int(0)}},
	},
	{
		Query:    "SELECT TO_BASE64('foo')",
		Expected: []sql.Row{{string("Zm9v")}},
	},
	{
		Query:    "SELECT FROM_BASE64('YmFy')",
		Expected: []sql.Row{{string("bar")}},
	},
	{
		Query:    "SELECT DATE_ADD('2018-05-02', INTERVAL 1 day)",
		Expected: []sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT DATE_SUB('2018-05-02', INTERVAL 1 DAY)",
		Expected: []sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT '2018-05-02' + INTERVAL 1 DAY",
		Expected: []sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    "SELECT '2018-05-02' - INTERVAL 1 DAY",
		Expected: []sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT i AS i FROM mytable ORDER BY i`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT i AS i FROM mytable GROUP BY s ORDER BY 1`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT i AS x FROM mytable GROUP BY s ORDER BY x`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query: `SELECT i as x, row_number() over (order by i DESC) FROM mytable ORDER BY x`,
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1}},
	},
	{
		Query: `SELECT i as i, row_number() over (order by i DESC) FROM mytable ORDER BY 1`,
		Expected: []sql.Row{
			{1, 3},
			{2, 2},
			{3, 1}},
	},
	{
		Query: `
		SELECT
			i,
			foo
		FROM (
			SELECT
				i,
				COUNT(s) AS foo
			FROM mytable
			GROUP BY i
		) AS q
		ORDER BY foo DESC, i ASC
		`,
		Expected: []sql.Row{
			{int64(1), int64(1)},
			{int64(2), int64(1)},
			{int64(3), int64(1)},
		},
	},
	{
		Query:    "SELECT n, COUNT(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		Expected: []sql.Row{{int64(1), int64(3)}, {int64(2), int64(3)}},
	},
	{
		Query:    "SELECT n, COUNT(n) as cnt FROM bigtable GROUP BY n HAVING cnt > 2",
		Expected: []sql.Row{{int64(1), int64(3)}, {int64(2), int64(3)}},
	},
	{
		Query:    "SELECT n, MAX(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		Expected: []sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}},
	},
	{
		Query:    "SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1 HAVING s = \"secon\"",
		Expected: []sql.Row{{"secon"}},
	},
	{
		Query: "SELECT s,  i FROM mytable GROUP BY i ORDER BY SUBSTRING(s, 1, 1) DESC",
		Expected: []sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		Query: "SELECT s, i FROM mytable GROUP BY i HAVING count(*) > 0 ORDER BY SUBSTRING(s, 1, 1) DESC",
		Expected: []sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		Query:    "SELECT CONVERT('9999-12-31 23:59:59', DATETIME)",
		Expected: []sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT DATETIME('9999-12-31 23:59:59')",
		Expected: []sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT TIMESTAMP('2020-12-31 23:59:59')",
		Expected: []sql.Row{{time.Date(2020, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		Query:    "SELECT CONVERT('10000-12-31 23:59:59', DATETIME)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT '9999-12-31 23:59:59' + INTERVAL 1 DAY",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT DATE_ADD('9999-12-31 23:59:59', INTERVAL 1 DAY)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) AS date_col) t WHERE t.date_col > '0000-01-01 00:00:00'`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY t.date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col as date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY t.date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT t.date_col as date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY date_col`,
		Expected: []sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		Query:    `SELECT i AS foo FROM mytable ORDER BY mytable.i`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('[1, 2, 3]', '$.[0]')`,
		Expected: []sql.Row{{sql.MustJSON(`1`)}},
	},
	// TODO(andy)
	{
		Query:    `SELECT ARRAY_LENGTH(JSON_EXTRACT('[1, 2, 3]', '$'))`,
		Expected: []sql.Row{{int32(3)}},
	},
	{
		Query:    `SELECT ARRAY_LENGTH(JSON_EXTRACT('[{"i":0}, {"i":1, "y":"yyy"}, {"i":2, "x":"xxx"}]', '$.i'))`,
		Expected: []sql.Row{{int32(3)}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, 3, 4)`,
		Expected: []sql.Row{{int64(4)}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, "3", 4)`,
		Expected: []sql.Row{{float64(4)}},
	},
	{
		Query:    `SELECT GREATEST(1, 2, "9", "foo999")`,
		Expected: []sql.Row{{float64(9)}},
	},
	{
		Query:    `SELECT GREATEST("aaa", "bbb", "ccc")`,
		Expected: []sql.Row{{"ccc"}},
	},
	{
		Query:    `SELECT GREATEST(i, s) FROM mytable`,
		Expected: []sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		Query:    "select abs(-i) from mytable order by 1",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query:    "select ceil(i + 0.5) from mytable order by 1",
		Expected: []sql.Row{{2.0}, {3.0}, {4.0}},
	},
	{
		Query:    "select floor(i + 0.5) from mytable order by 1",
		Expected: []sql.Row{{1.0}, {2.0}, {3.0}},
	},
	{
		Query:    "select round(i + 0.55, 1) from mytable order by 1",
		Expected: []sql.Row{{1.6}, {2.6}, {3.6}},
	},
	{
		Query:    "select date_format(da, '%s') from typestable order by 1",
		Expected: []sql.Row{{"00"}},
	},
	{
		Query: "select md5(i) from mytable order by 1",
		Expected: []sql.Row{
			{"c4ca4238a0b923820dcc509a6f75849b"},
			{"c81e728d9d4c2f636f067f89cc14862c"},
			{"eccbc87e4b5ce2fe28308fd9f2a7baf3"},
		},
	},
	{
		Query: "select sha1(i) from mytable order by 1",
		Expected: []sql.Row{
			{"356a192b7913b04c54574d18c28d46e6395428ab"},
			{"77de68daecd823babbb58edb1c8e14d7106e83bb"},
			{"da4b9237bacccdf19c0760cab7aec4a8359010b0"},
		},
	},
	{
		Query: "select sha2(i, 256) from mytable order by 1",
		Expected: []sql.Row{
			{"4e07408562bedb8b60ce05c1decfe3ad16b72230967de01f640b7e4729b49fce"},
			{"6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"},
			{"d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35"},
		},
	},
	{
		Query:    "select length(s) from mytable order by i",
		Expected: []sql.Row{{9}, {10}, {9}},
	},
	{
		Query:    "select char_length(s) from mytable order by i",
		Expected: []sql.Row{{9}, {10}, {9}},
	},
	{
		Query:    "select log2(i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {1.0}, {1.5849625007211563}},
	},
	{
		Query:    "select ln(i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {0.6931471805599453}, {1.0986122886681096}},
	},
	{
		Query:    "select log10(i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {0.3010299956639812}, {0.4771212547196624}},
	},
	{
		Query:    "select log(3, i) from mytable order by i",
		Expected: []sql.Row{{0.0}, {0.6309297535714575}, {1.0}},
	},
	{
		Query: "select lower(s) from mytable order by i",
		Expected: []sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		Query: "select upper(s) from mytable order by i",
		Expected: []sql.Row{
			{"FIRST ROW"},
			{"SECOND ROW"},
			{"THIRD ROW"},
		},
	},
	{
		Query:    "select reverse(s) from mytable order by i",
		Expected: []sql.Row{{"wor tsrif"}, {"wor dnoces"}, {"wor driht"}},
	},
	{
		Query:    "select repeat(s, 2) from mytable order by i",
		Expected: []sql.Row{{"first rowfirst row"}, {"second rowsecond row"}, {"third rowthird row"}},
	},
	{
		Query:    "select replace(s, 'row', '') from mytable order by i",
		Expected: []sql.Row{{"first "}, {"second "}, {"third "}},
	},
	{
		Query:    "select rpad(s, 13, ' ') from mytable order by i",
		Expected: []sql.Row{{"first row    "}, {"second row   "}, {"third row    "}},
	},
	{
		Query:    "select lpad(s, 13, ' ') from mytable order by i",
		Expected: []sql.Row{{"    first row"}, {"   second row"}, {"    third row"}},
	},
	{
		Query:    "select sqrt(i) from mytable order by i",
		Expected: []sql.Row{{1.0}, {1.4142135623730951}, {1.7320508075688772}},
	},
	{
		Query:    "select pow(2, i) from mytable order by i",
		Expected: []sql.Row{{2.0}, {4.0}, {8.0}},
	},
	{
		Query:    "select ltrim(concat(' ', concat(s, ' '))) from mytable order by i",
		Expected: []sql.Row{{"first row "}, {"second row "}, {"third row "}},
	},
	{
		Query:    "select rtrim(concat(' ', concat(s, ' '))) from mytable order by i",
		Expected: []sql.Row{{" first row"}, {" second row"}, {" third row"}},
	},
	{
		Query:    "select trim(concat(' ', concat(s, ' '))) from mytable order by i",
		Expected: []sql.Row{{"first row"}, {"second row"}, {"third row"}},
	},
	{
		Query:    `SELECT GREATEST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		Expected: []sql.Row{{time.Date(1980, 6, 22, 14, 32, 56, 0, time.UTC)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, 3, 4)`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, "3", 4)`,
		Expected: []sql.Row{{float64(1)}},
	},
	{
		Query:    `SELECT LEAST(1, 2, "9", "foo999")`,
		Expected: []sql.Row{{float64(1)}},
	},
	{
		Query:    `SELECT LEAST("aaa", "bbb", "ccc")`,
		Expected: []sql.Row{{"aaa"}},
	},
	{
		Query:    `SELECT LEAST(i, s) FROM mytable`,
		Expected: []sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		Query:    `SELECT LEAST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		Expected: []sql.Row{{time.Date(1920, 2, 3, 7, 41, 11, 0, time.UTC)}},
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable LEFT JOIN othertable ON i = i2 - 1",
		Expected: []sql.Row{
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
			{int64(3), nil, nil},
		},
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
		Expected: []sql.Row{
			{nil, int64(1), "third"},
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
		},
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable LEFT OUTER JOIN othertable ON i = i2 - 1",
		Expected: []sql.Row{
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
			{int64(3), nil, nil},
		},
	},
	{
		Query: "SELECT i, i2, s2 FROM mytable RIGHT OUTER JOIN othertable ON i = i2 - 1",
		Expected: []sql.Row{
			{nil, int64(1), "third"},
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
		},
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 
				FROM othertable ot INNER JOIN 
					(SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub 
				ON sub.i = ot.i2 order by 1`,
		Expected: []sql.Row{
			{1, 1, "third", 1, "third"},
			{2, 2, "second", 2, "second"},
			{3, 3, "first", 3, "first"},
		},
	},
	{
		Query: `SELECT sub.i, sub.i2, sub.s2, ot.i2, ot.s2 
				FROM (SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2) sub
				INNER JOIN othertable ot 
				ON sub.i = ot.i2 order by 1`,
		Expected: []sql.Row{
			{1, 1, "third", 1, "third"},
			{2, 2, "second", 2, "second"},
			{3, 3, "first", 3, "first"},
		},
	},
	{
		Query:    `SELECT CHAR_LENGTH('áé'), LENGTH('àè')`,
		Expected: []sql.Row{{int32(2), int32(4)}},
	},
	{
		Query:    "SELECT i, COUNT(i) AS `COUNT(i)` FROM (SELECT i FROM mytable) t GROUP BY i ORDER BY i, `COUNT(i)` DESC",
		Expected: []sql.Row{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(3), int64(1)}},
	},
	{
		Query: "SELECT i FROM mytable WHERE NOT s ORDER BY 1 DESC",
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query: "SELECT i FROM mytable WHERE NOT(NOT i) ORDER BY 1 DESC",
		Expected: []sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		Query:    `SELECT NOW() - NOW()`,
		Expected: []sql.Row{{int64(0)}},
	},
	{
		Query:    `SELECT DATETIME(NOW()) - NOW()`,
		Expected: []sql.Row{{int64(0)}},
	},
	{
		Query:    `SELECT TIMESTAMP(NOW()) - NOW()`,
		Expected: []sql.Row{{int64(0)}},
	},
	{
		Query:    `SELECT NOW() - (NOW() - INTERVAL 1 SECOND)`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT SUBSTR(SUBSTRING('0123456789ABCDEF', 1, 10), -4)`,
		Expected: []sql.Row{{"6789"}},
	},
	{
		Query:    `SELECT CASE i WHEN 1 THEN i ELSE NULL END FROM mytable`,
		Expected: []sql.Row{{int64(1)}, {nil}, {nil}},
	},
	{
		Query:    `SELECT (NULL+1)`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT ARRAY_LENGTH(null)`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT ARRAY_LENGTH("foo")`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT * FROM mytable WHERE NULL AND i = 3`,
		Expected: nil,
	},
	{
		Query:    `SELECT 1 FROM mytable GROUP BY i HAVING i > 1`,
		Expected: []sql.Row{{int8(1)}, {int8(1)}},
	},
	{
		Query:    `SELECT avg(i) FROM mytable GROUP BY i HAVING avg(i) > 1`,
		Expected: []sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		Query:    "SELECT avg(i) as `avg(i)` FROM mytable GROUP BY i HAVING avg(i) > 1",
		Expected: []sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		Query:    "SELECT avg(i) as `AVG(i)` FROM mytable GROUP BY i HAVING AVG(i) > 1",
		Expected: []sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		Query: `SELECT s AS s, COUNT(*) AS count,  AVG(i) AS ` + "`AVG(i)`" + `
		FROM  (
			SELECT * FROM mytable
		) AS expr_qry
		GROUP BY s
		HAVING ((AVG(i) > 0))
		ORDER BY count DESC, s ASC
		LIMIT 10000`,
		Expected: []sql.Row{
			{"first row", int64(1), float64(1)},
			{"second row", int64(1), float64(2)},
			{"third row", int64(1), float64(3)},
		},
	},
	{
		Query:    `SELECT FIRST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT LAST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    `SELECT COUNT(DISTINCT t.i) FROM tabletest t, mytable t2`,
		Expected: []sql.Row{{int64(3)}},
	},
	{
		Query:    `SELECT CASE WHEN NULL THEN "yes" ELSE "no" END AS test`,
		Expected: []sql.Row{{"no"}},
	},
	{
		Query: `SELECT
			table_schema,
			table_name,
			CASE
				WHEN table_type = 'BASE TABLE' THEN
					CASE
						WHEN table_schema = 'mysql'
							OR table_schema = 'performance_schema' THEN 'SYSTEM TABLE'
						ELSE 'TABLE'
					END
				WHEN table_type = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
				ELSE table_type
			END AS TABLE_TYPE
		FROM information_schema.tables
		WHERE table_schema = 'mydb'
			AND table_name = 'mytable'
		HAVING table_type IN ('TABLE', 'VIEW')
		ORDER BY table_type, table_schema, table_name`,
		Expected: []sql.Row{{"mydb", "mytable", "TABLE"}},
	},
	{
		Query: "SELECT REGEXP_LIKE('testing', 'TESTING');",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Query: "SELECT REGEXP_LIKE('testing', 'TESTING') FROM mytable;",
		Expected: []sql.Row{
			{1},
			{1},
			{1},
		},
	},
	{
		Query: "SELECT i, s, REGEXP_LIKE(s, '[a-z]+d row') FROM mytable;",
		Expected: []sql.Row{
			{1, "first row", 0},
			{2, "second row", 1},
			{3, "third row", 1},
		},
	},
	{
		Query: "SELECT * FROM newlinetable WHERE s LIKE '%text%'",
		Expected: []sql.Row{
			{int64(1), "\nthere is some text in here"},
			{int64(2), "there is some\ntext in here"},
			{int64(3), "there is some text\nin here"},
			{int64(4), "there is some text in here\n"},
			{int64(5), "there is some text in here"},
		},
	},
	{
		Query:    `SELECT i FROM mytable WHERE i = (SELECT 1)`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query: `SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable) ORDER BY i`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		Query: `SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2) ORDER BY i`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(2)},
		},
	},
	{
		Query: `SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2)`,
		Expected: []sql.Row{
			{int64(3)},
		},
	},
	{
		Query: `SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) ORDER BY i`,
		Expected: []sql.Row{
			{2},
			{3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i FROM mytable where i = mt.i and i > 2) IS NOT NULL
						 AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i FROM mytable where i = mt.i and i > 1) IS NOT NULL
						 AND (SELECT i2 FROM othertable where i2 = i and i < 3) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{2},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i FROM mytable where i = mt.i) IS NOT NULL
						 AND (SELECT i2 FROM othertable where i2 = i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{1}, {2}, {3},
		},
	},
	{
		Query: `SELECT pk,pk2, (SELECT pk from one_pk where pk = 1 limit 1) FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		Expected: []sql.Row{
			{1, 1, 1},
			{1, 1, 1},
		},
	},
	{
		Query: `SELECT i FROM mytable
						 WHERE (SELECT i2 FROM othertable where i2 = i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{1}, {2}, {3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT i2 FROM othertable ot where ot.i2 = mt.i) IS NOT NULL
						 ORDER BY i`,
		Expected: []sql.Row{
			{1}, {2}, {3},
		},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT row_number() over (order by ot.i2 desc) FROM othertable ot where ot.i2 = mt.i) = 2
						 ORDER BY i`,
		Expected: []sql.Row{},
	},
	{
		Query: `SELECT i FROM mytable mt
						 WHERE (SELECT row_number() over (order by ot.i2 desc) FROM othertable ot where ot.i2 = mt.i) = 1
						 ORDER BY i`,
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query:    `SELECT (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) AS x`,
		Expected: []sql.Row{{int64(1)}},
	},
	{
		Query:    `SELECT (SELECT s FROM mytable ORDER BY i ASC LIMIT 1) AS x`,
		Expected: []sql.Row{{"first row"}},
	},
	{
		Query: `SELECT pk, (SELECT pk FROM one_pk WHERE pk < opk.pk ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT c3 FROM one_pk WHERE pk < opk.pk ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil},
			{1, 2},
			{2, 12},
			{3, 22},
		},
	},
	{
		Query: `SELECT pk, (SELECT c5 FROM one_pk WHERE c5 < opk.c5 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil},
			{1, 4},
			{2, 14},
			{3, 24},
		},
	},
	{
		Query: `SELECT pk, (SELECT pk FROM one_pk WHERE c1 < opk.c1 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1) FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil},
			{1, 2},
			{2, 12},
			{3, 22},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1),
					(SELECT c5 + 1 FROM one_pk WHERE c5 < opk.c5 ORDER BY 1 DESC LIMIT 1)
					FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil, nil},
			{1, 2, 5},
			{2, 12, 15},
			{3, 22, 25},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk),
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk)
					FROM one_pk opk ORDER BY 1;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
			{3, 2, nil},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) IS NOT NULL
					ORDER BY max;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk) > 0
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0
					ORDER BY min;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0
					ORDER BY max;`,
		Expected: []sql.Row{
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x FROM one_pk opk GROUP BY x ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk)
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk FROM one_pk
					WHERE (SELECT max(pk1) FROM two_pk WHERE pk1 >= pk) IS NOT NULL
					ORDER BY 1;`,
		Expected: []sql.Row{
			{0},
			{1},
		},
	},
	{
		Query: `SELECT pk FROM one_pk opk
					WHERE (SELECT count(*) FROM two_pk where pk1 * 10 <= opk.c1) > 2
					ORDER BY 1;`,
		Expected: []sql.Row{
			{1},
			{2},
			{3},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS min
					FROM one_pk opk
					WHERE (SELECT max(pk) FROM one_pk WHERE pk >= opk.pk) > 0
					ORDER BY min;`,
		Expected: []sql.Row{
			{3, 2, nil},
			{0, nil, 1},
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE one_pk.pk * 10 <= opk.c1) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk <= opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) FROM one_pk opk ORDER BY 2`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x FROM one_pk opk ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL ORDER BY x`,
		Expected: []sql.Row{
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS max
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) IS NOT NULL ORDER BY max`,
		Expected: []sql.Row{
			{1, 0},
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0
						GROUP BY x ORDER BY x`,
		Expected: []sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) > 0
						GROUP BY (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) ORDER BY x`,
		Expected: []sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT max(pk) FROM one_pk WHERE pk > opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT min(pk) FROM one_pk WHERE pk < opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x
						FROM one_pk opk WHERE (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) > 0 ORDER BY x`,
		Expected: []sql.Row{
			{0, nil},
			{1, 0},
			{2, 1},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk1) FROM two_pk WHERE pk1 < pk) AS max,
					(SELECT min(pk2) FROM two_pk WHERE pk2 > pk) AS min
					FROM one_pk ORDER BY min, pk;`,
		Expected: []sql.Row{
			{1, 0, nil},
			{2, 1, nil},
			{3, 1, nil},
			{0, nil, 1},
		},
	},
	{
		Query: `SELECT pk,
						(SELECT max(pk1) FROM two_pk tpk WHERE pk1 IN (SELECT pk1 FROM two_pk WHERE pk1 = tpk.pk2)) AS one,
						(SELECT min(pk2) FROM two_pk tpk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE pk2 = tpk.pk1)) AS zero
						FROM one_pk ORDER BY pk;`,
		Expected: []sql.Row{
			{0, 1, 0},
			{1, 1, 0},
			{2, 1, 0},
			{3, 1, 0},
		},
	},
	{
		Query: `SELECT pk,
						(SELECT sum(pk1+pk2) FROM two_pk WHERE pk1+pk2 IN (SELECT pk1+pk2 FROM two_pk WHERE pk1+pk2 = pk)) AS sum,
						(SELECT min(pk2) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE pk2 = pk)) AS equal
						FROM one_pk ORDER BY pk;`,
		Expected: []sql.Row{
			{0, 0.0, 0},
			{1, 2.0, 1},
			{2, 2.0, nil},
			{3, nil, nil},
		},
	},
	{
		Query: `SELECT pk,
						(SELECT sum(c1) FROM two_pk WHERE c1 + 3 IN (SELECT c4 FROM two_pk WHERE c3 > opk.c5)) AS sum,
						(SELECT sum(c1) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE c1 + 1 < opk.c2)) AS sum2
					FROM one_pk opk ORDER BY pk`,
		Expected: []sql.Row{
			{0, 60.0, nil},
			{1, 50.0, 20.0},
			{2, 30.0, 60.0},
			{3, nil, 60.0},
		},
	},
	{
		Query: `SELECT pk, (SELECT min(pk) FROM one_pk WHERE pk > opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 1},
			{1, 2},
			{2, 3},
			{3, nil},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE one_pk.pk <= one_pk.pk) FROM one_pk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk as a, (SELECT max(pk) FROM one_pk WHERE pk <= a) FROM one_pk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk as a, (SELECT max(pk) FROM one_pk WHERE pk <= a) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= opk.pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk <= pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= pk) FROM one_pk opk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 3},
			{1, 3},
			{2, 3},
			{3, 3},
		},
	},
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= one_pk.pk) FROM one_pk ORDER BY 1`,
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: `SELECT DISTINCT n FROM bigtable ORDER BY t`,
		Expected: []sql.Row{
			{int64(1)},
			{int64(9)},
			{int64(7)},
			{int64(3)},
			{int64(2)},
			{int64(8)},
			{int64(6)},
			{int64(5)},
			{int64(4)},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk, two_pk ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 0},
			{0, 1, 1},
			{1, 0, 0},
			{1, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, 0, 0},
			{2, 0, 1},
			{2, 1, 0},
			{2, 1, 1},
			{3, 0, 0},
			{3, 0, 1},
			{3, 1, 0},
			{3, 1, 1},
		},
	},
	{
		Query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 AND pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{0, 31},
			{10, 31},
			{20, 31},
			{30, 31},
		},
	},
	{
		Query: "SELECT t1.i, t2.i FROM mytable t1, mytable t2 WHERE t2.i=1 AND t1.s = t2.s ORDER BY 1,2",
		Expected: []sql.Row{
			{1, 1},
		},
	},
	{
		Query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE t2.pk1=1 AND t2.pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{0, 31},
			{10, 31},
			{20, 31},
			{30, 31},
		},
	},
	{
		Query: "SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 OR pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{0, 11},
			{0, 21},
			{0, 31},
			{10, 11},
			{10, 21},
			{10, 31},
			{20, 11},
			{20, 21},
			{20, 31},
			{30, 11},
			{30, 21},
			{30, 31},
		},
	},
	{
		Query: "SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		Expected: []sql.Row{
			{1, 1},
			{1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE pk=0 AND pk1=0 OR pk2=1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 1},
			{1, 0, 1},
			{1, 1, 1},
			{2, 0, 1},
			{2, 1, 1},
			{3, 0, 1},
			{3, 1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{1, 0, 1},
			{2, 1, 0},
			{3, 1, 1},
		},
	},
	{
		Query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{4, 0, 0},
			{4, 0, 1},
			{14, 1, 0},
			{14, 1, 1},
		},
	},
	{
		Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk, two_pk tpk WHERE pk=pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{4, 0, 0},
			{4, 0, 1},
			{14, 1, 0},
			{14, 1, 1},
		},
	},
	{
		Query: "SELECT one_pk.c5,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{4, 0, 0},
			{4, 0, 1},
			{14, 1, 0},
			{14, 1, 1},
		},
	},
	{
		Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{4, 0, 0},
			{4, 0, 1},
			{14, 1, 0},
			{14, 1, 1},
		},
	},
	{
		Query: "SELECT opk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{4, 0, 0},
			{4, 0, 1},
			{14, 1, 0},
			{14, 1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE pk=1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{1, 0, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON opk.pk=tpk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON pk=tpk.pk1 AND pk=tpk.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk-1=tpk.pk2 
						LEFT JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1
						ORDER BY 1`,
		Expected: []sql.Row{
			{0, nil, nil, nil, nil},
			{1, 1, 0, 0, 1},
			{2, nil, nil, nil, nil},
			{3, nil, nil, nil, nil},
		},
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2 
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
		Expected: []sql.Row{
			{1, 1, 0, 0, 1},
		},
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						JOIN two_pk tpk ON pk=tpk.pk1 AND pk-1=tpk.pk2 
						JOIN two_pk tpk2 ON pk-1=TPK2.pk1 AND pk=tpk2.pk2
						ORDER BY 1`,
		Expected: []sql.Row{
			{1, 1, 0, 0, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{1, 1, 1},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{nil, 0, 1},
			{nil, 1, 0},
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		Query: "SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{int64(2), 1, 0},
		},
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0, 0},
			{0, 1, 1, 0},
			{1, 0, 0, 1},
			{1, 1, 1, 1},
		},
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0, 0},
			{0, 1, 0, 1},
			{1, 0, 1, 0},
			{1, 1, 1, 1},
		},
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a, two_pk b WHERE a.pk1=b.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0, 0},
			{0, 1, 0, 1},
			{1, 0, 1, 0},
			{1, 1, 1, 1},
		},
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON b.pk1=a.pk1 AND a.pk2=b.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0, 0},
			{0, 1, 0, 1},
			{1, 0, 1, 0},
			{1, 1, 1, 1},
		},
	},
	{
		Query: "SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1+1=b.pk1 AND a.pk2+1=b.pk2 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 1, 1},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 ORDER BY 1",
		Expected: []sql.Row{
			{0, nil, nil},
			{1, nil, nil},
			{2, int64(2), nil},
			{3, nil, nil},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 ORDER BY 2,3",
		Expected: []sql.Row{
			{nil, nil, nil},
			{nil, nil, nil},
			{nil, nil, 5.0},
			{2, int64(2), nil},
			{nil, int64(4), 4.0},
			{nil, int64(6), 6.0},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 AND f IS NOT NULL ORDER BY 1", // AND clause causes right table join miss
		Expected: []sql.Row{
			{0, nil, nil},
			{1, nil, nil},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 and pk > 0 ORDER BY 2,3", // > 0 clause in join condition is ignored
		Expected: []sql.Row{
			{nil, nil, nil},
			{nil, nil, nil},
			{nil, nil, 5.0},
			{2, int64(2), nil},
			{nil, int64(4), 4.0},
			{nil, int64(6), 6.0},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 IS NOT NULL ORDER BY 1",
		Expected: []sql.Row{
			{2, int64(2), nil},
		},
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NULL AND pk < 2 ORDER BY 1",
		Expected: []sql.Row{
			{0, nil, nil},
			{1, 1, nil},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
		Expected: []sql.Row{
			{nil, nil, 5.0},
			{nil, int64(4), 4.0},
			{nil, int64(6), 6.0},
		},
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1",
		Expected: []sql.Row{
			{2, 2, nil},
			{3, 3, nil},
		},
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i2 > 1 ORDER BY 1",
		Expected: []sql.Row{
			{2, 2, nil},
		},
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE i > 1 ORDER BY 1",
		Expected: []sql.Row{
			{2, 2, nil},
			{3, 3, nil},
		},
	},
	{
		Query: "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE c1 > 10 ORDER BY 1",
		Expected: []sql.Row{
			{2, 2, nil},
			{3, 3, nil},
		},
	},
	{
		Query: "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
		Expected: []sql.Row{
			{nil, 4, 4.0},
			{nil, 5, 5.0},
			{nil, 6, 6.0},
		},
	},
	{
		Query: "SELECT t1.i,t1.i2 FROM niltable t1 LEFT JOIN niltable t2 ON t1.i=t2.i2 WHERE t2.f IS NULL ORDER BY 1,2",
		Expected: []sql.Row{
			{1, nil},
			{2, 2},
			{3, nil},
			{5, nil},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk LEFT JOIN niltable ON pk=i2 WHERE pk > 1 ORDER BY 1",
		Expected: []sql.Row{
			{2, int64(2), nil},
			{3, nil, nil},
		},
	},
	{
		Query: "SELECT pk,i2,f FROM one_pk RIGHT JOIN niltable ON pk=i2 WHERE pk > 0 ORDER BY 2,3",
		Expected: []sql.Row{
			{2, int64(2), nil},
		},
	},
	{
		Query: "SELECT GREATEST(CAST(i AS CHAR), CAST(b AS CHAR)) FROM niltable order by i",
		Expected: []sql.Row{
			{nil},
			{"2"},
			{"3"},
			{nil},
			{"5"},
			{"6"},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0, 0, 0},
			{1, 0, 1, 10, 10},
			{2, 1, 0, 20, 20},
			{3, 1, 1, 30, 30},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
		Expected: []sql.Row{
			{1, 0, 1, 10, 10},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
		Expected: []sql.Row{
			{0, 1, 0},
		},
	},
	{
		Query: "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
		Expected: []sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 0},
			{0, 1, 1},
			{1, 0, 0},
			{1, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, 0, 0},
			{2, 0, 1},
			{2, 1, 0},
			{2, 1, 1},
			{3, 0, 0},
			{3, 0, 1},
			{3, 1, 0},
			{3, 1, 1},
		},
	},
	{
		Query: "SELECT a.pk,b.pk FROM one_pk a JOIN one_pk b ON a.pk = b.pk order by a.pk",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: "SELECT a.pk,b.pk FROM one_pk a, one_pk b WHERE a.pk = b.pk order by a.pk",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: "SELECT one_pk.pk,b.pk FROM one_pk JOIN one_pk b ON one_pk.pk = b.pk order by one_pk.pk",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query: "SELECT one_pk.pk,b.pk FROM one_pk, one_pk b WHERE one_pk.pk = b.pk order by one_pk.pk",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{2, 2},
			{3, 3},
		},
	},
	{
		Query:    "SELECT 2.0 + CAST(5 AS DECIMAL)",
		Expected: []sql.Row{{float64(7)}},
	},
	{
		Query:    "SELECT (CASE WHEN i THEN i ELSE 0 END) as cases_i from mytable",
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    "SELECT 1/0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT 0/0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT 1.0/0.0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT 0.0/0.0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT 1 div 0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT 1.0 div 0.0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT 0 div 0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT 0.0 div 0.0 FROM dual",
		Expected: []sql.Row{{sql.Null}},
	},
	{
		Query:    "SELECT NULL <=> NULL FROM dual",
		Expected: []sql.Row{{1}},
	},
	{
		Query:    "SELECT POW(2,3) FROM dual",
		Expected: []sql.Row{{float64(8)}},
	},
	{
		Query: "SELECT * FROM people WHERE last_name='doe' and first_name='jane' order by dob",
		Expected: []sql.Row{
			sql.NewRow(dob(1990, 2, 21), "jane", "doe", "", int64(68), int64(1)),
			sql.NewRow(dob(2010, 3, 15), "jane", "doe", "", int64(69), int64(1)),
		},
	},
	{
		Query: "SELECT count(*) FROM people WHERE last_name='doe' and first_name='jane' order by dob",
		Expected: []sql.Row{
			sql.NewRow(2),
		},
	},
	{
		Query: "SELECT VALUES(i) FROM mytable",
		Expected: []sql.Row{
			sql.NewRow(nil),
			sql.NewRow(nil),
			sql.NewRow(nil),
		},
	},
	{
		Query: `select i, row_number() over (order by i desc), 
				row_number() over (order by length(s),i) from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 3, 1},
			{2, 2, 3},
			{3, 1, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) from mytable where i = 2 order by 1;`,
		Expected: []sql.Row{
			{2, 1},
		},
	},
	{
		Query: `SELECT i, (SELECT row_number() over (order by ot.i2 desc) FROM othertable ot where ot.i2 = mt.i) from mytable mt order by 1;`,
		Expected: []sql.Row{
			{1, 1},
			{2, 1},
			{3, 1},
		},
	},
	{
		Query: `select row_number() over (order by i desc), 
				row_number() over (order by length(s),i) from mytable order by i;`,
		Expected: []sql.Row{
			{3, 1},
			{2, 3},
			{1, 2},
		},
	},
	{
		Query: `select *, row_number() over (order by i desc), 
				row_number() over (order by length(s),i) from mytable order by i;`,
		Expected: []sql.Row{
			{1, "first row", 3, 1},
			{2, "second row", 2, 3},
			{3, "third row", 1, 2},
		},
	},
	{
		Query: `select row_number() over (order by i desc), 
				row_number() over (order by length(s),i) 
				from mytable mt join othertable ot 
				on mt.i = ot.i2    
				order by mt.i;`,
		Expected: []sql.Row{
			{3, 1},
			{2, 3},
			{1, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc), 
				row_number() over (order by length(s),i) from mytable order by 1 desc;`,
		Expected: []sql.Row{
			{3, 1, 2},
			{2, 2, 3},
			{1, 3, 1},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) as i_num,
				row_number() over (order by length(s),i) as s_num from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 3, 1},
			{2, 2, 3},
			{3, 1, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) + 3,
			row_number() over (order by length(s),i) as s_asc, 
			row_number() over (order by length(s) desc,i desc) as s_desc 
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 6, 1, 3},
			{2, 5, 3, 1},
			{3, 4, 2, 2},
		},
	},
	{
		Query: `select i, row_number() over (order by i desc) + 3,
			row_number() over (order by length(s),i) + 0.0 / row_number() over (order by length(s) desc,i desc) + 0.0  
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 6, 1.0},
			{2, 5, 3.0},
			{3, 4, 2.0},
		},
	},
	{
		Query: "select pk1, pk2, row_number() over (partition by pk1 order by c1 desc) from two_pk order by 1,2;",
		Expected: []sql.Row{
			{0, 0, 2},
			{0, 1, 1},
			{1, 0, 2},
			{1, 1, 1},
		},
	},
	{
		Query: `select pk1, pk2, 
			row_number() over (partition by pk1 order by c1 desc) 
			from two_pk order by 1,2;`,
		Expected: []sql.Row{
			{0, 0, 2},
			{0, 1, 1},
			{1, 0, 2},
			{1, 1, 1},
		},
	},
	{
		Query: `select pk1, pk2, 
			row_number() over (partition by pk1 order by c1 desc), 
			row_number() over (partition by pk2 order by 10 - c1)
			from two_pk order by 1,2;`,
		Expected: []sql.Row{
			{0, 0, 2, 2},
			{0, 1, 1, 2},
			{1, 0, 2, 1},
			{1, 1, 1, 1},
		},
	},
	{
		Query: `select pk1, pk2, 
			row_number() over (partition by pk1 order by c1 desc), 
			row_number() over (partition by pk2 order by 10 - c1),
			max(c4) over ()
			from two_pk order by 1,2;`,
		Expected: []sql.Row{
			{0, 0, 2, 2, 33},
			{0, 1, 1, 2, 33},
			{1, 0, 2, 1, 33},
			{1, 1, 1, 1, 33},
		},
	},
	{
		Query: `select i,
			row_number() over (partition by case when i > 2 then "under two" else "over two" end order by i desc) as s_asc
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1, 2},
			{2, 1},
			{3, 1},
		},
	},
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		Expected: []sql.Row{
			{0, 0, 0, 0, 0},
			{1, 1, 1, 1, 1},
		},
	},
	{
		Query: `SELECT pk,nt.i,nt2.i FROM one_pk
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i - 1
						ORDER BY 3`,
		Expected: []sql.Row{
			{nil, nil, 1},
			{1, 1, 2},
			{2, 2, 3},
			{3, 3, 4},
			{nil, nil, 5},
			{nil, nil, 6},
		},
	},
	{
		Query: `SELECT pk,pk2,
							(SELECT opk.c5 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1 LIMIT 1)
							FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		Expected: []sql.Row{
			{1, 1, 4},
			{1, 1, 4},
		},
	},
	{
		Query: `SELECT pk,pk2,
							(SELECT opk.c5 FROM one_pk opk JOIN two_pk tpk ON opk.c5=tpk.c5 ORDER BY 1 LIMIT 1)
							FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		Expected: []sql.Row{
			{1, 1, 4},
			{1, 1, 4},
		},
	},
	{
		Query: `SELECT /*+ JOIN_ORDER(mytable, othertable) */ s2, i2, i FROM mytable INNER JOIN (SELECT * FROM othertable) othertable ON i2 = i`,
		Expected: []sql.Row{
			{"third", 1, 1},
			{"second", 2, 2},
			{"first", 3, 3},
		},
	},
	{
		Query: `SELECT lefttable.i, righttable.s
			FROM (SELECT * FROM mytable) lefttable
			JOIN (SELECT * FROM mytable) righttable
			ON lefttable.i = righttable.i AND righttable.s = lefttable.s
			ORDER BY lefttable.i ASC`,
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"},
		},
	},
	{
		Query: "SELECT BINARY 'hi'",
		Expected: []sql.Row{
			{"hi"},
		},
	},
	{
		Query: "SELECT BINARY 1",
		Expected: []sql.Row{
			{"1"},
		},
	},
	{
		Query: "SELECT BINARY 1 = 1",
		Expected: []sql.Row{
			{true},
		},
	},
	{
		Query: "SELECT BINARY 'hello' = 'hello'",
		Expected: []sql.Row{
			{true},
		},
	},
	{
		Query: "SELECT BINARY NULL",
		Expected: []sql.Row{
			{nil},
		},
	},
	{
		Query:    "SELECT JSON_CONTAINS(NULL, 1)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT JSON_CONTAINS(1, NULL)",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    "SELECT JSON_CONTAINS(1, NULL, '$.a')",
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '1', '$.a')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '1', '$.b')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '{"d": 4}', '$.a')`,
		Expected: []sql.Row{{false}},
	},
	{
		Query:    `SELECT JSON_CONTAINS('{"a": 1, "b": 2, "c": {"d": 4}}', '{"d": 4}', '$.c')`,
		Expected: []sql.Row{{true}},
	},
	{
		Query: "select one_pk.pk, one_pk.c1 from one_pk join two_pk on one_pk.c1 = two_pk.c1 order by two_pk.c1",
		Expected: []sql.Row{
			{0, 0},
			{1, 10},
			{2, 20},
			{3, 30},
		},
	},
	{
		Query: `SELECT JSON_OBJECT("i",i,"s",s) as js FROM mytable;`,
		Expected: []sql.Row{
			{sql.MustJSON(`{"i": 1, "s": "first row"}`)},
			{sql.MustJSON(`{"i": 2, "s": "second row"}`)},
			{sql.MustJSON(`{"i": 3, "s": "third row"}`)},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "js",
				Type: sql.JSON,
			},
		},
	},
}

var KeylessQueries = []QueryTest{
	{
		Query: "SELECT * FROM keyless ORDER BY c0",
		Expected: []sql.Row{
			{0, 0},
			{1, 1},
			{1, 1},
			{2, 2},
		},
	},
	{
		Query: "SELECT * FROM keyless ORDER BY c1 DESC",
		Expected: []sql.Row{
			{2, 2},
			{1, 1},
			{1, 1},
			{0, 0},
		},
	},
	{
		Query: "SELECT * FROM keyless JOIN myTable where c0 = i",
		Expected: []sql.Row{
			{1, 1, 1, "first row"},
			{1, 1, 1, "first row"},
			{2, 2, 2, "second row"},
		},
	},
	{
		Query: "SELECT * FROM myTable JOIN keyless WHERE i = c0 ORDER BY i",
		Expected: []sql.Row{
			{1, "first row", 1, 1},
			{1, "first row", 1, 1},
			{2, "second row", 2, 2},
		},
	},
	{
		Query: "DESCRIBE keyless",
		Expected: []sql.Row{
			{"c0", "bigint", "YES", "", "", ""},
			{"c1", "bigint", "YES", "", "", ""},
		},
	},
	{
		Query: "SHOW COLUMNS FROM keyless",
		Expected: []sql.Row{
			{"c0", "bigint", "YES", "", "", ""},
			{"c1", "bigint", "YES", "", "", ""},
		},
	},
	{
		Query: "SHOW FULL COLUMNS FROM keyless",
		Expected: []sql.Row{
			{"c0", "bigint", nil, "YES", "", "", "", "", ""},
			{"c1", "bigint", nil, "YES", "", "", "", "", ""},
		},
	},
	{
		Query: "SHOW CREATE TABLE keyless",
		Expected: []sql.Row{
			{"keyless", "CREATE TABLE `keyless` (\n  `c0` bigint,\n  `c1` bigint\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		},
	},
}

// Queries that are known to be broken in the engine.
var BrokenQueries = []QueryTest{
	{
		Query:    "SELECT pk1, SUM(c1) FROM two_pk",
		Expected: []sql.Row{{0, 60.0}},
	},
	// this doesn't parse in MySQL (can't use an alias in a where clause), panics in engine
	{
		Query: `SELECT pk, (SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS x 
						FROM one_pk opk WHERE x > 0 ORDER BY x`,
		Expected: []sql.Row{
			{2, 1},
			{3, 2},
		},
	},
	{
		Query: `SELECT pk,
					(SELECT max(pk) FROM one_pk WHERE pk < opk.pk) AS min,
					(SELECT min(pk) FROM one_pk WHERE pk > opk.pk) AS max
					FROM one_pk opk
					WHERE max > 1
					ORDER BY max;`,
		Expected: []sql.Row{
			{1, 0, 2},
			{2, 1, 3},
		},
	},
	// AVG gives the wrong result for the first row
	{
		Query: `SELECT pk,
						(SELECT sum(c1) FROM two_pk WHERE c1 IN (SELECT c4 FROM two_pk WHERE c3 > opk.c5)) AS sum,
						(SELECT avg(c1) FROM two_pk WHERE pk2 IN (SELECT pk2 FROM two_pk WHERE c1 < opk.c2)) AS avg
					FROM one_pk opk ORDER BY pk`,
		Expected: []sql.Row{
			{0, 60.0, nil},
			{1, 50.0, 10.0},
			{2, 30.0, 15.0},
			{3, nil, 15.0},
		},
	},
	// something broken in the resolve_having analysis for this
	{
		Query: `SELECT column_0, sum(column_1) FROM 
			(values row(1,1), row(1,3), row(2,2), row(2,5), row(3,9)) a 
			group by 1 having avg(column_1) > 2 order by 1`,
		Expected: []sql.Row{
			{2, 7.0},
			{3, 9.0},
		},
	},
	// The outer CTE currently resolves before the inner one, which causes
	// this to return { {1}, {1}, } instead.
	{
		Query: `WITH t AS (SELECT 1) SELECT * FROM t UNION (WITH t AS (SELECT 2) SELECT * FROM t)`,
		Expected: []sql.Row{
			{1},
			{2},
		},
	},
	{
		Query: "SELECT json_array() FROM dual;",
	},
	{
		Query: "SELECT json_array_append() FROM dual;",
	},
	{
		Query: "SELECT json_array_insert() FROM dual;",
	},
	{
		Query: "SELECT json_contains() FROM dual;",
	},
	{
		Query: "SELECT json_contains_path() FROM dual;",
	},
	{
		Query: "SELECT json_depth() FROM dual;",
	},
	{
		Query: "SELECT json_insert() FROM dual;",
	},
	{
		Query: "SELECT json_keys() FROM dual;",
	},
	{
		Query: "SELECT json_length() FROM dual;",
	},
	{
		Query: "SELECT json_merge_patch() FROM dual;",
	},
	{
		Query: "SELECT json_merge_preserve() FROM dual;",
	},
	{
		Query: "SELECT json_object() FROM dual;",
	},
	{
		Query: "SELECT json_overlaps() FROM dual;",
	},
	{
		Query: "SELECT json_pretty() FROM dual;",
	},
	{
		Query: "SELECT json_quote() FROM dual;",
	},
	{
		Query: "SELECT json_remove() FROM dual;",
	},
	{
		Query: "SELECT json_replace() FROM dual;",
	},
	{
		Query: "SELECT json_schema_valid() FROM dual;",
	},
	{
		Query: "SELECT json_schema_validation_report() FROM dual;",
	},
	{
		Query: "SELECT json_set() FROM dual;",
	},
	{
		Query: "SELECT json_search() FROM dual;",
	},
	{
		Query: "SELECT json_storage_free() FROM dual;",
	},
	{
		Query: "SELECT json_storage_size() FROM dual;",
	},
	{
		Query: "SELECT json_type() FROM dual;",
	},
	{
		Query: "SELECT json_table() FROM dual;",
	},
	{
		Query: "SELECT json_valid() FROM dual;",
	},
	{
		Query: "SELECT json_value() FROM dual;",
	},
	// This gets an error "unable to cast "second row" of type string to int64"
	// Should throw sql.ErrAmbiguousColumnInOrderBy
	{
		Query: `SELECT s as i, i as i from mytable order by i`,
	},
	// These three queries return the right results, but the casing is wrong in the result schema.
	{
		Query: "SELECT i, I, s, S FROM mytable;",
		Expected: []sql.Row{
			{1, 1, "first row", "first row"},
			{2, 2, "second row", "second row"},
			{3, 3, "third row", "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: sql.Int64,
			},
			{
				Name: "I",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "S",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `i`, `I`, `s`, `S` FROM mytable;",
		Expected: []sql.Row{
			{1, 1, "first row", "first row"},
			{2, 2, "second row", "second row"},
			{3, 3, "third row", "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: sql.Int64,
			},
			{
				Name: "I",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "S",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
	{
		Query: "SELECT `mytable`.`i`, `mytable`.`I`, `mytable`.`s`, `mytable`.`S` FROM mytable;",
		Expected: []sql.Row{
			{1, 1, "first row", "first row"},
			{2, 2, "second row", "second row"},
			{3, 3, "third row", "third row"},
		},
		ExpectedColumns: sql.Schema{
			{
				Name: "i",
				Type: sql.Int64,
			},
			{
				Name: "I",
				Type: sql.Int64,
			},
			{
				Name: "s",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
			{
				Name: "S",
				Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
			},
		},
	},
}

var VersionedQueries = []QueryTest{
	{
		Query: "SELECT *  FROM myhistorytable AS OF '2019-01-01' AS foo ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 1"},
			{int64(2), "second row, 1"},
			{int64(3), "third row, 1"},
		},
	},
	{
		Query: "SELECT *  FROM myhistorytable AS OF '2019-01-02' foo ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
	// Testing support of function evaluation in AS OF
	{
		Query: "SELECT *  FROM myhistorytable AS OF GREATEST('2019-01-02','2019-01-01','') foo ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
	{
		Query: "SELECT *  FROM myhistorytable ORDER BY i",
		Expected: []sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
	{
		Query: "SHOW TABLES AS OF '2019-01-02' LIKE 'myhistorytable'",
		Expected: []sql.Row{
			{"myhistorytable"},
		},
	},
	{
		Query: "SHOW TABLES FROM mydb AS OF '2019-01-02' LIKE 'myhistorytable'",
		Expected: []sql.Row{
			{"myhistorytable"},
		},
	},
}

var InfoSchemaQueries = []QueryTest{
	{
		Query: "SHOW TABLES",
		Expected: []sql.Row{
			{"auto_increment_tbl"},
			{"bigtable"},
			{"floattable"},
			{"fk_tbl"},
			{"mytable"},
			{"myview"},
			{"newlinetable"},
			{"niltable"},
			{"othertable"},
			{"tabletest"},
			{"people"},
			{"datetime_table"},
		},
	},
	{
		Query: "SHOW FULL TABLES",
		Expected: []sql.Row{
			{"auto_increment_tbl", "BASE TABLE"},
			{"bigtable", "BASE TABLE"},
			{"fk_tbl", "BASE TABLE"},
			{"floattable", "BASE TABLE"},
			{"mytable", "BASE TABLE"},
			{"myview", "VIEW"},
			{"newlinetable", "BASE TABLE"},
			{"niltable", "BASE TABLE"},
			{"othertable", "BASE TABLE"},
			{"tabletest", "BASE TABLE"},
			{"people", "BASE TABLE"},
			{"datetime_table", "BASE TABLE"},
		},
	},
	{
		Query: "SHOW TABLES FROM foo",
		Expected: []sql.Row{
			{"other_table"},
		},
	},
	{
		Query: "SHOW TABLES LIKE '%table'",
		Expected: []sql.Row{
			{"mytable"},
			{"othertable"},
			{"bigtable"},
			{"floattable"},
			{"niltable"},
			{"newlinetable"},
			{"datetime_table"},
		},
	},
	{
		Query: `SHOW COLUMNS FROM mytable`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
			{"s", "varchar(20)", "NO", "UNI", "", ""},
		},
	},
	{
		Query: `DESCRIBE mytable`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
			{"s", "varchar(20)", "NO", "UNI", "", ""},
		},
	},
	{
		Query: `DESC mytable`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
			{"s", "varchar(20)", "NO", "UNI", "", ""},
		},
	},
	{
		Query: `DESCRIBE auto_increment_tbl`,
		Expected: []sql.Row{
			{"pk", "bigint", "NO", "PRI", "", "auto_increment"},
			{"c0", "bigint", "YES", "", "", ""},
		},
	},
	{
		Query: `SHOW COLUMNS FROM mytable WHERE Field = 'i'`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
		},
	},
	{
		Query: `SHOW COLUMNS FROM mytable LIKE 'i'`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "", ""},
		},
	},
	{
		Query: `SHOW FULL COLUMNS FROM mytable`,
		Expected: []sql.Row{
			{"i", "bigint", nil, "NO", "PRI", "", "", "", ""},
			{"s", "varchar(20)", "utf8mb4_0900_ai_ci", "NO", "UNI", "", "", "", "column s"},
		},
	},
	{
		Query: "SHOW TABLES WHERE `Table` = 'mytable'",
		Expected: []sql.Row{
			{"mytable"},
		},
	},
	{
		Query: `
		SELECT
			LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'UNDO LOG'
			AND FILE_NAME IS NOT NULL
			AND LOGFILE_GROUP_NAME IS NOT NULL
		GROUP BY LOGFILE_GROUP_NAME, FILE_NAME, ENGINE, TOTAL_EXTENTS, INITIAL_SIZE
		ORDER BY LOGFILE_GROUP_NAME
		`,
		Expected: nil,
	},
	{
		Query: `
		SELECT DISTINCT
			TABLESPACE_NAME, FILE_NAME, LOGFILE_GROUP_NAME, EXTENT_SIZE, INITIAL_SIZE, ENGINE
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'DATAFILE'
		ORDER BY TABLESPACE_NAME, LOGFILE_GROUP_NAME
		`,
		Expected: nil,
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
	{
		Query: `
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA='mydb' AND (TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW')
		ORDER BY 1
		`,
		Expected: []sql.Row{
			{"auto_increment_tbl"},
			{"bigtable"},
			{"datetime_table"},
			{"fk_tbl"},
			{"floattable"},
			{"mytable"},
			{"myview"},
			{"newlinetable"},
			{"niltable"},
			{"othertable"},
			{"people"},
			{"tabletest"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='mytable'
		`,
		Expected: []sql.Row{
			{"s", "varchar(20)"},
			{"i", "bigint"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY COLUMN_NAME
		`,
		Expected: []sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
			{"date_col"},
			{"datetime_col"},
			{"timestamp_col"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		Expected: []sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
			{"date_col"},
			{"datetime_col"},
			{"timestamp_col"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		Expected: []sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
			{"date_col"},
			{"datetime_col"},
			{"timestamp_col"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1 HAVING SUBSTRING(COLUMN_NAME, 1, 1) = "s"
		`,
		Expected: []sql.Row{{"s"}, {"s2"}},
	},
	{
		Query: `SHOW INDEXES FROM mytaBLE`,
		Expected: []sql.Row{
			{"mytable", 0, "PRIMARY", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 0, "mytable_s", 1, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 2, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: `SHOW KEYS FROM mytaBLE`,
		Expected: []sql.Row{
			{"mytable", 0, "PRIMARY", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 0, "mytable_s", 1, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 2, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: `SHOW CREATE TABLE mytaBLE`,
		Expected: []sql.Row{
			{"mytable", "CREATE TABLE `mytable` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20) NOT NULL COMMENT 'column s',\n" +
				"  PRIMARY KEY (`i`),\n" +
				"  KEY `mytable_i_s` (`i`,`s`),\n" +
				"  UNIQUE KEY `mytable_s` (`s`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		},
	},
	{
		Query: `SHOW CREATE TABLE fk_TBL`,
		Expected: []sql.Row{
			{"fk_tbl", "CREATE TABLE `fk_tbl` (\n" +
				"  `pk` bigint NOT NULL,\n" +
				"  `a` bigint,\n" +
				"  `b` varchar(20),\n" +
				"  PRIMARY KEY (`pk`),\n" +
				"  CONSTRAINT `fk1` FOREIGN KEY (`a`,`b`) REFERENCES `mytable` (`i`,`s`) ON DELETE CASCADE\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"},
		},
	},
	{

		Query: "SELECT table_name, `auto_increment` FROM information_schema.tables " +
			"WHERE TABLE_SCHEMA='mydb' AND TABLE_TYPE='BASE TABLE' ORDER BY 1",
		Expected: []sql.Row{
			{"auto_increment_tbl", 4},
			{"bigtable", nil},
			{"datetime_table", nil},
			{"fk_tbl", nil},
			{"floattable", nil},
			{"mytable", nil},
			{"newlinetable", nil},
			{"niltable", nil},
			{"othertable", nil},
			{"people", nil},
			{"tabletest", nil},
		},
	},
	{
		Query: "SHOW ENGINES",
		Expected: []sql.Row{
			{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
		},
	},
	{
		Query: "SELECT * FROM information_schema.table_constraints ORDER BY table_name, constraint_type;",
		Expected: []sql.Row{
			{"def", "mydb", "PRIMARY", "mydb", "auto_increment_tbl", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "bigtable", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "datetime_table", "PRIMARY KEY", "YES"},
			{"def", "mydb", "fk1", "mydb", "fk_tbl", "FOREIGN KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "fk_tbl", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "floattable", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "mytable", "PRIMARY KEY", "YES"},
			{"def", "mydb", "mytable_s", "mydb", "mytable", "UNIQUE", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "newlinetable", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "niltable", "PRIMARY KEY", "YES"},
			{"def", "foo", "PRIMARY", "foo", "other_table", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "othertable", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "people", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "tabletest", "PRIMARY KEY", "YES"},
		},
	},
	{
		Query:    "SELECT * FROM information_schema.check_constraints ORDER BY constraint_schema, constraint_name, check_clause ",
		Expected: []sql.Row{},
	},
	{
		Query: "SELECT * FROM information_schema.key_column_usage ORDER BY constraint_schema, table_name",
		Expected: []sql.Row{
			{"def", "foo", "PRIMARY", "def", "foo", "other_table", "text", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "auto_increment_tbl", "pk", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "bigtable", "t", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "datetime_table", "i", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "fk_tbl", "pk", 1, nil, nil, nil, nil},
			{"def", "mydb", "fk1", "def", "mydb", "fk_tbl", "a", 1, 1, "mydb", "mytable", "i"},
			{"def", "mydb", "fk1", "def", "mydb", "fk_tbl", "b", 2, 2, "mydb", "mytable", "s"},
			{"def", "mydb", "PRIMARY", "def", "mydb", "floattable", "i", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "mytable", "i", 1, nil, nil, nil, nil},
			{"def", "mydb", "mytable_s", "def", "mydb", "mytable", "s", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "newlinetable", "i", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "niltable", "i", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "othertable", "i2", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "people", "dob", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "people", "first_name", 2, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "people", "last_name", 3, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "people", "middle_name", 4, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "tabletest", "i", 1, nil, nil, nil, nil},
		},
	},
	{
		Query:    "SELECT * FROM information_schema.partitions",
		Expected: []sql.Row{},
	},
}

var InfoSchemaScripts = []ScriptTest{
	{
		Name: "describe auto_increment table",
		SetUpScript: []string{
			"create table auto (pk int primary key auto_increment)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "describe auto;",
				Expected: []sql.Row{
					{"pk", "int", "NO", "PRI", "", "auto_increment"},
				},
			},
		},
	},
	{
		Name: "information_schema.table_constraints ignores non-unique indexes",
		SetUpScript: []string{
			"CREATE TABLE mytable (pk int primary key, test_score int, height int)",
			"CREATE INDEX myindex on mytable(test_score)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.table_constraints where table_name='mytable' ORDER BY constraint_type,constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "mydb", "mytable", "PRIMARY KEY", "YES"},
				},
			},
		},
	},
	{
		Name: "information_schema.key_column_usage ignores non-unique indexes",
		SetUpScript: []string{
			"CREATE TABLE mytable (pk int primary key, test_score int, height int)",
			"CREATE INDEX myindex on mytable(test_score)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.key_column_usage where table_name='mytable'",
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "def", "mydb", "mytable", "pk", 1, nil, nil, nil, nil},
				},
			},
		},
	},
	{
		Name: "information_schema.key_column_usage works with composite foreign keys",
		SetUpScript: []string{
			"CREATE TABLE ptable (pk int primary key, test_score int, height int)",
			"CREATE INDEX myindex on ptable(test_score, height)",
			"CREATE TABLE ptable2 (pk int primary key, test_score2 int, height2 int, CONSTRAINT fkr FOREIGN KEY (test_score2, height2) REFERENCES ptable(test_score,height));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.key_column_usage where table_name='ptable2' ORDER BY constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "def", "mydb", "ptable2", "pk", 1, nil, nil, nil, nil},
					{"def", "mydb", "fkr", "def", "mydb", "ptable2", "test_score2", 1, 1, "mydb", "ptable", "test_score"},
					{"def", "mydb", "fkr", "def", "mydb", "ptable2", "height2", 2, 2, "mydb", "ptable", "height"},
				},
			},
		},
	},
	{
		Name: "information_schema.key_column_usage works with composite primary keys",
		SetUpScript: []string{
			"CREATE TABLE ptable (pk int, test_score int, height int, PRIMARY KEY (pk, test_score))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.key_column_usage where table_name='ptable' ORDER BY constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "def", "mydb", "ptable", "pk", 1, nil, nil, nil, nil},
					{"def", "mydb", "PRIMARY", "def", "mydb", "ptable", "test_score", 2, nil, nil, nil, nil},
				},
			},
		},
	},
}

var ExplodeQueries = []QueryTest{
	{
		Query: `SELECT a, EXPLODE(b), c FROM t`,
		Expected: []sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		Query: `SELECT a, EXPLODE(b) AS x, c FROM t`,
		Expected: []sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		Query: `SELECT EXPLODE(SPLIT(c, "")) FROM t LIMIT 5`,
		Expected: []sql.Row{
			{"f"},
			{"i"},
			{"r"},
			{"s"},
			{"t"},
		},
	},
	{
		Query: `SELECT a, EXPLODE(b) AS x, c FROM t WHERE x = 'e'`,
		Expected: []sql.Row{
			{int64(3), "e", "third"},
		},
	},
	{
		Query: `SELECT HEX(UNHEX(375));`,
		Expected: []sql.Row{
			{"0375"},
		},
	},
}

type QueryErrorTest struct {
	Query          string
	Bindings       map[string]sql.Expression
	ExpectedErr    *errors.Kind
	ExpectedErrStr string
}

var errorQueries = []QueryErrorTest{
	{
		Query:       "select foo.i from mytable as a",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.i from mytable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.* from mytable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select foo.* from mytable as a",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select x from mytable",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "select mytable.x from mytable",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select a.x from mytable as a",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "select a from notable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select myTable.i from mytable as mt", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "select myTable.* from mytable as mt", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT one_pk.c5,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 ORDER BY 1,2,3", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk,pk1,pk2 FROM one_pk opk JOIN two_pk tpk ON one_pk.pk=two_pk.pk1 AND opk.pk=tpk.pk2 ORDER BY 1,2,3", // alias overwrites the original table name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT t.i, myview1.s FROM myview AS t ORDER BY i", // alias overwrites the original view name
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT * FROM mytable AS t, othertable as t", // duplicate alias
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "SELECT * FROM mytable AS t UNION SELECT * FROM mytable AS t, othertable AS t", // duplicate alias in union
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       "SELECT * FROM mytable AS OTHERTABLE, othertable", // alias / table conflict
		ExpectedErr: sql.ErrDuplicateAliasOrTable,
	},
	{
		Query:       `SELECT * FROM mytable WHERE s REGEXP("*main.go")`,
		ExpectedErr: expression.ErrInvalidRegexp,
	},
	{
		Query:       `SELECT SUBSTRING(s, 1, 10) AS sub_s, SUBSTRING(SUB_S, 2, 3) AS sub_sub_s FROM mytable`,
		ExpectedErr: sql.ErrMisusedAlias,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk b WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM one_pk WHERE b.pk <= one_pk.pk) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM two_pk WHERE pk <= one_pk.pk3) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM dne WHERE pk <= one_pk.pk3) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query:       "SELECT pk, (SELECT max(pk) FROM two_pk WHERE pk <= c6) FROM one_pk ORDER BY 1",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "SELECT i FROM myhistorytable AS OF abc",
		ExpectedErr: sql.ErrInvalidAsOfExpression,
	},
	{
		Query:       "SELECT i FROM myhistorytable AS OF MAX(abc)",
		ExpectedErr: sql.ErrInvalidAsOfExpression,
	},
	{
		Query:       "SELECT pk FROM one_pk WHERE pk > ?",
		ExpectedErr: sql.ErrUnboundPreparedStatementVariable,
	},
	{
		Query:       "SELECT pk FROM one_pk WHERE pk > :pk",
		ExpectedErr: sql.ErrUnboundPreparedStatementVariable,
	},
	{
		Query:       "with cte1 as (SELECT c3 FROM one_pk WHERE c4 < opk.c2 ORDER BY 1 DESC LIMIT 1)  SELECT pk, (select c3 from cte1) FROM one_pk opk ORDER BY 1",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Query: `WITH mt1 (x,y) as (select i,s FROM mytable)
			SELECT mt1.i, mt1.s FROM mt1`,
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query: `WITH mt1 (x,y) as (select i,s FROM mytable)
			SELECT i, s FROM mt1`,
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query: `WITH mt1 (x,y,z) as (select i,s FROM mytable)
			SELECT i, s FROM mt1`,
		ExpectedErr: sql.ErrColumnCountMismatch,
	},
	// TODO: this results in a stack overflow, need to check for this
	// {
	// 	Query: `WITH mt1 as (select i,s FROM mt2), mt2 as (select i,s from mt1)
	// 		SELECT i, s FROM mt1`,
	// 	ExpectedErr: sql.ErrColumnCountMismatch,
	// },
	// TODO: related to the above issue, CTEs are only allowed to mentioned previously defined CTEs (to prevent cycles).
	//  This query works, but shouldn't
	// {
	// 	Query: `WITH mt1 as (select i,s FROM mt2), mt2 as (select i,s from mytable)
	// 		SELECT i, s FROM mt1`,
	// 	ExpectedErr: sql.ErrColumnCountMismatch,
	// },
	{
		Query: `WITH mt1 as (select i,s FROM mytable), mt2 as (select i+1, concat(s, '!') from mytable)
			SELECT mt1.i, mt2.s FROM mt1 join mt2 on mt1.i = mt2.i;`,
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	// TODO: this should be an error, as every table alias (including subquery aliases) must be unique
	// {
	// 	Query: "SELECT s,i FROM (select i,s FROM mytable) mt join (select i,s FROM mytable) mt;",
	// 	ExpectedErr: sql.ErrDuplicateAliasOrTable,
	// },
	// TODO: this should be an error, as every table alias must be unique.
	// {
	// 	Query: "WITH mt as (select i,s FROM mytable) SELECT s,i FROM mt join mt;",
	// 	ExpectedErr: sql.ErrDuplicateAliasOrTable,
	// },
	// TODO: Bug: the having column must appear in the select list
	// {
	// 	Query:       "SELECT pk1, sum(c1) FROM two_pk GROUP BY 1 having c1 > 10;",
	// 	ExpectedErr: sql.ErrColumnNotFound,
	// },
	{
		Query:       `SHOW TABLE STATUS FROM baddb`,
		ExpectedErr: sql.ErrDatabaseNotFound,
	},
	{
		Query:       `SELECT s as i, i as i from mytable order by 1`,
		ExpectedErr: sql.ErrAmbiguousColumnInOrderBy,
	},
	{
		Query: `SELECT pk as pk, nt.i  as i, nt2.i as i FROM one_pk
						RIGHT JOIN niltable nt ON pk=nt.i
						RIGHT JOIN niltable nt2 ON pk=nt2.i - 1
						ORDER BY 3`,
		ExpectedErr: sql.ErrAmbiguousColumnInOrderBy,
	},
	{
		Query:       "SELECT C FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "SELECT i FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		ExpectedErr: sql.ErrColumnNotFound,
	},
	{
		Query:       "SELECT mt.i FROM (select i,s FROM mytable) mt (a,b) order by a desc;",
		ExpectedErr: sql.ErrTableColumnNotFound,
	},
	{
		Query:       "SELECT a FROM (select i,s FROM mytable) mt (a) order by a desc;",
		ExpectedErr: sql.ErrColumnCountMismatch,
	},
	{
		Query:       "SELECT a FROM (select i,s FROM mytable) mt (a,b,c) order by a desc;",
		ExpectedErr: sql.ErrColumnCountMismatch,
	},
	{
		Query:       "SELECT i FROM mytable limit ?",
		ExpectedErr: sql.ErrInvalidSyntax,
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(-100, sql.Int8),
		},
	},
	{
		Query:       "SELECT i FROM mytable limit ?",
		ExpectedErr: sql.ErrInvalidType,
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral("100", sql.LongText),
		},
	},
	{
		Query:       "SELECT i FROM mytable limit 10, ?",
		ExpectedErr: sql.ErrInvalidSyntax,
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral(-100, sql.Int8),
		},
	},
	{
		Query:       "SELECT i FROM mytable limit 10, ?",
		ExpectedErr: sql.ErrInvalidType,
		Bindings: map[string]sql.Expression{
			"v1": expression.NewLiteral("100", sql.LongText),
		},
	},
	{
		Query:       `SELECT JSON_OBJECT("a","b","c") FROM dual`,
		ExpectedErr: sql.ErrInvalidArgumentNumber,
	},
	{
		Query:       `SELECT JSON_OBJECT(1, 2) FROM dual`,
		ExpectedErr: sql.ErrInvalidType,
	},
	{
		Query:          `select JSON_EXTRACT('{"id":"abc"}', '$.id')-1;`,
		ExpectedErrStr: "unable to cast \"abc\" of type string to float64",
	},
	{
		Query:          `select JSON_EXTRACT('{"id":{"a": "abc"}}', '$.id')-1;`,
		ExpectedErrStr: `unable to cast map[string]interface {}{"a":"abc"} of type map[string]interface {} to float64`,
	},
}

// WriteQueryTest is a query test for INSERT, UPDATE, etc. statements. It has a query to run and a select query to
// validate the results.
type WriteQueryTest struct {
	WriteQuery          string
	ExpectedWriteResult []sql.Row
	SelectQuery         string
	ExpectedSelect      []sql.Row
	Bindings            map[string]sql.Expression
}

// GenericErrorQueryTest is a query test that is used to assert an error occurs for some query, without specifying what
// the error was.
type GenericErrorQueryTest struct {
	Name     string
	Query    string
	Bindings map[string]sql.Expression
}

var ViewTests = []QueryTest{
	{
		Query: "SELECT * FROM myview ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		Query: "SELECT myview.* FROM myview ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		Query: "SELECT i FROM myview ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		Query: "SELECT t.* FROM myview AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
			sql.NewRow(int64(2), "second row"),
			sql.NewRow(int64(3), "third row"),
		},
	},
	{
		Query: "SELECT t.i FROM myview AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		Query: "SELECT * FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		Query: "SELECT i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.* FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		Query: "SELECT t.* FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row"),
		},
	},
	{
		Query: "SELECT t.i FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	// info schema support
	{
		Query: "select * from information_schema.views where table_schema = 'mydb'",
		Expected: []sql.Row{
			sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8mb4_0900_ai_ci"),
			sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview WHERE i = 1", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8mb4_0900_ai_ci"),
		},
	},
	{
		Query: "select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
		Expected: []sql.Row{
			sql.NewRow("myview"),
			sql.NewRow("myview2"),
		},
	},
}

var VersionedViewTests = []QueryTest{
	{
		Query: "SELECT * FROM myview1 ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
			sql.NewRow(int64(2), "second row, 2"),
			sql.NewRow(int64(3), "third row, 2"),
		},
	},
	{
		Query: "SELECT t.* FROM myview1 AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
			sql.NewRow(int64(2), "second row, 2"),
			sql.NewRow(int64(3), "third row, 2"),
		},
	},
	{
		Query: "SELECT t.i FROM myview1 AS t ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
			sql.NewRow(int64(2)),
			sql.NewRow(int64(3)),
		},
	},
	{
		Query: "SELECT * FROM myview1 AS OF '2019-01-01' ORDER BY i",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 1"),
			sql.NewRow(int64(2), "second row, 1"),
			sql.NewRow(int64(3), "third row, 1"),
		},
	},
	{
		Query: "SELECT * FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
		},
	},
	{
		Query: "SELECT i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.i FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT myview2.* FROM myview2",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
		},
	},
	{
		Query: "SELECT t.* FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 2"),
		},
	},
	{
		Query: "SELECT t.i FROM myview2 as t",
		Expected: []sql.Row{
			sql.NewRow(int64(1)),
		},
	},
	{
		Query: "SELECT * FROM myview2 AS OF '2019-01-01'",
		Expected: []sql.Row{
			sql.NewRow(int64(1), "first row, 1"),
		},
	},
	// info schema support
	{
		Query: "select * from information_schema.views where table_schema = 'mydb'",
		Expected: []sql.Row{
			sql.NewRow("def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8mb4_0900_ai_ci"),
			sql.NewRow("def", "mydb", "myview1", "SELECT * FROM myhistorytable", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8mb4_0900_ai_ci"),
			sql.NewRow("def", "mydb", "myview2", "SELECT * FROM myview1 WHERE i = 1", "NONE", "YES", "", "DEFINER", "utf8mb4", "utf8mb4_0900_ai_ci"),
		},
	},
	{
		Query: "select table_name from information_schema.tables where table_schema = 'mydb' and table_type = 'VIEW' order by 1",
		Expected: []sql.Row{
			sql.NewRow("myview"),
			sql.NewRow("myview1"),
			sql.NewRow("myview2"),
		},
	},
}

var ShowTableStatusQueries = []QueryTest{
	{
		Query: `SHOW TABLE STATUS FROM mydb`,
		Expected: []sql.Row{
			{"auto_increment_tbl", "InnoDB", "10", "Fixed", uint64(3), uint64(16), uint64(48), uint64(0), int64(0), int64(0), int64(4), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(65540), uint64(196620), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", uint64(3), uint64(65540), uint64(196620), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", uint64(14), uint64(65540), uint64(917560), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", uint64(6), uint64(24), uint64(144), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"fk_tbl", "InnoDB", "10", "Fixed", uint64(3), uint64(96), uint64(288), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", uint64(6), uint64(32), uint64(192), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", uint64(5), uint64(65540), uint64(327700), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"people", "InnoDB", "10", "Fixed", uint64(5), uint64(196620), uint64(983100), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"datetime_table", "InnoDB", "10", "Fixed", uint64(3), uint64(32), uint64(96), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS LIKE '%table'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(65540), uint64(196620), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", uint64(14), uint64(65540), uint64(917560), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", uint64(6), uint64(24), uint64(144), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", uint64(6), uint64(32), uint64(192), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", uint64(5), uint64(65540), uint64(327700), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"datetime_table", "InnoDB", "10", "Fixed", uint64(3), uint64(32), uint64(96), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS FROM mydb LIKE 'othertable'`,
		Expected: []sql.Row{
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(65540), uint64(196620), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS WHERE Name = 'mytable'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS`,
		Expected: []sql.Row{
			{"auto_increment_tbl", "InnoDB", "10", "Fixed", uint64(3), uint64(16), uint64(48), uint64(0), int64(0), int64(0), int64(4), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"mytable", "InnoDB", "10", "Fixed", uint64(3), uint64(88), uint64(264), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(65540), uint64(196620), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", uint64(3), uint64(65540), uint64(196620), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", uint64(14), uint64(65540), uint64(917560), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", uint64(6), uint64(24), uint64(144), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"fk_tbl", "InnoDB", "10", "Fixed", uint64(3), uint64(96), uint64(288), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", uint64(6), uint64(32), uint64(192), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", uint64(5), uint64(65540), uint64(327700), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"people", "InnoDB", "10", "Fixed", uint64(5), uint64(196620), uint64(983100), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
			{"datetime_table", "InnoDB", "10", "Fixed", uint64(3), uint64(32), uint64(96), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS FROM mydb LIKE 'othertable'`,
		Expected: []sql.Row{
			{"othertable", "InnoDB", "10", "Fixed", uint64(3), uint64(65540), uint64(196620), uint64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, nil},
		},
	},
}

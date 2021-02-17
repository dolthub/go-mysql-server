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
	"math"
	"time"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type QueryTest struct {
	Query    string
	Expected []sql.Row
	Bindings map[string]sql.Expression
}

var QueryTests = []QueryTest{
	{
		Query: "SELECT * FROM mytable;",
		Expected: []sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
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
		Query: "SELECT pk1, SUM(c1) FROM two_pk GROUP BY pk1 ORDER BY pk1;",
		Expected: []sql.Row{
			{0, 10.0},
			{1, 50.0},
		},
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
			{"third row", int64(3)}},
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
		Query:    "SELECT :foo * 2",
		Expected: []sql.Row{
			{2},
		},
		Bindings: map[string]sql.Expression{
			"foo": expression.NewLiteral(int64(1), sql.Int64),
		},
	},
	{
		Query:    "SELECT i from mytable where i in (:foo, :bar) order by 1",
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
		Query:    "SELECT i from mytable where i = :foo * 2",
		Expected: []sql.Row{
			{2},
		},
		Bindings: map[string]sql.Expression{
			"foo": expression.NewLiteral(int64(1), sql.Int64),
		},
	},
	{
		Query:    "SELECT i from mytable where 4 = :foo * 2 order by 1",
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
		`SELECT 'a' IN ('b','c',null,'d')`,
		[]sql.Row{{nil}},
		nil,
	},
	{
		`SELECT 'a' IN ('a','b','c','d')`,
		[]sql.Row{{true}},
		nil,
	},
	{
		`SELECT 'a' IN ('b','c','d')`,
		[]sql.Row{{false}},
		nil,
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
		`SELECT 'a' NOT IN ('b','c',null,'d')`,
		[]sql.Row{{nil}},
		nil,
	},
	{
		`SELECT 'a' NOT IN ('a','b','c','d')`,
		[]sql.Row{{false}},
		nil,
	},
	{
		`SELECT 'a' NOT IN ('b','c','d')`,
		[]sql.Row{{true}},
		nil,
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
		Query:    "SELECT i FROM mytable ORDER BY i LIMIT 1 OFFSET 1;",
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
		Query:    "SELECT id FROM typestable WHERE da > '2019-12-31'",
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
		Query: "SELECT i, SUM(i) FROM mytable GROUP BY i ORDER BY SUM(i) DESC",
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
			{"user"},
		},
	},
	{
		Query: `SELECT CURRENT_USER()`,
		Expected: []sql.Row{
			{"user"},
		},
	},
	{
		Query: `SELECT CURRENT_USER`,
		Expected: []sql.Row{
			{"user"},
		},
	},
	{
		Query: `SHOW VARIABLES`,
		Expected: []sql.Row{
			{"autocommit", int64(0)},
			{"auto_increment_increment", int64(1)},
			{"time_zone", "SYSTEM"},
			{"system_time_zone", time.Now().UTC().Location().String()},
			{"max_allowed_packet", math.MaxInt32},
			{"sql_mode", ""},
			{"gtid_mode", int32(0)},
			{"collation_database", "utf8mb4_0900_ai_ci"},
			{"ndbinfo_version", ""},
			{"sql_select_limit", math.MaxInt32},
			{"transaction_isolation", "READ UNCOMMITTED"},
			{"version", ""},
			{"version_comment", ""},
			{"character_set_client", sql.Collation_Default.CharacterSet().String()},
			{"character_set_connection", sql.Collation_Default.CharacterSet().String()},
			{"character_set_results", sql.Collation_Default.CharacterSet().String()},
			{"collation_connection", sql.Collation_Default.String()},
		},
	},
	{
		Query: `SHOW VARIABLES LIKE 'gtid_mode`,
		Expected: []sql.Row{
			{"gtid_mode", int32(0)},
		},
	},
	{
		Query: `SHOW VARIABLES LIKE 'gtid%`,
		Expected: []sql.Row{
			{"gtid_mode", int32(0)},
		},
	},
	{
		Query: `SHOW GLOBAL VARIABLES LIKE '%mode`,
		Expected: []sql.Row{
			{"sql_mode", ""},
			{"gtid_mode", int32(0)},
		},
	},
	{
		Query:    `SELECT JSON_EXTRACT("foo", "$")`,
		Expected: []sql.Row{{"foo"}},
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
				sql.CollationToMySQLVals[sql.Collation_binary].ID,
				sql.CollationToMySQLVals[sql.Collation_binary].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_binary].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_binary].SortLen,
				sql.CollationToMySQLVals[sql.Collation_binary].PadSpace,
			},
			{
				sql.Collation_utf8_general_ci.String(),
				"utf8",
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].PadSpace,
			},
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].PadSpace,
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
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].PadSpace,
			},
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].PadSpace,
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
				sql.CollationToMySQLVals[sql.Collation_binary].ID,
				sql.CollationToMySQLVals[sql.Collation_binary].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_binary].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_binary].SortLen,
				sql.CollationToMySQLVals[sql.Collation_binary].PadSpace,
			},
			{
				sql.Collation_utf8_general_ci.String(),
				"utf8",
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8_general_ci].PadSpace,
			},
			{
				sql.Collation_utf8mb4_0900_ai_ci.String(),
				"utf8mb4",
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].ID,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].IsDefault,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].IsCompiled,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].SortLen,
				sql.CollationToMySQLVals[sql.Collation_utf8mb4_0900_ai_ci].PadSpace,
			},
		},
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
		Query:    `SELECT i AS foo FROM mytable ORDER BY mytable.i`,
		Expected: []sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		Query:    `SELECT JSON_EXTRACT('[1, 2, 3]', '$.[0]')`,
		Expected: []sql.Row{{float64(1)}},
	},
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
		Query:    `SELECT REGEXP_MATCHES("bopbeepbop", "bop")`,
		Expected: []sql.Row{{[]interface{}{"bop", "bop"}}},
	},
	{
		Query:    `SELECT EXPLODE(REGEXP_MATCHES("bopbeepbop", "bop"))`,
		Expected: []sql.Row{{"bop"}, {"bop"}},
	},
	{
		Query:    `SELECT EXPLODE(REGEXP_MATCHES("helloworld", "bop"))`,
		Expected: nil,
	},
	{
		Query:    `SELECT EXPLODE(REGEXP_MATCHES("", ""))`,
		Expected: []sql.Row{{""}},
	},
	{
		Query:    `SELECT REGEXP_MATCHES(NULL, "")`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT REGEXP_MATCHES("", NULL)`,
		Expected: []sql.Row{{nil}},
	},
	{
		Query:    `SELECT REGEXP_MATCHES("", "", NULL)`,
		Expected: []sql.Row{{nil}},
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
		Query:    `select i, row_number() over (order by i desc), 
				row_number() over (order by length(s),i) from mytable order by 1;`,
		Expected: []sql.Row{
			{1,3,1},
			{2,2,3},
			{3,1,2},
		},
	},
	{
		Query:    `select i, row_number() over (order by i desc) as i_num,
				row_number() over (order by length(s),i) as s_num from mytable order by 1;`,
		Expected: []sql.Row{
			{1,3,1},
			{2,2,3},
			{3,1,2},
		},
	},
	{
		Query:    `select i, row_number() over (order by i desc) + 3,
			row_number() over (order by length(s),i) as s_asc, 
			row_number() over (order by length(s) desc,i desc) as s_desc 
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1,6,1,3},
			{2,5,3,1},
			{3,4,2,2},
		},
	},
	{
		Query:    `select i, row_number() over (order by i desc) + 3,
			row_number() over (order by length(s),i) + 0.0 / row_number() over (order by length(s) desc,i desc) + 0.0  
			from mytable order by 1;`,
		Expected: []sql.Row{
			{1,6,1.0},
			{2,5,3.0},
			{3,4,2.0},
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
	// Indexed joins in subqueries are broken
	{
		Query: `SELECT pk,pk2, 
							(SELECT opk.c5 FROM one_pk opk JOIN two_pk tpk ON pk=pk1 ORDER BY 1 LIMIT 1) 
							FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		Expected: []sql.Row{
			{1, 1, 4},
			{1, 1, 4},
		},
	},
	// Non-indexed joins in subqueries are broken
	{
		Query: `SELECT pk,pk2, 
							(SELECT opk.c5 FROM one_pk opk JOIN two_pk tpk ON opk.c5=tpk.c5 ORDER BY 1 LIMIT 1) 
							FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2`,
		Expected: []sql.Row{
			{1, 1, 4},
			{1, 1, 4},
		},
	},
	// 3+ table joins with one LEFT join, one INNER join, have the wrong semantics according to MySQL. Should be 2 rows,
	// but get 4.
	{
		Query: `SELECT pk,tpk.pk1,tpk2.pk1,tpk.pk2,tpk2.pk2 FROM one_pk 
						LEFT JOIN two_pk tpk ON one_pk.pk=tpk.pk1 AND one_pk.pk=tpk.pk2 
						JOIN two_pk tpk2 ON tpk2.pk1=TPK.pk2 AND TPK2.pk2=tpk.pk1`,
		Expected: []sql.Row{
			{0, 0, 0, 0, 0},
			{1, 1, 1, 1, 1},
		},
	},
	// More broken RIGHT / LEFT semantics. Mysql gives these results, we give different ones.
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
		Query: `SHOW TABLE STATUS FROM mydb`,
		Expected: []sql.Row{
			{"auto_increment_tbl", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"fk_tbl", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"people", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS LIKE '%table'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS WHERE Name = 'mytable'`,
		Expected: []sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
		},
	},
	{
		Query: `SHOW TABLE STATUS`,
		Expected: []sql.Row{
			{"auto_increment_tbl", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"fk_tbl", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
			{"people", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil},
		},
	},
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
	Query       string
	ExpectedErr *errors.Kind
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
	// TODO: Bug: the having column must appear in the select list
	// {
	// 	Query:       "SELECT pk1, sum(c1) FROM two_pk GROUP BY 1 having c1 > 10;",
	// 	ExpectedErr: sql.ErrColumnNotFound,
	// },
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

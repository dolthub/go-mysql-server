// Copyright 2026 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql"
)

// IndexLookupsScriptTests contains self-contained index lookups script tests.
var IndexLookupsScriptTests = []ScriptTest{
	{
		Name: "keyless reverse index",
		SetUpScript: []string{
			"create table x (x int);",
			"CREATE INDEX idx_x_x ON x(x)",
			"insert into x values (0),(1)",
		},
		Query: "select * from x order by x desc limit 1",
		Expected: []sql.Row{
			{1},
		},
	},
	{
		Name: "missing indexes",
		SetUpScript: []string{
			`
create table t (
  id varchar(500),
  from_ varchar(500),
  to_ varchar(500),
  key (to_, from_),
  Primary key (id, from_, to_)
);`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "select * from t where to_ = 'L1' and from_ = 'L2'",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"to_"},
			},
			{
				Query:           "select * from t where BIN_TO_UUID(id) = '0' and  to_ = 'L1' and from_ = 'L2'",
				Expected:        []sql.Row{},
				ExpectedIndexes: []string{"to_"},
			},
		},
	},
	{
		Name: "correctness test indexes",
		SetUpScript: []string{
			`
CREATE TABLE tab3 (
  pk int NOT NULL,
  col0 int,
  col1 float,
  col2 text,
  col3 int,
  col4 float,
  col5 text,
  PRIMARY KEY (pk),
  KEY idx_tab3_0 (col1),
  UNIQUE KEY idx_tab3_1 (col0),
  UNIQUE KEY idx_tab3_4 (col3,col4)
)`,
			"insert into tab3 values (1 , 101 , 83.86, 'pgprm', 50  , 58.56, 'nugdy')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select count(*) from tab3 WHERE (80 < col0 AND (((col0 BETWEEN 87 AND 9 OR (((col0 IS NULL)))))) AND (71.70 <= col1 OR 94 <= col0 AND ((66 > col0) OR (85 = col0 AND ((42.15 >= col1))) OR 30 = col0)));",
				Expected: []sql.Row{{0}},
			},
		},
	},
	{
		Name: "sqllogictest index/commute/10/slt_good_1.test",
		SetUpScript: []string{
			"CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"INSERT INTO tab0 VALUES(0,42,58.92,'fnbtk',54,68.41,'xmttf')",
			"INSERT INTO tab0 VALUES(1,31,46.55,'sksjf',46,53.20,'wiuva')",
			"INSERT INTO tab0 VALUES(2,30,31.11,'oldqn',41,5.26,'ulaay')",
			"INSERT INTO tab0 VALUES(3,77,44.90,'pmsir',70,84.14,'vcmyo')",
			"INSERT INTO tab0 VALUES(4,23,95.26,'qcwxh',32,48.53,'rvtbr')",
			"INSERT INTO tab0 VALUES(5,43,6.75,'snvwg',3,14.38,'gnfxz')",
			"INSERT INTO tab0 VALUES(6,47,98.26,'bzzva',60,15.2,'imzeq')",
			"INSERT INTO tab0 VALUES(7,98,40.9,'lsrpi',78,66.30,'ephwy')",
			"INSERT INTO tab0 VALUES(8,19,15.16,'ycvjz',55,38.70,'dnkkz')",
			"INSERT INTO tab0 VALUES(9,7,84.4,'ptovf',17,2.46,'hrxsf')",
			"CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab1_0 on tab1 (col0)",
			"CREATE INDEX idx_tab1_1 on tab1 (col1)",
			"CREATE INDEX idx_tab1_3 on tab1 (col3)",
			"CREATE INDEX idx_tab1_4 on tab1 (col4)",
			"INSERT INTO tab1 SELECT * FROM tab0",
			"CREATE TABLE tab2(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE UNIQUE INDEX idx_tab2_1 ON tab2 (col4 DESC,col3)",
			"CREATE UNIQUE INDEX idx_tab2_2 ON tab2 (col3 DESC,col0)",
			"CREATE UNIQUE INDEX idx_tab2_3 ON tab2 (col3 DESC,col1)",
			"INSERT INTO tab2 SELECT * FROM tab0",
			"CREATE TABLE tab3(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab3_0 ON tab3 (col3 DESC)",
			"INSERT INTO tab3 SELECT * FROM tab0",
			"CREATE TABLE tab4(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT)",
			"CREATE INDEX idx_tab4_0 ON tab4 (col0 DESC)",
			"CREATE UNIQUE INDEX idx_tab4_2 ON tab4 (col4 DESC,col3)",
			"CREATE INDEX idx_tab4_3 ON tab4 (col3 DESC)",
			"INSERT INTO tab4 SELECT * FROM tab0",
		},
		Query: "SELECT pk FROM tab2 WHERE 78 < col0 AND 19 < col3",
		Expected: []sql.Row{
			{7},
		},
	},
	{
		Name: "Partial indexes are used and return the expected result",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT, INDEX vx (v3, v2, v1));",
			"INSERT INTO test VALUES (1,2,3,4), (2,3,4,5), (3,4,5,6), (4,5,6,7), (5,6,7,8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE v3 = 4;",
				Expected: []sql.Row{{1, 2, 3, 4}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 8 AND v2 = 7;",
				Expected: []sql.Row{{5, 6, 7, 8}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 >= 6 AND v2 >= 6;",
				Expected: []sql.Row{{4, 5, 6, 7}, {5, 6, 7, 8}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 7 AND v2 >= 6;",
				Expected: []sql.Row{{4, 5, 6, 7}},
			},
		},
	},
	{
		Name: "Multiple indexes on the same columns in a different order",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, v3 BIGINT, INDEX v123 (v1, v2, v3), INDEX v321 (v3, v2, v1), INDEX v132 (v1, v3, v2));",
			"INSERT INTO test VALUES (1,2,3,4), (2,3,4,5), (3,4,5,6), (4,5,6,7), (5,6,7,8);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE v1 = 2 AND v2 > 1;",
				Expected: []sql.Row{{1, 2, 3, 4}},
			},
			{
				Query:    "SELECT * FROM test WHERE v2 = 4 AND v3 > 1;",
				Expected: []sql.Row{{2, 3, 4, 5}},
			},
			{
				Query:    "SELECT * FROM test WHERE v3 = 6 AND v1 > 1;",
				Expected: []sql.Row{{3, 4, 5, 6}},
			},
			{
				Query:    "SELECT * FROM test WHERE v1 = 5 AND v3 <= 10 AND v2 >= 1;",
				Expected: []sql.Row{{4, 5, 6, 7}},
			},
		},
	},
	{
		Name: "Point lookups with dropped filters",
		SetUpScript: []string{
			`create table t1 (
    			  id varchar(255),
    			  a  varchar(255),
    			  unique key key1 (id, a)
    			);`,
			`create table t2 (
    			  id varchar(255),
    			  b  varchar(255),
    			  unique key key2 (id, b)
    			);`,
			`insert into t1 values 
    			  ('id1', 'a1'),
    			  ('id1', 'a2');`,
			`insert into t2 values
    			  ('id1', 'b1'),
    			  ('id1', 'b2'),
    			  ('id1', 'b3'),
    			  ('id2', 'b4'),
    			  ('id2', 'b5');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
    				select /*+ LOOKUP_JOIN(t1, t3)*/ t1.id, t1.a, t2.b from
                      t1
                    inner join
                      t2
                    on
                      t1.id = t2.id and t1.a = t2.b;`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "Complex Filter Index Scan",
		SetUpScript: []string{
			`CREATE TABLE tab2 (
              pk int NOT NULL,
              col0 int,
              col1 float,
              col2 text,
              col3 int,
              col4 float,
              col5 text,
              PRIMARY KEY (pk),
              UNIQUE KEY idx_tab2_0 (col3,col4),
              UNIQUE KEY idx_tab2_1 (col1,col4),
              UNIQUE KEY idx_tab2_2 (col3,col0,col4),
              UNIQUE KEY idx_tab2_3 (col1,col3)
            );`,
			`insert into tab2 values ( 63, 587, 465.59 , 'aggxb', 303 , 763.91, 'tgpqr');`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT pk FROM tab2 WHERE col4 IS NULL OR col0 > 560 AND (col3 < 848) OR (col3 > 883) OR (((col4 >= 539.78 AND col3 <= 953))) OR ((col3 IN (258)) OR (col3 IN (583,234,372)) AND col4 >= 488.43)",
				Expected: []sql.Row{
					{63},
				},
			},
		},
	},
	{
		Name: "Complex Filter Index Scan #2",
		SetUpScript: []string{
			"create table t (pk int primary key, v1 int, v2 int, v3 int, v4 int);",
			"create index v_idx on t (v1, v2, v3, v4);",
			"insert into t values (0, 26, 24, 91, 0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where (((v1>25 and v2 between 23 and 54) or (v1<>40 and v3>90)) or (v1<>7 and v4<=78));",
				Expected: []sql.Row{
					{0, 26, 24, 91, 0},
				},
			},
		},
	},
	{
		Name: "Complex Filter Index Scan #3",
		SetUpScript: []string{
			"create table t (pk integer primary key, col0 integer, col1 float);",
			"create index idx on t (col0, col1);",
			"insert into t values (0, 22, 1.23);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select pk, col0 from t where (col0 in (73,69)) or col0 in (4,12,3,17,70,20) or (col0 in (39) or (col1 < 69.67));",
				Expected: []sql.Row{
					{0, 22},
				},
			},
		},
	},
	{
		Name: "complicated range tree",
		SetUpScript: []string{
			"create table t1 (a1 int, b1 int, primary key(a1, b1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `
SELECT *
FROM t1
WHERE
    a1 in (702, 584, 607, 479, 330, 445, 513, 678, 406, 314, 880, 953, 75, 268) OR
    b1 in (213, 55,  992, 922, 619, 972, 654, 130,  88, 141, 679, 761) OR
    (a1=145 AND b1=818);
`,
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "not null not unique index works on server engine",
		SetUpScript: []string{
			"create table t (i int not null, index (i));",
			"insert into t values (1), (1), (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t where i = 1;",
				Expected: []sql.Row{
					{1},
					{1},
					{1},
				},
			},
		},
	},
}

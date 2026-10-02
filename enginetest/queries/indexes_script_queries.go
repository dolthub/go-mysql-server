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
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// IndexesScriptTests contains self-contained indexes script tests.
var IndexesScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9936
		Name:    "invisible hash index with different key types",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t0(c0 varchar(500))",
			"insert into t0(c0) values (77367106)",
			"create index i0 using hash on t0(c0) invisible",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select c0 from t0 where 1630944823 >= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
			{
				Query:    "select c0 from t0 where 1630944823 <= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823' >= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823' <= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
			{
				Query:    "select c0 from t0 where 1630944823.2 >= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
			{
				Query:    "select c0 from t0 where 1630944823.2 <= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823.2' >= t0.c0",
				Expected: []sql.Row{},
			},
			{
				Query:    "select c0 from t0 where '1630944823.2' <= t0.c0",
				Expected: []sql.Row{{"77367106"}},
			},
		},
	},
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
		Name: "histogram bucket merging error for implementor buckets",
		SetUpScript: []string{
			"CREATE TABLE xy (x int primary key, y varchar(10), key(y));",
			"insert into xy select x, 'x' from (with recursive inputs(x) as (select 1 union select x+1 from inputs where x < 5000) select * from inputs) dt",
			"analyze table xy",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select (select count(*) from information_schema.statistics) > 0",
				Expected: []sql.Row{{true}},
			},
			{
				Query:    "select a.y from xy a join xy b on a.y = b.y limit 1",
				Expected: []sql.Row{{"x"}},
			},
			{
				Query:    "select y from xy where y = 'x' limit 1",
				Expected: []sql.Row{{"x"}},
			},
		},
	},
	{
		Name: "index match only exact string, no prefix",
		SetUpScript: []string{
			"CREATE TABLE pk (x varchar(10) primary key)",
			"INSERT INTO pk values ('3'), ('30'), ('3#')",
			"CREATE TABLE uniq (y int primary key, x varchar(10), constraint idx_uniq_x unique key (x))",
			"INSERT INTO uniq values (1,'3'), (2,'30'), (3,'3#')",
			"CREATE TABLE noncov (y int primary key, x varchar(10), z int)",
			"CREATE INDEX idx_noncov_x ON noncov(x);",
			"INSERT INTO noncov values (1,'3',1), (2,'30',2), (3,'3#',3)",
			"CREATE TABLE keyless (y int, x varchar(10))",
			"CREATE INDEX idx_keyless_x ON keyless(x);",
			"INSERT INTO keyless values (1,'3'), (2,'30'), (3,'3#')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from pk where x = '3'",
				Expected: []sql.Row{{"3"}},
			},
			{
				Query:    "delete from pk where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from pk",
				Expected: []sql.Row{{"30"}, {"3#"}},
			},
			{
				Query:    "select * from uniq where x = '3'",
				Expected: []sql.Row{{1, "3"}},
			},
			{
				Query:    "delete from uniq where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from uniq",
				Expected: []sql.Row{{2, "30"}, {3, "3#"}},
			},
			{
				Query:    "select * from noncov where x = '3'",
				Expected: []sql.Row{{1, "3", 1}},
			},
			{
				Query:    "delete from noncov where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from noncov",
				Expected: []sql.Row{{2, "30", 2}, {3, "3#", 3}},
			},
			{
				Query:    "select * from keyless where x = '3'",
				Expected: []sql.Row{{1, "3"}},
			},
			{
				Query:    "delete from keyless where x = '3' ",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from keyless",
				Expected: []sql.Row{{2, "30"}, {3, "3#"}},
			},
		},
	},
	{
		Name: "keyless unique index bug",
		SetUpScript: []string{
			"CREATE TABLE mytable (pk int UNIQUE)",
			"INSERT INTO mytable values (1),(2),(3),(4)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM mytable order by pk",
				Expected: []sql.Row{{1}, {2}, {3}, {4}},
			},
			{
				Query:       "INSERT INTO mytable VALUES (1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
			{
				Query: "INSERT INTO mytable VALUES (500000), (5000001)",
			},
			{
				Query: "SELECT count(*) FROM mytable where pk in (500000,5000001)",
			},
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
		Name: "show create table with duplicate primary key",
		SetUpScript: []string{
			"create table t (i int primary key)",
			"create index notpk on t(i)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  PRIMARY KEY (`i`),\n" +
						"  KEY `notpk` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:          "create index `primary` on t(i)",
				ExpectedErrStr: "invalid index name 'primary'",
			},
		},
	},
	{
		Name: "recreate primary key rebuilds secondary indexes",
		SetUpScript: []string{
			"create table a (x int, y int, z int, primary key (x,y,z), index idx1 (y))",
			"insert into a values (1,2,3), (4,5,6), (7,8,9)",
			"alter table a drop primary key",
			"alter table a add primary key (y,z,x)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "delete from a where y = 2",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from a where y = 2",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select * from a where y = 2",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from a where y = 5",
				Expected: []sql.Row{{4, 5, 6}},
			},
		},
	},
	{
		Name:    "Multialter DDL with ADD/DROP Primary Key",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(pk int primary key, v1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "ALTER TABLE t ADD COLUMN (v2 int), drop primary key, add primary key (v2)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
			{
				Query:       "ALTER TABLE t ADD COLUMN (v3 int), drop primary key, add primary key (notacolumn)",
				ExpectedErr: sql.ErrKeyColumnDoesNotExist,
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v1", "int", "YES", "", nil, ""},
					{"v2", "int", "NO", "PRI", nil, ""},
				},
			},
		},
	},
	{
		Name: "test index scan over floats",
		SetUpScript: []string{
			"CREATE TABLE tab2(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);",
			"CREATE UNIQUE INDEX idx_tab2_0 ON tab2 (col1 DESC,col4 DESC);",
			"CREATE INDEX idx_tab2_1 ON tab2 (col1,col0);",
			"CREATE INDEX idx_tab2_2 ON tab2 (col4,col0);",
			"CREATE INDEX idx_tab2_3 ON tab2 (col3 DESC);",
			"INSERT INTO tab2 VALUES(0,344,171.98,'nwowg',833,149.54,'wjiif');",
			"INSERT INTO tab2 VALUES(1,353,589.18,'femmh',44,621.85,'qedct');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT pk FROM tab2 WHERE ((((((col0 IN (SELECT col3 FROM tab2 WHERE ((col1 = 672.71)) AND col4 IN (SELECT col1 FROM tab2 WHERE ((col4 > 169.88 OR col0 > 939 AND ((col3 > 578))))) AND col0 >= 377) AND col4 >= 817.87 AND (col4 > 597.59)) OR col4 >= 434.59 AND ((col4 < 158.43)))))) AND col0 < 303) OR ((col0 > 549)) AND (col4 BETWEEN 816.92 AND 983.96) OR (col3 BETWEEN 421 AND 96);",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:    "case insensitive index handling",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table table_One (Id int primary key, Val1 int);",
			"create table TableTwo (iD int primary key, VAL2 int, vAL3 int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create index idx_one on TABLE_ONE (vAL1);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table TABLE_one;",
				Expected: []sql.Row{{"table_One",
					"CREATE TABLE `table_One` (\n" +
						"  `Id` int NOT NULL,\n" +
						"  `Val1` int,\n" +
						"  PRIMARY KEY (`Id`),\n" +
						"  KEY `idx_one` (`Val1`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "show index from TABLE_one;",
				Expected: []sql.Row{
					{"table_One", 0, "PRIMARY", 1, "Id", "A", 0, nil, nil, "", "BTREE", "", "", "YES", nil},
					{"table_One", 1, "idx_one", 1, "Val1", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query:    "create index idx_one on TABLEtwo (VAL2, VAL3);",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table TABLETWO;",
				Expected: []sql.Row{{"TableTwo", "CREATE TABLE `TableTwo` (\n" +
					"  `iD` int NOT NULL,\n" +
					"  `VAL2` int,\n" +
					"  `vAL3` int,\n" +
					"  PRIMARY KEY (`iD`),\n" +
					"  KEY `idx_one` (`VAL2`,`vAL3`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "show index from tABLEtwo;",
				Expected: []sql.Row{
					{"TableTwo", 0, "PRIMARY", 1, "iD", "A", 0, nil, nil, "", "BTREE", "", "", "YES", nil},
					{"TableTwo", 1, "idx_one", 1, "VAL2", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
					{"TableTwo", 1, "idx_one", 2, "vAL3", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query:    "drop index IDX_ONE on TABLE_one;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "drop index IDX_ONE on TABLEtwo;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table TABLE_one;",
				Expected: []sql.Row{{"table_One",
					"CREATE TABLE `table_One` (\n" +
						"  `Id` int NOT NULL,\n" +
						"  `Val1` int,\n" +
						"  PRIMARY KEY (`Id`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query: "show create table TABLETWO;",
				Expected: []sql.Row{{"TableTwo", "CREATE TABLE `TableTwo` (\n" +
					"  `iD` int NOT NULL,\n" +
					"  `VAL2` int,\n" +
					"  `vAL3` int,\n" +
					"  PRIMARY KEY (`iD`)\n" +
					") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
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
		Name: "int index with float filter",
		SetUpScript: []string{
			"create table t0 (i int primary key);",
			"insert into t0 values (-1), (0), (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t0 where i > 0.0 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.5 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.9 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:    "select * from t0 where i > 1.0 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i > 1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i > -0.0 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -0.1 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -0.5 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -0.9 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -1.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > -1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i >= 0.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 0.5 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 0.9 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= 1.0 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query:    "select * from t0 where i >= 1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i >= -0.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -0.1 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -0.5 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -0.9 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -1.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i >= -1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i < 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 1.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i < 1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i < -0.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < -0.1 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < -0.5 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i < -0.9 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query:    "select * from t0 where i < -1.0 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i < -1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i <= 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 1.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i <= 1.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i <= -0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= -0.1 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i <= -0.5 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i <= -0.9 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query: "select * from t0 where i <= -1.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},
			{
				Query:    "select * from t0 where i <= -1.1 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i = 0.0 order by i;",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query:    "select * from t0 where i = 0.1 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = 0.5 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = 0.9 order by i;",
				Expected: []sql.Row{},
			},

			{
				Query: "select * from t0 where i = -0.0 order by i;",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query:    "select * from t0 where i = -0.1 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = -0.5 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from t0 where i = -0.9 order by i;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from t0 where i = -1.0 order by i;",
				Expected: []sql.Row{
					{-1},
				},
			},

			{
				Query: "select * from t0 where i != 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != 0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != 0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i != -0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -0.5 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -0.9 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i != -1.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},

			{
				Query: "select * from t0 where i <= 0.0 and i >= 0.0 order by i;",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from t0 where i <= 0.1 or i >= 0.1 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.1 and i >= 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "select * from t0 where i > 0.1 or i >= 0.1 order by i;",
				Expected: []sql.Row{
					{1},
				},
			},
		},
	},
	{
		Name: "int secondary index with float filter",
		SetUpScript: []string{
			"create table t0 (i int);",
			"create index idx on t0(i);",
			"insert into t0 values (null), (-1), (0), (1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from t0 where i >= 0.0 order by i;",
				Expected: []sql.Row{
					{0},
					{1},
				},
			},
			{
				Query: "select * from t0 where i <= 0.0 order by i;",
				Expected: []sql.Row{
					{-1},
					{0},
				},
			},
			{
				// cot(-939932070) = -1.1919623754564008
				Query: "SELECT * from t0 where (cot(-939932070) < i);",
				Expected: []sql.Row{
					{-1},
					{0},
					{1},
				},
			},
		},
	},
	{
		Name:    "resolve foreign key on indexed update",
		Dialect: "mysql", // no way to disable foreign keys in doltgres yet
		SetUpScript: []string{
			"set foreign_key_checks=0;",
			"create table parent (i int primary key);",
			"create table child (i int primary key, foreign key (i) references parent(i));",
			"set foreign_key_checks=1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "update child set i = 1 where i = 1;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 0, Info: plan.UpdateInfo{Matched: 0, Updated: 0}}},
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
		Name: "binary type primary key",
		SetUpScript: []string{
			"create table t (b binary(3) primary key);",
			"insert into t values ('abc'), ('def'), ('ghi');",
			"create table tt (b binary(10) primary key);",
			"insert into tt values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select cast(b as char) from t where b < cast('def' as binary(3));",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select cast(b as char) from t where b = cast('def' as binary(3));",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select cast(b as char) from t where b > cast('def' as binary(3));",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select cast(b as char(3)) from tt where b < cast('def' as binary(10));",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select cast(b as char(3)) from tt where b = cast('def' as binary(10));",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select cast(b as char(3)) from tt where b > cast('def' as binary(10));",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
		},
	},
	{
		Name: "varchar primary key",
		SetUpScript: []string{
			"create table vt (v varchar(3) primary key);",
			"insert into vt values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from vt where v = 'def';",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select * from vt where v < 'def';",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select * from vt where v > 'def';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select * from vt where v <= 'def';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v >= 'def';",
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},

			{
				Query:    "select * from vt where v = 'defdef';",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from vt where v < 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v > 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select * from vt where v <= 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v >= 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},

			// MySQL behavior around null bytes is strange
			{
				Skip:  true,
				Query: `select * from vt where v = 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select * from vt where v < 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: `select * from vt where v > 'def\0\0';`,
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: `select * from vt where v <= 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select * from vt where v >= 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},

			{
				Query: "select * from vt where v = cast('def' as char(6));",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select * from vt where v < cast('def' as char(6));",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select * from vt where v > cast('def' as char(6));",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select * from vt where v <= cast('def' as char(6));",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select * from vt where v >= cast('def' as char(6));",
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},
		},
	},
	{
		Name: "varbinary primary key",
		SetUpScript: []string{
			"create table vt (v varbinary(3) primary key);",
			"insert into vt values ('abc'), ('def'), ('ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select cast(v as char(3)) from vt where v = 'def';",
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v < 'def';",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v > 'def';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v <= 'def';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v >= 'def';",
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},

			{
				Query:    "select cast(v as char(3)) from vt where v = 'defdef';",
				Expected: []sql.Row{},
			},
			{
				Query: "select cast(v as char(3)) from vt where v < 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v > 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v <= 'defdef';",
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Query: "select cast(v as char(3)) from vt where v >= 'defdef';",
				Expected: []sql.Row{
					{"ghi"},
				},
			},

			// MySQL behavior around null bytes is strange
			{
				Skip:  true,
				Query: `select cast(v as char(3)) from vt where v = 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select cast(v as char(3)) from vt where v < 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: `select cast(v as char(3)) from vt where v > 'def\0\0';`,
				Expected: []sql.Row{
					{"ghi"},
				},
			},
			{
				Query: `select cast(v as char(3)) from vt where v <= 'def\0\0';`,
				Expected: []sql.Row{
					{"abc"},
					{"def"},
				},
			},
			{
				Skip:  true,
				Query: `select cast(v as char(3)) from vt where v >= 'def\0\0';`,
				Expected: []sql.Row{
					{"def"},
					{"ghi"},
				},
			},
		},
	},
	{
		Name: "primary key order",
		SetUpScript: []string{
			"create table t1 (a varchar(5), b varchar(10), primary key(a, b));",
			"create table t2 (a varchar(5), b varchar(10), primary key(b, a));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "insert into t1 (a, b) values ('1234567890', '12345')",
				ExpectedErrStr: "string '1234567890' is too large for column 'a'",
			},
			{
				Query: "insert into t1 (b, a) values ('1234567890', '12345')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select a, b from t1",
				Expected: []sql.Row{
					{"12345", "1234567890"},
				},
			},
			{
				Query:          "insert into t2 (a, b) values ('1234567890', '12345')",
				ExpectedErrStr: "string '1234567890' is too large for column 'a'",
			},
			{
				Query: "insert into t2 (b, a) values ('1234567890', '12345')",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select a, b from t2",
				Expected: []sql.Row{
					{"12345", "1234567890"},
				},
			},
		},
	},
	{
		Name:    "test index naming",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int);",
			"alter table t add index (i);",
			"alter table t add index (i);",
			"alter table t add index (i);",

			"create table tt (i int);",
			"alter table tt add index i_3(i);",
			"alter table tt add index (i);",
			"alter table tt add index (i);",
			"alter table tt add index (i);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`),\n" +
						"  KEY `i_2` (`i`),\n" +
						"  KEY `i_3` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				// MySQL preserves the other that indexes are created
				// We store them in a map, so we have to sort to have some consistency
				Query: "show create table tt",
				Expected: []sql.Row{
					{"tt", "CREATE TABLE `tt` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`),\n" +
						"  KEY `i_2` (`i`),\n" +
						"  KEY `i_3` (`i`),\n" +
						"  KEY `i_4` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
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
	{
		Name: "decimal unique key",
		SetUpScript: []string{
			"create table t (i int primary key, d decimal(10, 2) unique)",
			"insert into t values (1, 1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "insert into t values (2, 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10246
		Dialect: "mysql",
		Name:    "boolean keys are not used for string column lookups",
		SetUpScript: []string{
			"create table t1(c0 varchar(500), primary key(c0))",
			"insert into t1(c0) values ('')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select 1 from t1 where false=t1.c0",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name: "Keyless Table with Unique Index",
		SetUpScript: []string{
			"create table a (x int, val int unique)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "INSERT INTO a VALUES (1, 1)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:       "INSERT INTO a VALUES (1, 1)",
				ExpectedErr: sql.ErrUniqueKeyViolation,
			},
		},
	},
}

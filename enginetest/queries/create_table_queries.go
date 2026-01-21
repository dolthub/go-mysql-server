// Copyright 2022 Dolthub, Inc.
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
	"github.com/dolthub/go-mysql-server/sql/types"
)

var CreateTableQueries = []WriteQueryTest{
	{
		WriteQuery:          `create table tableWithComment (pk int) COMMENT 'Table Comments Work!'`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithComment",
		ExpectedSelect:      []sql.Row{{"tableWithComment", "CREATE TABLE `tableWithComment` (\n  `pk` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT='Table Comments Work!'"}},
	},
	{
		WriteQuery:          `create table tableWithComment (pk int) COMMENT='Table Comments=Still Work'`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithComment",
		ExpectedSelect:      []sql.Row{{"tableWithComment", "CREATE TABLE `tableWithComment` (\n  `pk` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT='Table Comments=Still Work'"}},
	},
	{
		WriteQuery:          `create table tableWithComment (pk int) COMMENT "~!@ #$ %^ &* ()"`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithComment",
		ExpectedSelect:      []sql.Row{{"tableWithComment", "CREATE TABLE `tableWithComment` (\n  `pk` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT='~!@ #$ %^ &* ()'"}},
	},
	{
		WriteQuery:          `create table tableWithComment (pk int) COMMENT "'"`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithComment",
		ExpectedSelect:      []sql.Row{{"tableWithComment", "CREATE TABLE `tableWithComment` (\n  `pk` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT=''''"}},
	},
	{
		WriteQuery:          `create table tableWithComment (pk int) COMMENT "\'"`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithComment",
		ExpectedSelect:      []sql.Row{{"tableWithComment", "CREATE TABLE `tableWithComment` (\n  `pk` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT=''''"}},
	},
	{
		WriteQuery:          `create table tableWithComment (pk int) COMMENT "newline \n | return \r | backslash \\ | NUL \0 \x00 | ctrlz \Z \x1A"`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithComment",
		ExpectedSelect:      []sql.Row{{"tableWithComment", "CREATE TABLE `tableWithComment` (\n  `pk` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT='newline \\n | return \\r | backslash \\\\ | NUL \\0 x00 | ctrlz \x1A x1A'"}},
	},
	{
		WriteQuery:          `create table tableWithComment (pk int) COMMENT "ctrlz \Z \x1A \\Z \\\Z"`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithComment",
		ExpectedSelect:      []sql.Row{{"tableWithComment", "CREATE TABLE `tableWithComment` (\n  `pk` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT='ctrlz \x1A x1A \\\\Z \\\\\x1A'"}},
	},
	{
		WriteQuery:          `create table tableWithColumnComment (pk int COMMENT "'")`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithColumnComment",
		ExpectedSelect:      []sql.Row{{"tableWithColumnComment", "CREATE TABLE `tableWithColumnComment` (\n  `pk` int COMMENT ''''\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table tableWithColumnComment (pk int COMMENT "\'")`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithColumnComment",
		ExpectedSelect:      []sql.Row{{"tableWithColumnComment", "CREATE TABLE `tableWithColumnComment` (\n  `pk` int COMMENT ''''\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table tableWithColumnComment (pk int COMMENT "newline \n | return \r | backslash \\ | NUL \0 \x00 | ctrlz \Z \x1A")`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE tableWithColumnComment",
		ExpectedSelect:      []sql.Row{{"tableWithColumnComment", "CREATE TABLE `tableWithColumnComment` (\n  `pk` int COMMENT 'newline \\n | return \\r | backslash \\\\ | NUL \\0 x00 | ctrlz \x1A x1A'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table floattypedefs (a float(10), b float(10, 2), c double(10, 2))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE floattypedefs",
		ExpectedSelect:      []sql.Row{{"floattypedefs", "CREATE TABLE `floattypedefs` (\n  `a` float,\n  `b` float,\n  `c` double\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER, b TEXT, c DATE, d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, b1 BOOL, b2 BOOLEAN NOT NULL, g DATETIME, h CHAR(40))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `a` int,\n  `b` text,\n  `c` date,\n  `d` timestamp,\n  `e` varchar(20),\n  `f` blob NOT NULL,\n  `b1` tinyint(1),\n  `b2` tinyint(1) NOT NULL,\n  `g` datetime,\n  `h` char(40)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) NOT NULL)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER, b TEXT NOT NULL COMMENT 'comment', c bool, primary key (a))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` text NOT NULL COMMENT 'comment',\n  `c` tinyint(1),\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER, create_time timestamp(6) NOT NULL DEFAULT NOW(6), primary key (a))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `create_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 LIKE mytable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` bigint NOT NULL,\n  `s` varchar(20) NOT NULL COMMENT 'column s',\n  PRIMARY KEY (`i`),\n  KEY `idx_si` (`s`,`i`),\n  KEY `mytable_i_s` (`i`,`s`),\n  UNIQUE KEY `mytable_s` (`s`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery: `CREATE TABLE t1 (
			pk bigint primary key,
			v1 bigint default (2) comment 'hi there',
			index idx_v1 (v1) comment 'index here'
			)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` bigint NOT NULL,\n  `v1` bigint DEFAULT (2) COMMENT 'hi there',\n  PRIMARY KEY (`pk`),\n  KEY `idx_v1` (`v1`) COMMENT 'index here'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 like foo.othertable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `text` varchar(20) NOT NULL,\n  `number` mediumint,\n  PRIMARY KEY (`text`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) UNIQUE)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10),\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) UNIQUE KEY)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10),\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 SELECT * from mytable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` bigint NOT NULL,\n  `s` varchar(20) NOT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE mydb.t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) NOT NULL)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE mydb.t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment unique)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `j` (`j`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment, index (j))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`i`),\n  KEY `j` (`j`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment, k int, unique(j,k))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int NOT NULL AUTO_INCREMENT,\n  `k` int,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `j` (`j`,`k`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment, k int, index (j,k))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int NOT NULL AUTO_INCREMENT,\n  `k` int,\n  PRIMARY KEY (`i`),\n  KEY `j` (`j`,`k`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery: `CREATE TABLE t1 (
		  pk int NOT NULL,
		  col1 blob DEFAULT (_utf8mb4'abc'),
		  col2 json DEFAULT (json_object(_utf8mb4'a',1)),
		  col3 text DEFAULT (_utf8mb4'abc'),
		  PRIMARY KEY (pk)
		)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` int NOT NULL,\n  `col1` blob DEFAULT ('abc'),\n  `col2` json DEFAULT (json_object('a',1)),\n  `col3` text DEFAULT ('abc'),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery: `CREATE TABLE td (
		  pk int PRIMARY KEY,
		  col2 int NOT NULL DEFAULT 2,
 		  col3 double NOT NULL DEFAULT (round(-(1.58),0)),
		  col4 varchar(10) DEFAULT 'new row',
          col5 float DEFAULT 33.33,
          col6 int DEFAULT NULL,
		  col7 timestamp DEFAULT NOW(),
		  col8 bigint DEFAULT (NOW())
		)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE td",
		ExpectedSelect:      []sql.Row{{"td", "CREATE TABLE `td` (\n  `pk` int NOT NULL,\n  `col2` int NOT NULL DEFAULT '2',\n  `col3` double NOT NULL DEFAULT (round(-1.58,0)),\n  `col4` varchar(10) DEFAULT 'new row',\n  `col5` float DEFAULT '33.33',\n  `col6` int DEFAULT NULL,\n  `col7` timestamp DEFAULT CURRENT_TIMESTAMP,\n  `col8` bigint DEFAULT CURRENT_TIMESTAMP,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 (i int primary key, b1 blob, b2 blob, index(b1(123), b2(456)))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `show create table t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `b1` blob,\n  `b2` blob,\n  PRIMARY KEY (`i`),\n  KEY `b1` (`b1`(123),`b2`(456))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 (i int primary key, b1 blob, b2 blob, unique index(b1(123), b2(456)))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `show create table t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `b1` blob,\n  `b2` blob,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `b1` (`b1`(123),`b2`(456))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 (i int primary key, b1 blob, b2 blob, index(b1(10)), index(b2(20)), index(b1(123), b2(456)))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `show create table t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `b1` blob,\n  `b2` blob,\n  PRIMARY KEY (`i`),\n  KEY `b1` (`b1`(10)),\n  KEY `b1_2` (`b1`(123),`b2`(456)),\n  KEY `b2` (`b2`(20))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select * from mytable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         `select * from t1 order by i`,
		ExpectedSelect:      []sql.Row{{1, "first row"}, {2, "second row"}, {3, "third row"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select * from mytable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         `show create table t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` bigint NOT NULL,\n  `s` varchar(20) NOT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select s, i from mytable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         `select * from t1 order by i`,
		ExpectedSelect:      []sql.Row{{"first row", 1}, {"second row", 2}, {"third row", 3}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select distinct s, i from mytable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         `select * from t1 order by i`,
		ExpectedSelect:      []sql.Row{{"first row", 1}, {"second row", 2}, {"third row", 3}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select s, i from mytable order by s`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         `select * from t1 order by i`,
		ExpectedSelect:      []sql.Row{{"first row", 1}, {"second row", 2}, {"third row", 3}},
	},
	// TODO: the second column should be named `sum(i)` but is `SUM(mytable.i)`
	{
		WriteQuery:          `CREATE TABLE t1 as select s, sum(i) from mytable group by s`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         `select * from t1 order by s`, // other column is named `SUM(mytable.i)`
		ExpectedSelect:      []sql.Row{{"first row", float64(1)}, {"second row", float64(2)}, {"third row", float64(3)}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select s, sum(i) from mytable group by s having sum(i) > 2`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "select * from t1",
		ExpectedSelect:      []sql.Row{{"third row", float64(3)}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select s, i from mytable order by s limit 1`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         `select * from t1 order by i`,
		ExpectedSelect:      []sql.Row{{"first row", 1}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 as select concat("new", s), i from mytable`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(3)}},
		SelectQuery:         `select * from t1 order by i`,
		ExpectedSelect:      []sql.Row{{"newfirst row", 1}, {"newsecond row", 2}, {"newthird row", 3}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (pk varchar(10) primary key collate binary)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` varbinary(10) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (pk varchar(10) primary key charset binary)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` varbinary(10) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (pk varchar(10) primary key character set binary)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` varbinary(10) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (pk varchar(10) primary key charset binary collate binary)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` varbinary(10) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (pk varchar(10) primary key character set binary collate binary)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` varbinary(10) NOT NULL,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 (pk bit(2) default 2)`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `pk` bit(2) DEFAULT b'10'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE embeddings (id INT, vector_col VECTOR(128) NOT NULL, small_vec VECTOR(1))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE embeddings`,
		ExpectedSelect:      []sql.Row{{"embeddings", "CREATE TABLE `embeddings` (\n  `id` int,\n  `vector_col` VECTOR(128) NOT NULL,\n  `small_vec` VECTOR(1)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		// https://github.com/dolthub/dolt/issues/10345
		WriteQuery:          "create table t1 (id serial)",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `SHOW CREATE TABLE t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  UNIQUE KEY `id` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
}

var CreateTableScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/9316
		Name:         "CREATE TABLE with constraints AS SELECT osticket repro",
		SkipPrepared: true, // SHOW KEYS with WHERE clause doesn't work with prepared statements
		SetUpScript: []string{
			"CREATE TABLE ost_form_entry (id INT PRIMARY KEY, object_id INT, object_type VARCHAR(1))",
			"CREATE TABLE ost_form_entry_values (entry_id INT, field_id INT, value VARCHAR(100), value_id INT)",
			"CREATE TABLE ost_form_field (id INT PRIMARY KEY)",
			"INSERT INTO ost_form_entry VALUES (1, 100, 'U'), (2, 101, 'U'), (3, 102, 'X')",
			"INSERT INTO ost_form_entry_values VALUES (1, 1, 'user100@example.com', 1000), (2, 1, 'user101@example.com', 1001), (3, 2, 'other', 2000)",
			"INSERT INTO ost_form_field VALUES (1), (2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE TABLE IF NOT EXISTS ost_user__cdata (
						PRIMARY KEY (user_id)
					) DEFAULT CHARSET=utf8 AS
					SELECT
						entry.object_id as user_id,
						MAX(IF(field.id='1',coalesce(ans.value_id, ans.value),NULL)) as email
						FROM ost_form_entry entry
						JOIN ost_form_entry_values ans
						ON ans.entry_id = entry.id
						JOIN ost_form_field field
						ON field.id=ans.field_id
						WHERE entry.object_type='U' GROUP BY entry.object_id`,
			},
			{
				Query: "SELECT * FROM ost_user__cdata ORDER BY user_id",
				Expected: []sql.Row{
					{100, "1000"},
					{101, "1001"},
				},
			},
			{
				Query: "SHOW KEYS FROM ost_user__cdata WHERE Key_name = 'PRIMARY'",
				Expected: []sql.Row{
					{"ost_user__cdata", 0, "PRIMARY", 1, "user_id", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9316
		Name:         "CREATE TABLE with constraints AS SELECT",
		SkipPrepared: true,
		SetUpScript: []string{
			"CREATE TABLE t1 (a int not null, b varchar(10))",
			"INSERT INTO t1 VALUES (1, 'one'), (2, 'two'), (3, 'three')",
			"CREATE TABLE source (id int, name varchar(20))",
			"INSERT INTO source VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
			"CREATE TABLE override_src (a bigint, b int)",
			"INSERT INTO override_src VALUES (100, 200)",
			"CREATE TABLE base (a int, b varchar(10))",
			"INSERT INTO base VALUES (1, 'alpha'), (2, 'beta')",
			"CREATE TABLE multi_src (id int, email varchar(50), age int)",
			"INSERT INTO multi_src VALUES (1, 'a@test.com', 25), (2, 'b@test.com', 30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE TABLE t2 (PRIMARY KEY(a)) SELECT * FROM t1",
			},
			{
				Query: "SELECT * FROM t2 ORDER BY a",
				Expected: []sql.Row{
					{1, "one"},
					{2, "two"},
					{3, "three"},
				},
			},
			{
				Query: "SHOW KEYS FROM t2 WHERE Key_name = 'PRIMARY'",
				Expected: []sql.Row{
					{"t2", 0, "PRIMARY", 1, "a", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query: "CREATE TABLE indexed (KEY(name)) SELECT * FROM source",
			},
			{
				Query: "SELECT * FROM indexed ORDER BY id",
				Expected: []sql.Row{
					{1, "alice"},
					{2, "bob"},
					{3, "charlie"},
				},
			},
			{
				Query: "SHOW KEYS FROM indexed WHERE Key_name = 'name'",
				Expected: []sql.Row{
					{"indexed", 1, "name", 1, "name", nil, 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query: "CREATE TABLE override (a TINYINT NOT NULL) SELECT a, b FROM override_src",
			},
			{
				Query: "SELECT * FROM override",
				Expected: []sql.Row{
					{int8(100), 200},
				},
			},
			{
				Query: "SHOW CREATE TABLE override",
				Expected: []sql.Row{
					{"override", "CREATE TABLE `override` (\n  `a` tinyint NOT NULL,\n  `b` int\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "CREATE TABLE uniq (UNIQUE KEY(a)) SELECT * FROM base",
			},
			{
				Query: "SELECT * FROM uniq ORDER BY a",
				Expected: []sql.Row{
					{1, "alpha"},
					{2, "beta"},
				},
			},
			{
				Query: "SHOW KEYS FROM uniq WHERE Key_name = 'a'",
				Expected: []sql.Row{
					{"uniq", 0, "a", 1, "a", nil, 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
				},
			},
			{
				Query: "CREATE TABLE multi_idx (PRIMARY KEY(id), KEY(email), KEY(age)) SELECT * FROM multi_src",
			},
			{
				Query: "SELECT * FROM multi_idx ORDER BY id",
				Expected: []sql.Row{
					{1, "a@test.com", 25},
					{2, "b@test.com", 30},
				},
			},
			{
				Query: "SELECT COUNT(*) FROM information_schema.statistics WHERE table_name = 'multi_idx'",
				Expected: []sql.Row{
					{3},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/6682
		Name: "display width for numeric types",
		SetUpScript: []string{
			"CREATE TABLE numericDisplayWidthTest (pk int primary key, b boolean, ti tinyint, ti1 tinyint(1), ti2 tinyint(2), i1 int(1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SHOW CREATE TABLE numericDisplayWidthTest;",
				Expected: []sql.Row{{"numericDisplayWidthTest",
					"CREATE TABLE `numericDisplayWidthTest` (\n  `pk` int NOT NULL,\n  `b` tinyint(1),\n  `ti` tinyint,\n  `ti1` tinyint(1),\n  `ti2` tinyint,\n  `i1` int,\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				// MySQL only honors display width when it is set to 1 and used with the TINYINT type;
				// all other uses parse correctly, but are dropped.
				Query: "SHOW FULL FIELDS FROM numericDisplayWidthTest;",
				Expected: []sql.Row{
					{"pk", "int", interface{}(nil), "NO", "PRI", nil, "", "", ""},
					{"b", "tinyint(1)", interface{}(nil), "YES", "", nil, "", "", ""},
					{"ti", "tinyint", interface{}(nil), "YES", "", nil, "", "", ""},
					{"ti1", "tinyint(1)", interface{}(nil), "YES", "", nil, "", "", ""},
					{"ti2", "tinyint", interface{}(nil), "YES", "", nil, "", "", ""},
					{"i1", "int", interface{}(nil), "YES", "", nil, "", "", ""},
				},
			},
			{
				Query:          "CREATE TABLE errorTest(pk int primary key, ti tinyint(-1));",
				ExpectedErrStr: "syntax error at position 56 near 'tinyint'",
			},
		},
	},
	{
		Name: "Validate that CREATE LIKE preserves checks",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int primary key, test_score int, height int CHECK (height < 10) , CONSTRAINT mycheck CHECK (test_score >= 50))",
			"CREATE TABLE t2 LIKE t1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "INSERT INTO t2 VALUE (1, 40, 5)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
			{
				Query:       "INSERT INTO t2 VALUE (1, 60, 15)",
				ExpectedErr: sql.ErrCheckConstraintViolated,
			},
		},
	},
	{
		Name: "datetime precision",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int primary key, d datetime)",
			"CREATE TABLE t2 (pk int primary key, d datetime(3))",
			"CREATE TABLE t3 (pk int primary key, d datetime(6))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `d` datetime,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1 values (1, '2020-01-01 00:00:00.123456')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t1 order by pk",
				Expected: []sql.Row{{1, MustParseTime(time.DateTime, "2020-01-01 00:00:00")}},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{{"t2",
					"CREATE TABLE `t2` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `d` datetime(3),\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t2 values (1, '2020-01-01 00:00:00.123456')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t2 order by pk",
				Expected: []sql.Row{{1, MustParseTime(time.RFC3339Nano, "2020-01-01T00:00:00.123000000Z")}},
			},
			{
				Query: "show create table t3",
				Expected: []sql.Row{{"t3",
					"CREATE TABLE `t3` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `d` datetime(6),\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t3 values (1, '2020-01-01 00:00:00.123456')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "select * from t3 order by pk",
				Expected: []sql.Row{{1, MustParseTime(time.RFC3339Nano, "2020-01-01T00:00:00.123456000Z")}},
			},
			{
				Query:       "create table t4 (pk int primary key, d datetime(-1))",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:          "create table t4 (pk int primary key, d datetime(7))",
				ExpectedErrStr: "DATETIME supports precision from 0 to 6",
			},
			{
				Query:       "CREATE TABLE tt (pk int primary key, d datetime(3) default current_timestamp(6))",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Query:       "CREATE TABLE tt (pk int primary key, d datetime(6) default current_timestamp(3))",
				ExpectedErr: sql.ErrInvalidColumnDefaultValue,
			},
			{
				Query:    "CREATE TABLE tt (pk int primary key, d datetime(6) default current_timestamp(6))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name: "timestamp precision",
		SetUpScript: []string{
			"CREATE TABLE t1 (pk int primary key, d timestamp)",
			"CREATE TABLE t2 (pk int primary key, d timestamp(3))",
			"CREATE TABLE t3 (pk int primary key, d timestamp(6))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `d` timestamp,\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t1 values (1, '2020-01-01 00:00:00.123456')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				SkipResultCheckOnServerEngine: true, // the nanosecond is returned over the wire
				Query:                         "select * from t1 order by pk",
				Expected:                      []sql.Row{{1, MustParseTime(time.DateTime, "2020-01-01 00:00:00")}},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{{"t2",
					"CREATE TABLE `t2` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `d` timestamp(3),\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t2 values (1, '2020-01-01 00:00:00.123456')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				SkipResultCheckOnServerEngine: true, // the nanosecond is returned over the wire
				Query:                         "select * from t2 order by pk",
				Expected:                      []sql.Row{{1, MustParseTime(time.RFC3339Nano, "2020-01-01T00:00:00.123000000Z")}},
			},
			{
				Query: "show create table t3",
				Expected: []sql.Row{{"t3",
					"CREATE TABLE `t3` (\n" +
						"  `pk` int NOT NULL,\n" +
						"  `d` timestamp(6),\n" +
						"  PRIMARY KEY (`pk`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "insert into t3 values (1, '2020-01-01 00:00:00.123456')",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				SkipResultCheckOnServerEngine: true, // the nanosecond is returned over the wire
				Query:                         "select * from t3 order by pk",
				Expected:                      []sql.Row{{1, MustParseTime(time.RFC3339Nano, "2020-01-01T00:00:00.123456000Z")}},
			},
			{
				Query:       "create table t4 (pk int primary key, d TIMESTAMP(-1))",
				ExpectedErr: sql.ErrSyntaxError,
			},
			{
				Query:          "create table t4 (pk int primary key, d TIMESTAMP(7))",
				ExpectedErrStr: "TIMESTAMP supports precision from 0 to 6",
			},
		},
	},
	{
		Name: "Identifier lengths",
		SetUpScript: []string{
			"create table parent (a int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				// 64 characters
				Query:    "create table abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl (a int primary key)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "create table abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm (a int primary key)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "create table a (abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl int primary key)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "create table a (abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm int primary key)",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "create table b (a int primary key, constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl check (a > 0))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "create table b (a int primary key, constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm check (a > 0))",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "create table c (a int primary key, b int, key abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl (b))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "create table c (a int primary key, b int, key abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm (b))",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
			{
				// 64 characters
				Query:    "create table d (a int primary key, constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl foreign key (a) references parent(a))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// 65 characters
				Query:       "create table d (a int primary key, constraint abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklm foreign key (a) references parent(a))",
				ExpectedErr: sql.ErrInvalidIdentifier,
			},
		},
	},
	{
		Name: "case insensitive column name uniqueness",
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table t1 (abc int, abc int)",
				ExpectedErr: sql.ErrDuplicateColumn,
			},
			{
				Query:       "create table t2 (ABC int, ABC int)",
				ExpectedErr: sql.ErrDuplicateColumn,
			},
			{
				Query:       "create table t3 (a int, A int)",
				ExpectedErr: sql.ErrDuplicateColumn,
			},
			{
				Query:       "create table t4 (abc int, def int, Abc int)",
				ExpectedErr: sql.ErrDuplicateColumn,
			},
		},
	},
	{
		Name: "valid character set and collation options",
		SetUpScript: []string{
			"create table parent (a int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       `CREATE TABLE t1 (pk varbinary(10) primary key collate utf8mb4_0900_bin)`,
				ExpectedErr: types.ErrBinaryCollation,
			},
			{
				Query:       `CREATE TABLE t1 (pk varbinary(10) primary key charset utf8mb4_0900_bin)`,
				ExpectedErr: types.ErrCharacterSetOnInvalidType,
			},
			{
				Query:       `CREATE TABLE t1 (pk varbinary(10) primary key character set utf8mb4)`,
				ExpectedErr: types.ErrCharacterSetOnInvalidType,
			},
			{
				Query:       `CREATE TABLE t1 (pk varbinary(10) primary key charset utf8mb4 collate utf8mb4_0900_bin)`,
				ExpectedErr: types.ErrCharacterSetOnInvalidType,
			},
			{
				Query:       `CREATE TABLE t1 (pk varbinary(10) primary key character set utf8mb4 collate utf8mb4_0900_bin)`,
				ExpectedErr: types.ErrCharacterSetOnInvalidType,
			},
			{
				Query:       `CREATE TABLE t1 (pk int primary key character set utf8mb4)`,
				ExpectedErr: types.ErrCharacterSetOnInvalidType,
			},
			{
				Query:          `create table t (i int, primary key(i)) charset=utf8mb4 collate=utf8mb3_esperanto_ci;`,
				ExpectedErrStr: "utf8mb4 is not a valid character set for utf8mb3_esperanto_ci",
			},
			{
				Query:    `create table t (i int, primary key(i)) charset=utf8mb4 collate=utf8mb4_esperanto_ci;`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
	{
		Name:        "table charset options",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    `create table t1 (i int) charset latin1`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `show create table t1`,
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci"},
				},
			},
			{
				Query:    `create table t2 (i int) character set latin1`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `show create table t2`,
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci"},
				},
			},
			{
				Query:    `create table t3 (i int) charset binary`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `show create table t3`,
				Expected: []sql.Row{
					{"t3", "CREATE TABLE `t3` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=binary COLLATE=binary"},
				},
			},
			{
				Query:    `create table t4 (i int) character set binary`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `show create table t4`,
				Expected: []sql.Row{
					{"t4", "CREATE TABLE `t4` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=binary COLLATE=binary"},
				},
			},
		},
	},
	{
		Name: "if not exists option blocks",
		SetUpScript: []string{
			"create table t1 (i int, index (i));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},

			{
				Query:    "create table if not exists t1 (i int, index (i));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},

			{
				Query:    "create table if not exists t1 (i int, index notthesamename (i));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},

			{
				Query:    "create table if not exists t1 (i int primary key, foreign key (i) references t1(i));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},

			{
				Query:    "create table if not exists t1 (i int, check (i > 10));",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int,\n" +
						"  KEY `i` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin",
					},
				},
			},
		},
	},
	{
		Name: "create table with select preserves default",
		SetUpScript: []string{
			"create table a (i int primary key, j int default 100);",
			"create table b (x int primary key, y int default 200);",
			"create table c (p int primary key, q int default 300, u int as (q));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create table t1 select * from a;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t1;",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `j` int DEFAULT '100'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t2 select j from a;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t2;",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `j` int DEFAULT '100'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t3 select j as i from a;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t3;",
				Expected: []sql.Row{
					{"t3", "CREATE TABLE `t3` (\n" +
						"  `i` int DEFAULT '100'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t4 select j + 1 from a;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t4;",
				Expected: []sql.Row{
					{"t4", "CREATE TABLE `t4` (\n" +
						"  `j + 1` bigint\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t5 select a.j from a;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t5;",
				Expected: []sql.Row{
					{"t5", "CREATE TABLE `t5` (\n" +
						"  `j` int DEFAULT '100'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t6 select sqa.j from (select i, j from a) sqa;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t6;",
				Expected: []sql.Row{
					{"t6", "CREATE TABLE `t6` (\n" +
						"  `j` int DEFAULT '100'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t7 select (select j from a) sq from dual;",
				Expected: []sql.Row{
					{types.NewOkResult(1)}, // ???
				},
			},
			{
				Query: "show create table t7;",
				Expected: []sql.Row{
					{"t7", "CREATE TABLE `t7` (\n" +
						"  `sq` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t8 select * from (select * from a) a join (select * from b) b;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t8;",
				Expected: []sql.Row{
					{"t8", "CREATE TABLE `t8` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `j` int DEFAULT '100',\n" +
						"  `x` int NOT NULL,\n" +
						"  `y` int DEFAULT '200'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: `create table t9 select * from json_table('[{"c1": 1}]', '$[*]' columns (c1 int path '$.c1' default '100' on empty)) as jt;`,
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "show create table t9;",
				Expected: []sql.Row{
					{"t9", "CREATE TABLE `t9` (\n" +
						"  `c1` int NOT NULL\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Skip:  true, // syntax unsupported
				Query: `create table t10 (select j from a) union (select y from b);`,
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Skip:  true, // syntax unsupported
				Query: "show create table t10;",
				Expected: []sql.Row{
					{"t9", "CREATE TABLE `t9` (\n" +
						"  `c1` int NOT NULL\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t11 select sum(j) over() as jj from a;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t11;",
				Expected: []sql.Row{
					{"t11", "CREATE TABLE `t11` (\n" +
						"  `jj` int NOT NULL\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t12 select j from a group by j;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t12;",
				Expected: []sql.Row{
					{"t12", "CREATE TABLE `t12` (\n" +
						"  `j` int DEFAULT '100'\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "create table t13 select * from c;",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "show create table t13;",
				Expected: []sql.Row{
					{"t13", "CREATE TABLE `t13` (\n" +
						"  `p` int NOT NULL,\n" +
						"  `q` int DEFAULT '300',\n" +
						"  `u` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "create table columns from aggregate functions",
		SetUpScript: []string{
			"create table t1 (i int)",
			"insert into t1 values (1)",
			"create table t2 select sum(i), max(i), min(i), avg(i) from t1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t2;",
				// TODO: MySQL column types are different https://github.com/dolthub/dolt/issues/9754
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `sum(i)` double NOT NULL,\n" +
						"  `max(i)` int NOT NULL,\n" +
						"  `min(i)` int NOT NULL,\n" +
						"  `avg(i)` double\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
}

var CreateTableInSubroutineTests = []ScriptTest{
	//TODO: Match MySQL behavior (https://github.com/dolthub/dolt/issues/8053)
	{
		// Skipped because Dolt doesn't support this, although MySQL does.
		Name: "procedure contains CREATE TABLE AS",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE PROCEDURE foo() CREATE TABLE bar AS SELECT 1;",
				Skip:  true,
			},
			{
				Query: "CALL foo();",
				Skip:  true,
			},
			{
				Query:    "SELECT * from bar;",
				Expected: []sql.Row{{1}},
				Skip:     true,
			},
		},
	},
	{
		Name: "event contains CREATE TABLE AS",
		//TODO: Verify that event executes correctly. (https://github.com/dolthub/dolt/issues/8053)
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE EVENT foo ON SCHEDULE EVERY 1 YEAR DO CREATE TABLE bar AS SELECT 1;",
			},
		},
	},
	{
		Name: "trigger contains CREATE TABLE AS",
		//TODO: Verify that trigger executes correctly. (https://github.com/dolthub/dolt/issues/8053)
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE TRIGGER foo AFTER UPDATE ON t FOR EACH ROW BEGIN CREATE TABLE bar AS SELECT 1; END;",
			},
		},
	},
}

var CreateTableAutoIncrementTests = []ScriptTest{
	{
		Name:        "create table with non primary auto_increment column",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create table t1 (a int auto_increment unique, b int, primary key(b))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "insert into t1 (b) values (1), (2)",
				Expected: []sql.Row{
					{
						types.OkResult{
							RowsAffected: 2,
							InsertID:     1,
						},
					},
				},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL AUTO_INCREMENT,\n" +
						"  `b` int NOT NULL,\n" +
						"  PRIMARY KEY (`b`),\n" +
						"  UNIQUE KEY `a` (`a`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name:        "create table with non primary auto_increment column, separate unique key",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "create table t1 (a int auto_increment, b int, primary key(b), unique key(a))",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "insert into t1 (b) values (1), (2)",
				Expected: []sql.Row{
					{
						types.OkResult{
							RowsAffected: 2,
							InsertID:     1,
						},
					},
				},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{{"t1",
					"CREATE TABLE `t1` (\n" +
						"  `a` int NOT NULL AUTO_INCREMENT,\n" +
						"  `b` int NOT NULL,\n" +
						"  PRIMARY KEY (`b`),\n" +
						"  UNIQUE KEY `a` (`a`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
			},
			{
				Query:    "select * from t1 order by b",
				Expected: []sql.Row{{1, 1}, {2, 2}},
			},
		},
	},
	{
		Name:        "create table with non primary auto_increment column, missing unique key",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "create table t1 (a int auto_increment, b int, primary key(b))",
				ExpectedErr: sql.ErrInvalidAutoIncCols,
			},
		},
	},
	{
		Name:        "table with auto_increment table option",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				// this just ignores the auto_increment argument
				Query:    "create table t1 (i int) auto_increment=10;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t1",
				Expected: []sql.Row{
					{"t1", "CREATE TABLE `t1` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query:    "create table t2 (i int auto_increment primary key) auto_increment=10;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "show create table t2",
				Expected: []sql.Row{
					{"t2", "CREATE TABLE `t2` (\n" +
						"  `i` int NOT NULL AUTO_INCREMENT,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query:    "insert into t2 values (null), (null), (null)",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 3, InsertID: 10}}},
			},
			{
				Query: "select * from t2",
				Expected: []sql.Row{
					{10},
					{11},
					{12},
				},
			},
		},
	},
}

var BrokenCreateTableQueries = []WriteQueryTest{
	{
		WriteQuery:          `create table t1 (b blob, primary key(b(1)))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `show create table t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `b` blob NOT NULL,\n  PRIMARY KEY (`b`(1))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 (b1 blob, b2 blob, primary key(b1(123), b2(456)))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `show create table t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `b1` blob NOT NULL,\n  `b2` blob NOT NULL,\n  PRIMARY KEY (`b1`(123),`b2`(456))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 (i int, b1 blob, b2 blob, primary key(b1(123), b2(456), i))`,
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         `show create table t1`,
		ExpectedSelect:      []sql.Row{{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `b1` blob NOT NULL,\n  `b2` blob NOT NULL,\n  PRIMARY KEY (`b1`(123),`b2`(456),`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
}

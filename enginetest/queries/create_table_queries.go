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

import "github.com/dolthub/go-mysql-server/sql"

var CreateTableQueries = []WriteQueryTest{
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER, b TEXT, c DATE, d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, b1 BOOL, b2 BOOLEAN NOT NULL, g DATETIME, h CHAR(40))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int,\n  `b` text,\n  `c` date,\n  `d` timestamp,\n  `e` varchar(20),\n  `f` blob NOT NULL,\n  `b1` tinyint,\n  `b2` tinyint NOT NULL,\n  `g` datetime,\n  `h` char(40)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) NOT NULL)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER NOT NULL, b TEXT NOT NULL, c bool, primary key (a,b))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` text NOT NULL,\n  `c` tinyint,\n  PRIMARY KEY (`a`,`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1(a INTEGER NOT NULL, b TEXT NOT NULL, c bool, primary key (a,b))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` text NOT NULL,\n  `c` tinyint,\n  PRIMARY KEY (`a`,`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER, b TEXT NOT NULL COMMENT 'comment', c bool, primary key (a))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` text NOT NULL COMMENT 'comment',\n  `c` tinyint,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER, create_time timestamp(6) NOT NULL DEFAULT NOW(6), primary key (a))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `create_time` timestamp NOT NULL DEFAULT (NOW(6)),\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 LIKE mytable`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `i` bigint NOT NULL,\n  `s` varchar(20) NOT NULL COMMENT 'column s',\n  PRIMARY KEY (`i`),\n  KEY `idx_si` (`s`,`i`),\n  KEY `mytable_i_s` (`i`,`s`),\n  UNIQUE KEY `mytable_s` (`s`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery: `CREATE TABLE t1 (
			pk bigint primary key,
			v1 bigint default (2) comment 'hi there',
			index idx_v1 (v1) comment 'index here'
			)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `pk` bigint NOT NULL,\n  `v1` bigint DEFAULT (2) COMMENT 'hi there',\n  PRIMARY KEY (`pk`),\n  KEY `idx_v1` (`v1`) COMMENT 'index here'\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `create table t1 like foo.other_table`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `text` text NOT NULL,\n  `number` mediumint,\n  PRIMARY KEY (`text`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) UNIQUE)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10),\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) UNIQUE KEY)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10),\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `b` (`b`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 SELECT * from mytable`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(3)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `i` bigint NOT NULL,\n  `s` varchar(20) NOT NULL COMMENT 'column s',\n  PRIMARY KEY (`i`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE mydb.t1 (a INTEGER NOT NULL PRIMARY KEY, b VARCHAR(10) NOT NULL)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE mydb.t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `a` int NOT NULL,\n  `b` varchar(10) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment unique)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int AUTO_INCREMENT,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `j` (`j`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment, index (j))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int AUTO_INCREMENT,\n  PRIMARY KEY (`i`),\n  KEY `j` (`j`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment, k int, unique(j,k))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int AUTO_INCREMENT,\n  `k` int,\n  PRIMARY KEY (`i`),\n  UNIQUE KEY `jk` (`j`,`k`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery:          `CREATE TABLE t1 (i int primary key, j int auto_increment, k int, index (j,k))`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `i` int NOT NULL,\n  `j` int AUTO_INCREMENT,\n  `k` int,\n  PRIMARY KEY (`i`),\n  KEY `jk` (`j`,`k`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
	{
		WriteQuery: `CREATE TABLE t1 (
		  pk int NOT NULL,
		  col1 blob DEFAULT (_utf8mb4'abc'),
		  col2 json DEFAULT (json_object(_utf8mb4'a',1)),
		  col3 text DEFAULT (_utf8mb4'abc'),
		  PRIMARY KEY (pk)
		)`,
		ExpectedWriteResult: []sql.Row{{sql.NewOkResult(0)}},
		SelectQuery:         "SHOW CREATE TABLE t1",
		ExpectedSelect:      []sql.Row{sql.Row{"t1", "CREATE TABLE `t1` (\n  `pk` int NOT NULL,\n  `col1` blob DEFAULT (\"abc\"),\n  `col2` json DEFAULT (JSON_OBJECT(\"a\", 1)),\n  `col3` text DEFAULT (\"abc\"),\n  PRIMARY KEY (`pk`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"}},
	},
}

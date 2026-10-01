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
	"github.com/dolthub/go-mysql-server/sql/types"
)

var BrokenScriptTests = []ScriptTest{
	{
		Name: "ALTER TABLE RENAME on a column when another column has a default dependency on it",
		SetUpScript: []string{
			"CREATE TABLE `test` (`pk` bigint NOT NULL,`v2` int NOT NULL DEFAULT '100',`v3` int DEFAULT ((`v2` + 1)),PRIMARY KEY (`pk`));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "alter table test rename column v2 to mycol",
				ExpectedErr: sql.ErrAlterTableNotSupported, // Not the correct error. The point is that this query needs to fail.
			},
		},
	},
	// TODO: We should implement unique indexes with GMS
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
	{
		Name: "Multialter DDL with ADD/DROP Primary Key",
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
			{
				// This last modification ends up with a UNIQUE constraint on pk
				// This is caused by Table.dropColumnFromSchema, not dropping the pkOrdinal, but this causes other problems specific to GMS
				Query:    "ALTER TABLE t ADD column `v4` int NOT NULL, ADD column `v5` int NOT NULL, DROP COLUMN `v1`, ADD COLUMN `v6` int NOT NULL, DROP COLUMN `v2`, ADD COLUMN v7 int NOT NULL",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: "DESCRIBE t",
				Expected: []sql.Row{
					{"pk", "int", "NO", "", nil, ""},
					{"v4", "int", "NO", "", nil, ""},
					{"v5", "int", "NO", "", nil, ""},
					{"v6", "int", "NO", "", nil, ""},
					{"v7", "int", "NO", "", nil, ""},
				},
			},
		},
	},
	{
		Name: "REGEXP operator",
		SetUpScript: []string{
			"CREATE TABLE IF NOT EXISTS `person` (`id` INTEGER AUTO_INCREMENT NOT NULL PRIMARY KEY, `name` VARCHAR(255) NOT NULL);",
			"INSERT INTO `person` (`name`) VALUES ('n1'), ('n2'), ('n3')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT `t1`.`id`, `t1`.`name` FROM `person` AS `t1` WHERE (`t1`.`name` REGEXP 'N[1,3]') ORDER BY `t1`.`name`;",
				Expected: []sql.Row{{1, "n1"}, {3, "n3"}},
			},
		},
	},
	{
		Name: "non-existent procedure in trigger body",
		SetUpScript: []string{
			"CREATE TABLE XA(YW VARCHAR(24) NOT NULL, XB VARCHAR(100), XC VARCHAR(2500),\n  XD VARCHAR(2500), XE VARCHAR(100), XF VARCHAR(100), XG VARCHAR(100),\n  XI VARCHAR(100), XJ VARCHAR(100), XK VARCHAR(100), XL VARCHAR(100),\n  XM VARCHAR(1000), XN TEXT, XO TEXT, PRIMARY KEY (YW));",
			"CREATE TABLE XP(YW VARCHAR(24) NOT NULL, XQ VARCHAR(100) NOT NULL,\n  XR VARCHAR(1000), PRIMARY KEY (YW));",
			"CREATE TABLE XS(YW VARCHAR(24) NOT NULL, XT VARCHAR(24) NOT NULL,\n  XU VARCHAR(24), XV VARCHAR(100) NOT NULL, XW DOUBLE NOT NULL,\n  XX DOUBLE NOT NULL, XY VARCHAR(100), XC VARCHAR(100), XZ VARCHAR(100) NOT NULL,\n  YA DOUBLE, YB VARCHAR(24) NOT NULL, YC VARCHAR(1000), XO VARCHAR(1000),\n  YD DOUBLE NOT NULL, YE DOUBLE NOT NULL, PRIMARY KEY (YW));",
			"CREATE TABLE YF(YW VARCHAR(24) NOT NULL, XB VARCHAR(100) NOT NULL, YG VARCHAR(100),\n  YH VARCHAR(100), XO TEXT, PRIMARY KEY (YW));",
			"CREATE TABLE yp(YW VARCHAR(24) NOT NULL, XJ VARCHAR(100) NOT NULL, XL VARCHAR(100),\n  XT VARCHAR(24) NOT NULL, YI INT NOT NULL, XO VARCHAR(1000), PRIMARY KEY (YW),\n  FOREIGN KEY (XT) REFERENCES XP (YW));",
			"INSERT INTO XS VALUES ('', '', NULL, 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC', 0, 0,\n  NULL, NULL, '', NULL, '', NULL, NULL, 0, 0);",
			"INSERT INTO YF VALUES ('', '', NULL, NULL, NULL);",
			"INSERT INTO XA VALUES ('', '', '', '', '', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC',\n  '', '', '', '', '', '', '', '');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT DISTINCT YM.YW AS YW,\n  (SELECT YW FROM YF WHERE YF.XB = YM.XB) AS YF_YW,\n  (\n    SELECT YW\n    FROM yp\n    WHERE\n      yp.XJ = YM.XJ AND\n      (yp.XL = YM.XL OR (yp.XL IS NULL AND YM.XL IS NULL)) AND\n      yp.XT = nd.XT\n    ) AS YJ,\n  XE AS XE,\n  XI AS YO,\n  XK AS XK,\n  XM AS XM,\n  CASE\n    WHEN YM.XO <> 'Z'\n  THEN YM.XO\n  ELSE NULL\n  END AS XO\n  FROM (\n    SELECT YW, XB, XC, XE, XF, XI, XJ, XK,\n      CASE WHEN XL = 'Z' OR XL = 'Z' THEN NULL ELSE XL END AS XL,\n      XM, XO\n    FROM XA\n  ) YM\n  INNER JOIN XS nd\n    ON nd.XV = XF\n  WHERE\n    XB IN (SELECT XB FROM YF) AND\n    (XF IS NOT NULL AND XF <> 'Z')\n  UNION\n  SELECT DISTINCT YL.YW AS YW,\n    (\n      SELECT YW\n      FROM YF\n      WHERE YF.XB = YL.XB\n    ) AS YF_YW,\n    (\n      SELECT YW FROM yp\n      WHERE\n        yp.XJ = YL.XJ AND\n        (yp.XL = YL.XL OR (yp.XL IS NULL AND YL.XL IS NULL)) AND\n        yp.XT = YN.XT\n    ) AS YJ,\n    XE AS XE,\n    XI AS YO,\n    XK AS XK,\n    XM AS XM,\n    CASE WHEN YL.XO <> 'Z' THEN YL.XO ELSE NULL END AS XO\n  FROM (\n    SELECT YW, XB, XC, XE, XF, XI, XJ, XK,\n      CASE WHEN XL = 'Z' OR XL = 'Z' THEN NULL ELSE XL END AS XL,\n      XM, XO\n      FROM XA\n  ) YL\n  INNER JOIN XS YN\n    ON YN.XC = YL.XC\n  WHERE\n    XB IN (SELECT XB FROM YF) AND \n    (XF IS NULL OR XF = 'Z');",
				Expected: []sql.Row{{"", "", "", "", "", "", "", ""}},
			},
		},
	},
	{
		Name: "non-existent procedure in trigger body",
		SetUpScript: []string{
			"create table tbl_I (i int primary key);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "alter table tbl_i add column j int, add check (j < 10);",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name: "renaming table name that is referenced in existing view",
		SetUpScript: []string{
			"create table t1 (id int primary key, v1 int);",
			"insert into t1 values (1,1);",
			"create view v1 as select * from t1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from v1;",
				Expected: []sql.Row{{1, 1}},
			},
			{
				Query:    "rename table t1 to t2;",
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "show tables;",
				Expected: []sql.Row{{"myview"}, {"t2"}, {"v1"}},
			},
			{
				Query:          "select * from v1;",
				ExpectedErrStr: "View 'v1' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them",
			},
			{
				Query:                 "show create view v1;",
				Expected:              []sql.Row{{"v1", "CREATE VIEW `v1` AS select * from t1", "utf8mb4", "utf8mb4_0900_bin"}},
				ExpectedWarningsCount: 1,
			},
			{
				Query:    "show warnings;",
				Expected: []sql.Row{{"Warning", 1356, "View 'v1' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them"}},
			},
		},
	},
	{
		Name: "TIMESTAMP type value should be converted from session TZ to UTC TZ to be stored",
		SetUpScript: []string{
			"CREATE TABLE timezone_test (ts TIMESTAMP, dt DATETIME)",
			"INSERT INTO timezone_test VALUES ('2023-02-14 08:47', '2023-02-14 08:47');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SET SESSION time_zone = '-05:00';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT DATE_FORMAT(ts, '%H:%i:%s'), DATE_FORMAT(dt, '%H:%i:%s') from timezone_test;",
				Expected: []sql.Row{{"11:47:00", "08:47:00"}},
			},
			{
				Query:    "SELECT UNIX_TIMESTAMP(ts), UNIX_TIMESTAMP(dt) from timezone_test;",
				Expected: []sql.Row{{float64(1676393220), float64(1676382420)}},
			},
		},
	},
}

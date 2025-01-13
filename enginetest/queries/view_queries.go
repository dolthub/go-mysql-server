// Copyright 2023 Dolthub, Inc.
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

var ViewScripts = []ScriptTest{
	{
		Name: "multi database view",
		SetUpScript: []string{
			"Create database base;",
			"Create table base.xy (x int primary key, y int);",
			"Insert into base.xy values (1, 2);",
			"Create database live;",
			"create view live.xy as select base.xy.x AS x, base.xy.y AS y from base.xy;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT database()",
				Expected: []sql.UntypedSqlRow{{"mydb"}},
			},
			{
				Query:    "SELECT * from live.xy;",
				Expected: []sql.UntypedSqlRow{{1, 2}},
			},
		},
	},
	{
		Name: "view of join with projections",
		SetUpScript: []string{
			`
CREATE TABLE tab1 (
  pk int NOT NULL,
  col0 int,
  col1 float,
  col2 text,
  col3 int,
  col4 float,
  col5 text,
  PRIMARY KEY (pk),
  KEY idx_tab1_0 (col0),
  KEY idx_tab1_1 (col1),
  KEY idx_tab1_3 (col3),
  KEY idx_tab1_4 (col4)
)`,
			"insert into tab1 values (6, 0, 52.14, 'jxmel', 22, 2.27, 'pzxbn')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "CREATE VIEW view_2_tab1_157 AS SELECT pk, col0 FROM tab1 WHERE NOT ((col0 IN (SELECT col3 FROM tab1 WHERE ((col0 IS NULL) OR col3 > 5 OR col3 <= 50 OR col1 < 83.11))) OR col0 > 75)",
				Expected: []sql.UntypedSqlRow{{types.OkResult{}}},
			},
			{
				Query:    "select pk, col0 from view_2_tab1_157",
				Expected: []sql.UntypedSqlRow{{6, 0}},
			},
		},
	},
	{
		Name: "view with expression name",
		SetUpScript: []string{
			`create view v as select 2+2`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from v;",
				Expected: []sql.UntypedSqlRow{{4}},
				ExpectedColumns: sql.Schema{
					{
						Name: "2+2",
						Type: types.Int64,
					},
				},
			},
		},
	},
	{
		Name: "view with column names",
		SetUpScript: []string{
			`CREATE TABLE xy (x int primary key, y int);`,
			`create view v_today(today) as select CURRENT_DATE()`,
			`CREATE VIEW xyv (u,v) AS SELECT * from xy;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from xyv;",
				Expected: []sql.UntypedSqlRow{},
				ExpectedColumns: sql.Schema{
					{
						Name: "u",
						Type: types.Int32,
					},
					{
						Name: "v",
						Type: types.Int32,
					},
				},
			},
			{
				Query: "SELECT * from v_today;",
				ExpectedColumns: sql.Schema{
					{
						Name: "today",
						Type: types.LongText,
					},
				},
			},
			{
				Query:       "CREATE VIEW xyv (u) AS SELECT * from xy;",
				ExpectedErr: sql.ErrInvalidColumnNumber,
			},
		},
	},
	{
		Name: "view columns retain original case",
		SetUpScript: []string{
			`CREATE TABLE strs ( id int NOT NULL AUTO_INCREMENT,
                                 str  varchar(15) NOT NULL,
                                 PRIMARY KEY (id));`,
			`CREATE VIEW caseSensitive AS SELECT id as AbCdEfG FROM strs;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * from caseSensitive;",
				Expected: []sql.UntypedSqlRow{},
				ExpectedColumns: sql.Schema{
					{
						Name: "AbCdEfG",
						Type: types.Int32,
					},
				},
			},
		},
	},
	{
		Name: "check view with escaped strings",
		SetUpScript: []string{
			`CREATE TABLE strs ( id int NOT NULL AUTO_INCREMENT,
                                 str  varchar(15) NOT NULL,
                                 PRIMARY KEY (id));`,
			`CREATE VIEW quotes AS SELECT * FROM strs WHERE str IN ('joe''s',
                                                                    "jan's",
                                                                    'mia\\''s',
                                                                    'bob\'s'
                                                                   );`,
			`INSERT INTO strs VALUES (0,"joe's");`,
			`INSERT INTO strs VALUES (0,"mia\\'s");`,
			`INSERT INTO strs VALUES (0,"bob's");`,
			`INSERT INTO strs VALUES (0,"joe's");`,
			`INSERT INTO strs VALUES (0,"notInView");`,
			`INSERT INTO strs VALUES (0,"jan's");`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * from quotes order by id",
				Expected: []sql.UntypedSqlRow{
					{1, "joe's"},
					{2, "mia\\'s"},
					{3, "bob's"},
					{4, "joe's"},
					{6, "jan's"}},
			},
		},
	},
	{
		Name: "show view",
		SetUpScript: []string{
			"create table xy (x int primary key, y int)",
			"create view v as select * from xy",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show keys from v",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "show index from v from mydb",
				Expected: []sql.UntypedSqlRow{},
			},
			{
				Query:    "show index from v where Column_name = 'x'",
				Expected: []sql.UntypedSqlRow{},
			},
		},
	},
	{
		Name: "views with defaults",
		SetUpScript: []string{
			"create table t (i int primary key, j int default 100);",
			"insert into t(i) values (1);",
			"create table tt (ii int primary key, jj int default (pow(11, 2)));",
			"insert into tt values (1, default), (3, 4);",
			"create view v as select * from t;",
			"create view v1 as select i, j + 10 as jj from t;",
			"create view vv as select i, ii, j, jj, i + ii + j + jj from t join tt where i = ii;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show full columns from v;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", nil, "NO", "", nil, "", "", ""},
					{"j", "int", nil, "YES", "", "100", "", "", ""},
				},
			},
			{
				Query: "show columns from v;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "NO", "", nil, ""},
					{"j", "int", "YES", "", "100", ""},
				},
			},
			{
				Query: "describe v;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "NO", "", nil, ""},
					{"j", "int", "YES", "", "100", ""},
				},
			},
			{
				Query: "select * from v",
				Expected: []sql.UntypedSqlRow{
					{1, 100},
				},
			},
			{
				Query: "show full columns from v1;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", nil, "NO", "", nil, "", "", ""},
					{"jj", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "show columns from v1;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "NO", "", nil, ""},
					{"jj", "bigint", "YES", "", nil, ""},
				},
			},
			{
				Query: "describe v1;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "NO", "", nil, ""},
					{"jj", "bigint", "YES", "", nil, ""},
				},
			},
			{
				Query: "select * from v1",
				Expected: []sql.UntypedSqlRow{
					{1, 110},
				},
			},
			{
				Query: "show full columns from vv;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", nil, "NO", "", nil, "", "", ""},
					{"ii", "int", nil, "NO", "", nil, "", "", ""},
					{"j", "int", nil, "YES", "", "100", "", "", ""},
					{"jj", "int", nil, "YES", "", "(power(11, 2))", "DEFAULT_GENERATED", "", ""},
					{"i + ii + j + jj", "bigint", nil, "YES", "", nil, "", "", ""},
				},
			},
			{
				Query: "show columns from vv;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "NO", "", nil, ""},
					{"ii", "int", "NO", "", nil, ""},
					{"j", "int", "YES", "", "100", ""},
					{"jj", "int", "YES", "", "(power(11, 2))", "DEFAULT_GENERATED"},
					{"i + ii + j + jj", "bigint", "YES", "", nil, ""},
				},
			},
			{
				Query: "describe vv;",
				Expected: []sql.UntypedSqlRow{
					{"i", "int", "NO", "", nil, ""},
					{"ii", "int", "NO", "", nil, ""},
					{"j", "int", "YES", "", "100", ""},
					{"jj", "int", "YES", "", "(power(11, 2))", "DEFAULT_GENERATED"},
					{"i + ii + j + jj", "bigint", "YES", "", nil, ""},
				},
			},
			{
				Query: "select * from vv",
				Expected: []sql.UntypedSqlRow{
					{1, 1, 100, 121, 223},
				},
			},
		},
	},
}

var ViewCreateInSubroutineTests = []ScriptTest{
	//TODO: Match MySQL behavior (https://github.com/dolthub/dolt/issues/8053)
	{
		// Skipped because we return an error even though MySQL supports this.
		Name: "procedure contains CREATE VIEW AS",
		Assertions: []ScriptTestAssertion{
			{
				Query: "CREATE PROCEDURE foo() CREATE VIEW bar AS SELECT 1;",
				Skip:  true,
			},
			{
				Query: "CALL foo();",
				Skip:  true,
			},
			{
				Query:    "SELECT * from bar;",
				Expected: []sql.UntypedSqlRow{{1}},
				Skip:     true,
			},
		},
	},
	{
		Name: "event contains CREATE VIEW AS",
		Assertions: []ScriptTestAssertion{
			{
				// Tests that the query doesn't panic.
				Query: "CREATE EVENT foo ON SCHEDULE EVERY 1 YEAR DO CREATE VIEW bar AS SELECT 1;",
			},
		},
	},
	{
		Name: "trigger contains CREATE VIEW AS",
		SetUpScript: []string{
			"CREATE TABLE t (pk INT PRIMARY KEY);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// Tests that the query doesn't panic.
				Query: "CREATE TRIGGER foo AFTER UPDATE ON t FOR EACH ROW BEGIN CREATE TABLE bar AS SELECT 1; END;",
			},
		},
	},
}

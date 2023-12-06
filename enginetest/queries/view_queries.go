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
				Expected: []sql.Row{{types.OkResult{}}},
			},
			{
				Query:    "select pk, col0 from view_2_tab1_157",
				Expected: []sql.Row{{6, 0}},
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
				Expected: []sql.Row{{4}},
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
				Expected: []sql.Row{},
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
				Expected: []sql.Row{},
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
				Expected: []sql.Row{
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
				Expected: []sql.Row{},
			},
			{
				Query:    "show index from v from mydb",
				Expected: []sql.Row{},
			},
			{
				Query:    "show index from v where Column_name = 'x'",
				Expected: []sql.Row{},
			},
		},
	},
}

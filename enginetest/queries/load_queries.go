// Copyright 2021 Dolthub, Inc.
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
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var LoadDataScripts = []ScriptTest{
	{
		Name: "Basic load data with enclosed values.",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
			"LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{int8(1)}, {int8(2)}, {int8(3)}, {int8(4)}},
			},
		},
	},
	{
		Name: "Load data with csv",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, c1 longtext)",
			"LOAD DATA INFILE './testdata/test2.csv' INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{int8(1), "hi"}, {int8(2), "hello"}},
			},
		},
	},
	{
		Name: "Load data with csv but use IGNORE ROWS syntax",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, c1 longtext)",
			"LOAD DATA INFILE './testdata/test2.csv' INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 ROWS",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{int8(1), "hi"}, {int8(2), "hello"}},
			},
		},
	},
	{
		Name: "Load data with csv with prefix.",
		SetUpScript: []string{
			"create table loadtable(pk longtext, c1 int)",
			"LOAD DATA INFILE './testdata/test3.csv' INTO TABLE loadtable FIELDS TERMINATED BY ',' LINES STARTING BY 'xxx' IGNORE 1 LINES (`pk`, `c1`)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"\"abc\"", int8(1)}, {"\"def\"", int8(2)}, {"\"hello\"", nil}},
			},
		},
	},
	{
		Name: "LOAD DATA with all columns reordered in projection",
		SetUpScript: []string{
			"create table loadtable(pk longtext, c1 int)",
			"LOAD DATA INFILE './testdata/test3backwards.csv' INTO TABLE loadtable FIELDS TERMINATED BY ',' LINES STARTING BY 'xxx' IGNORE 1 LINES (`c1`, `pk`)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"\"abc\"", int8(1)}, {"\"def\"", int8(2)}, {"\"hello\"", nil}},
			},
		},
	},
	{
		Name: "Table has more columns than import.",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, c1 int)",
			"LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable ORDER BY pk",
				Expected: []sql.Row{{1, nil}, {2, nil}, {3, nil}, {4, nil}},
			},
		},
	},
	{
		Name: "LOAD DATA handles Windows line-endings and a subset of columns that are not in order",
		SetUpScript: []string{
			"CREATE TABLE inmate_population_snapshots (id char(21) NOT NULL, snapshot_date date NOT NULL, total int," +
				"total_off_site int, male int, female int, other_gender int, white int, black int, hispanic int," +
				"asian int, american_indian int, mexican_american int, multi_racial int, other_race int," +
				"on_probation int, on_parole int, felony int, misdemeanor int, other_offense int," +
				"convicted_or_sentenced int, detained_or_awaiting_trial int, first_time_incarcerated int, employed int," +
				"unemployed int, citizen int, noncitizen int, juvenile int, juvenile_male int, juvenile_female int," +
				"death_row_condemned int, solitary_confinement int, technical_parole_violators int," +
				"source_url varchar(2043) NOT NULL, source_url_2 varchar(2043), civil_offense int, federal_offense int," +
				"PRIMARY KEY (id,snapshot_date), KEY id (id));",
			"LOAD DATA INFILE './testdata/test6.csv' INTO TABLE inmate_population_snapshots " +
				"FIELDS TERMINATED BY ',' " +
				"LINES TERMINATED BY '\r\n' " +
				"IGNORE 1 LINES " +
				"(federal_offense, misdemeanor, total, detained_or_awaiting_trial, felony, snapshot_date, id, source_url, source_url_2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM inmate_population_snapshots",
				Expected: []sql.Row{
					{"8946", time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 0, nil, nil, nil, 0, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "https://www.website.gov", "https://www.website.gov/other.html", nil, nil},
					{"8976", time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC), 196, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 0, 73, nil, nil, 123, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "https://www.website.gov", "https://www.website.gov/other.html", nil, 0},
					{"8978", time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC), 0, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 0, nil, nil, nil, 0, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "https://www.website.gov", "https://www.website.gov/other.html", nil, nil},
					{"8979", time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC), 71, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, 5, 3, nil, nil, 63, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "https://www.website.gov", "https://www.website.gov/other.html", nil, 0},
				},
			},
		},
	},
	{
		Name: "LOAD DATA handles non-nil default values",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 * 10), v2 BIGINT DEFAULT 5);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 * 10), v2 BIGINT DEFAULT 5);",
			"CREATE TABLE test3 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk * 10), v2 BIGINT DEFAULT (v1 - 1));",
			"CREATE TABLE test4 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk * 10), v2 BIGINT DEFAULT (v1 - 1));",
			"LOAD DATA INFILE './testdata/test7.txt' INTO TABLE test1;",
			"LOAD DATA INFILE './testdata/test7.txt' INTO TABLE test2 (pk);", // The (pk) projection results in a different tree
			"LOAD DATA INFILE './testdata/test7.txt' INTO TABLE test3;",
			"LOAD DATA INFILE './testdata/test7.txt' INTO TABLE test4 (pk);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test1",
				Expected: []sql.Row{
					{1, 50, 5},
					{2, 50, 5},
					{3, 50, 5},
				},
			},
			{
				Query: "SELECT * FROM test2",
				Expected: []sql.Row{
					{1, 50, 5},
					{2, 50, 5},
					{3, 50, 5},
				},
			},
			{
				Query: "SELECT * FROM test3",
				Expected: []sql.Row{
					{1, 10, 9},
					{2, 20, 19},
					{3, 30, 29},
				},
			},
			{
				Query: "SELECT * FROM test4",
				Expected: []sql.Row{
					{1, 10, 9},
					{2, 20, 19},
					{3, 30, 29},
				},
			},
		},
	},
	{
		Name: "LOAD DATA handles non-nil default values with varying field counts per row",
		SetUpScript: []string{
			"CREATE TABLE test1 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (v2 * 10), v2 BIGINT DEFAULT 5);",
			"CREATE TABLE test2 (pk BIGINT PRIMARY KEY, v1 BIGINT DEFAULT (pk * 10), v2 BIGINT DEFAULT (v1 - 1));",
			"LOAD DATA INFILE './testdata/test8.txt' INTO TABLE test1 FIELDS TERMINATED BY ',';",
			"LOAD DATA INFILE './testdata/test8.txt' INTO TABLE test2 FIELDS TERMINATED BY ',';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test1",
				Expected: []sql.Row{
					{1, 50, 5},
					{2, 100, 5},
					{3, 50, 5},
				},
			},
			{
				Query: "SELECT * FROM test2",
				Expected: []sql.Row{
					{1, 10, 9},
					{2, 100, 99},
					{3, 30, 29},
				},
			},
		},
	},
}

var LoadDataErrorScripts = []ScriptTest{
	{
		Name:        "Load data into table that doesn't exist throws error.",
		Query:       "LOAD DATA INFILE 'test1.txt' INTO TABLE loadtable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Name: "Load data with unknown files throws an error.",
		SetUpScript: []string{
			"create table loadtable(pk longtext, c1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE '/x/ytx' INTO TABLE loadtable",
				ExpectedErr: sql.ErrLoadDataCannotOpen,
			},
		},
	},
	{
		Name: "Load data with unknown columns throws an error",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (bad)",
				ExpectedErr: plan.ErrInsertIntoNonexistentColumn,
			},
		},
	},
	{
		Name: "Load data escaped by terms longer than 1 character throws an error",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ESCAPED BY 'xx' (pk)",
				ExpectedErr: sql.ErrLoadDataCharacterLength,
			},
		},
	},
	{
		Name: "Load data enclosed by term longer than 1 character throws an error",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY 'xx' (pk)",
				ExpectedErr: sql.ErrLoadDataCharacterLength,
			},
		},
	},
}

var LoadDataFailingScripts = []ScriptTest{
	{
		Name: "Escaped values are correctly parsed.",
		SetUpScript: []string{
			"create table loadtable(pk longtext)",
			"LOAD DATA INFILE 'test5.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' IGNORE 1 LINES",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"hi"}, {"hello"}, {nil}, {"TryN"}, {fmt.Sprintf("%c", 26)}, {fmt.Sprintf("%c", 0)}, {"new\n"}},
			},
		},
	},
	{
		Name: "Load and terminate have the same values.",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
			"LOAD DATA INFILE 'test1.txt' INTO TABLE loadtable FIELDS TERMINATED BY '\"' ENCLOSED BY '\"'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{int8(1)}, {int8(2)}, {int8(3)}, {int8(4)}},
			},
		},
	},
	{
		Name: "Loading value into different column type results in default value.",
		SetUpScript: []string{
			"create table loadtable(pk longtext, c1 int)",
			"LOAD DATA INFILE 'test4.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (c1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{nil, 0}, {nil, 0}},
			},
		},
	},
	{
		Name: "LOAD DATA handles nulls",
		SetUpScript: []string{
			"create table loadtable(pk longtext, c1 int)",
			"LOAD DATA INFILE 'test4.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"hi", 1}, {"hello", nil}},
			},
		},
	},
	{
		Name: "LOAD DATA can handle a differing column order",
		SetUpScript: []string{
			"create table loadtable(pk int, c1 string) ",
			"LOAD DATA INFILE 'test4.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (c1, pk)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{1, "hi"}, {nil, "hello"}},
			},
		},
	},
}

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

	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
)

var LoadDataScripts = []ScriptTest{
	{
		Name: "LOAD DATA with unterminated enclosed field",
		SetUpScript: []string{
			"CREATE TABLE t_unterminated (val VARCHAR(255))",
			"LOAD DATA INFILE './testdata/loaddata_unterminated.dat' INTO TABLE t_unterminated FIELDS ENCLOSED BY '\"'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_unterminated",
				Expected: []sql.Row{
					{"\"unterminated field"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA with extra fields, user variables, and missing fields",
		SetUpScript: []string{
			"CREATE TABLE t_extra (id INT PRIMARY KEY, val VARCHAR(255))",
			"LOAD DATA INFILE './testdata/loaddata_extra_fields.dat' INTO TABLE t_extra FIELDS TERMINATED BY ',' (id, val, @extra1, @extra2)",
			"CREATE TABLE t_short (id INT PRIMARY KEY, val VARCHAR(255) NOT NULL DEFAULT 'default')",
			"LOAD DATA INFILE './testdata/loaddata_extra_fields.dat' INTO TABLE t_short FIELDS TERMINATED BY ',' (id, val)",
			"CREATE TABLE t_defaults (id INT PRIMARY KEY, val VARCHAR(255) DEFAULT 'default')",
			"LOAD DATA INFILE './testdata/loaddata_extra_fields.dat' INTO TABLE t_defaults FIELDS TERMINATED BY ',' (id)",
			"CREATE TABLE t_discard (id INT PRIMARY KEY, val VARCHAR(255))",
			"LOAD DATA INFILE './testdata/loaddata_extra_fields.dat' INTO TABLE t_discard FIELDS TERMINATED BY ',' (id, @discard, val)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM t_extra ORDER BY id",
				Expected: []sql.Row{
					{1, "val1"},
					{2, "val2"},
					{3, nil},
				},
			},
			{
				Query: "SELECT * FROM t_short ORDER BY id",
				Expected: []sql.Row{
					{1, "val1"},
					{2, "val2"},
					{3, ""},
				},
			},
			{
				Query: "SELECT * FROM t_defaults ORDER BY id",
				Expected: []sql.Row{
					{1, "default"},
					{2, "default"},
					{3, "default"},
				},
			},
			{
				Query: "SELECT * FROM t_discard ORDER BY id",
				Expected: []sql.Row{
					{1, "extra1"},
					{2, "extra3"},
					{3, nil},
				},
			},
			{
				Query: "SELECT @extra1, @extra2, @discard",
				Expected: []sql.Row{
					{nil, nil, nil},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9969
		Name: "LOAD DATA with ENCLOSED BY and ESCAPED BY parsing",
		SetUpScript: []string{
			"create table t1(pk int primary key, c1 longtext)",
			"LOAD DATA INFILE './testdata/loaddata_term_in_field.dat' INTO TABLE t1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'",
			"create table t2(pk int primary key, c1 longtext)",
			"LOAD DATA INFILE './testdata/loaddata_escape.dat' INTO TABLE t2 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\'",
			"create table t3(a varchar(20), b varchar(20))",
			"LOAD DATA INFILE './testdata/loaddata_enclosed.dat' INTO TABLE t3 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"'",
			"create table t4(a varchar(20), b varchar(20))",
			"LOAD DATA INFILE './testdata/loaddata_mixed_escapes.dat' INTO TABLE t4 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\\\\'",
			"create table t5(a text, b text)",
			"LOAD DATA INFILE './testdata/loaddata_single_quotes.dat' INTO TABLE t5 FIELDS TERMINATED BY ',' ENCLOSED BY ''''",
			"create table t6(pk int, a varchar(20), b varchar(20))",
			"LOAD DATA INFILE './testdata/loaddata_nulls.dat' INTO TABLE t6 FIELDS TERMINATED BY ','",
			"create table t7(i int, v text)",
			"LOAD DATA INFILE './testdata/loaddata_eof.dat' INTO TABLE t7 FIELDS TERMINATED BY ',' ENCLOSED BY '$' ESCAPED BY '$'",
			"create table t8(i int, v text)",
			"LOAD DATA INFILE './testdata/loaddata_enc_esc_eq.dat' INTO TABLE t8 FIELDS TERMINATED BY ',' ENCLOSED BY '$' ESCAPED BY '$'",
			"create table t9(i int, v text)",
			"LOAD DATA INFILE './testdata/loaddata_lborder_null.dat' INTO TABLE t9 FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY ''",
			"create table t10(i int, v text)",
			"LOAD DATA INFILE './testdata/loaddata_null_in_field.dat' INTO TABLE t10 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY ''",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t1",
				Expected: []sql.Row{{1, "foo,bar"}},
			},
			{
				Query:    "select * from t2",
				Expected: []sql.Row{{1, "foo,bar"}},
			},
			{
				Query: "select * from t3 ORDER BY a",
				Expected: []sql.Row{
					{"a\"b", "cd\"ef"},
					{"field1", "field2"},
					{"foo,bar", "baz,qux"},
				},
			},
			{
				Query: "select * from t4",
				Expected: []sql.Row{
					{nil, "\x1A"},
					{"a,b", "c,d"},
					{"hello\nworld", "foo\tbar"},
				},
			},
			{
				Query: "select * from t5",
				Expected: []sql.Row{
					{"Field A", "Field B"},
					{"Field 1", "Field 2"},
					{"Field 3", "Field 4"},
					{"Field 5", "Field 6"},
				},
			},
			{
				Query: "select * from t6 ORDER BY pk",
				Expected: []sql.Row{
					{1, "hello", "world"},
					{2, nil, "test"},
					{3, "", "empty"},
					{4, nil, nil},
				},
			},
			{
				Query: "select * from t7",
				Expected: []sql.Row{
					{1, "foo $0 $b $n $t $Z $N bar"},
					{2, "$foo $ bar$"},
				},
			},
			{
				Query: "select * from t8",
				Expected: []sql.Row{
					{1, "foo $0 $b $n $t $Z $N bar"},
					{2, "foo $ bar"},
				},
			},
			{
				Query: "select * from t9",
				Expected: []sql.Row{
					{1, "\x00foo bar"},
				},
			},
			{
				Query: "select * from t10",
				Expected: []sql.Row{
					{1, "foo \x00 bar"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA does not apply column defaults when \\N provided",
		SetUpScript: []string{
			"create table t (pk int primary key, c1 int default 1, c2 int)",
			// Explicitly use Windows-style line endings to be robust on Windows CI
			"LOAD DATA INFILE './testdata/load_defaults_null.csv' INTO TABLE t FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t",
				Expected: []sql.Row{{1, nil, 1}},
			},
		},
	},
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
		Name: "Basic load data check error",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, check (pk > 1))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'",
				ExpectedErrStr: "Check constraint \"loadtable_chk_1\" violated",
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
		Name: "Load JSON data. EnclosedBy and EscapedBy are the same.",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, j json)",
			"LOAD DATA INFILE './testdata/simple_json.txt' INTO TABLE loadtable FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\"';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from loadtable",
				Expected: []sql.Row{
					{1, types.MustJSON(`{"foo": "bar"}`)},
				},
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
	{
		Name: "Load data can ignore row with existing primary key",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, c1 varchar(10))",
			"insert into loadtable values (1, 'test')",
			"LOAD DATA INFILE './testdata/test2.csv' IGNORE INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from loadtable",
				Expected: []sql.Row{
					{1, "test"},
					{2, "hello"},
				},
			},
		},
	},
	{
		Name: "Load data can replace row with existing primary key",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, c1 varchar(10))",
			"insert into loadtable values (1, 'test')",
			"LOAD DATA INFILE './testdata/test2.csv' REPLACE INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from loadtable",
				Expected: []sql.Row{
					{1, "hi"},
					{2, "hello"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA with set columns no projections",
		SetUpScript: []string{
			"create table lt1(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt1 FIELDS TERMINATED BY '\t' SET i = '123'",
			"create table lt2(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt2 set i = '123', j = '456'",
			"create table lt3(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt3 set i = '123', j = '456', k = '789'",
			"create table lt4(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt4 set i = '123', i = '321'",
			"create table lt5(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt5 set j = concat(j, j)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from lt1 order by i, j, k",
				Expected: []sql.Row{
					{"123", "def", "ghi"},
					{"123", "mno", "pqr"},
				},
			},
			{
				Query: "select * from lt2 order by i, j, k",
				Expected: []sql.Row{
					{"123", "456", "ghi"},
					{"123", "456", "pqr"},
				},
			},
			{
				Query: "select * from lt3 order by i, j, k",
				Expected: []sql.Row{
					{"123", "456", "789"},
					{"123", "456", "789"},
				},
			},
			{
				Query: "select * from lt4 order by i, j, k",
				Expected: []sql.Row{
					{"321", "def", "ghi"},
					{"321", "mno", "pqr"},
				},
			},
			{
				Query: "select * from lt5 order by i, j, k",
				Expected: []sql.Row{
					{"abc", "defdef", "ghi"},
					{"jkl", "mnomno", "pqr"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA with set columns with projections",
		SetUpScript: []string{
			"create table lt1(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt1 (i, j, k) set i = '123'",
			"create table lt2(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt2 (k, i, j) set i = '123'",
			"create table lt3(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt3 (j, k) set i = '123'",
			"create table lt4(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt4 (k, i) set i = '123'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from lt1 order by i, j, k",
				Expected: []sql.Row{
					{"123", "def", "ghi"},
					{"123", "mno", "pqr"},
				},
			},
			{
				Query: "select * from lt2 order by i, j, k",
				Expected: []sql.Row{
					{"123", "ghi", "abc"},
					{"123", "pqr", "jkl"},
				},
			},
			{
				Query: "select * from lt3 order by i, j, k",
				Expected: []sql.Row{
					{"123", "abc", "def"},
					{"123", "jkl", "mno"},
				},
			},
			{
				Query: "select * from lt4 order by i, j, k",
				Expected: []sql.Row{
					{"123", nil, "abc"},
					{"123", nil, "jkl"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA assign to static User Variables",
		SetUpScript: []string{
			"set @i = '123';",
			"set @j = '456';",
			"set @k = '789';",
			"create table lt(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt set i = @i",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt set i = @i, j = @j",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt set i = @i, j = @j, k = @k",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from lt order by i, j, k",
				Expected: []sql.Row{
					{"123", "456", "789"},
					{"123", "456", "789"},
					{"123", "456", "ghi"},
					{"123", "456", "pqr"},
					{"123", "def", "ghi"},
					{"123", "mno", "pqr"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA assign to User Variables",
		SetUpScript: []string{
			"create table lt1(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt1 (@i, j, k)",
			"create table lt2(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt2 (i, @j, k)",
			"create table lt3(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt3 (i, j, @k)",
			"create table lt4(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt4 (@ii, @jj, @kk)",
			"create table lt5(i text, j text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt5 (i, j, @trash1)",
			"create table lt6(j text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt6 (@trash2, j, @trash2)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from lt1 order by i, j, k",
				Expected: []sql.Row{
					{nil, "def", "ghi"},
					{nil, "mno", "pqr"},
				},
			},
			{
				Query: "select * from lt2 order by i, j, k",
				Expected: []sql.Row{
					{"abc", nil, "ghi"},
					{"jkl", nil, "pqr"},
				},
			},
			{
				Query: "select * from lt3 order by i, j, k",
				Expected: []sql.Row{
					{"abc", "def", nil},
					{"jkl", "mno", nil},
				},
			},
			{
				Query: "select @i, @j, @k",
				Expected: []sql.Row{
					{"jkl", "mno", "pqr"},
				},
			},
			{
				Query: "select * from lt4 order by i, j, k",
				Expected: []sql.Row{
					{nil, nil, nil},
					{nil, nil, nil},
				},
			},
			{
				Query: "select @ii, @jj, @kk",
				Expected: []sql.Row{
					{"jkl", "mno", "pqr"},
				},
			},
			{
				Query: "select * from lt5 order by i, j",
				Expected: []sql.Row{
					{"abc", "def"},
					{"jkl", "mno"},
				},
			},
			{
				Query: "select @trash1",
				Expected: []sql.Row{
					{"pqr"},
				},
			},
			{
				Query: "select * from lt6 order by j",
				Expected: []sql.Row{
					{"def"},
					{"mno"},
				},
			},
			{
				Query: "select @trash2",
				Expected: []sql.Row{
					{"pqr"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA with user vars and set expressions",
		SetUpScript: []string{
			"create table lt1(i text, j text, k text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt1 (k, @j, i) set j = @j",
			"create table lt2(i text, j text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt2 (i, j, @k) set j = concat(@k, @k)",
			"create table lt3(i text, j text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt3 (i, @j, @k) set j = concat(@j, @k)",
			"create table lt4(i text, j text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt4 (i, j, @k) set j = concat(j, @k)",
			"create table lt5(i text, j text);",
			"LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt5 (@i, @j) set i = @j, j = @i",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from lt1 order by i, j, k",
				Expected: []sql.Row{
					{"ghi", "def", "abc"},
					{"pqr", "mno", "jkl"},
				},
			},
			{
				Query: "select * from lt2 order by i, j",
				Expected: []sql.Row{
					{"abc", "ghighi"},
					{"jkl", "pqrpqr"},
				},
			},
			{
				Query: "select * from lt3 order by i, j",
				Expected: []sql.Row{
					{"abc", "defghi"},
					{"jkl", "mnopqr"},
				},
			},
			{
				Query: "select * from lt4 order by i, j",
				Expected: []sql.Row{
					{"abc", "defghi"},
					{"jkl", "mnopqr"},
				},
			},
			{
				Query: "select * from lt5 order by i, j",
				Expected: []sql.Row{
					{"def", "abc"},
					{"mno", "jkl"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA with set columns errors",
		SetUpScript: []string{
			"create table lt(i text, j text, k text);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt set noti = '123'",
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "LOAD DATA with user var alias edge case",
		SetUpScript: []string{
			"create table lt(i text, `@j` text, `@@k` text);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt(@@k)",
				ExpectedErrStr: "syntax error near '@@k'",
			},
			{
				Skip:  true, // escaped column names are ok
				Query: "LOAD DATA INFILE './testdata/test9.txt' INTO TABLE lt(i, @j, `@@k`)",
				Expected: []sql.Row{
					{"abc", "def", "ghi"},
					{"jkl", "mno", "pqr"},
				},
			},
		},
	},
	{
		Name: "LOAD DATA with column data larger than 64KB",
		SetUpScript: []string{
			"create table t(id int primary key, lt longtext);",
			"load data infile './testdata/test10.txt' into table t fields terminated by ',' lines terminated by '\n';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select id, length(lt) from t order by id",
				Expected: []sql.Row{
					{1, 65535},
					{2, 100000},
					{3, 1000000},
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
				Query:       "LOAD DATA INFILE './bad/doesnotexist.txt' INTO TABLE loadtable",
				ExpectedErr: sql.ErrLoadDataCannotOpen,
			},
		},
	},
	{
		Name: "Load data with unknown columns throws an error",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, i int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (fake_col, pk, i)",
				ExpectedErr: sql.ErrUnknownColumn,
			},
			{
				Query:       "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (pk, fake_col, i)",
				ExpectedErr: sql.ErrUnknownColumn,
			},
			{
				Query:       "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (pk, i, fake_col)",
				ExpectedErr: sql.ErrUnknownColumn,
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
				ExpectedErr: sql.ErrUnexpectedSeparator,
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
				ExpectedErr: sql.ErrUnexpectedSeparator,
			},
		},
	},
	{
		Name: "Load data errors on primary key duplicate",
		SetUpScript: []string{
			"create table loadtable(pk int primary key, c1 varchar(10))",
			"insert into loadtable values (1, 'test')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "LOAD DATA INFILE './testdata/test2.csv' INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES",
				ExpectedErrStr: "duplicate primary key given: [1]",
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

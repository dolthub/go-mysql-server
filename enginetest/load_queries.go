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

package enginetest

import (
	"fmt"

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
		Name: "Load data with csv with prefix.",
		SetUpScript: []string{
			"create table loadtable(pk longtext primary key, c1 int)",
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
			"create table loadtable(pk longtext primary key, c1 int)",
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

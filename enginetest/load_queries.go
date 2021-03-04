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
)

var LoadDataScripts = []ScriptTest{
	{
		Name: "Basic load data with enclosed values.",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'", "./testdata/test1.txt"),
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
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES", "./testdata/test2.csv"),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:   "select * from loadtable",
				Expected: []sql.Row{{int8(1), "hi"}, {int8(2), "hello"}},
			},
		},
	},
	{
		Name: "Load data with csv with prefix.",
		SetUpScript: []string{
			"create table loadtable(pk longtext primary key, c1 int)",
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable FIELDS TERMINATED BY ',' LINES STARTING BY 'xxx' IGNORE 1 LINES (`pk`, `c1`)", "./testdata/test3.csv"),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"\"abc\"", int8(1)}, {"\"def\"", int8(2)}, {"\"hello\"", nil}},
			},
		},
	},
	{
		Name: "Load data into table that doesn't exist throws error.",
		Query: fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable", "./testdata/test1.txt"),
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Name: "Load data with unknown files throws an error.",
		SetUpScript: []string{
			"create table loadtable(pk longtext primary key, c1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "LOAD DATA INFILE '/x/ytx' INTO TABLE loadtable",
				RequiredErr: true, // path error
			},
		},
	},
	{
		Name: "Load and terminate have the same values.",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable FIELDS TERMINATED BY '\"' ENCLOSED BY '\"'", "./testdata/test1.txt"),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{int8(1)}, {int8(2)}, {int8(3)}, {int8(4)}},
			},
		},
		Skip: true,
	},
	{
		Name: "Loading value into different column type results in default value.",
		SetUpScript: []string{
			"create table loadtable(pk longtext, c1 int)",
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (c1)", "./testdata/test4.txt"),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{nil, 0}, {nil, 0}},
			},
		},
		Skip: true,
	},
	{
		Name: "LOAD DATA handles nulls",
		SetUpScript: []string{
			"create table loadtable(pk longtext, c1 int)",
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'", "./testdata/test4.txt"),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"hi", 1}, {"hello", nil}},
			},
		},
		Skip: true,
	},
	{
		Name: "LOAD DATA can handle a differing column order",
		SetUpScript: []string{
			"create table loadtable(pk int, c1 string) ",
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (c1, pk)", "./testdata/test4.txt"),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{1, "hi"}, {nil, "hello"}},
			},
		},
		Skip: true,
	},
}

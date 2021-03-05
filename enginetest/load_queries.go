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
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'",
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
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test2.csv' INTO TABLE loadtable FIELDS TERMINATED BY ',' IGNORE 1 LINES",
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
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test3.csv' INTO TABLE loadtable FIELDS TERMINATED BY ',' LINES STARTING BY 'xxx' IGNORE 1 LINES (`pk`, `c1`)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"\"abc\"", int8(1)}, {"\"def\"", int8(2)}, {"\"hello\"", nil}},
			},
		},
	},
	{
		Name: "Escaped values are correctly parsed.",
		SetUpScript: []string{
			"create table loadtable(pk longtext)",
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test5.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' IGNORE 1 LINES",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from loadtable",
				Expected: []sql.Row{{"hi"}, {"hello"}, {nil}, {"TryN"}, {fmt.Sprintf("%c", 26)}, {fmt.Sprintf("%c", 0)}, {"new\n"}},
			},
		},
		Skip: true,
	},
	{
		Name: "Load and terminate have the same values.",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test1.txt' INTO TABLE loadtable FIELDS TERMINATED BY '\"' ENCLOSED BY '\"'",
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
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test4.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (c1)",
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
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test4.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"'",
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
			"SET secure_file_priv='./testdata'",
			"LOAD DATA INFILE 'test4.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (c1, pk)",
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

var LoadDataErrorScripts = []ScriptTest{
	{
		Name: "Load data without secure file throws error.",
		SetUpScript: []string{
			"create table loadtable(pk longtext primary key, c1 int)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE '/x/ytx' INTO TABLE loadtable",
				ExpectedErr: sql.ErrSecureFileDirNotSet,
			},
		},
	},
	{
		Name:        "Load data into table that doesn't exist throws error.",
		Query:       "LOAD DATA INFILE 'test1.txt' INTO TABLE loadtable",
		ExpectedErr: sql.ErrTableNotFound,
	},
	{
		Name: "Load data with unknown files throws an error.",
		SetUpScript: []string{
			"create table loadtable(pk longtext primary key, c1 int)",
			"SET secure_file_priv='./testdata'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE '/x/ytx' INTO TABLE loadtable",
				RequiredErr: true, // path error
			},
		},
	},
	{
		Name: "Load data with unknown columns throws an error",
		SetUpScript: []string{
			"create table loadtable(pk int primary key)",
			"SET secure_file_priv='./testdata'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "LOAD DATA INFILE './testdata/test1.txt' INTO TABLE loadtable FIELDS ENCLOSED BY '\"' (bad)",
				ExpectedErr: plan.ErrInsertIntoNonexistentColumn,
			},
		},
	},
}

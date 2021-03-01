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

const (
	tableNameConst = "LOADTABLE"
)

var LoadDataScripts = []ScriptTest{
	{
		Name: "Super basic load data",
		SetUpScript: []string{
			fmt.Sprintf("create table %s(pk int primary key)", tableNameConst),
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s", "./testdata/test1.txt", tableNameConst),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("select * from %s", tableNameConst),
				Expected: []sql.Row{{int8(1)}, {int8(2)}, {int8(3)}, {int8(4)}},
			},
		},
	},
	{
		Name: "Load data with csv",
		SetUpScript: []string{
			fmt.Sprintf("create table %s(pk int primary key, c1 longtext)", tableNameConst),
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' IGNORE 1 LINES", "./testdata/test2.csv", tableNameConst),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("select * from %s", tableNameConst),
				Expected: []sql.Row{{int8(1), "hi"}, {int8(2), "hello"}},
			},
		},
	},
	{
		Name: "Load data with csv with prefix.",
		SetUpScript: []string{
			fmt.Sprintf("create table %s(pk longtext primary key, c1 int)", tableNameConst),
			fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' LINES STARTING BY 'xxx' IGNORE 1 LINES (`pk`, `c1`)", "./testdata/test3.csv", tableNameConst),
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("select * from %s", tableNameConst),
				Expected: []sql.Row{{"\"abc\"", int8(1)}, {"\"def\"", int8(2)}, {"\"hello\"", "NULL"}},
			},
		},
	},
	// TODO: Test partial inserts
}

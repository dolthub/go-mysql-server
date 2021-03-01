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
	"io/ioutil"

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
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    fmt.Sprintf("select * from %s", tableNameConst),
				Expected: []sql.Row{{"\"abc\"", int8(1)}, {"\"def\"", int8(2)}},
			},
		},
	},
}

// Creates a directory of files
func CreateDummyFiles(dir string) error {
	// Test Case 1. Create a simple text file with one value per line
	file1, err := ioutil.TempFile(dir, "test1.txt")
	if err != nil {
		return err
	}

	_, err = file1.WriteString("1\n2\n3\n4")
	if err != nil {
		return err
	}

	loadStatement := fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s", file1.Name(), tableNameConst)

	LoadDataScripts[0].SetUpScript = append(LoadDataScripts[0].SetUpScript, loadStatement)

	// Test Case 2. Create a simple csv file with multiple columns. Use the ignore syntax here
	file2, err := ioutil.TempFile(dir, "test2.csv")
	if err != nil {
		return err
	}

	_, err = file2.WriteString("pk,c1\n1,hi\n2,hello")
	if err != nil {
		return err
	}

	loadStatement = fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' IGNORE 1 LINES", file2.Name(), tableNameConst)

	LoadDataScripts[1].SetUpScript = append(LoadDataScripts[1].SetUpScript, loadStatement)

	// Test Case 3. CSV with a prefix and quoted characters.
	file3, err := ioutil.TempFile(dir, "test3.csv")
	if err != nil {
		return err
	}

	_, err = file3.WriteString("pk,c1\nxxx\"abc\",1\nsomething xxx\"def\",2\n\"ghi\",3")
	if err != nil {
		return err
	}

	loadStatement = fmt.Sprintf("LOAD DATA INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY ',' LINES STARTING BY 'xxx' IGNORE 1 LINES", file3.Name(), tableNameConst)
	LoadDataScripts[2].SetUpScript = append(LoadDataScripts[2].SetUpScript, loadStatement)

	return nil
}

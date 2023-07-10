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
)

var FulltextTests = []ScriptTest{
	/*{
		Name: "Basic matching",
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES ('abc', 'def pqr'), ('ghi', 'jkl'), ('mno', 'mno'), ('stu vwx', 'xyz zyx yzx');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"ghi", "jkl"}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{"ghi", "jkl"}},
			},
		},
	},*/
	{
		Name: "Basic matching", //TODO: DELETE ME, TEMPORARY INCLUSION OF THE FTS_DOC_ID
		SetUpScript: []string{
			"CREATE TABLE test (v1 VARCHAR(200), v2 VARCHAR(200), FULLTEXT idx (v1, v2));",
			"INSERT INTO test VALUES ('abc', 'def pqr', 1), ('ghi', 'jkl', 2), ('mno', 'mno', 3), ('stu vwx', 'xyz zyx yzx', 4), ('ghs', 'mno shg', 5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM test WHERE MATCH(v1, v2) AGAINST ('ghi');",
				Expected: []sql.Row{{"ghi", "jkl", uint64(2)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl');",
				Expected: []sql.Row{{"ghi", "jkl", uint64(2)}},
			},
			{
				Query:    "SELECT * FROM test WHERE MATCH(v2, v1) AGAINST ('jkl mno');",
				Expected: []sql.Row{{"ghi", "jkl", uint64(2)}, {"mno", "mno", uint64(3)}, {"ghs", "mno shg", uint64(5)}},
			},
		},
	},
}

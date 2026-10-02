// Copyright 2026 Dolthub, Inc.
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

// DatabaseDefinitionsScriptTests contains self-contained database definitions script tests.
var DatabaseDefinitionsScriptTests = []ScriptTest{
	{
		Name:    "test show create database",
		Dialect: "mysql",
		SetUpScript: []string{
			"create database def_db;",
			"create database latin1_db character set latin1;",
			"create database bin_db charset binary;",
			"create database mb3_db collate utf8mb3_general_ci;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create database def_db",
				Expected: []sql.Row{
					{"def_db", "CREATE DATABASE `def_db` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */"},
				},
			},
			{
				Query: "show create database latin1_db",
				Expected: []sql.Row{
					{"latin1_db", "CREATE DATABASE `latin1_db` /*!40100 DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci */"},
				},
			},
			{
				Query: "show create database bin_db",
				Expected: []sql.Row{
					{"bin_db", "CREATE DATABASE `bin_db` /*!40100 DEFAULT CHARACTER SET binary COLLATE binary */"},
				},
			},
			{
				Query: "show create database mb3_db",
				Expected: []sql.Row{
					{"mb3_db", "CREATE DATABASE `mb3_db` /*!40100 DEFAULT CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci */"},
				},
			},
		},
	},
	{
		Name:    "test create database with modified server variables",
		Dialect: "mysql",
		SetUpScript: []string{
			"set @@session.character_set_server = 'latin1';",
			"create database latin1_db;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select @@global.character_set_server, @@global.collation_server;",
				Expected: []sql.Row{
					{"utf8mb4", "utf8mb4_0900_bin"},
				},
			},
			{
				Query: "select @@session.character_set_server, @@session.collation_server;",
				Expected: []sql.Row{
					{"latin1", "latin1_swedish_ci"},
				},
			},
			{
				// Interestingly, session actually takes priority over global
				Query: "show create database latin1_db",
				Expected: []sql.Row{
					{"latin1_db", "CREATE DATABASE `latin1_db` /*!40100 DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci */"},
				},
			},
		},
	},
}

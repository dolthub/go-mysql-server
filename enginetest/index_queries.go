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

import "github.com/dolthub/go-mysql-server/sql"

var ComplexIndexQueries = []ScriptTest{
	{
		Name: "Two column index",
		SetUpScript: []string{
			"CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT, v2 BIGINT, INDEX (v1, v2));",
			"INSERT INTO test VALUES (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,1,7),(8,1,8),(9,1,9),(11,2,1)," +
				"(12,2,2),(13,2,3),(14,2,4),(15,2,5),(16,2,6),(17,2,7),(18,2,8),(19,2,9),(21,3,1),(22,3,2),(23,3,3)," +
				"(24,3,4),(25,3,5),(26,3,6),(27,3,7),(28,3,8),(29,3,9),(31,4,1),(32,4,2),(33,4,3),(34,4,4),(35,4,5)," +
				"(36,4,6),(37,4,7),(38,4,8),(39,4,9),(41,5,1),(42,5,2),(43,5,3),(44,5,4),(45,5,5),(46,5,6),(47,5,7)," +
				"(48,5,8),(49,5,9),(51,6,1),(52,6,2),(53,6,3),(54,6,4),(55,6,5),(56,6,6),(57,6,7),(58,6,8),(59,6,9)," +
				"(61,7,1),(62,7,2),(63,7,3),(64,7,4),(65,7,5),(66,7,6),(67,7,7),(68,7,8),(69,7,9),(71,8,1),(72,8,2)," +
				"(73,8,3),(74,8,4),(75,8,5),(76,8,6),(77,8,7),(78,8,8),(79,8,9),(81,9,1),(82,9,2),(83,9,3),(84,9,4)," +
				"(85,9,5),(86,9,6),(87,9,7),(88,9,8),(89,9,9);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM test WHERE v1 > 4 AND v2 < 7 OR v1 <= 6 AND v2 >= 6 ORDER BY 1;",
				Expected: []sql.Row{{6, 1, 6}, {7, 1, 7}, {8, 1, 8}, {9, 1, 9}, {16, 2, 6}, {17, 2, 7}, {18, 2, 8},
					{19, 2, 9}, {26, 3, 6}, {27, 3, 7}, {28, 3, 8}, {29, 3, 9}, {36, 4, 6}, {37, 4, 7}, {38, 4, 8},
					{39, 4, 9}, {41, 5, 1}, {42, 5, 2}, {43, 5, 3}, {44, 5, 4}, {45, 5, 5}, {46, 5, 6}, {47, 5, 7},
					{48, 5, 8}, {49, 5, 9}, {51, 6, 1}, {52, 6, 2}, {53, 6, 3}, {54, 6, 4}, {55, 6, 5}, {56, 6, 6},
					{57, 6, 7}, {58, 6, 8}, {59, 6, 9}, {61, 7, 1}, {62, 7, 2}, {63, 7, 3}, {64, 7, 4}, {65, 7, 5},
					{66, 7, 6}, {71, 8, 1}, {72, 8, 2}, {73, 8, 3}, {74, 8, 4}, {75, 8, 5}, {76, 8, 6}, {81, 9, 1},
					{82, 9, 2}, {83, 9, 3}, {84, 9, 4}, {85, 9, 5}, {86, 9, 6}},
			},
			//TODO: add more of these
		},
	},
	//TODO: add three columns
	//TODO: add four columns
}

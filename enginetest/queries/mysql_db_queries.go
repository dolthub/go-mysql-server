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

import "github.com/dolthub/go-mysql-server/sql"

var MySqlDbTests = []ScriptTest{
	{
		Name: "test mysql database help_ tables ",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "show create table mysql.help_topic;",
				Expected: []sql.UntypedSqlRow{{"help_topic", "CREATE TABLE `help_topic` (\n  `help_topic_id` bigint unsigned NOT NULL,\n  `name` char(64) COLLATE utf8mb3_general_ci NOT NULL,\n  `help_category_id` tinyint unsigned NOT NULL,\n  `description` text COLLATE utf8mb3_general_ci NOT NULL,\n  `example` text COLLATE utf8mb3_general_ci NOT NULL,\n  `url` text COLLATE utf8mb3_general_ci NOT NULL,\n  PRIMARY KEY (`help_topic_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin"}},
			},
			{
				Query:    "show create table mysql.help_category;",
				Expected: []sql.UntypedSqlRow{{"help_category", "CREATE TABLE `help_category` (\n  `help_category_id` tinyint unsigned NOT NULL,\n  `name` char(64) COLLATE utf8mb3_general_ci NOT NULL,\n  `parent_category_id` tinyint unsigned NOT NULL,\n  `url` text COLLATE utf8mb3_general_ci NOT NULL,\n  PRIMARY KEY (`help_category_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin"}},
			},
			{
				Query:    "show create table mysql.help_keyword;",
				Expected: []sql.UntypedSqlRow{{"help_keyword", "CREATE TABLE `help_keyword` (\n  `help_keyword_id` bigint unsigned NOT NULL,\n  `name` char(64) COLLATE utf8mb3_general_ci NOT NULL,\n  PRIMARY KEY (`help_keyword_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin"}},
			},
			{
				Query:    "show create table mysql.help_relation;",
				Expected: []sql.UntypedSqlRow{{"help_relation", "CREATE TABLE `help_relation` (\n  `help_keyword_id` bigint unsigned NOT NULL,\n  `help_topic_id` bigint unsigned NOT NULL,\n  PRIMARY KEY (`help_keyword_id`,`help_topic_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin"}},
			},
		},
	},
}

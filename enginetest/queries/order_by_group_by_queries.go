// Copyright 2022 Dolthub, Inc.
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

var OrderByGroupByScriptTests = []ScriptTest{
	{
		Name: "Basic order by/group by cases",
		SetUpScript: []string{
			"use mydb;",
			"create table members (id bigint primary key, team text);",
			"insert into members values (3,'red'), (4,'red'),(5,'orange'),(6,'orange'),(7,'orange'),(8,'purple');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY 2",
				Expected: []sql.Row{
					{"purple", int64(1)},
					{"red", int64(2)},
					{"orange", int64(3)},
				},
			},
			{
				Query: "SELECT team, COUNT(*) FROM members GROUP BY 1 ORDER BY 2",
				Expected: []sql.Row{
					{"purple", int64(1)},
					{"red", int64(2)},
					{"orange", int64(3)},
				},
			},
			{
				Query:       "SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY columndoesnotexist",
				ExpectedErr: sql.ErrColumnNotFound,
			},
		},
	},
	{
		Name: "https://github.com/dolthub/dolt/issues/3016",
		SetUpScript: []string{
			"CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT,  `username` varchar(255) NOT NULL,  PRIMARY KEY (`id`));",
			"INSERT INTO `users` (`id`,`username`) VALUES (1,'u2');",
			"INSERT INTO `users` (`id`,`username`) VALUES (2,'u3');",
			"INSERT INTO `users` (`id`,`username`) VALUES (3,'u4');",
			"CREATE TABLE `tweet` (`id` int NOT NULL AUTO_INCREMENT,  `user_id` int NOT NULL,  `content` text NOT NULL,  `timestamp` bigint NOT NULL,  PRIMARY KEY (`id`),  KEY `tweet_user_id` (`user_id`),  CONSTRAINT `0qpfesgd` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`));",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (1,1,'meow',1647463727);",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (2,1,'purr',1647463727);",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (3,2,'hiss',1647463727);",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (4,3,'woof',1647463727);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT t1.username, COUNT(t1.id) FROM ((SELECT t2.id, t2.content, t3.username FROM tweet AS t2 INNER JOIN users AS t3 ON (t2.user_id = t3.id) WHERE (t3.username = 'u3')) UNION (SELECT t4.id, t4.content, `t5`.`username` FROM `tweet` AS t4 INNER JOIN users AS t5 ON (t4.user_id = t5.id) WHERE (t5.username IN ('u2', 'u4')))) AS t1 GROUP BY `t1`.`username` ORDER BY COUNT(t1.id) DESC;",
				Expected: []sql.Row{{"u2", 2}, {"u3", 1}, {"u4", 1}},
			},
			{
				Query:    "SELECT t1.username, COUNT(t1.id) AS ct FROM ((SELECT t2.id, t2.content, t3.username FROM tweet AS t2 INNER JOIN users AS t3 ON (t2.user_id = t3.id) WHERE (t3.username = 'u3')) UNION (SELECT t4.id, t4.content, `t5`.`username` FROM `tweet` AS t4 INNER JOIN users AS t5 ON (t4.user_id = t5.id) WHERE (t5.username IN ('u2', 'u4')))) AS t1 GROUP BY `t1`.`username` ORDER BY COUNT(t1.id) DESC;",
				Expected: []sql.Row{{"u2", 2}, {"u3", 1}, {"u4", 1}},
			},
		},
	},
}

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

import (
	"github.com/dolthub/go-mysql-server/sql"
)

var JoinQueryTests = []QueryTest{
	{
		Query: `
				`,
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		// Repro for: https://github.com/dolthub/dolt/issues/4183
		Query: "SELECT mytable.i " +
			"FROM mytable " +
			"INNER JOIN othertable ON (mytable.i = othertable.i2) " +
			"LEFT JOIN othertable T4 ON (mytable.i = T4.i2) " +
			"ORDER BY othertable.i2, T4.s2",
		Expected: []sql.Row{{1}, {2}, {3}},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v2 where b.pk = 0 and a.v2 = 1;",
		Expected: []sql.Row{
			{2, 0},
			{2, 0},
			{2, 0},
			{2, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v3 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b inner join (select * from one_pk_three_idx where v2 = 0) c on b.pk = c.v3 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1+1 where b.pk = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, nil},
			{1, nil},
			{2, nil},
			{3, nil},
			{4, nil},
			{5, nil},
			{6, nil},
			{7, nil},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b RIGHT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{1, "first row", 2, "second row", 1, "first row"},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 2, "second row", 1, "first row"},
			{1, "first row", 3, "third row", 2, "second row"},
			{2, "second row", 3, "third row", 2, "second row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{nil, nil, nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b LEFT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{3, "third row", 1, "first row", nil, nil},
			{2, "second row", 1, "first row", nil, nil},
			{1, "first row", 1, "first row", nil, nil},
			{3, "third row", 2, "second row", 1, "first row"},
			{2, "second row", 2, "second row", 1, "first row"},
			{1, "first row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{2, "second row", 3, "third row", 2, "second row"},
			{1, "first row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b LEFT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{3, "third row", 1, "first row", 2, "second row"},
			{2, "second row", 1, "first row", 2, "second row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{3, "third row", 2, "second row", 3, "third row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{1, "first row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
			{2, "second row", 3, "third row", nil, nil},
			{1, "first row", 3, "third row", nil, nil},
		}},
	{
		Query: "select * from mytable a LEFT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", nil, nil},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a LEFT JOIN  mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{nil, nil, nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{nil, nil, nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", nil, nil},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a LEFT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
		}},
	{
		Query: "select * from mytable a LEFT JOIN  mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
		}},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i+1= c.i;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
		}},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b RIGHT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 1, "first row", 2, "second row"},
			{3, "third row", 1, "first row", 2, "second row"},
			{1, "first row", 2, "second row", 3, "third row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 2, "second row", 3, "third row"},
		},
	},
	{
		Query: "with a as (select a.i, a.s from mytable a CROSS JOIN mytable b) select * from a RIGHT JOIN mytable c on a.i+1 = c.i-1;",
		Expected: []sql.Row{
			{nil, nil, 1, "first row"},
			{nil, nil, 2, "second row"},
			{1, "first row", 3, "third row"},
			{1, "first row", 3, "third row"},
			{1, "first row", 3, "third row"},
		},
	},
	{
		Query: "select a.* from mytable a RIGHT JOIN mytable b on a.i = b.i+1 LEFT JOIN mytable c on a.i = c.i-1 RIGHT JOIN mytable d on b.i = d.i;",
		Expected: []sql.Row{
			{2, "second row"},
			{3, "third row"},
			{nil, nil},
		},
	},
	{
		Query: "select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 LEFT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;",
		Expected: []sql.Row{
			{2, "second row", "third", 1},
			{3, "third row", "second", 2},
			{nil, nil, "first", 3},
		},
	},
	{
		Query: "select a.*,b.* from mytable a RIGHT JOIN othertable b on a.i = b.i2+1 RIGHT JOIN mytable c on a.i = c.i-1 LEFT JOIN othertable d on b.i2 = d.i2;",
		Expected: []sql.Row{
			{nil, nil, nil, nil},
			{nil, nil, nil, nil},
			{2, "second row", "third", 1},
		},
	},
	{
		Query:    "select i.pk, j.v3 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk;",
		Expected: []sql.Row{{0, 0}, {1, 1}, {2, 0}, {3, 2}, {4, 0}, {5, 3}, {6, 0}, {7, 4}},
	},
	{
		Query:    "select i.pk, j.v3, k.c1 from one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk JOIN one_pk k on j.v3 = k.pk;",
		Expected: []sql.Row{{0, 0, 0}, {1, 1, 10}, {2, 0, 0}, {3, 2, 20}, {4, 0, 0}, {5, 3, 30}, {6, 0, 0}},
	},
	{
		Query:    "select i.pk, j.v3 from (one_pk_two_idx i JOIN one_pk_three_idx j on((i.v1 = j.pk)));",
		Expected: []sql.Row{{0, 0}, {1, 1}, {2, 0}, {3, 2}, {4, 0}, {5, 3}, {6, 0}, {7, 4}},
	},
	{
		Query:    "select i.pk, j.v3, k.c1 from ((one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk))) JOIN one_pk k on((j.v3 = k.pk)));",
		Expected: []sql.Row{{0, 0, 0}, {1, 1, 10}, {2, 0, 0}, {3, 2, 20}, {4, 0, 0}, {5, 3, 30}, {6, 0, 0}},
	},
	{
		Query:    "select i.pk, j.v3, k.c1 from (one_pk_two_idx i JOIN one_pk_three_idx j on ((i.v1 = j.pk)) JOIN one_pk k on((j.v3 = k.pk)));",
		Expected: []sql.Row{{0, 0, 0}, {1, 1, 10}, {2, 0, 0}, {3, 2, 20}, {4, 0, 0}, {5, 3, 30}, {6, 0, 0}},
	},
	{
		Query: "select a.* from one_pk_two_idx a RIGHT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.v1 = j.pk) on a.pk = i.v1 LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v1 = l.pk) on a.pk = l.v2;",
		Expected: []sql.Row{{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{1, 1, 1},
			{2, 2, 2},
			{3, 3, 3},
			{4, 4, 4},
			{5, 5, 5},
			{6, 6, 6},
			{7, 7, 7}},
	},
	{
		Query: "select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk RIGHT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;",
		Expected: []sql.Row{{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{1, 1, 1},
			{2, 2, 2},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{0, 0, 0},
			{3, 3, 3},
			{4, 4, 4},
		},
	},
	{
		Query: "select a.* from mytable a join mytable b on a.i = b.i and a.i > 2",
		Expected: []sql.Row{
			{3, "third row"},
		},
	},
	{
		Query: "select a.* from mytable a join mytable b on a.i = b.i and now() >= coalesce(NULL, NULL, now())",
		Expected: []sql.Row{
			{1, "first row"},
			{2, "second row"},
			{3, "third row"}},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and b <=> NULL",
		Expected: []sql.Row{
			{1, "first row", 1, nil, nil, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and s IS NOT NULL",
		Expected: []sql.Row{
			{1, "first row", 1, nil, nil, nil},
			{2, "second row", 2, 2, 1, nil},
			{3, "third row", 3, nil, 0, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and b IS NOT NULL",
		Expected: []sql.Row{
			{2, "second row", 2, 2, 1, nil},
			{3, "third row", 3, nil, 0, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i = b.i and b != 0",
		Expected: []sql.Row{
			{2, "second row", 2, 2, 1, nil},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i <> b.i and b != 0;",
		Expected: []sql.Row{
			{3, "third row", 2, 2, 1, nil},
			{1, "first row", 2, 2, 1, nil},
			{3, "third row", 5, nil, 1, float64(5)},
			{2, "second row", 5, nil, 1, float64(5)},
			{1, "first row", 5, nil, 1, float64(5)},
		},
	},
	{
		Query: "select * from mytable a join niltable  b on a.i <> b.i;",
		Expected: []sql.Row{
			{3, "third row", 1, nil, nil, nil},
			{2, "second row", 1, nil, nil, nil},
			{3, "third row", 2, 2, 1, nil},
			{1, "first row", 2, 2, 1, nil},
			{2, "second row", 3, nil, 0, nil},
			{1, "first row", 3, nil, 0, nil},
			{3, "third row", 5, nil, 1, float64(5)},
			{2, "second row", 5, nil, 1, float64(5)},
			{1, "first row", 5, nil, 1, float64(5)},
			{3, "third row", 4, 4, nil, float64(4)},
			{2, "second row", 4, 4, nil, float64(4)},
			{1, "first row", 4, 4, nil, float64(4)},
			{3, "third row", 6, 6, 0, float64(6)},
			{2, "second row", 6, 6, 0, float64(6)},
			{1, "first row", 6, 6, 0, float64(6)},
		},
	},
}

var SkippedJoinQueryTests = []QueryTest{
	{
		Query: "select a.* from one_pk_two_idx a LEFT JOIN (one_pk_two_idx i JOIN one_pk_three_idx j on i.pk = j.v3) on a.pk = i.pk LEFT JOIN (one_pk_two_idx k JOIN one_pk_three_idx l on k.v2 = l.v3) on a.v1 = l.v2;",
		Expected: []sql.Row{{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0},
			{0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {0, 0, 0}, {1, 1, 1}, {2, 2, 2}, {3, 3, 3}, {4, 4, 4}, {5, 5, 5}, {6, 6, 6}, {7, 7, 7},
		},
	},
	{
		Query:    "with recursive a(x,y) as (select i,i from mytable where i < 4 union select a.x, mytable.i from a join mytable on a.x+1 = mytable.i limit 2) select * from a;",
		Expected: []sql.Row{{1, 1}, {2, 2}},
	},
}

var SkippedJoinScripts = []ScriptTest{
	{
		Name: "Complex join query currently returning a planning error",
		SetUpScript: []string{
			"CREATE TABLE `tweet` ( id` int NOT NULL AUTO_INCREMENT, `user_id` int NOT NULL, `content` text NOT NULL, `timestamp` bigint NOT NULL, PRIMARY KEY (`id`), KEY `tweet_user_id` (`user_id`), CONSTRAINT `0qpfesgd` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`));",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (1,1,'meow',1647463727), (2,1,'purr',1647463727), (3,2,'hiss',1647463727), (4,3,'woof',1647463727)",
			"CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT, `username` varchar(255) NOT NULL, PRIMARY KEY (`id`));",
			"INSERT INTO `users` (`id`,`username`) VALUES (1,'huey'), (2,'zaizee'), (3,'mickey')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    " SELECT `t1`.`username`, COUNT(`t1`.`id`) AS `ct` FROM ((SELECT `t2`.`id`, `t2`.`content`, `t3`.`username` FROM `tweet` AS `t2` INNER JOIN `users` AS `t3` ON (`t2`.`user_id` = `t3`.`id`) WHERE (`t3`.`username` = 'u3')) UNION (SELECT `t4`.`id`, `t4`.`content`, `t5`.`username` FROM `tweet` AS `t4` INNER JOIN `users` AS `t5` ON (`t4`.`user_id` = `t5`.`id`) WHERE (`t5`.`username` IN ('u2', 'u4')))) AS `t1` GROUP BY `t1`.`username` ORDER BY COUNT(`t1`.`id`) DESC;",
				Expected: []sql.Row{},
			},
		},
	},
}

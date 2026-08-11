package queries

import (
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var PreparedScriptTests = []ScriptTest{
	{
		Name: "table_count optimization refreshes result",
		SetUpScript: []string{
			"create table a (a int primary key);",
			"insert into a values (0), (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "prepare cnt from 'select count(*) from a';",
				Expected: []sql.Row{{types.OkResult{Info: plan.PrepareInfo{}}}},
			},
			{
				Query:    "execute cnt",
				Expected: []sql.Row{{3}},
			},
			{
				Query: "insert into a values (3), (4)",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 2}},
				},
			},
			{
				Query:    "execute cnt",
				Expected: []sql.Row{{5}},
			},
		},
	},
	{
		Name:        "bad prepare",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:          "prepare s from 'prepare t from ?'",
				ExpectedErrStr: "syntax error at position 17 near ':v1'",
			},
			{
				Query:          "prepare s from 'a very real query'",
				ExpectedErrStr: "syntax error at position 2 near 'a'",
			},
			{
				Query:       "deallocate prepare idontexist",
				ExpectedErr: sql.ErrUnknownPreparedStatement,
			},
		},
	},
	{
		Name:        "simple select case no bindings",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "execute s",
				ExpectedErr: sql.ErrUnknownPreparedStatement,
			},
			{
				Query: "prepare s from 'select 1'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute s",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute s",
				ExpectedErr: sql.ErrUnknownPreparedStatement,
			},
		},
	},
	{
		Name: "simple select case one binding",
		SetUpScript: []string{
			"set @a = 1",
			"set @b = 100",
			"set @c = 'abc'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare s from 'select ?'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query:          "execute s",
				ExpectedErrStr: "bind variable not provided: 'v1'",
			},
			{
				Query: "execute s using @abc",
				Expected: []sql.Row{
					{nil},
				},
			},
			{
				Query:          "execute s using @a, @b, @c, @abc",
				ExpectedErrStr: "invalid arguments. expected: 1, found: 4",
			},
			{
				Query: "execute s using @a",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "execute s using @b",
				Expected: []sql.Row{
					{100},
				},
			},
			{
				Query: "execute s using @c",
				Expected: []sql.Row{
					{"abc"},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute s using @a",
				ExpectedErr: sql.ErrUnknownPreparedStatement,
			},
		},
	},
	{
		Name: "prepare with time type binding",
		SetUpScript: []string{
			"create table t (d date, dt datetime, t time, ts timestamp);",
			"set @d = date('2001-02-03');",
			"set @dt = datetime('2001-02-03 12:34:56');",
			"set @t = time('12:34:56');",
			"set @ts = timestamp('2001-02-03 12:34:56');",
			"prepare s from 'select ?';",
			"prepare sd from 'insert into t(d) values(?)';",
			"prepare sdt from 'insert into t(dt) values(?)';",
			"prepare st from 'insert into t(t) values(?)';",
			"prepare sts from 'insert into t(ts) values(?)';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "execute s using @d;",
				Expected: []sql.Row{
					{"2001-02-03"},
				},
			},
			{
				Query: "execute s using @dt;",
				Expected: []sql.Row{
					{time.Date(2001, time.February, 3, 12, 34, 56, 0, time.UTC)},
				},
			},
			{
				// types.Timespan not supported as bindvar
				Skip:  true,
				Query: "execute s using @t;",
				Expected: []sql.Row{
					{"12:34:56"},
				},
			},
			{
				Query: "execute s using @ts;",
				Expected: []sql.Row{
					{time.Date(2001, time.February, 3, 12, 34, 56, 0, time.UTC)},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute sd using @d;",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute sdt using @dt;",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				// types.Timespan not supported as bindvar
				Skip:                          true,
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute st using @t;",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute sts using @ts;",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				// TODO: should also select t when we fix that
				Query: "select d, dt, ts from t",
				Expected: []sql.Row{
					{time.Date(2001, time.February, 3, 0, 0, 0, 0, time.UTC), nil, nil},
					{nil, time.Date(2001, time.February, 3, 12, 34, 56, 0, time.UTC), nil},
					{nil, nil, time.Date(2001, time.February, 3, 12, 34, 56, 0, time.UTC)},
				},
			},
		},
	},
	{
		Name: "prepare with decimal type binding",
		SetUpScript: []string{
			"create table t (d decimal);",
			"set @d = cast(123.45 as Decimal(5,2));",
			"prepare s from 'select ?';",
			"prepare sd from 'insert into t values(?)';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Skip:  true,
				Query: "execute s using @d;",
				Expected: []sql.Row{
					{"123.45"},
				},
			},
			{
				Skip:                          true,
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute sd using @d;",
				Expected: []sql.Row{
					{"123.45"},
				},
			},
			{
				Skip:  true,
				Query: "select * from t",
				Expected: []sql.Row{
					{"123.45"},
				},
			},
		},
	},
	{
		Name: "prepare insert",
		SetUpScript: []string{
			"set @a = 123",
			"set @b = 'abc'",
			"create table t (i int, j varchar(100))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare s from 'insert into t values (?,?)'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query:          "execute s using @a",
				ExpectedErrStr: "bind variable not provided: 'v2'",
			},
			{
				// execute depends on prepare stmt for whether to use 'query' or 'exec' from go sql driver.
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute s using @a, @b",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{123, "abc"},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute s using @a",
				ExpectedErr: sql.ErrUnknownPreparedStatement,
			},
			{
				Query: "prepare s from 'insert into t values (100, \"def\")'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				// execute depends on prepare stmt for whether to use 'query' or 'exec' from go sql driver.
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute s;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{100, "def"},
					{123, "abc"},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
		},
	},
	{
		Name: "prepare update",
		SetUpScript: []string{
			"set @a = 1;",
			"set @b = 'abc';",
			"create table t (i int, j varchar(100));",
			"insert into t values (1, ''), (2, ''), (3, '');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare s from 'update t set j = ? where i = ?'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query:          "execute s using @a",
				ExpectedErrStr: "bind variable not provided: 'v2'",
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute s using @b, @a",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched: 1,
							Updated: 1,
						},
					}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{1, "abc"},
					{2, ""},
					{3, ""},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query: "prepare s from 'update t set j = \"def\" where i = 2'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				// execute depends on prepare stmt for whether to use 'query' or 'exec' from go sql driver.
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute s;",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
						Info: plan.UpdateInfo{
							Matched: 1,
							Updated: 1,
						},
					}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{1, "abc"},
					{2, "def"},
					{3, ""},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
		},
	},
	{
		Name: "prepare delete",
		SetUpScript: []string{
			"set @a = 1;",
			"create table t (i int, j varchar(100));",
			"insert into t values (1, 'abc'), (2, 'def'), (3, 'ghi');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare s from 'delete from t where i = ?'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute s using @a",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{2, "def"},
					{3, "ghi"},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query: "prepare s from 'delete from t where i = 2'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				// execute depends on prepare stmt for whether to use 'query' or 'exec' from go sql driver.
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute s;",
				Expected: []sql.Row{
					{types.OkResult{RowsAffected: 1}},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{3, "ghi"},
				},
			},
			{
				Query: "deallocate prepare s",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
		},
	},
	{
		Name:        "prepare create table",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare stmt from 'create table t (i int);' ",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute stmt",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute stmt",
				ExpectedErr: sql.ErrTableAlreadyExists,
			},
			{
				Query: "show create table t",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "prepare create index",
		SetUpScript: []string{
			"create table t (i int);",
			"insert into t values (0), (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "prepare stmt from 'create index idx on t (i)';",
				Expected: []sql.Row{
					{types.OkResult{
						Info: plan.PrepareInfo{},
					}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute stmt",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute stmt",
				ExpectedErr: sql.ErrDuplicateKey,
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int,\n" +
						"  KEY `idx` (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name:        "prepare create database",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare stmt from 'create database prepdb;' ",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute stmt",
				Expected: []sql.Row{
					{types.OkResult{
						RowsAffected: 1,
					}},
				},
			},
			{
				Query:       "execute stmt",
				ExpectedErr: sql.ErrDatabaseExists,
			},
			{
				Query: "show create database prepdb",
				Expected: []sql.Row{
					{"prepdb", "CREATE DATABASE `prepdb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin */"},
				},
			},
		},
	},
	{
		Name:        "prepare create event",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "prepare bad from 'create event e on schedule at current_timestamp do select 1';",
				ExpectedErr: sql.ErrUnsupportedPreparedStatement,
			},
		},
	},
	{
		Name:        "prepare create procedure",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "prepare bad from 'create procedure p(i int) select 1';",
				ExpectedErr: sql.ErrUnsupportedPreparedStatement,
			},
		},
	},
	{
		Name: "prepare alter table column",
		SetUpScript: []string{
			"create table t (i int);",
			"insert into t values (0), (1), (2);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare stmt from 'alter table t add column j int';",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute stmt;",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute stmt;",
				ExpectedErr: sql.ErrColumnExists,
			},
			{
				Query: "show columns from t",
				Expected: []sql.Row{
					{"i", "int", "YES", "", nil, ""},
					{"j", "int", "YES", "", nil, ""},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, nil},
				},
			},

			{
				Query: "prepare stmt from 'alter table t change column j k int';",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute stmt;",
				Expected:                      []sql.Row{{types.OkResult{}}},
			},
			{
				Query:       "execute stmt;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "show columns from t",
				Expected: []sql.Row{
					{"i", "int", "YES", "", nil, ""},
					{"k", "int", "YES", "", nil, ""},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{0, nil},
					{1, nil},
					{2, nil},
				},
			},

			{
				Query: "prepare stmt from 'alter table t drop column k';",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "execute stmt;",
				Expected:                      []sql.Row{{types.OkResult{}}},
			},
			{
				Query:       "execute stmt;",
				ExpectedErr: sql.ErrTableColumnNotFound,
			},
			{
				Query: "show columns from t",
				Expected: []sql.Row{
					{"i", "int", "YES", "", nil, ""},
				},
			},
			{
				Query: "select * from t order by i",
				Expected: []sql.Row{
					{0},
					{1},
					{2},
				},
			},
		},
	},
	{
		Name: "prepare alter table index",
		SetUpScript: []string{
			"create table t (i int, j int);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare stmt from 'alter table t add primary key (i)';",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute stmt;",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute stmt;",
				ExpectedErr: sql.ErrMultiplePrimaryKeysDefined,
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `j` int,\n" +
						"  PRIMARY KEY (`i`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
			{
				Query: "prepare stmt from 'alter table t drop primary key';",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute stmt;",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute stmt;",
				ExpectedErr: sql.ErrCantDropFieldOrKey,
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `j` int\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},

			{
				Query: "prepare stmt from 'alter table t add unique index idx (j)';",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute stmt;",
				Expected: []sql.Row{
					{types.OkResult{}},
				},
			},
			{
				Query:       "execute stmt;",
				ExpectedErr: sql.ErrDuplicateKey,
			},
			{
				Query: "show create table t;",
				Expected: []sql.Row{
					{"t", "CREATE TABLE `t` (\n" +
						"  `i` int NOT NULL,\n" +
						"  `j` int,\n" +
						"  UNIQUE KEY `idx` (`j`)\n" +
						") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "prepare using user vars",
		SetUpScript: []string{
			"create table t (i int primary key);",
			"insert into t values (0), (1), (2);",
			"set @num = 123",
			"set @bad = 'bad'",
			"set @a = 'select * from t order by i'",
			"set @b = concat('select 1',' + 1')",
			"set @c = 'select 1 from dual limit ?'",
			"set @d = 'select @num'",
		},
		Assertions: []ScriptTestAssertion{
			{
				// non-existent vars is the same as preparing with NULL
				Query:          "prepare stmt from @asdf",
				ExpectedErrStr: "syntax error at position 5 near 'NULL'",
			},
			{
				Query:          "prepare stmt from @num",
				ExpectedErrStr: "syntax error at position 4 near '123'",
			},
			{
				Query:          "prepare stmt from @bad",
				ExpectedErrStr: "syntax error at position 4 near 'bad'",
			},
			{
				Query: "prepare stmt from @a",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute stmt",
				Expected: []sql.Row{
					{0},
					{1},
					{2},
				},
			},
			{
				Query: "prepare stmt from @b",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute stmt",
				Expected: []sql.Row{
					{2},
				},
			},
			{
				Query: "prepare stmt from @c",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute stmt using @num",
				Expected: []sql.Row{
					{1},
				},
			},
			{
				Query: "prepare stmt from @d",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query: "execute stmt",
				Expected: []sql.Row{
					{123},
				},
			},
		},
	},
	{
		Name: "Complex join query with foreign key constraints",
		SetUpScript: []string{
			"CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT, `username` varchar(255) NOT NULL, PRIMARY KEY (`id`));",
			"CREATE TABLE `tweet` ( `id` int NOT NULL AUTO_INCREMENT, `user_id` int NOT NULL, `content` text NOT NULL, `timestamp` bigint NOT NULL, PRIMARY KEY (`id`), KEY `tweet_user_id` (`user_id`), CONSTRAINT `0qpfesgd` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`));",
			"INSERT INTO `users` (`id`,`username`) VALUES (1,'huey'), (2,'zaizee'), (3,'mickey');",
			"INSERT INTO `tweet` (`id`,`user_id`,`content`,`timestamp`) VALUES (1,1,'meow',1647463727), (2,1,'purr',1647463727), (3,2,'hiss',1647463727), (4,3,'woof',1647463727);",
			"set @u2 = 'u2';",
			"set @u3 = 'u3';",
			"set @u4 = 'u4';",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "prepare s from 'SELECT `t1`.`username`, COUNT(`t1`.`id`) AS `ct` FROM ((SELECT `t2`.`id`, `t2`.`content`, `t3`.`username` FROM `tweet` AS `t2` INNER JOIN `users` AS `t3` ON (`t2`.`user_id` = `t3`.`id`) WHERE (`t3`.`username` = ?)) UNION (SELECT `t4`.`id`, `t4`.`content`, `t5`.`username` FROM `tweet` AS `t4` INNER JOIN `users` AS `t5` ON (`t4`.`user_id` = `t5`.`id`) WHERE (`t5`.`username` IN (?, ?)))) AS `t1` GROUP BY `t1`.`username` ORDER BY COUNT(`t1`.`id`) DESC'",
				Expected: []sql.Row{
					{types.OkResult{Info: plan.PrepareInfo{}}},
				},
			},
			{
				Query:    "execute s using @u3, @u2, @u4",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// https://github.com/dolthub/dolthub-issues/issues/489
		Name: "Large character data",
		SetUpScript: []string{
			"CREATE TABLE `test` (`id` int NOT NULL AUTO_INCREMENT, `data` blob NOT NULL, PRIMARY KEY (`id`))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `INSERT INTO test (data) values (?)`,
				Bindings: map[string]sqlparser.Expr{
					// Vitess chooses VARBINARY as the bindvar type if the client sends CHAR data
					// If we change how Vitess interprets client bindvar types, we should update this test
					// Or better yet: have a test harness that uses the server directly
					"v1": sqlparser.NewStrVal([]byte(
						"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz" +
							"")),
				},
				Expected: []sql.Row{{types.OkResult{
					RowsAffected: 1,
					InsertID:     1,
				}}},
			},
		},
	},
}

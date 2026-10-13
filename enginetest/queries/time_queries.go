// Copyright 2020-2025 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var TimeQueryTests = []ScriptTest{
	{
		// time zone tests the current time set as July 23, 2025 at 9:43:21am America/Phoenix (-7:00) (does not observe
		// daylight savings time so time zone does not change)
		Name:        "time zone tests",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "set time_zone='UTC'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 16, 43, 21, 0, time.UTC)}},
			},
			{
				Query:    "set time_zone='-5:00'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 11, 43, 21, 0, time.UTC)}},
			},
			{
				// doesn't observe daylight savings time
				Query:    "set time_zone='Pacific/Honolulu'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 6, 43, 21, 0, time.UTC)}},
			},
			{
				Query:       "set time_zone='invalid time zone'",
				ExpectedErr: sql.ErrInvalidTimeZone,
			},
		},
	},
	{
		Name: "set time zone from table value",
		SetUpScript: []string{
			"create table timezones(pk int primary key, tz varchar(20))",
			"insert into timezones values (1, 'invalid time zone'), (2, '-5:00')",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:       "set time_zone=(select tz from timezones where pk = 1)",
				ExpectedErr: sql.ErrInvalidTimeZone,
			},
			{
				Query:    "set time_zone=(select tz from timezones where pk = 2)",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select now()",
				Expected: []sql.Row{{time.Date(2025, time.July, 23, 11, 43, 21, 0, time.UTC)}},
			},
		},
	},
	{
		Name:        "set timezone to SYSTEM",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select @@time_zone",
				Expected: []sql.Row{{"SYSTEM"}},
			},
			{
				Query:    "set @old_time_zone=@@time_zone",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "set @@time_zone=@old_time_zone",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
		},
	},
}

// TemporalScriptTests contains self-contained temporal script tests.
var TemporalScriptTests = []ScriptTest{
	{
		Name:    "unix_timestamp function usage",
		Dialect: "mysql",
		SetUpScript: []string{
			// NOTE: session time zone needs to be set as UNIX_TIMESTAMP function depends on it and converts the final result
			"SET @@SESSION.time_zone = 'UTC';",
			"CREATE TABLE `datetime_table` (   `i` bigint NOT NULL,   `date_col` date,   `datetime_col` datetime,   `timestamp_col` timestamp,   `time_col` time(6),   PRIMARY KEY (`i`) )",
			`insert into datetime_table values
    (1, '2019-12-31T12:00:00Z', '2020-01-01T12:00:00Z', '2020-01-02T12:00:00Z', '03:10:0'),
    (2, '2020-01-03T12:00:00Z', '2020-01-04T12:00:00Z', '2020-01-05T12:00:00Z', '04:00:44'),
    (3, '2020-01-07T00:00:00Z', '2020-01-07T12:00:00Z', '2020-01-07T12:00:01Z', '15:00:00.005000')`,
			`create index datetime_table_d on datetime_table (date_col)`,
			`create index datetime_table_dt on datetime_table (datetime_col)`,
			`create index datetime_table_ts on datetime_table (timestamp_col)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT unix_timestamp(timestamp_col) div 60 * 60 as timestamp_col, avg(i) from datetime_table group by 1 order by unix_timestamp(timestamp_col) div 60 * 60",
				Expected: []sql.Row{
					{int64(1577966400), 1.0},
					{int64(1578225600), 2.0},
					{int64(1578398400), 3.0}},
			},
		},
	},
	{
		Name:    "from_unixtime",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			// null parameter
			{
				Query:    "select from_unixtime(null)",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "select from_unixtime(1, null)",
				Expected: []sql.Row{{nil}},
			},
			// out of range
			{
				Query:    "select from_unixtime(-1)",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:    "select from_unixtime(32536771200)",
				Expected: []sql.Row{{nil}},
			},
			// in +8:00
			{
				Query:    "set @@session.time_zone='+08:00'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "select from_unixtime(1)",
				Expected: []sql.Row{{time.Unix(1, 0).Add(time.Hour * 8).In(time.UTC)}},
			},
			{
				Query:    "select from_unixtime(32536771199)",
				Expected: []sql.Row{{time.Unix(32536771199, 0).Add(time.Hour * 8).In(time.UTC)}},
			},
			{
				Query:    "SELECT FROM_UNIXTIME(1,'%Y %D %M %H:%i:%s %x')",
				Expected: []sql.Row{{"1970 1st January 08:00:01 1970"}},
			},
			// in utc
			{
				Query:    "set @@session.time_zone='UTC'",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// https://github.com/dolthub/dolt/issues/10534
				Query:    "SELECT CASE WHEN @@session.time_zone = 'SYSTEM' THEN @@system_time_zone ELSE @@session.time_zone END;",
				Expected: []sql.Row{{"UTC"}},
			},
			{
				Query:    "select from_unixtime(1)",
				Expected: []sql.Row{{time.Unix(1, 0).In(time.UTC)}},
			},
			{
				Query:    "select from_unixtime(32536771199)",
				Expected: []sql.Row{{time.Unix(32536771199, 0).In(time.UTC)}},
			},
			{
				Query:    "SELECT FROM_UNIXTIME(1,'%Y %D %M %H:%i:%s %x')",
				Expected: []sql.Row{{"1970 1st January 00:00:01 1970"}},
			},
		},
	},
	{
		Name:    "unix_timestamp with non UTC timezone",
		Dialect: "mysql",
		SetUpScript: []string{
			"SET @@SESSION.time_zone = 'UTC';",
			"CREATE TABLE `datetime_table` (   `i` bigint NOT NULL,   `datetime_col` datetime,   `timestamp_col` timestamp,   PRIMARY KEY (`i`) )",
			"insert into datetime_table(i,datetime_col,timestamp_col)values(1, '1970-01-02 00:00:00', '1970-01-02 00:00:00')",
			"SET @@SESSION.time_zone = 'Asia/Shanghai';", // +8:00
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT unix_timestamp(timestamp_col), unix_timestamp(datetime_col) from datetime_table",
				Expected: []sql.Row{
					{"86400", "57600"},
				},
			},
		},
	},
	{
		Name:    "failed conversion shows warning",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "SELECT CONVERT('10000-12-31 23:59:59', DATETIME)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect datetime value: '10000-12-31 23:59:59'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CONVERT('this is not a datetime', DATETIME)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect datetime value: 'this is not a datetime'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CAST('this is not a datetime' as DATETIME)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect datetime value: 'this is not a datetime'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CONVERT('this is not a date', DATE)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect date value: 'this is not a date'",
				SkipResultsCheck:                true,
			},
			{
				Query:                           "SELECT CAST('this is not a date' as DATE)",
				ExpectedWarning:                 1292,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "Incorrect date value: 'this is not a date'",
				SkipResultsCheck:                true,
			},
		},
	},
	{
		Name:    "year type behavior",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (pk int primary key, col1 year);",
		},
		Assertions: []ScriptTestAssertion{
			// 1901 - 2155 are interpreted as 1901 - 2155
			{
				Query:    "INSERT INTO t VALUES (1, '1901'), (2, 1901);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO t VALUES (3, '2000'), (4, 2000);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO t VALUES (5, '2155'), (6, 2155);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			// 1 - 69 are interpreted as 2001 - 2069
			{
				Query:    "INSERT INTO t VALUES (7, '1'), (8, 1);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO t VALUES (9, '35'), (10, 35);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO t VALUES (11, '69'), (12, 69);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			// 70 - 99 are interpreted as 1970 - 1999
			{
				Query:    "INSERT INTO t VALUES (13, '70'), (14, 70);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO t VALUES (15, '85'), (16, 85);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			{
				Query:    "INSERT INTO t VALUES (17, '99'), (18, 99);",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			// '0', and '00' are interpreted as 2000
			{
				Query:    "INSERT INTO t VALUES (19, '0'), (20, '00');",
				Expected: []sql.Row{{types.NewOkResult(2)}},
			},
			// 0 is interpreted as 0000
			{
				Query:    "INSERT INTO t VALUES (21, 0)",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			// Assert that returned values are correct.
			{
				Query: "SELECT * from t order by pk;",
				Expected: []sql.Row{
					{1, int16(1901)},
					{2, int16(1901)},
					{3, int16(2000)},
					{4, int16(2000)},
					{5, int16(2155)},
					{6, int16(2155)},
					{7, int16(2001)},
					{8, int16(2001)},
					{9, int16(2035)},
					{10, int16(2035)},
					{11, int16(2069)},
					{12, int16(2069)},
					{13, int16(1970)},
					{14, int16(1970)},
					{15, int16(1985)},
					{16, int16(1985)},
					{17, int16(1999)},
					{18, int16(1999)},
					{19, int16(2000)},
					{20, int16(2000)},
					{21, int16(0)},
				},
			},
		},
	},
	{
		Name:    "timezone default settings",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				// TODO: Skipping this test while we figure out why this change causes the mysql java
				// connector integration test to fail.
				Skip: true,
				// To match MySQL's behavior, this comes from the operating system's timezone setting
				// TODO: the "global" shouldn't be necessary here, but GMS goes to session without it
				Query:    `select @@global.system_time_zone;`,
				Expected: []sql.Row{{sql.SystemTimezoneOffset()}},
			},
			{
				// The default time_zone setting for MySQL is SYSTEM, which means timezone comes from @@system_time_zone
				Query:    `select @@time_zone;`,
				Expected: []sql.Row{{"SYSTEM"}},
			},
		},
	},
	{
		Name:    "current time functions",
		Dialect: "mysql",
		Assertions: []ScriptTestAssertion{
			{
				// Smoke test that NOW() and UTC_TIMESTAMP() return non-null values with the SYSTEM time zone
				Query:    `select @@time_zone, NOW() IS NOT NULL, UTC_TIMESTAMP() IS NOT NULL;`,
				Expected: []sql.Row{{"SYSTEM", true, true}},
			},
			{
				// CURTIME() returns the same time as NOW() with the SYSTEM timezone
				// TODO: TIME(NOW()) would be simpler test logic, but doesn't work correctly here.
				Query:    `select @@time_zone, NOW() LIKE CONCAT('%', CURTIME(), '%');`,
				Expected: []sql.Row{{"SYSTEM", true}},
			},
			{
				// Set the timezone set to UTC as an offset
				Query:    `set @@time_zone='+00:00';`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// When the session's time zone is set to UTC, NOW() and UTC_TIMESTAMP() should return the same value
				Query:    `select @@time_zone, NOW(6) = UTC_TIMESTAMP();`,
				Expected: []sql.Row{{"+00:00", true}},
			},
			{
				// CURTIME() returns the same time as NOW() with UTC's timezone offset
				Query:    `select @@time_zone, NOW() LIKE CONCAT('%', CURTIME(), '%');`,
				Expected: []sql.Row{{"+00:00", true}},
			},
			{
				Query:    `set @@time_zone='+02:00';`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// When the session's time zone is set to +2:00, NOW() should report two hours ahead of UTC_TIMESTAMP()
				Query:    `select @@time_zone, TIMESTAMPDIFF(MINUTE, NOW(6), UTC_TIMESTAMP());`,
				Expected: []sql.Row{{"+02:00", -120}},
			},
			{
				// CURTIME() returns the same time as NOW() with a +2:00 timezone offset
				Query:    `select @@time_zone, NOW() LIKE CONCAT('%', CURTIME(), '%');`,
				Expected: []sql.Row{{"+02:00", true}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11512
		Name:    "HOUR preserves extended TIME column values",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE hour_time_values (v TIME)",
			"INSERT INTO hour_time_values VALUES ('02:00:00'), ('13:04:05'), ('25:00:00'), (NULL)",
		},
		Query:    "SELECT HOUR(v) FROM hour_time_values WHERE HOUR(v) >= 13 ORDER BY HOUR(v) DESC",
		Expected: []sql.Row{{int32(25)}, {int32(13)}},
	},
	{
		Name:    "timestamp timezone conversion",
		Dialect: "mysql",
		SetUpScript: []string{
			"set time_zone='+00:00';",
			"create table timezonetest(pk int primary key, dt datetime, ts timestamp);",
			"insert into timezonetest values(1, '2020-02-14 12:00:00', '2020-02-14 12:00:00');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// When reading back the datetime and timestamp values in the same time zone we entered them,
				// we should get the exact same results back.
				Query: `select * from timezonetest;`,
				Expected: []sql.Row{{1,
					time.Date(2020, time.February, 14, 12, 0, 0, 0, time.UTC),
					time.Date(2020, time.February, 14, 12, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    `set @@time_zone='-08:00';`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// TODO: Unskip after adding support for converting timestamp values to/from session time_zone
				Skip: true,
				// After changing the session's time zone, we should get back a different result for the timestamp
				// column, but the same result for the datetime column.
				Query: `select * from timezonetest;`,
				Expected: []sql.Row{{1,
					time.Date(2020, time.February, 14, 12, 0, 0, 0, time.UTC),
					time.Date(2020, time.February, 14, 4, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    `set @@time_zone='+5:00';`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// Test with explicit timezone in datetime literal
				Query:    `insert into timezonetest values(3, '2020-02-16 12:00:00 +0800 CST', '2020-02-16 12:00:00 +0800 CST');`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				// TODO: Unskip after adding support for converting timestamp values to/from session time_zone
				Skip:  true,
				Query: `select * from timezonetest;`,
				Expected: []sql.Row{
					{1, time.Date(2020, time.February, 14, 12, 0, 0, 0, time.UTC),
						time.Date(2020, time.February, 14, 17, 0, 0, 0, time.UTC)},
					{3, time.Date(2020, time.February, 16, 9, 0, 0, 0, time.UTC),
						time.Date(2020, time.February, 16, 9, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    `set @@time_zone='+0:00';`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				// TODO: Unskip after adding support for converting timestamp values to/from session time_zone
				Skip:  true,
				Query: `select * from timezonetest;`,
				Expected: []sql.Row{
					{1, time.Date(2020, time.February, 14, 12, 0, 0, 0, time.UTC),
						time.Date(2020, time.February, 14, 12, 0, 0, 0, time.UTC)},
					{3, time.Date(2020, time.February, 16, 9, 0, 0, 0, time.UTC),
						time.Date(2020, time.February, 16, 4, 0, 0, 0, time.UTC)}},
			},
		},
	},
	{
		Dialect: "mysql",
		Name:    "UNIX_TIMESTAMP function usage with session different time zones",
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SET time_zone = '+07:00';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT UNIX_TIMESTAMP('2023-09-25 07:02:57');",
				Expected: []sql.Row{{1695600177}},
			},
			{
				Query:    "SELECT UNIX_TIMESTAMP(CONVERT_TZ('2023-09-25 07:02:57', '+00:00', @@session.time_zone));",
				Expected: []sql.Row{{"1695625377.000000"}},
			},
			{
				Query:    "SET time_zone = '+00:00';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT UNIX_TIMESTAMP('2023-09-25 07:02:57');",
				Expected: []sql.Row{{1695625377}},
			},
			{
				Query:    "SELECT UNIX_TIMESTAMP((SELECT '2023-01-01 12:34:56.789'));",
				Expected: []sql.Row{{"1672576496.789000"}},
			},
			{
				Query:    "SET time_zone = '-06:00';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT UNIX_TIMESTAMP('2023-09-25 07:02:57');",
				Expected: []sql.Row{{1695646977}},
			},
		},
	},
	{
		Dialect: "mysql",
		Name:    "UNIX_TIMESTAMP function preserves trailing 0s",
		SetUpScript: []string{
			"SET time_zone = '+07:00';",
			"create table dt (dt0 datetime(0), dt1 datetime(1), dt2 datetime(2), dt3 datetime(3), dt4 datetime(4), dt5 datetime(5), dt6 datetime(6));",
			"insert into dt values ('2020-01-02 12:34:56.123456', '2020-01-02 12:34:56.123456', '2020-01-02 12:34:56.123456', '2020-01-02 12:34:56.123456', '2020-01-02 12:34:56.123456', '2020-01-02 12:34:56.123456', '2020-01-02 12:34:56.123456')",
			"create table t (d date, tt time(6));",
			"insert into t values ('2020-01-02 12:34:56.123456', '12:34:56.123456');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select unix_timestamp('2001-02-03 12:34:56.10');",
				Expected: []sql.Row{
					{"981178496.10"},
				},
			},
			{
				Query: "select unix_timestamp('2001-02-03 12:34:56.000000');",
				Expected: []sql.Row{
					{"981178496.000000"},
				},
			},
			{
				Query: "select unix_timestamp('2001-02-03 12:34:56.1234567');",
				Expected: []sql.Row{
					{"981178496.123457"},
				},
			},
			{
				Query: "select unix_timestamp(dt0), unix_timestamp(dt1), unix_timestamp(dt2), unix_timestamp(dt3), unix_timestamp(dt4), unix_timestamp(dt5), unix_timestamp(dt6) from dt;",
				Expected: []sql.Row{
					{"1577943296", "1577943296.1", "1577943296.12", "1577943296.123", "1577943296.1235", "1577943296.12346", "1577943296.123456"},
				},
			},
			{
				Query: "select unix_timestamp(d), substring(cast(unix_timestamp(tt) as char(128)), -6) from t;",
				Expected: []sql.Row{
					{"1577898000", "123456"},
				},
			},
		},
	},
	{
		Name:    "unix_timestamp script tests",
		Dialect: "mysql",
		SetUpScript: []string{
			"set time_zone = 'UTC';",
			"create table t1 (i int primary key, v varchar(100));",
			"insert into t1 values (0, '2000-01-01 12:34:56');",
			"insert into t1 values (1, '2000-01-01 12:34:56.1');",
			"insert into t1 values (2, '2000-01-01 12:34:56.12');",
			"insert into t1 values (3, '2000-01-01 12:34:56.123');",
			"insert into t1 values (4, '2000-01-01 12:34:56.1234');",
			"insert into t1 values (5, '2000-01-01 12:34:56.12345');",
			"insert into t1 values (6, '2000-01-01 12:34:56.123456');",
		},
		Assertions: []ScriptTestAssertion{
			{
				// TODO: server engine is not respecting timezone
				SkipResultCheckOnServerEngine: true,
				Query:                         "select i, unix_timestamp(v) from t1",
				Expected: []sql.Row{
					{0, "946730096.000000"},
					{1, "946730096.100000"},
					{2, "946730096.120000"},
					{3, "946730096.123000"},
					{4, "946730096.123400"},
					{5, "946730096.123450"},
					{6, "946730096.123456"},
				},
			},
		},
	},
	{
		// PostgreSQL has no DATETIME type or SHOW WARNINGS.
		Dialect:     "mysql",
		Name:        "delimited datetime strings with trailing delimiters and zero-padded time portions",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select cast('2012-12-12 12:' as datetime);",
				Expected: []sql.Row{{time.Date(2012, time.December, 12, 12, 0, 0, 0, time.UTC)}},
			},
			{
				Query:    "show warnings;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select cast('2012-12-12 12:12:' as datetime);",
				Expected: []sql.Row{{time.Date(2012, time.December, 12, 12, 12, 0, 0, time.UTC)}},
			},
			{
				Query:    "show warnings;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select cast('2012-12-12 12:12:0012' as datetime);",
				Expected: []sql.Row{{time.Date(2012, time.December, 12, 12, 12, 12, 0, time.UTC)}},
			},
			{
				Query:    "show warnings;",
				Expected: []sql.Row{},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/10088
		Name:    "datetime with zero date and non-zero times",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (i int primary key, d datetime(6));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into t values (0, '0000-00-00 12:34:56');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t values (1, '0000-00-00 00:00:00.123456');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "insert into t values (2, '0000-00-00 12:34:56.123456');",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from t;",
				Expected: []sql.Row{
					{0, time.Date(0, 0, 0, 12, 34, 56, 0, time.UTC)},
					{1, time.Date(0, 0, 0, 0, 0, 0, 123456000, time.UTC)},
					{2, time.Date(0, 0, 0, 12, 34, 56, 123456000, time.UTC)},
				},
			},
		},
	},

	// Time Tests
	{
		Dialect: "mysql",
		Name:    "time with precision",
		SetUpScript: []string{
			"create table tbl (t0 time(0), t1 time(1), t2 time(2), t3 time(3), t4 time(4), t5 time(5), t6 time(6));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "insert into tbl values(" +
					"'12:34:56.123456', " +
					"'12:34:56.123456', " +
					"'12:34:56.123456', " +
					"'12:34:56.123456', " +
					"'12:34:56.123456', " +
					"'12:34:56.123456', " +
					"'12:34:56.123456'" +
					")",
				Expected: []sql.Row{
					{types.NewOkResult(1)},
				},
			},
			{
				Query: "select * from tbl;",
				Expected: []sql.Row{
					{
						types.Timespan(45296_000000),
						types.Timespan(45296_100000),
						types.Timespan(45296_120000),
						types.Timespan(45296_123000),
						types.Timespan(45296_123500),
						types.Timespan(45296_123460),
						types.Timespan(45296_123456),
					},
				},
			},
		},
	},
}

var BrokenTemporalScriptTests = []ScriptTest{
	{
		Name: "TIMESTAMP type value should be converted from session TZ to UTC TZ to be stored",
		SetUpScript: []string{
			"CREATE TABLE timezone_test (ts TIMESTAMP, dt DATETIME)",
			"INSERT INTO timezone_test VALUES ('2023-02-14 08:47', '2023-02-14 08:47');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SET SESSION time_zone = '-05:00';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "SELECT DATE_FORMAT(ts, '%H:%i:%s'), DATE_FORMAT(dt, '%H:%i:%s') from timezone_test;",
				Expected: []sql.Row{{"11:47:00", "08:47:00"}},
			},
			{
				Query:    "SELECT UNIX_TIMESTAMP(ts), UNIX_TIMESTAMP(dt) from timezone_test;",
				Expected: []sql.Row{{float64(1676393220), float64(1676382420)}},
			},
		},
	},
}

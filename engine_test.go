package sqle_test

import (
	"context"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sqle "gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index/pilosa"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
	"gopkg.in/src-d/go-mysql-server.v0/test"

	"github.com/stretchr/testify/require"
)

var queries = []struct {
	query    string
	expected []sql.Row
}{
	{
		"SELECT i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i = 2;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i DESC;",
		[]sql.Row{{int64(3)}, {int64(2)}, {int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC;",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 1;",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT COUNT(*) FROM mytable;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT COUNT(*) FROM mytable LIMIT 1;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT COUNT(*) AS c FROM mytable;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT substring(s, 2, 3) FROM mytable",
		[]sql.Row{{"irs"}, {"eco"}, {"hir"}},
	},
	{
		`SELECT substring("foo", 2, 2)`,
		[]sql.Row{{"oo"}},
	},
	{
		`SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', 2)`,
		[]sql.Row{
			{"a.b"},
		},
	},
	{
		`SELECT SUBSTRING_INDEX('a.b.c.d.e.f', '.', -2)`,
		[]sql.Row{
			{"e.f"},
		},
	},
	{
		`SELECT SUBSTRING_INDEX(SUBSTRING_INDEX('source{d}', '{d}', 1), 'r', -1)`,
		[]sql.Row{
			{"ce"},
		},
	},
	{
		`SELECT SUBSTRING_INDEX(mytable.s, "d", 1) as s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY 1 HAVING s = 'secon'`,
		[]sql.Row{{"secon"}},
	},
	{
		"SELECT YEAR('2007-12-11') FROM mytable",
		[]sql.Row{{int32(2007)}, {int32(2007)}, {int32(2007)}},
	},
	{
		"SELECT MONTH('2007-12-11') FROM mytable",
		[]sql.Row{{int32(12)}, {int32(12)}, {int32(12)}},
	},
	{
		"SELECT DAY('2007-12-11') FROM mytable",
		[]sql.Row{{int32(11)}, {int32(11)}, {int32(11)}},
	},
	{
		"SELECT HOUR('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(20)}, {int32(20)}, {int32(20)}},
	},
	{
		"SELECT MINUTE('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(21)}, {int32(21)}, {int32(21)}},
	},
	{
		"SELECT SECOND('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		"SELECT DAYOFYEAR('2007-12-11 20:21:22') FROM mytable",
		[]sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		"SELECT SECOND('2007-12-11T20:21:22Z') FROM mytable",
		[]sql.Row{{int32(22)}, {int32(22)}, {int32(22)}},
	},
	{
		"SELECT DAYOFYEAR('2007-12-11') FROM mytable",
		[]sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		"SELECT DAYOFYEAR('20071211') FROM mytable",
		[]sql.Row{{int32(345)}, {int32(345)}, {int32(345)}},
	},
	{
		"SELECT YEARWEEK('0000-01-01')",
		[]sql.Row{{int32(1)}},
	},
	{
		"SELECT YEARWEEK('9999-12-31')",
		[]sql.Row{{int32(999952)}},
	},
	{
		"SELECT YEARWEEK('2008-02-20', 1)",
		[]sql.Row{{int32(200808)}},
	},
	{
		"SELECT YEARWEEK('1987-01-01')",
		[]sql.Row{{int32(198652)}},
	},
	{
		"SELECT YEARWEEK('1987-01-01', 20), YEARWEEK('1987-01-01', 1), YEARWEEK('1987-01-01', 2), YEARWEEK('1987-01-01', 3), YEARWEEK('1987-01-01', 4), YEARWEEK('1987-01-01', 5), YEARWEEK('1987-01-01', 6), YEARWEEK('1987-01-01', 7)",
		[]sql.Row{{int32(198653), int32(198701), int32(198652), int32(198701), int32(198653), int32(198652), int32(198653), int32(198652)}},
	},
	{
		"SELECT i FROM mytable WHERE i BETWEEN 1 AND 2",
		[]sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i NOT BETWEEN 1 AND 2",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT substring(mytable.s, 1, 5) as s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1",
		[]sql.Row{
			{"third"},
			{"secon"},
			{"first"},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	{
		"SELECT substring(s2, 1), substring(s2, 2), substring(s2, 3) FROM othertable ORDER BY i2",
		[]sql.Row{
			{"third", "hird", "ird"},
			{"second", "econd", "cond"},
			{"first", "irst", "rst"},
		},
	},
	{
		`SELECT substring("first", 1), substring("second", 2), substring("third", 3)`,
		[]sql.Row{
			{"first", "econd", "ird"},
		},
	},
	{
		"SELECT substring(s2, -1), substring(s2, -2), substring(s2, -3) FROM othertable ORDER BY i2",
		[]sql.Row{
			{"d", "rd", "ird"},
			{"d", "nd", "ond"},
			{"t", "st", "rst"},
		},
	},
	{
		`SELECT substring("first", -1), substring("second", -2), substring("third", -3)`,
		[]sql.Row{
			{"t", "nd", "ird"},
		},
	},
	{
		"SELECT s FROM mytable INNER JOIN othertable " +
			"ON substring(s2, 1, 2) != '' AND i = i2",
		[]sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		`SELECT COUNT(*) as cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT fi, COUNT(*) FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC`,
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(1)},
			{"third row", int64(1)},
		},
	},
	{
		`SELECT COUNT(*), fi  FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi
		ORDER BY COUNT(*) ASC`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) as cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY 2`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) as cnt, s as fi FROM mytable GROUP BY fi`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) as cnt, s as fi FROM mytable GROUP BY 2`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		"SELECT CAST(-3 AS UNSIGNED) FROM mytable",
		[]sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		"SELECT CONVERT(-3, UNSIGNED) FROM mytable",
		[]sql.Row{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	},
	{
		"SELECT '3' > 2 FROM tabletest",
		[]sql.Row{
			{true},
			{true},
			{true},
		},
	},
	{
		"SELECT s > 2 FROM tabletest",
		[]sql.Row{
			{false},
			{false},
			{false},
		},
	},
	{
		"SELECT * FROM tabletest WHERE s > 0",
		nil,
	},
	{
		"SELECT * FROM tabletest WHERE s = 0",
		[]sql.Row{
			{int64(1), "first row"},
			{int64(2), "second row"},
			{int64(3), "third row"},
		},
	},
	{
		"SELECT * FROM tabletest WHERE s = 'first row'",
		[]sql.Row{
			{int64(1), "first row"},
		},
	},
	{
		"SELECT s FROM mytable WHERE i IN (1, 2, 5)",
		[]sql.Row{
			{"first row"},
			{"second row"},
		},
	},
	{
		"SELECT s FROM mytable WHERE i NOT IN (1, 2, 5)",
		[]sql.Row{
			{"third row"},
		},
	},
	{
		"SELECT 1 + 2",
		[]sql.Row{
			{int64(3)},
		},
	},
	{
		`SELECT i AS foo FROM mytable WHERE foo NOT IN (1, 2, 5)`,
		[]sql.Row{{int64(3)}},
	},
	{
		`SELECT * FROM tabletest, mytable mt INNER JOIN othertable ot ON mt.i = ot.i2`,
		[]sql.Row{
			{int64(1), "first row", int64(1), "first row", "third", int64(1)},
			{int64(1), "first row", int64(2), "second row", "second", int64(2)},
			{int64(1), "first row", int64(3), "third row", "first", int64(3)},
			{int64(2), "second row", int64(1), "first row", "third", int64(1)},
			{int64(2), "second row", int64(2), "second row", "second", int64(2)},
			{int64(2), "second row", int64(3), "third row", "first", int64(3)},
			{int64(3), "third row", int64(1), "first row", "third", int64(1)},
			{int64(3), "third row", int64(2), "second row", "second", int64(2)},
			{int64(3), "third row", int64(3), "third row", "first", int64(3)},
		},
	},
	{
		`SELECT split(s," ") FROM mytable`,
		[]sql.Row{
			sql.NewRow([]interface{}{"first", "row"}),
			sql.NewRow([]interface{}{"second", "row"}),
			sql.NewRow([]interface{}{"third", "row"}),
		},
	},
	{
		`SELECT split(s,"s") FROM mytable`,
		[]sql.Row{
			sql.NewRow([]interface{}{"fir", "t row"}),
			sql.NewRow([]interface{}{"", "econd row"}),
			sql.NewRow([]interface{}{"third row"}),
		},
	},
	{
		`SELECT SUM(i) FROM mytable`,
		[]sql.Row{{float64(6)}},
	},
	{
		`SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2`,
		[]sql.Row{
			{int64(3), "third row", "first", int64(3)},
		},
	},
	{
		`SELECT i as foo FROM mytable ORDER BY i DESC`,
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i as foo FROM mytable GROUP BY i ORDER BY i DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i as foo FROM mytable GROUP BY 2 ORDER BY 2 DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i as foo FROM mytable GROUP BY i ORDER BY foo DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i as foo FROM mytable GROUP BY 2 ORDER BY foo DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i as i FROM mytable GROUP BY 2`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT i as i FROM mytable GROUP BY 1`,
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		`SELECT CONCAT("a", "b", "c")`,
		[]sql.Row{
			{string("abc")},
		},
	},
	{
		`SELECT COALESCE(NULL, NULL, NULL, 'example', NULL, 1234567890)`,
		[]sql.Row{
			{string("example")},
		},
	},
	{
		`SELECT COALESCE(NULL, NULL, NULL, COALESCE(NULL, 1234567890))`,
		[]sql.Row{
			{int64(1234567890)},
		},
	},
	{
		"SELECT concat(s, i) FROM mytable",
		[]sql.Row{
			{string("first row1")},
			{string("second row2")},
			{string("third row3")},
		},
	},
	{
		"SELECT version()",
		[]sql.Row{
			{string("8.0.11")},
		},
	},
	{
		"SELECT * FROM mytable WHERE 1 > 5",
		[]sql.Row{},
	},
	{
		"SELECT SUM(i) + 1, i FROM mytable GROUP BY i ORDER BY i",
		[]sql.Row{
			{float64(2), int64(1)},
			{float64(3), int64(2)},
			{float64(4), int64(3)},
		},
	},
	{
		"SELECT SUM(i), i FROM mytable GROUP BY i ORDER BY 1+SUM(i) ASC",
		[]sql.Row{
			{float64(1), int64(1)},
			{float64(2), int64(2)},
			{float64(3), int64(3)},
		},
	},
	{
		"SELECT i, SUM(i) FROM mytable GROUP BY i ORDER BY SUM(i) DESC",
		[]sql.Row{
			{int64(3), float64(3)},
			{int64(2), float64(2)},
			{int64(1), float64(1)},
		},
	},
	{
		`/*!40101 SET NAMES utf8 */`,
		[]sql.Row{},
	},
	{
		`SHOW DATABASES`,
		[]sql.Row{{"mydb"}, {"foo"}},
	},
	{
		`SHOW SCHEMAS`,
		[]sql.Row{{"mydb"}, {"foo"}},
	},
	{
		`SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA`,
		[]sql.Row{
			{"mydb", "utf8mb4", "utf8_bin"},
			{"foo", "utf8mb4", "utf8_bin"},
		},
	},
	{
		`SELECT s FROM mytable WHERE s LIKE '%d row'`,
		[]sql.Row{
			{"second row"},
			{"third row"},
		},
	},
	{
		`SELECT SUBSTRING(s, -3, 3) as s FROM mytable WHERE s LIKE '%d row' GROUP BY 1`,
		[]sql.Row{
			{"row"},
		},
	},
	{
		`SELECT s FROM mytable WHERE s NOT LIKE '%d row'`,
		[]sql.Row{
			{"first row"},
		},
	},
	{
		`SHOW COLUMNS FROM mytable`,
		[]sql.Row{
			{"i", "INT64", "NO", "", "", ""},
			{"s", "TEXT", "NO", "", "", ""},
		},
	},
	{
		`SHOW COLUMNS FROM mytable WHERE Field = 'i'`,
		[]sql.Row{
			{"i", "INT64", "NO", "", "", ""},
		},
	},
	{
		`SHOW COLUMNS FROM mytable LIKE 'i'`,
		[]sql.Row{
			{"i", "INT64", "NO", "", "", ""},
		},
	},
	{
		`SHOW FULL COLUMNS FROM mytable`,
		[]sql.Row{
			{"i", "INT64", nil, "NO", "", "", "", "", ""},
			{"s", "TEXT", "utf8_bin", "NO", "", "", "", "", ""},
		},
	},
	{
		`SHOW TABLE STATUS FROM mydb`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SHOW TABLE STATUS LIKE '%table'`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SHOW TABLE STATUS WHERE Name = 'mytable'`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SELECT i FROM mytable NATURAL JOIN tabletest`,
		[]sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		`SELECT * FROM foo.other_table`,
		[]sql.Row{
			{"a", int32(4)},
			{"b", int32(2)},
			{"c", int32(0)},
		},
	},
	{
		`SELECT AVG(23.222000)`,
		[]sql.Row{
			{float64(23.222)},
		},
	},
	{
		`SELECT DATABASE()`,
		[]sql.Row{
			{"mydb"},
		},
	},
	{
		`SHOW VARIABLES`,
		[]sql.Row{
			{"auto_increment_increment", int64(1)},
			{"time_zone", time.Local.String()},
			{"system_time_zone", time.Local.String()},
			{"max_allowed_packet", math.MaxInt32},
			{"sql_mode", ""},
			{"gtid_mode", int32(0)},
			{"collation_database", "utf8_bin"},
			{"ndbinfo_version", ""},
			{"sql_select_limit", math.MaxInt32},
			{"transaction_isolation", "READ UNCOMMITTED"},
		},
	},
	{
		`SHOW VARIABLES LIKE 'gtid_mode`,
		[]sql.Row{
			{"gtid_mode", int32(0)},
		},
	},
	{
		`SHOW VARIABLES LIKE 'gtid%`,
		[]sql.Row{
			{"gtid_mode", int32(0)},
		},
	},
	{
		`SHOW GLOBAL VARIABLES LIKE '%mode`,
		[]sql.Row{
			{"sql_mode", ""},
			{"gtid_mode", int32(0)},
		},
	},
	{
		`SELECT JSON_EXTRACT("foo", "$")`,
		[]sql.Row{{"foo"}},
	},
	{
		`SELECT CONNECTION_ID()`,
		[]sql.Row{{uint32(1)}},
	},
	{
		`
		SELECT
			LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'UNDO LOG'
			AND FILE_NAME IS NOT NULL
			AND LOGFILE_GROUP_NAME IS NOT NULL
		GROUP BY LOGFILE_GROUP_NAME, FILE_NAME, ENGINE, TOTAL_EXTENTS, INITIAL_SIZE
		ORDER BY LOGFILE_GROUP_NAME
		`,
		[]sql.Row{},
	},
	{
		`
		SELECT DISTINCT
			TABLESPACE_NAME, FILE_NAME, LOGFILE_GROUP_NAME, EXTENT_SIZE, INITIAL_SIZE, ENGINE
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'DATAFILE'
		ORDER BY TABLESPACE_NAME, LOGFILE_GROUP_NAME
		`,
		[]sql.Row{},
	},
	{
		`
		SELECT
			COLUMN_NAME,
			JSON_EXTRACT(HISTOGRAM, '$."number-of-buckets-specified"')
		FROM information_schema.COLUMN_STATISTICS
		WHERE SCHEMA_NAME = 'mydb'
		AND TABLE_NAME = 'mytable'
		`,
		[]sql.Row{},
	},
	{
		`
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA='mydb' AND (TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW')
		`,
		[]sql.Row{
			{"mytable"},
			{"othertable"},
			{"tabletest"},
			{"bigtable"},
		},
	},
	{
		`
		SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='mytable'
		`,
		[]sql.Row{
			{"s", "TEXT"},
			{"i", "BIGINT"},
		},
	},
	{
		`
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY COLUMN_NAME
		`,
		[]sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
		},
	},
	{
		`
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		[]sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
		},
	},
	{
		`
		SELECT COLUMN_NAME as COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		[]sql.Row{
			{"s"},
			{"i"},
			{"s2"},
			{"i2"},
			{"t"},
			{"n"},
		},
	},
	{
		`SHOW CREATE DATABASE mydb`,
		[]sql.Row{{
			"mydb",
			"CREATE DATABASE `mydb` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8_bin */",
		}},
	},
	{
		`SELECT -1`,
		[]sql.Row{{int64(-1)}},
	},
	{
		`
		SHOW WARNINGS
		`,
		[]sql.Row{},
	},
	{
		`SHOW WARNINGS LIMIT 0`,
		[]sql.Row{},
	},
	{
		`SET SESSION NET_READ_TIMEOUT= 700, SESSION NET_WRITE_TIMEOUT= 700`,
		[]sql.Row{},
	},
	{
		`SHOW TABLE STATUS`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SELECT NULL`,
		[]sql.Row{
			{nil},
		},
	},
	{
		`SELECT nullif('abc', NULL)`,
		[]sql.Row{
			{"abc"},
		},
	},
	{
		`SELECT nullif(NULL, NULL)`,
		[]sql.Row{
			{sql.Null},
		},
	},
	{
		`SELECT nullif(NULL, 123)`,
		[]sql.Row{
			{nil},
		},
	},
	{
		`SELECT nullif(123, 123)`,
		[]sql.Row{
			{sql.Null},
		},
	},
	{
		`SELECT nullif(123, 321)`,
		[]sql.Row{
			{int64(123)},
		},
	},
	{
		`SELECT ifnull(123, NULL)`,
		[]sql.Row{
			{int64(123)},
		},
	},
	{
		`SELECT ifnull(NULL, NULL)`,
		[]sql.Row{
			{nil},
		},
	},
	{
		`SELECT ifnull(NULL, 123)`,
		[]sql.Row{
			{int64(123)},
		},
	},
	{
		`SELECT ifnull(123, 123)`,
		[]sql.Row{
			{int64(123)},
		},
	},
	{
		`SELECT ifnull(123, 321)`,
		[]sql.Row{
			{int64(123)},
		},
	},
	{
		`SELECT round(15728640/1024/1024)`,
		[]sql.Row{
			{int64(15)},
		},
	},
	{
		`SELECT round(15, 1)`,
		[]sql.Row{
			{int64(15)},
		},
	},
	{
		`SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM mytable`,
		[]sql.Row{
			{"one"},
			{"two"},
			{"other"},
		},
	},
	{
		`SELECT CASE WHEN i > 2 THEN 'more than two' WHEN i < 2 THEN 'less than two' ELSE 'two' END FROM mytable`,
		[]sql.Row{
			{"less than two"},
			{"two"},
			{"more than two"},
		},
	},
	{
		`SELECT CASE i WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM mytable`,
		[]sql.Row{
			{"one"},
			{"two"},
			{nil},
		},
	},
	{
		"SHOW TABLES",
		[]sql.Row{
			{"mytable"},
			{"othertable"},
			{"tabletest"},
			{"bigtable"},
		},
	},
	{
		"SHOW FULL TABLES",
		[]sql.Row{
			{"mytable", "BASE TABLE"},
			{"othertable", "BASE TABLE"},
			{"tabletest", "BASE TABLE"},
			{"bigtable", "BASE TABLE"},
		},
	},
	{
		"SHOW TABLES FROM foo",
		[]sql.Row{
			{"other_table"},
		},
	},
	{
		"SHOW TABLES LIKE '%table'",
		[]sql.Row{
			{"mytable"},
			{"othertable"},
			{"bigtable"},
		},
	},
	{
		"SHOW TABLES WHERE `Table` = 'mytable'",
		[]sql.Row{
			{"mytable"},
		},
	},
	{
		`SHOW COLLATION`,
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1)}},
	},
	{
		`SHOW COLLATION LIKE 'foo'`,
		[]sql.Row{},
	},
	{
		`SHOW COLLATION LIKE 'utf8%'`,
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1)}},
	},
	{
		`SHOW COLLATION WHERE charset = 'foo'`,
		[]sql.Row{},
	},
	{
		"SHOW COLLATION WHERE `Default` = 'Yes'",
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1)}},
	},
	{
		"ROLLBACK",
		[]sql.Row{},
	},
	{
		"SELECT substring(s, 1, 1) FROM mytable ORDER BY substring(s, 1, 1)",
		[]sql.Row{{"f"}, {"s"}, {"t"}},
	},
	{
		"SELECT substring(s, 1, 1), count(*) FROM mytable GROUP BY substring(s, 1, 1)",
		[]sql.Row{{"f", int64(1)}, {"s", int64(1)}, {"t", int64(1)}},
	},
	{
		"SELECT SLEEP(0.5)",
		[]sql.Row{{int(0)}},
	},
	{
		"SELECT TO_BASE64('foo')",
		[]sql.Row{{string("Zm9v")}},
	},
	{
		"SELECT FROM_BASE64('YmFy')",
		[]sql.Row{{string("bar")}},
	},
	{
		"SELECT DATE_ADD('2018-05-02', INTERVAL 1 DAY)",
		[]sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		"SELECT DATE_SUB('2018-05-02', INTERVAL 1 DAY)",
		[]sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		"SELECT '2018-05-02' + INTERVAL 1 DAY",
		[]sql.Row{{time.Date(2018, time.May, 3, 0, 0, 0, 0, time.UTC)}},
	},
	{
		"SELECT '2018-05-02' - INTERVAL 1 DAY",
		[]sql.Row{{time.Date(2018, time.May, 1, 0, 0, 0, 0, time.UTC)}},
	},
	{
		`SELECT i AS i FROM mytable ORDER BY i`,
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		`
		SELECT
			i,
			foo
		FROM (
			SELECT
				i,
				COUNT(DISTINCT s) AS foo
			FROM mytable
			GROUP BY i
		) AS q
		ORDER BY foo DESC
		`,
		[]sql.Row{
			{int64(1), int64(1)},
			{int64(2), int64(1)},
			{int64(3), int64(1)},
		},
	},
	{
		"SELECT n, COUNT(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		[]sql.Row{{int64(1), int64(3)}, {int64(2), int64(3)}},
	},
	{
		"SELECT n, MAX(n) FROM bigtable GROUP BY n HAVING COUNT(n) > 2",
		[]sql.Row{{int64(1), int64(1)}, {int64(2), int64(2)}},
	},
	{
		"SELECT substring(mytable.s, 1, 5) as s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1 HAVING s = \"secon\"",
		[]sql.Row{{"secon"}},
	},
	{
		`
		SELECT COLUMN_NAME as COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1 HAVING SUBSTRING(COLUMN_NAME, 1, 1) = "s"
		`,
		[]sql.Row{{"s"}, {"s2"}},
	},
	{
		"SELECT s,  i FROM mytable GROUP BY i ORDER BY SUBSTRING(s, 1, 1) DESC",
		[]sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		"SELECT s, i FROM mytable GROUP BY i HAVING count(*) > 0 ORDER BY SUBSTRING(s, 1, 1) DESC",
		[]sql.Row{
			{string("third row"), int64(3)},
			{string("second row"), int64(2)},
			{string("first row"), int64(1)},
		},
	},
	{
		"SELECT CONVERT('9999-12-31 23:59:59', DATETIME)",
		[]sql.Row{{time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)}},
	},
	{
		"SELECT CONVERT('10000-12-31 23:59:59', DATETIME)",
		[]sql.Row{{nil}},
	},
	{
		"SELECT '9999-12-31 23:59:59' + INTERVAL 1 DAY",
		[]sql.Row{{nil}},
	},
	{
		"SELECT DATE_ADD('9999-12-31 23:59:59', INTERVAL 1 DAY)",
		[]sql.Row{{nil}},
	},
	{
		`SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t WHERE t.date_col > '0000-01-01 00:00:00'`,
		[]sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		`SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) as date_col) t GROUP BY t.date_col`,
		[]sql.Row{{time.Date(2019, time.June, 6, 0, 0, 0, 0, time.UTC)}},
	},
	{
		`SELECT i AS foo FROM mytable ORDER BY mytable.i`,
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		`SELECT JSON_EXTRACT('[1, 2, 3]', '$.[0]')`,
		[]sql.Row{{float64(1)}},
	},
	{
		`SELECT ARRAY_LENGTH(JSON_EXTRACT('[1, 2, 3]', '$'))`,
		[]sql.Row{{int32(3)}},
	},
	{
		`SELECT ARRAY_LENGTH(JSON_EXTRACT('[{"i":0}, {"i":1, "y":"yyy"}, {"i":2, "x":"xxx"}]', '$.i'))`,
		[]sql.Row{{int32(3)}},
	},
	{
		`SELECT GREATEST(1, 2, 3, 4)`,
		[]sql.Row{{int64(4)}},
	},
	{
		`SELECT GREATEST(1, 2, "3", 4)`,
		[]sql.Row{{float64(4)}},
	},
	{
		`SELECT GREATEST(1, 2, "9", "foo999")`,
		[]sql.Row{{float64(9)}},
	},
	{
		`SELECT GREATEST("aaa", "bbb", "ccc")`,
		[]sql.Row{{"ccc"}},
	},
	{
		`SELECT GREATEST(i, s) FROM mytable`,
		[]sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		`SELECT LEAST(1, 2, 3, 4)`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT LEAST(1, 2, "3", 4)`,
		[]sql.Row{{float64(1)}},
	},
	{
		`SELECT LEAST(1, 2, "9", "foo999")`,
		[]sql.Row{{float64(1)}},
	},
	{
		`SELECT LEAST("aaa", "bbb", "ccc")`,
		[]sql.Row{{"aaa"}},
	},
	{
		`SELECT LEAST(i, s) FROM mytable`,
		[]sql.Row{{float64(1)}, {float64(2)}, {float64(3)}},
	},
	{
		"SELECT i, i2, s2 FROM mytable LEFT JOIN othertable ON i = i2",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{int64(1), nil, nil},
			{int64(1), nil, nil},
			{int64(2), int64(2), "second"},
			{int64(2), nil, nil},
			{int64(2), nil, nil},
			{int64(3), int64(3), "first"},
			{int64(3), nil, nil},
			{int64(3), nil, nil},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{nil, int64(1), "third"},
			{nil, int64(1), "third"},
			{int64(2), int64(2), "second"},
			{nil, int64(2), "second"},
			{nil, int64(2), "second"},
			{int64(3), int64(3), "first"},
			{nil, int64(3), "first"},
			{nil, int64(3), "first"},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable LEFT OUTER JOIN othertable ON i = i2",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{int64(1), nil, nil},
			{int64(1), nil, nil},
			{int64(2), int64(2), "second"},
			{int64(2), nil, nil},
			{int64(2), nil, nil},
			{int64(3), int64(3), "first"},
			{int64(3), nil, nil},
			{int64(3), nil, nil},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable RIGHT OUTER JOIN othertable ON i = i2",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{nil, int64(1), "third"},
			{nil, int64(1), "third"},
			{int64(2), int64(2), "second"},
			{nil, int64(2), "second"},
			{nil, int64(2), "second"},
			{int64(3), int64(3), "first"},
			{nil, int64(3), "first"},
			{nil, int64(3), "first"},
		},
	},
	{
		`SELECT CHAR_LENGTH('áé'), LENGTH('àè')`,
		[]sql.Row{{int32(2), int32(4)}},
	},
	{
		"SELECT i, COUNT(i) AS `COUNT(i)` FROM (SELECT i FROM mytable) t GROUP BY i ORDER BY i, `COUNT(i)` DESC",
		[]sql.Row{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(3), int64(1)}},
	},
}

func TestQueries(t *testing.T) {
	e := newEngine(t)
	t.Run("sequential", func(t *testing.T) {
		for _, tt := range queries {
			testQuery(t, e, tt.query, tt.expected)
		}
	})

	ep := newEngineWithParallelism(t, 2)
	t.Run("parallel", func(t *testing.T) {
		for _, tt := range queries {
			testQuery(t, ep, tt.query, tt.expected)
		}
	})
}

func TestSessionSelectLimit(t *testing.T) {
	ctx := newCtx()
	ctx.Session.Set("sql_select_limit", sql.Int64, int64(1))

	q := []struct {
		query    string
		expected []sql.Row
	}{
		{
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{{int64(1), "first row"}},
		},
		{
			"SELECT * FROM mytable ORDER BY i LIMIT 2",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT i FROM (SELECT i FROM mytable LIMIT 2) t ORDER BY i",
			[]sql.Row{{int64(1)}},
		},
		{
			"SELECT i FROM (SELECT i FROM mytable) t ORDER BY i LIMIT 2",
			[]sql.Row{{int64(1)}},
		},
	}
	e := newEngine(t)
	t.Run("sql_select_limit", func(t *testing.T) {
		for _, tt := range q {
			testQueryWithContext(ctx, t, e, tt.query, tt.expected)
		}
	})
}

func TestSessionDefaults(t *testing.T) {
	ctx := newCtx()
	ctx.Session.Set("auto_increment_increment", sql.Int64, 0)
	ctx.Session.Set("max_allowed_packet", sql.Int64, 0)
	ctx.Session.Set("sql_select_limit", sql.Int64, 0)
	ctx.Session.Set("ndbinfo_version", sql.Text, "non default value")

	q := `SET @@auto_increment_increment=DEFAULT,
			  @@max_allowed_packet=DEFAULT,
			  @@sql_select_limit=DEFAULT,
			  @@ndbinfo_version=DEFAULT`

	e := newEngine(t)

	defaults := sql.DefaultSessionConfig()
	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		_, _, err := e.Query(ctx, q)
		require.NoError(err)

		typ, val := ctx.Get("auto_increment_increment")
		require.Equal(defaults["auto_increment_increment"].Typ, typ)
		require.Equal(defaults["auto_increment_increment"].Value, val)

		typ, val = ctx.Get("max_allowed_packet")
		require.Equal(defaults["max_allowed_packet"].Typ, typ)
		require.Equal(defaults["max_allowed_packet"].Value, val)

		typ, val = ctx.Get("sql_select_limit")
		require.Equal(defaults["sql_select_limit"].Typ, typ)
		require.Equal(defaults["sql_select_limit"].Value, val)

		typ, val = ctx.Get("ndbinfo_version")
		require.Equal(defaults["ndbinfo_version"].Typ, typ)
		require.Equal(defaults["ndbinfo_version"].Value, val)
	})
}

func TestWarnings(t *testing.T) {
	ctx := newCtx()
	ctx.Session.Warn(&sql.Warning{Code: 1})
	ctx.Session.Warn(&sql.Warning{Code: 2})
	ctx.Session.Warn(&sql.Warning{Code: 3})

	var queries = []struct {
		query    string
		expected []sql.Row
	}{
		{
			`
			SHOW WARNINGS
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1
			`,
			[]sql.Row{
				{"", 3, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 1,2
			`,
			[]sql.Row{
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 0
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 2,0
			`,
			[]sql.Row{
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10
			`,
			[]sql.Row{
				{"", 3, ""},
				{"", 2, ""},
				{"", 1, ""},
			},
		},
		{
			`
			SHOW WARNINGS LIMIT 10,1
			`,
			[]sql.Row{},
		},
	}

	e := newEngine(t)
	ep := newEngineWithParallelism(t, 2)

	t.Run("sequential", func(t *testing.T) {
		for _, tt := range queries {
			testQueryWithContext(ctx, t, e, tt.query, tt.expected)
		}
	})

	t.Run("parallel", func(t *testing.T) {
		for _, tt := range queries {
			testQueryWithContext(ctx, t, ep, tt.query, tt.expected)
		}
	})
}

func TestClearWarnings(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)
	ctx := newCtx()

	_, iter, err := e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "-- some empty query as a comment")
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	_, iter, err = e.Query(ctx, "SHOW WARNINGS")
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(3, len(rows))

	_, iter, err = e.Query(ctx, "SHOW WARNINGS LIMIT 1")
	require.NoError(err)
	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)
	require.Equal(1, len(rows))

	_, _, err = e.Query(ctx, "SELECT * FROM mytable LIMIT 1")
	require.NoError(err)
	_, err = sql.RowIterToRows(iter)
	require.NoError(err)
	err = iter.Close()
	require.NoError(err)

	require.Equal(0, len(ctx.Session.Warnings()))
}

func TestDescribe(t *testing.T) {
	e := newEngine(t)

	ep := newEngineWithParallelism(t, 2)

	query := `DESCRIBE FORMAT=TREE SELECT * FROM mytable`
	expectedSeq := []sql.Row{
		sql.NewRow("Table(mytable): Projected "),
		sql.NewRow(" ├─ Column(i, INT64, nullable=false)"),
		sql.NewRow(" └─ Column(s, TEXT, nullable=false)"),
	}

	expectedParallel := []sql.Row{
		{"Exchange(parallelism=2)"},
		{" └─ Table(mytable): Projected "},
		{"     ├─ Column(i, INT64, nullable=false)"},
		{"     └─ Column(s, TEXT, nullable=false)"},
	}

	t.Run("sequential", func(t *testing.T) {
		testQuery(t, e, query, expectedSeq)
	})

	t.Run("parallel", func(t *testing.T) {
		testQuery(t, ep, query, expectedParallel)
	})
}

func TestOrderByColumns(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	_, iter, err := e.Query(newCtx(), "SELECT s, i FROM mytable ORDER BY 2 DESC")
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"third row", int64(3)},
			{"second row", int64(2)},
			{"first row", int64(1)},
		},
		rows,
	)
}

func TestInsertInto(t *testing.T) {
	e := newEngine(t)
	testQuery(t, e,
		"INSERT INTO mytable (s, i) VALUES ('x', 999);",
		[]sql.Row{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'x';",
		[]sql.Row{{int64(999)}},
	)
}

const testNumPartitions = 5

func TestAmbiguousColumnResolution(t *testing.T) {
	require := require.New(t)

	table := mem.NewPartitionedTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Text, Source: "foo"},
	}, testNumPartitions)

	insertRows(
		t, table,
		sql.NewRow(int64(1), "foo"),
		sql.NewRow(int64(2), "bar"),
		sql.NewRow(int64(3), "baz"),
	)

	table2 := mem.NewPartitionedTable("bar", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "bar"},
		{Name: "c", Type: sql.Int64, Source: "bar"},
	}, testNumPartitions)
	insertRows(
		t, table2,
		sql.NewRow("qux", int64(3)),
		sql.NewRow("mux", int64(2)),
		sql.NewRow("pux", int64(1)),
	)

	db := mem.NewDatabase("mydb")
	db.AddTable("foo", table)
	db.AddTable("bar", table2)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	q := `SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c`
	ctx := newCtx()

	_, rows, err := e.Query(ctx, q)
	require.NoError(err)

	var rs [][]interface{}
	for {
		row, err := rows.Next()
		if err == io.EOF {
			break
		}
		require.NoError(err)

		rs = append(rs, row)
	}

	expected := [][]interface{}{
		{int64(1), "pux", "foo"},
		{int64(2), "mux", "bar"},
		{int64(3), "qux", "baz"},
	}

	require.Equal(expected, rs)
}

func TestDDL(t *testing.T) {
	require := require.New(t)

	e := newEngine(t)
	testQuery(t, e,
		"CREATE TABLE t1(a INTEGER, b TEXT, c DATE, "+
			"d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, "+
			"b1 BOOL, b2 BOOLEAN NOT NULL)",
		[]sql.Row(nil),
	)

	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok := db.Tables()["t1"]
	require.True(ok)

	s := sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: true, Source: "t1"},
		{Name: "b", Type: sql.Text, Nullable: true, Source: "t1"},
		{Name: "c", Type: sql.Date, Nullable: true, Source: "t1"},
		{Name: "d", Type: sql.Timestamp, Nullable: true, Source: "t1"},
		{Name: "e", Type: sql.Text, Nullable: true, Source: "t1"},
		{Name: "f", Type: sql.Blob, Source: "t1"},
		{Name: "b1", Type: sql.Uint8, Nullable: true, Source: "t1"},
		{Name: "b2", Type: sql.Uint8, Source: "t1"},
	}

	require.Equal(s, testTable.Schema())
}

func TestNaturalJoin(t *testing.T) {
	require := require.New(t)

	t1 := mem.NewPartitionedTable("t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1"},
		{Name: "b", Type: sql.Text, Source: "t1"},
		{Name: "c", Type: sql.Text, Source: "t1"},
	}, testNumPartitions)

	insertRows(
		t, t1,
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"),
	)

	t2 := mem.NewPartitionedTable("t2", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t2"},
		{Name: "b", Type: sql.Text, Source: "t2"},
		{Name: "d", Type: sql.Text, Source: "t2"},
	}, testNumPartitions)

	insertRows(
		t, t2,
		sql.NewRow("a_1", "b_1", "d_1"),
		sql.NewRow("a_2", "b_2", "d_2"),
		sql.NewRow("a_3", "b_3", "d_3"),
	)

	db := mem.NewDatabase("mydb")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	_, iter, err := e.Query(newCtx(), `SELECT * FROM t1 NATURAL JOIN t2`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"a_1", "b_1", "c_1", "d_1"},
			{"a_2", "b_2", "c_2", "d_2"},
			{"a_3", "b_3", "c_3", "d_3"},
		},
		rows,
	)
}

func TestNaturalJoinEqual(t *testing.T) {
	require := require.New(t)

	t1 := mem.NewPartitionedTable("t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1"},
		{Name: "b", Type: sql.Text, Source: "t1"},
		{Name: "c", Type: sql.Text, Source: "t1"},
	}, testNumPartitions)

	insertRows(
		t, t1,
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"),
	)

	t2 := mem.NewPartitionedTable("t2", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t2"},
		{Name: "b", Type: sql.Text, Source: "t2"},
		{Name: "c", Type: sql.Text, Source: "t2"},
	}, testNumPartitions)

	insertRows(
		t, t2,
		sql.NewRow("a_1", "b_1", "c_1"),
		sql.NewRow("a_2", "b_2", "c_2"),
		sql.NewRow("a_3", "b_3", "c_3"),
	)

	db := mem.NewDatabase("mydb")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	_, iter, err := e.Query(newCtx(), `SELECT * FROM t1 NATURAL JOIN t2`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"a_1", "b_1", "c_1"},
			{"a_2", "b_2", "c_2"},
			{"a_3", "b_3", "c_3"},
		},
		rows,
	)
}

func TestNaturalJoinDisjoint(t *testing.T) {
	require := require.New(t)

	t1 := mem.NewPartitionedTable("t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1"},
	}, testNumPartitions)

	insertRows(
		t, t1,
		sql.NewRow("a1"),
		sql.NewRow("a2"),
		sql.NewRow("a3"),
	)

	t2 := mem.NewPartitionedTable("t2", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "t2"},
	}, testNumPartitions)
	insertRows(
		t, t2,
		sql.NewRow("b1"),
		sql.NewRow("b2"),
		sql.NewRow("b3"),
	)

	db := mem.NewDatabase("mydb")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	_, iter, err := e.Query(newCtx(), `SELECT * FROM t1 NATURAL JOIN t2`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{"a1", "b1"},
			{"a1", "b2"},
			{"a1", "b3"},
			{"a2", "b1"},
			{"a2", "b2"},
			{"a2", "b3"},
			{"a3", "b1"},
			{"a3", "b2"},
			{"a3", "b3"},
		},
		rows,
	)
}

func TestInnerNestedInNaturalJoins(t *testing.T) {
	require := require.New(t)

	table1 := mem.NewPartitionedTable("table1", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "table1"},
		{Name: "f", Type: sql.Float64, Source: "table1"},
		{Name: "t", Type: sql.Text, Source: "table1"},
	}, testNumPartitions)

	insertRows(
		t, table1,
		sql.NewRow(int32(1), float64(2.1), "table1"),
		sql.NewRow(int32(1), float64(2.1), "table1"),
		sql.NewRow(int32(10), float64(2.1), "table1"),
	)

	table2 := mem.NewPartitionedTable("table2", sql.Schema{
		{Name: "i2", Type: sql.Int32, Source: "table2"},
		{Name: "f2", Type: sql.Float64, Source: "table2"},
		{Name: "t2", Type: sql.Text, Source: "table2"},
	}, testNumPartitions)

	insertRows(
		t, table2,
		sql.NewRow(int32(1), float64(2.2), "table2"),
		sql.NewRow(int32(1), float64(2.2), "table2"),
		sql.NewRow(int32(20), float64(2.2), "table2"),
	)

	table3 := mem.NewPartitionedTable("table3", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "table3"},
		{Name: "f2", Type: sql.Float64, Source: "table3"},
		{Name: "t3", Type: sql.Text, Source: "table3"},
	}, testNumPartitions)

	insertRows(
		t, table3,
		sql.NewRow(int32(1), float64(2.3), "table3"),
		sql.NewRow(int32(2), float64(2.3), "table3"),
		sql.NewRow(int32(30), float64(2.3), "table3"),
	)

	db := mem.NewDatabase("mydb")
	db.AddTable("table1", table1)
	db.AddTable("table2", table2)
	db.AddTable("table3", table3)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	_, iter, err := e.Query(newCtx(), `SELECT * FROM table1 INNER JOIN table2 ON table1.i = table2.i2 NATURAL JOIN table3`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(
		[]sql.Row{
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
			{int32(1), float64(2.2), float64(2.1), "table1", int32(1), "table2", "table3"},
		},
		rows,
	)
}

func testQuery(t *testing.T, e *sqle.Engine, q string, expected []sql.Row) {
	testQueryWithContext(newCtx(), t, e, q, expected)
}

func testQueryWithContext(ctx *sql.Context, t *testing.T, e *sqle.Engine, q string, expected []sql.Row) {
	orderBy := strings.Contains(strings.ToUpper(q), " ORDER BY ")

	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		_, iter, err := e.Query(ctx, q)
		require.NoError(err)

		rows, err := sql.RowIterToRows(iter)
		require.NoError(err)

		if orderBy {
			require.Equal(expected, rows)
		} else {
			require.ElementsMatch(expected, rows)
		}
	})
}

func newEngine(t *testing.T) *sqle.Engine {
	return newEngineWithParallelism(t, 1)
}

func newEngineWithParallelism(t *testing.T, parallelism int) *sqle.Engine {
	table := mem.NewPartitionedTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, testNumPartitions)

	insertRows(
		t, table,
		sql.NewRow(int64(1), "first row"),
		sql.NewRow(int64(2), "second row"),
		sql.NewRow(int64(3), "third row"),
	)

	table2 := mem.NewPartitionedTable("othertable", sql.Schema{
		{Name: "s2", Type: sql.Text, Source: "othertable"},
		{Name: "i2", Type: sql.Int64, Source: "othertable"},
	}, testNumPartitions)

	insertRows(
		t, table2,
		sql.NewRow("first", int64(3)),
		sql.NewRow("second", int64(2)),
		sql.NewRow("third", int64(1)),
	)

	table3 := mem.NewPartitionedTable("tabletest", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "tabletest"},
		{Name: "s", Type: sql.Text, Source: "tabletest"},
	}, testNumPartitions)

	insertRows(
		t, table3,
		sql.NewRow(int64(1), "first row"),
		sql.NewRow(int64(2), "second row"),
		sql.NewRow(int64(3), "third row"),
	)

	table4 := mem.NewPartitionedTable("other_table", sql.Schema{
		{Name: "text", Type: sql.Text, Source: "tabletest"},
		{Name: "number", Type: sql.Int32, Source: "tabletest"},
	}, testNumPartitions)

	insertRows(
		t, table4,
		sql.NewRow("a", int32(4)),
		sql.NewRow("b", int32(2)),
		sql.NewRow("c", int32(0)),
	)

	bigtable := mem.NewPartitionedTable("bigtable", sql.Schema{
		{Name: "t", Type: sql.Text, Source: "bigtable"},
		{Name: "n", Type: sql.Int64, Source: "bigtable"},
	}, testNumPartitions)

	insertRows(
		t, bigtable,
		sql.NewRow("a", int64(1)),
		sql.NewRow("s", int64(2)),
		sql.NewRow("f", int64(3)),
		sql.NewRow("g", int64(1)),
		sql.NewRow("h", int64(2)),
		sql.NewRow("j", int64(3)),
		sql.NewRow("k", int64(1)),
		sql.NewRow("l", int64(2)),
		sql.NewRow("ñ", int64(4)),
		sql.NewRow("z", int64(5)),
		sql.NewRow("x", int64(6)),
		sql.NewRow("c", int64(7)),
		sql.NewRow("v", int64(8)),
		sql.NewRow("b", int64(9)),
	)

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("othertable", table2)
	db.AddTable("tabletest", table3)
	db.AddTable("bigtable", bigtable)

	db2 := mem.NewDatabase("foo")
	db2.AddTable("other_table", table4)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	catalog.AddDatabase(db2)
	catalog.AddDatabase(sql.NewInformationSchemaDatabase(catalog))

	var a *analyzer.Analyzer
	if parallelism > 1 {
		a = analyzer.NewBuilder(catalog).WithParallelism(parallelism).Build()
	} else {
		a = analyzer.NewDefault(catalog)
	}

	return sqle.New(catalog, a, new(sqle.Config))
}

const expectedTree = `Offset(2)
 └─ Limit(5)
     └─ Project(t.foo, bar.baz)
         └─ Filter(foo > qux)
             └─ InnerJoin(foo = baz)
                 ├─ TableAlias(t)
                 │   └─ UnresolvedTable(tbl)
                 └─ UnresolvedTable(bar)
`

func TestPrintTree(t *testing.T) {
	require := require.New(t)
	node, err := parse.Parse(newCtx(), `
		SELECT t.foo, bar.baz
		FROM tbl t
		INNER JOIN bar
			ON foo = baz
		WHERE foo > qux
		LIMIT 5
		OFFSET 2`)
	require.NoError(err)
	require.Equal(expectedTree, node.String())
}

// see: https://github.com/src-d/go-mysql-server/issues/197
func TestStarPanic197(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	ctx := newCtx()
	_, iter, err := e.Query(ctx, `SELECT * FROM mytable GROUP BY i, s`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Len(rows, 3)
}

func TestInvalidRegexp(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	ctx := newCtx()
	_, iter, err := e.Query(ctx, `SELECT * FROM mytable WHERE s REGEXP("*main.go")`)
	require.NoError(err)

	_, err = sql.RowIterToRows(iter)
	require.Error(err)
}

func TestIndexes(t *testing.T) {
	e := newEngine(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(tmpDir, 0644))
	e.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	_, _, err = e.Query(
		newCtx(),
		"CREATE INDEX idx_i ON mytable USING pilosa (i) WITH (async = false)",
	)
	require.NoError(t, err)

	_, _, err = e.Query(
		newCtx(),
		"CREATE INDEX idx_s ON mytable USING pilosa (s) WITH (async = false)",
	)
	require.NoError(t, err)

	_, _, err = e.Query(
		newCtx(),
		"CREATE INDEX idx_is ON mytable USING pilosa (i, s) WITH (async = false)",
	)
	require.NoError(t, err)

	defer func() {
		done, err := e.Catalog.DeleteIndex("mydb", "idx_i", true)
		require.NoError(t, err)
		<-done

		done, err = e.Catalog.DeleteIndex("mydb", "idx_s", true)
		require.NoError(t, err)
		<-done

		done, err = e.Catalog.DeleteIndex("foo", "idx_is", true)
		require.NoError(t, err)
		<-done
	}()

	testCases := []struct {
		query    string
		expected []sql.Row
	}{
		{
			"SELECT * FROM mytable WHERE i = 2",
			[]sql.Row{
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i > 1",
			[]sql.Row{
				{int64(3), "third row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i < 3",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i <= 2",
			[]sql.Row{
				{int64(2), "second row"},
				{int64(1), "first row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i >= 2",
			[]sql.Row{
				{int64(2), "second row"},
				{int64(3), "third row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 2 AND s = 'second row'",
			[]sql.Row{
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 2 AND s = 'third row'",
			([]sql.Row)(nil),
		},
		{
			"SELECT * FROM mytable WHERE i BETWEEN 1 AND 2",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 1 OR i = 2",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
			},
		},
		{
			"SELECT * FROM mytable WHERE i = 1 AND i = 2",
			([]sql.Row)(nil),
		},
		{
			"SELECT i as mytable_i FROM mytable WHERE mytable_i = 2",
			[]sql.Row{
				{int64(2)},
			},
		},
		{
			"SELECT i as mytable_i FROM mytable WHERE mytable_i > 1",
			[]sql.Row{
				{int64(3)},
				{int64(2)},
			},
		},
		{
			"SELECT i as mytable_i, s as mytable_s FROM mytable WHERE mytable_i = 2 AND mytable_s = 'second row'",
			[]sql.Row{
				{int64(2), "second row"},
			},
		},
		{
			"SELECT s, SUBSTRING(s, 1, 1) AS sub_s FROM mytable WHERE sub_s = 's'",
			[]sql.Row{
				{"second row", "s"},
			},
		},
		{
			"SELECT count(i) AS mytable_i, SUBSTR(s, -3) AS mytable_s FROM mytable WHERE i > 0 AND mytable_s='row' GROUP BY mytable_s",
			[]sql.Row{
				{int64(3), "row"},
			},
		},
		{
			"SELECT mytable_i FROM (SELECT i AS mytable_i FROM mytable) as t WHERE mytable_i > 1",
			[]sql.Row{
				{int64(2)},
				{int64(3)},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			require := require.New(t)

			tracer := new(test.MemTracer)
			ctx := sql.NewContext(context.TODO(), sql.WithTracer(tracer))

			_, it, err := e.Query(ctx, tt.query)
			require.NoError(err)

			rows, err := sql.RowIterToRows(it)
			require.NoError(err)

			require.ElementsMatch(tt.expected, rows)
			require.Equal("plan.ResolvedTable", tracer.Spans[len(tracer.Spans)-1])
		})
	}
}

func TestCreateIndex(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	require.NoError(err)

	require.NoError(os.MkdirAll(tmpDir, 0644))
	e.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	_, iter, err := e.Query(newCtx(), "CREATE INDEX myidx ON mytable USING pilosa (i)")
	require.NoError(err)
	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	defer func() {
		time.Sleep(1 * time.Second)
		done, err := e.Catalog.DeleteIndex("foo", "myidx", true)
		require.NoError(err)
		<-done

		require.NoError(os.RemoveAll(tmpDir))
	}()
}

func TestOrderByGroupBy(t *testing.T) {
	require := require.New(t)

	table := mem.NewPartitionedTable("members", sql.Schema{
		{Name: "id", Type: sql.Int64, Source: "members"},
		{Name: "team", Type: sql.Text, Source: "members"},
	}, testNumPartitions)

	insertRows(
		t, table,
		sql.NewRow(int64(3), "red"),
		sql.NewRow(int64(4), "red"),
		sql.NewRow(int64(5), "orange"),
		sql.NewRow(int64(6), "orange"),
		sql.NewRow(int64(7), "orange"),
		sql.NewRow(int64(8), "purple"),
	)

	db := mem.NewDatabase("db")
	db.AddTable("members", table)

	e := sqle.NewDefault()
	e.AddDatabase(db)

	_, iter, err := e.Query(
		newCtx(),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY 2",
	)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	expected := []sql.Row{
		{"purple", int64(1)},
		{"red", int64(2)},
		{"orange", int64(3)},
	}

	require.Equal(expected, rows)

	_, iter, err = e.Query(
		newCtx(),
		"SELECT team, COUNT(*) FROM members GROUP BY 1 ORDER BY 2",
	)
	require.NoError(err)

	rows, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(expected, rows)
}

func TestTracing(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	tracer := new(test.MemTracer)

	ctx := sql.NewContext(context.TODO(), sql.WithTracer(tracer))

	_, iter, err := e.Query(ctx, `SELECT DISTINCT i
		FROM mytable
		WHERE s = 'first row'
		ORDER BY i DESC
		LIMIT 1`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.Len(rows, 1)
	require.NoError(err)

	spans := tracer.Spans
	var expectedSpans = []string{
		"plan.Limit",
		"plan.Sort",
		"plan.Distinct",
		"plan.Project",
		"plan.ResolvedTable",
	}

	var spanOperations []string
	for _, s := range spans {
		// only check the ones inside the execution tree
		if strings.HasPrefix(s, "plan.") ||
			strings.HasPrefix(s, "expression.") ||
			strings.HasPrefix(s, "function.") ||
			strings.HasPrefix(s, "aggregation.") {
			spanOperations = append(spanOperations, s)
		}
	}

	require.Equal(expectedSpans, spanOperations)
}

func TestReadOnly(t *testing.T) {
	require := require.New(t)

	table := mem.NewPartitionedTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, testNumPartitions)

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	au := auth.NewNativeSingle("user", "pass", auth.ReadPerm)
	cfg := &sqle.Config{Auth: au}
	a := analyzer.NewBuilder(catalog).Build()
	e := sqle.New(catalog, a, cfg)

	_, _, err := e.Query(newCtx(), `SELECT i FROM mytable`)
	require.NoError(err)

	_, _, err = e.Query(newCtx(), `CREATE INDEX foo ON mytable USING pilosa (i, s)`)
	require.Error(err)
	require.True(auth.ErrNotAuthorized.Is(err))

	_, _, err = e.Query(newCtx(), `DROP INDEX foo ON mytable`)
	require.Error(err)
	require.True(auth.ErrNotAuthorized.Is(err))

	_, _, err = e.Query(newCtx(), `INSERT INTO mytable (i, s) VALUES(42, 'yolo')`)
	require.Error(err)
	require.True(auth.ErrNotAuthorized.Is(err))
}

func TestSessionVariables(t *testing.T) {
	require := require.New(t)

	e := newEngine(t)

	session := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(1))

	_, _, err := e.Query(ctx, `set autocommit=1, sql_mode = concat(@@sql_mode,',STRICT_TRANS_TABLES')`)
	require.NoError(err)

	ctx = sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(2))

	_, iter, err := e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int64(1), ",STRICT_TRANS_TABLES"}}, rows)
}

func TestSessionVariablesONOFF(t *testing.T) {
	require := require.New(t)

	e := newEngine(t)

	session := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(1))

	_, _, err := e.Query(ctx, `set autocommit=ON, sql_mode = OFF, autoformat="true"`)
	require.NoError(err)

	ctx = sql.NewContext(context.Background(), sql.WithSession(session), sql.WithPid(2))

	_, iter, err := e.Query(ctx, `SELECT @@autocommit, @@session.sql_mode, @@autoformat`)
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal([]sql.Row{{int64(1), int64(0), true}}, rows)
}

func TestNestedAliases(t *testing.T) {
	require := require.New(t)

	_, _, err := newEngine(t).Query(newCtx(), `
	SELECT SUBSTRING(s, 1, 10) AS sub_s, SUBSTRING(sub_s, 2, 3) as sub_sub_s
	FROM mytable`)
	require.Error(err)
	require.True(analyzer.ErrMisusedAlias.Is(err))
}

func TestUse(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	_, _, err := e.Query(newCtx(), "USE bar")
	require.Error(err)

	_, iter, err := e.Query(newCtx(), "USE foo")
	require.NoError(err)

	rows, err := sql.RowIterToRows(iter)
	require.NoError(err)
	require.Len(rows, 0)

	require.Equal("foo", e.Catalog.CurrentDatabase())
}

func TestLocks(t *testing.T) {
	require := require.New(t)

	t1 := newLockableTable(mem.NewTable("t1", nil))
	t2 := newLockableTable(mem.NewTable("t2", nil))
	t3 := mem.NewTable("t3", nil)
	catalog := sql.NewCatalog()
	db := mem.NewDatabase("db")
	db.AddTable("t1", t1)
	db.AddTable("t2", t2)
	db.AddTable("t3", t3)
	catalog.AddDatabase(db)

	analyzer := analyzer.NewDefault(catalog)
	engine := sqle.New(catalog, analyzer, new(sqle.Config))

	_, iter, err := engine.Query(newCtx(), "LOCK TABLES t1 READ, t2 WRITE, t3 READ")
	require.NoError(err)

	_, err = sql.RowIterToRows(iter)
	require.NoError(err)

	_, iter, err = engine.Query(newCtx(), "UNLOCK TABLES")
	require.NoError(err)

	_, err = sql.RowIterToRows(iter)
	require.NoError(err)

	require.Equal(1, t1.readLocks)
	require.Equal(0, t1.writeLocks)
	require.Equal(1, t1.unlocks)
	require.Equal(0, t2.readLocks)
	require.Equal(1, t2.writeLocks)
	require.Equal(1, t2.unlocks)
}

func TestDescribeNoPruneColumns(t *testing.T) {
	require := require.New(t)
	ctx := newCtx()
	e := newEngine(t)
	query := `DESCRIBE FORMAT=TREE SELECT SUBSTRING(s, 1, 1) as foo, s, i FROM mytable WHERE foo = 'f'`
	parsed, err := parse.Parse(ctx, query)
	require.NoError(err)
	result, err := e.Analyzer.Analyze(ctx, parsed)
	require.NoError(err)

	qp, ok := result.(*plan.QueryProcess)
	require.True(ok)

	d, ok := qp.Child.(*plan.DescribeQuery)
	require.True(ok)

	p, ok := d.Child.(*plan.Project)
	require.True(ok)

	require.Len(p.Schema(), 3)
}

func insertRows(t *testing.T, table sql.Inserter, rows ...sql.Row) {
	t.Helper()

	for _, r := range rows {
		require.NoError(t, table.Insert(newCtx(), r))
	}
}

var pid uint64

func newCtx() *sql.Context {
	session := sql.NewSession("address", "client", "user", 1)
	return sql.NewContext(
		context.Background(),
		sql.WithPid(atomic.AddUint64(&pid, 1)),
		sql.WithSession(session),
	)
}

type lockableTable struct {
	sql.Table
	readLocks  int
	writeLocks int
	unlocks    int
}

func newLockableTable(t sql.Table) *lockableTable {
	return &lockableTable{Table: t}
}

var _ sql.Lockable = (*lockableTable)(nil)

func (l *lockableTable) Lock(ctx *sql.Context, write bool) error {
	if write {
		l.writeLocks++
	} else {
		l.readLocks++
	}
	return nil
}

func (l *lockableTable) Unlock(ctx *sql.Context, id uint32) error {
	l.unlocks++
	return nil
}

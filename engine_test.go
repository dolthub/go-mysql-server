package sqle_test

import (
	"context"
	"fmt"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"vitess.io/vitess/go/sqltypes"

	"github.com/opentracing/opentracing-go"

	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/memory"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/analyzer"
	"github.com/src-d/go-mysql-server/sql/parse"
	"github.com/src-d/go-mysql-server/sql/plan"
	"github.com/src-d/go-mysql-server/test"

	"github.com/stretchr/testify/require"
)

type queryTest struct {
	query string
	expected []sql.Row
}

var queries = []queryTest{
	{
		"SELECT i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT s,i FROM mytable;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT s,i FROM (select i,s FROM mytable) mt;",
		[]sql.Row{
			{"first row", int64(1)},
			{"second row", int64(2)},
			{"third row", int64(3)}},
	},
	{
		"SELECT i + 1 FROM mytable;",
		[]sql.Row{{int64(2)}, {int64(3)}, {int64(4)}},
	},
	{
		"SELECT -i FROM mytable;",
		[]sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		"SELECT +i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT + - i FROM mytable;",
		[]sql.Row{{int64(-1)}, {int64(-2)}, {int64(-3)}},
	},
	{
		"SELECT i FROM mytable WHERE -i = -2;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i = 2;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i > 2;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i < 2;",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE i <> 2;",
		[]sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i IN (1, 3)",
		[]sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i = 1 OR i = 3",
		[]sql.Row{{int64(1)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i >= 2 ORDER BY 1",
		[]sql.Row{{int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i <= 2 ORDER BY 1",
		[]sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i > 2",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT i FROM mytable WHERE i < 2",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE i >= 2 OR i = 1 ORDER BY 1",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 = 2.0;",
		[]sql.Row{{float32(2.0)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 < 2.0;",
		[]sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 > 2.0;",
		[]sql.Row{{float32(2.5)}},
	},
	{
		"SELECT f32 FROM floattable WHERE f64 <> 2.0;",
		[]sql.Row{{float32(-1.0)}, {float32(-1.5)}, {float32(1.0)}, {float32(1.5)}, {float32(2.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 = 2.0;",
		[]sql.Row{{float64(2.0)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 = -1.5;",
		[]sql.Row{{float64(-1.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE -f32 = -2.0;",
		[]sql.Row{{float64(2.0)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 < 2.0;",
		[]sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 > 2.0;",
		[]sql.Row{{float64(2.5)}},
	},
	{
		"SELECT f64 FROM floattable WHERE f32 <> 2.0;",
		[]sql.Row{{float64(-1.0)}, {float64(-1.5)}, {float64(1.0)}, {float64(1.5)}, {float64(2.5)}},
	},
	{
		"SELECT f32 FROM floattable ORDER BY f64;",
		[]sql.Row{{float32(-1.5)}, {float32(-1.0)}, {float32(1.0)}, {float32(1.5)}, {float32(2.0)}, {float32(2.5)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i DESC;",
		[]sql.Row{{int64(3)}, {int64(2)}, {int64(1)}},
	},
	{
		"SELECT i FROM mytable WHERE 'hello';",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT 'hello';",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
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
		"SELECT i FROM mytable ORDER BY i LIMIT 1 OFFSET 1;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i LIMIT 1,1;",
		[]sql.Row{{int64(2)}},
	},
	{
		"SELECT i FROM mytable ORDER BY i LIMIT 3,1;",
		nil,
	},
	{
		"SELECT i FROM mytable ORDER BY i LIMIT 2,100;",
		[]sql.Row{{int64(3)}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NULL",
		[]sql.Row{{int64(2)}, {nil}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NOT NULL",
		[]sql.Row{{int64(1)}, {nil}, {int64(4)}},
	},
	{
		"SELECT i FROM niltable WHERE b",
		[]sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		"SELECT i FROM niltable WHERE NOT b",
		[]sql.Row{{nil}},
	},
	{
		"SELECT i FROM niltable WHERE b IS TRUE",
		[]sql.Row{{int64(1)}, {int64(4)}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NOT TRUE",
		[]sql.Row{{int64(2)}, {nil}, {nil}},
	},
	{
		"SELECT f FROM niltable WHERE b IS FALSE",
		[]sql.Row{{3.0}},
	},
	{
		"SELECT i FROM niltable WHERE b IS NOT FALSE",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(4)}, {nil}},
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
		"SELECT *  FROM myhistorytable AS OF '2019-01-01' AS foo ORDER BY i",
		[]sql.Row{
			{int64(1), "first row, 1"},
			{int64(2), "second row, 1"},
			{int64(3), "third row, 1"},
		},
	},
	{
		"SELECT *  FROM myhistorytable AS OF '2019-01-02' foo ORDER BY i",
		[]sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
	},
	{
		"SELECT *  FROM myhistorytable ORDER BY i",
		[]sql.Row{
			{int64(1), "first row, 2"},
			{int64(2), "second row, 2"},
			{int64(3), "third row, 2"},
		},
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
		`SELECT SUBSTRING_INDEX(mytable.s, "d", 1) AS s FROM mytable INNER JOIN othertable ON (SUBSTRING_INDEX(mytable.s, "d", 1) = SUBSTRING_INDEX(othertable.s2, "d", 1)) GROUP BY 1 HAVING s = 'secon'`,
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
		"SELECT id FROM typestable WHERE ti > '2019-12-31'",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE da > '2019-12-31'",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE ti < '2019-12-31'",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da < '2019-12-31'",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE ti > date_add('2019-12-30', INTERVAL 1 DAY)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE da > date_add('2019-12-30', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da >= date_add('2019-12-30', INTERVAL 1 DAY)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE ti < date_add('2019-12-30', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da < date_add('2019-12-30', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE ti > date_sub('2020-01-01', INTERVAL 1 DAY)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE da > date_sub('2020-01-01', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da >= date_sub('2020-01-01', INTERVAL 1 DAY)",
		[]sql.Row{{int64(1)}},
	},
	{
		"SELECT id FROM typestable WHERE ti < date_sub('2020-01-01', INTERVAL 1 DAY)",
		nil,
	},
	{
		"SELECT id FROM typestable WHERE da < date_sub('2020-01-01', INTERVAL 1 DAY)",
		nil,
	},	{
		"SELECT * from stringandtable WHERE i",
		[]sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT * from stringandtable WHERE i AND i",
		[]sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT * from stringandtable WHERE i OR i",
		[]sql.Row{
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT * from stringandtable WHERE NOT i",
		[]sql.Row{{int64(0), "0"}},
	},
	{
		"SELECT * from stringandtable WHERE NOT i AND NOT i",
		[]sql.Row{{int64(0), "0"}},
	},
	{
		"SELECT * from stringandtable WHERE NOT i OR NOT i",
		[]sql.Row{{int64(0), "0"}},
	},
	{
		"SELECT * from stringandtable WHERE i OR NOT i",
		[]sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{int64(5), nil},
		},
	},
	{
		"SELECT * from stringandtable WHERE v",
		[]sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		"SELECT * from stringandtable WHERE v AND v",
		[]sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		"SELECT * from stringandtable WHERE v OR v",
		[]sql.Row{{int64(1), "1"}, {nil, "2"}},
	},
	{
		"SELECT * from stringandtable WHERE NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		"SELECT * from stringandtable WHERE NOT v AND NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		"SELECT * from stringandtable WHERE NOT v OR NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
		},
	},
	{
		"SELECT * from stringandtable WHERE v OR NOT v",
		[]sql.Row{
			{int64(0), "0"},
			{int64(1), "1"},
			{int64(2), ""},
			{int64(3), "true"},
			{int64(4), "false"},
			{nil, "2"},
		},
	},
	{
		"SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1",
		[]sql.Row{
			{"third"},
			{"secon"},
			{"first"},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	{
		"SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2 ORDER BY i",
		[]sql.Row{
			{"third", int64(1), int64(1)},
			{"second", int64(2), int64(2)},
			{ "first", int64(3), int64(3)},
		},
	},
	{
		"SELECT i, i2, s2 FROM othertable JOIN mytable  ON i = i2 ORDER BY i",
		[]sql.Row{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	},
	{
		"SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2 ORDER BY i",
		[]sql.Row{
			{"third", int64(1), int64(1)},
			{"second", int64(2), int64(2)},
			{ "first", int64(3), int64(3)},
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
				"ON substring(s2, 1, 2) != '' AND i = i2 ORDER BY 1",
		[]sql.Row{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		`SELECT COUNT(*) AS cnt, fi FROM (
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
		`SELECT COUNT(*) AS cnt, fi FROM (
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
		`SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY fi`,
		[]sql.Row{
			{int64(1), "first row"},
			{int64(1), "second row"},
			{int64(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) AS cnt, s AS fi FROM mytable GROUP BY 2`,
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
		`SELECT i AS foo FROM mytable ORDER BY i DESC`,
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY i DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY 2 DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY i ORDER BY foo DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS foo FROM mytable GROUP BY 2 ORDER BY foo DESC`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i AS i FROM mytable GROUP BY 2`,
		[]sql.Row{
			{int64(1), int64(3)},
			{int64(1), int64(2)},
			{int64(1), int64(1)},
		},
	},
	{
		`SELECT i AS i FROM mytable GROUP BY 1`,
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
			{int32(1234567890)},
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
		nil,
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
		"SELECT i FROM mytable UNION SELECT i+10 FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		"SELECT i FROM mytable UNION DISTINCT SELECT i+10 FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(11)}, {int64(12)}, {int64(13)}},
	},
	{
		"SELECT i FROM mytable UNION SELECT i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}, {int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable UNION DISTINCT SELECT i FROM mytable;",
		[]sql.Row{{int64(1)}, {int64(2)}, {int64(3)}},
	},
	{
		"SELECT i FROM mytable UNION SELECT s FROM mytable;",
		[]sql.Row{
			{"1"},
			{"2"},
			{"3"},
			{"first row"},
			{"second row"},
			{"third row"},
		},
	},
	{
		`/*!40101 SET NAMES utf8 */`,
		nil,
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
		`SELECT SUBSTRING(s, -3, 3) AS s FROM mytable WHERE s LIKE '%d row' GROUP BY 1`,
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
			{"i", "BIGINT", "NO", "", "", ""},
			{"s", "TEXT", "NO", "", "", ""},
		},
	},
	{
		`DESCRIBE mytable`,
		[]sql.Row{
			{"i", "BIGINT", "NO", "", "", ""},
			{"s", "TEXT", "NO", "", "", ""},
		},
	},
	{
		`DESC mytable`,
		[]sql.Row{
			{"i", "BIGINT", "NO", "", "", ""},
			{"s", "TEXT", "NO", "", "", ""},
		},
	},
	{
		`SHOW COLUMNS FROM one_pk`,
		[]sql.Row{
			{"pk", "TINYINT", "NO", "PRI", "", ""},
			{"c1", "TINYINT", "NO", "", "", ""},
			{"c2", "TINYINT", "NO", "", "", ""},
			{"c3", "TINYINT", "NO", "", "", ""},
			{"c4", "TINYINT", "NO", "", "", ""},
			{"c5", "TINYINT", "NO", "", "", ""},
		},
	},
	{
		`DESCRIBE one_pk`,
		[]sql.Row{
			{"pk", "TINYINT", "NO", "PRI", "", ""},
			{"c1", "TINYINT", "NO", "", "", ""},
			{"c2", "TINYINT", "NO", "", "", ""},
			{"c3", "TINYINT", "NO", "", "", ""},
			{"c4", "TINYINT", "NO", "", "", ""},
			{"c5", "TINYINT", "NO", "", "", ""},
		},
	},
	{
		`DESC one_pk`,
		[]sql.Row{
			{"pk", "TINYINT", "NO", "PRI", "", ""},
			{"c1", "TINYINT", "NO", "", "", ""},
			{"c2", "TINYINT", "NO", "", "", ""},
			{"c3", "TINYINT", "NO", "", "", ""},
			{"c4", "TINYINT", "NO", "", "", ""},
			{"c5", "TINYINT", "NO", "", "", ""},
		},
	},

	{
		`SHOW COLUMNS FROM mytable WHERE Field = 'i'`,
		[]sql.Row{
			{"i", "BIGINT", "NO", "", "", ""},
		},
	},
	{
		`SHOW COLUMNS FROM mytable LIKE 'i'`,
		[]sql.Row{
			{"i", "BIGINT", "NO", "", "", ""},
		},
	},
	{
		`SHOW FULL COLUMNS FROM mytable`,
		[]sql.Row{
			{"i", "BIGINT", nil, "NO", "", "", "", "", ""},
			{"s", "TEXT", "utf8_bin", "NO", "", "", "", "", ""},
		},
	},
	{
		`SHOW FULL COLUMNS FROM one_pk`,
		[]sql.Row{
			{"pk", "TINYINT", nil, "NO", "PRI", "", "", "", ""},
			{"c1", "TINYINT", nil, "NO", "", "", "", "", ""},
			{"c2", "TINYINT", nil, "NO", "", "", "", "", ""},
			{"c3", "TINYINT", nil, "NO", "", "", "", "", ""},
			{"c4", "TINYINT", nil, "NO", "", "", "", "", ""},
			{"c5", "TINYINT", nil, "NO", "", "", "", "", "column 5"},
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
			{"version", ""},
			{"version_comment", ""},
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
		`SELECT JSON_UNQUOTE('"foo"')`,
		[]sql.Row{{"foo"}},
	},
	{
		`SELECT JSON_UNQUOTE('[1, 2, 3]')`,
		[]sql.Row{{"[1, 2, 3]"}},
	},
	{
		`SELECT JSON_UNQUOTE('"\\t\\u0032"')`,
		[]sql.Row{{"\t2"}},
	},
	{
		`SELECT JSON_UNQUOTE('"\t\\u0032"')`,
		[]sql.Row{{"\t2"}},
	},
	{
		`SELECT CONNECTION_ID()`,
		[]sql.Row{{uint32(1)}},
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
		[]sql.Row{{int8(-1)}},
	},
	{
		`
		SHOW WARNINGS
		`,
		nil,
	},
	{
		`SHOW WARNINGS LIMIT 0`,
		nil,
	},
	{
		`SET SESSION NET_READ_TIMEOUT= 700, SESSION NET_WRITE_TIMEOUT= 700`,
		nil,
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
			{int8(123)},
		},
	},
	{
		`SELECT ifnull(123, NULL)`,
		[]sql.Row{
			{int8(123)},
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
			{int8(123)},
		},
	},
	{
		`SELECT ifnull(123, 123)`,
		[]sql.Row{
			{int8(123)},
		},
	},
	{
		`SELECT ifnull(123, 321)`,
		[]sql.Row{
			{int8(123)},
		},
	},
	{
		"SELECT i FROM mytable WHERE NULL > 10;",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NULL IN (10);",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NULL IN (NULL, NULL);",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT NULL NOT IN (NULL);",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT (NULL) <> 10;",
		nil,
	},
	{
		"SELECT i FROM mytable WHERE NOT NULL <> NULL;",
		nil,
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
			{int8(15)},
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
		`SHOW COLLATION`,
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1)}},
	},
	{
		`SHOW COLLATION LIKE 'foo'`,
		nil,
	},
	{
		`SHOW COLLATION LIKE 'utf8%'`,
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1)}},
	},
	{
		`SHOW COLLATION WHERE charset = 'foo'`,
		nil,
	},
	{
		"SHOW COLLATION WHERE `Default` = 'Yes'",
		[]sql.Row{{"utf8_bin", "utf8mb4", int64(1), "Yes", "Yes", int64(1)}},
	},
	{
		"ROLLBACK",
		nil,
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
				COUNT(s) AS foo
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
		"SELECT substring(mytable.s, 1, 5) AS s FROM mytable INNER JOIN othertable ON (substring(mytable.s, 1, 5) = SUBSTRING(othertable.s2, 1, 5)) GROUP BY 1 HAVING s = \"secon\"",
		[]sql.Row{{"secon"}},
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
		`SELECT t.date_col FROM (SELECT CONVERT('2019-06-06 00:00:00', DATETIME) AS date_col) t WHERE t.date_col > '0000-01-01 00:00:00'`,
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
		`SELECT GREATEST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		[]sql.Row{{time.Date(1980, 6, 22, 14, 32, 56, 0, time.UTC)}},
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
		`SELECT LEAST(CAST("1920-02-03 07:41:11" AS DATETIME), CAST("1980-06-22 14:32:56" AS DATETIME))`,
		[]sql.Row{{time.Date(1920, 2, 3, 7, 41, 11, 0, time.UTC)}},
	},
	{
		"SELECT i, i2, s2 FROM mytable LEFT JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
			{int64(3), nil, nil},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{nil, int64(1), "third"},
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable LEFT OUTER JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
			{int64(3), nil, nil},
		},
	},
	{
		"SELECT i, i2, s2 FROM mytable RIGHT OUTER JOIN othertable ON i = i2 - 1",
		[]sql.Row{
			{nil, int64(1), "third"},
			{int64(1), int64(2), "second"},
			{int64(2), int64(3), "first"},
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
	{
		"SELECT i FROM mytable WHERE NOT s ORDER BY 1 DESC",
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		"SELECT i FROM mytable WHERE NOT(NOT i) ORDER BY 1 DESC",
		[]sql.Row{
			{int64(3)},
			{int64(2)},
			{int64(1)},
		},
	},
	{
		`SELECT NOW() - NOW()`,
		[]sql.Row{{int64(0)}},
	},
	{
		`SELECT NOW() - (NOW() - INTERVAL 1 SECOND)`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT SUBSTR(SUBSTRING('0123456789ABCDEF', 1, 10), -4)`,
		[]sql.Row{{"6789"}},
	},
	{
		`SELECT CASE i WHEN 1 THEN i ELSE NULL END FROM mytable`,
		[]sql.Row{{int64(1)}, {nil}, {nil}},
	},
	{
		`SELECT (NULL+1)`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT ARRAY_LENGTH(null)`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT ARRAY_LENGTH("foo")`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT * FROM mytable WHERE NULL AND i = 3`,
		nil,
	},
	{
		`SELECT 1 FROM mytable GROUP BY i HAVING i > 1`,
		[]sql.Row{{int8(1)}, {int8(1)}},
	},
	{
		`SELECT avg(i) FROM mytable GROUP BY i HAVING avg(i) > 1`,
		[]sql.Row{{float64(2)}, {float64(3)}},
	},
	{
		`SELECT s AS s, COUNT(*) AS count,  AVG(i) AS ` + "`AVG(i)`" + `
		FROM  (
			SELECT * FROM mytable
		) AS expr_qry
		GROUP BY s
		HAVING ((AVG(i) > 0))
		ORDER BY count DESC
		LIMIT 10000`,
		[]sql.Row{
			{"first row", int64(1), float64(1)},
			{"second row", int64(1), float64(2)},
			{"third row", int64(1), float64(3)},
		},
	},
	{
		`SELECT FIRST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT LAST(i) FROM (SELECT i FROM mytable ORDER BY i) t`,
		[]sql.Row{{int64(3)}},
	},
	{
		`SELECT COUNT(DISTINCT t.i) FROM tabletest t, mytable t2`,
		[]sql.Row{{int64(3)}},
	},
	{
		`SELECT CASE WHEN NULL THEN "yes" ELSE "no" END AS test`,
		[]sql.Row{{"no"}},
	},
	{
		`SELECT
			table_schema,
			table_name,
			CASE
				WHEN table_type = 'BASE TABLE' THEN
					CASE
						WHEN table_schema = 'mysql'
							OR table_schema = 'performance_schema' THEN 'SYSTEM TABLE'
						ELSE 'TABLE'
					END
				WHEN table_type = 'TEMPORARY' THEN 'LOCAL_TEMPORARY'
				ELSE table_type
			END AS TABLE_TYPE
		FROM information_schema.tables
		WHERE table_schema = 'mydb'
			AND table_name = 'mytable'
		HAVING table_type IN ('TABLE', 'VIEW')
		ORDER BY table_type, table_schema, table_name`,
		[]sql.Row{{"mydb", "mytable", "TABLE"}},
	},
	{
		`SELECT REGEXP_MATCHES("bopbeepbop", "bop")`,
		[]sql.Row{{[]interface{}{"bop", "bop"}}},
	},
	{
		`SELECT EXPLODE(REGEXP_MATCHES("bopbeepbop", "bop"))`,
		[]sql.Row{{"bop"}, {"bop"}},
	},
	{
		`SELECT EXPLODE(REGEXP_MATCHES("helloworld", "bop"))`,
		nil,
	},
	{
		`SELECT EXPLODE(REGEXP_MATCHES("", ""))`,
		[]sql.Row{{""}},
	},
	{
		`SELECT REGEXP_MATCHES(NULL, "")`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT REGEXP_MATCHES("", NULL)`,
		[]sql.Row{{nil}},
	},
	{
		`SELECT REGEXP_MATCHES("", "", NULL)`,
		[]sql.Row{{nil}},
	},
	{
		"SELECT * FROM newlinetable WHERE s LIKE '%text%'",
		[]sql.Row{
			{int64(1), "\nthere is some text in here"},
			{int64(2), "there is some\ntext in here"},
			{int64(3), "there is some text\nin here"},
			{int64(4), "there is some text in here\n"},
			{int64(5), "there is some text in here"},
		},
	},
	{
		`SELECT i FROM mytable WHERE i = (SELECT 1)`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT i FROM mytable WHERE i IN (SELECT i FROM mytable)`,
		[]sql.Row{
			{int64(1)},
			{int64(2)},
			{int64(3)},
		},
	},
	{
		`SELECT i FROM mytable WHERE i NOT IN (SELECT i FROM mytable ORDER BY i ASC LIMIT 2)`,
		[]sql.Row{
			{int64(3)},
		},
	},
	{
		`SELECT (SELECT i FROM mytable ORDER BY i ASC LIMIT 1) AS x`,
		[]sql.Row{{int64(1)}},
	},
	{
		`SELECT DISTINCT n FROM bigtable ORDER BY t`,
		[]sql.Row{
			{int64(1)},
			{int64(9)},
			{int64(7)},
			{int64(3)},
			{int64(2)},
			{int64(8)},
			{int64(6)},
			{int64(5)},
			{int64(4)},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk, two_pk ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 0},
			{0, 1, 1},
			{1, 0, 0},
			{1, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, 0, 0},
			{2, 0, 1},
			{2, 1, 0},
			{2, 1, 1},
			{3, 0, 0},
			{3, 0, 1},
			{3, 1, 0},
			{3, 1, 1},
		},
	},
	{
		"SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 AND pk2=1 ORDER BY 1,2",
		[]sql.Row{
			{0, 30},
			{10, 30},
			{20, 30},
			{30, 30},
		},
	},
	{
		"SELECT t1.c1,t2.c2 FROM one_pk t1, two_pk t2 WHERE pk1=1 OR pk2=1 ORDER BY 1,2",
		[]sql.Row{
			{0, 10},
			{0, 20},
			{0, 30},
			{10, 10},
			{10, 20},
			{10, 30},
			{20, 10},
			{20, 20},
			{20, 30},
			{30, 10},
			{30, 20},
			{30, 30},
		},
	},
	{
		"SELECT pk,pk2 FROM one_pk t1, two_pk t2 WHERE pk=1 AND pk2=1 ORDER BY 1,2",
		[]sql.Row{
			{1, 1},
			{1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE pk=0 AND pk1=0 OR pk2=1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 1},
			{1, 0, 1},
			{1, 1, 1},
			{2, 0, 1},
			{2, 1, 1},
			{3, 0, 1},
			{3, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk,two_pk WHERE one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 0, 1},
			{2, 1, 0},
			{3, 1, 1},
		},
	},
	{
		"SELECT one_pk.c5,pk1,pk2 FROM one_pk,two_pk WHERE pk=pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{10, 1, 0},
			{10, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE pk=1 ORDER BY 1,2,3",
		[]sql.Row{
			{1, 0, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{1, 1, 1},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{nil, 0, 1},
			{nil, 1, 0},
			{0, 0, 0},
			{1, 1, 1},
		},
	},
	{
		"SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2 ORDER BY 1,2,3",
		[]sql.Row{
			{int64(2), 1, 0},
		},
	},
	// TODO: this is broken, we can't join a table to itself and i'm so angry
	// {
	// 	"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
	// 	[]sql.Row{
	// 		{0, 0, 0, 0},
	// 		{0, 1, 1, 0},
	// 		{1, 0, 0, 1},
	// 		{1, 1, 1, 1},
	// 	},
	// },
	// TODO: this is broken, can't resolve the columns despite the aliases:
	//  ambiguous column name "pk1", it's present in all these tables: two_pk, two_pk2
	// {
	// 	"SELECT a.pk1,a.pk2,b.pk1,b.pk2 FROM two_pk a JOIN two_pk2 b ON a.pk1=b.pk2 AND a.pk2=b.pk1 ORDER BY 1,2,3",
	// 	[]sql.Row{
	// 		{0, 0, 0, 0},
	// 		{0, 1, 1, 0},
	// 		{1, 0, 0, 1},
	// 		{1, 1, 1, 1},
	// 	},
	// },
	{
		"SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, nil, nil},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i ORDER BY 1",
		[]sql.Row{
			{0, nil, nil},
			{1, int64(1), float64(1.0)},
			{2, int64(2), float64(2.0)},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i ORDER BY 2,3",
		[]sql.Row{
			{nil, nil, nil},
			{nil, nil, float64(3.0)},
			{1, int64(1), float64(1.0)},
			{2, int64(2), float64(2.0)},
			{nil, int64(4), nil},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL ORDER BY 1", // NOT NULL clause in join condition is ignored
		[]sql.Row{
			{0, nil, nil},
			{1, int64(1), float64(1.0)},
			{2, int64(2), float64(2.0)},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0 ORDER BY 2,3", // > 0 clause in join condition is ignored
		[]sql.Row{
			{nil, nil, nil},
			{nil, nil, float64(3.0)},
			{1, int64(1), float64(1.0)},
			{2, int64(2), float64(2.0)},
			{nil, int64(4), nil},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 1",
		[]sql.Row{
			{1, int64(1), float64(1.0)},
			{2, int64(2), float64(2.0)},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL ORDER BY 2,3",
		[]sql.Row{
			{nil, nil, float64(3.0)},
			{1, int64(1), float64(1.0)},
			{2, int64(2), float64(2.0)},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1 ORDER BY 1",
		[]sql.Row{
			{2, int64(2), float64(2.0)},
			{3, nil, nil},
		},
	},
	{
		"SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0 ORDER BY 2,3",
		[]sql.Row{
			{1, int64(1), float64(1.0)},
			{2, int64(2), float64(2.0)},
		},
	},
	{
		"SELECT GREATEST(CAST(i AS CHAR), CAST(b AS CHAR)) FROM niltable",
		[]sql.Row{
			{nil},
			{"true"},
			{nil},
			{nil},
			{"true"},
		},
	},
	{
		"SELECT pk,pk1,pk2,one_pk.c1 AS foo, two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0, 0, 0},
			{1, 0, 1, 10, 10},
			{2, 1, 0, 20, 20},
			{3, 1, 1, 30, 30},
		},
	},
	{
		"SELECT pk,pk1,pk2,one_pk.c1 AS foo,two_pk.c1 AS bar FROM one_pk JOIN two_pk ON one_pk.c1=two_pk.c1 WHERE one_pk.c1=10",
		[]sql.Row{
			{1, 0, 1, 10, 10},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk1-pk>0 AND pk2<1",
		[]sql.Row{
			{0, 1, 0},
		},
	},
	{
		"SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ORDER BY 1,2,3",
		[]sql.Row{
			{0, 0, 0},
			{0, 0, 1},
			{0, 1, 0},
			{0, 1, 1},
			{1, 0, 0},
			{1, 0, 1},
			{1, 1, 0},
			{1, 1, 1},
			{2, 0, 0},
			{2, 0, 1},
			{2, 1, 0},
			{2, 1, 1},
			{3, 0, 0},
			{3, 0, 1},
			{3, 1, 0},
			{3, 1, 1},
		},
	},
	{
		"SELECT 2.0 + CAST(5 AS DECIMAL)",
		[]sql.Row{{float64(7)}},
	},
}

var infoSchemaQueries = []queryTest {
	{
		`SHOW TABLE STATUS FROM mydb`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"typestable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		`SHOW TABLE STATUS LIKE '%table'`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"typestable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
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
		`SHOW TABLE STATUS`,
		[]sql.Row{
			{"mytable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"othertable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"tabletest", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"bigtable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"floattable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"niltable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"newlinetable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
			{"typestable", "InnoDB", "10", "Fixed", int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, "utf8_bin", nil, nil},
		},
	},
	{
		"SHOW TABLES",
		[]sql.Row{
			{"mytable"},
			{"othertable"},
			{"tabletest"},
			{"bigtable"},
			{"floattable"},
			{"niltable"},
			{"newlinetable"},
			{"typestable"},
		},

	},
	{
		"SHOW FULL TABLES",
		[]sql.Row{
			{"mytable", "BASE TABLE"},
			{"othertable", "BASE TABLE"},
			{"tabletest", "BASE TABLE"},
			{"bigtable", "BASE TABLE"},
			{"floattable", "BASE TABLE"},
			{"niltable", "BASE TABLE"},
			{"newlinetable", "BASE TABLE"},
			{"typestable", "BASE TABLE"},
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
			{"floattable"},
			{"niltable"},
			{"newlinetable"},
			{"typestable"},
		},
	},
	{
		"SHOW TABLES WHERE `Table` = 'mytable'",
		[]sql.Row{
			{"mytable"},
		},
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
		nil,
	},
	{
		`
		SELECT DISTINCT
			TABLESPACE_NAME, FILE_NAME, LOGFILE_GROUP_NAME, EXTENT_SIZE, INITIAL_SIZE, ENGINE
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'DATAFILE'
		ORDER BY TABLESPACE_NAME, LOGFILE_GROUP_NAME
		`,
		nil,
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
		nil,
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
			{"floattable"},
			{"niltable"},
			{"newlinetable"},
			{"typestable"},
		},
	},
	{
		`
		SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='mytable'
		`,
		[]sql.Row{
			{"s", "text"},
			{"i", "bigint"},
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
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
			{"id"},
			{"i8"},
			{"i16"},
			{"i32"},
			{"i64"},
			{"u8"},
			{"u16"},
			{"u32"},
			{"u64"},
			{"ti"},
			{"da"},
			{"te"},
			{"bo"},
			{"js"},
			{"bl"},
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
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
			{"id"},
			{"i8"},
			{"i16"},
			{"i32"},
			{"i64"},
			{"u8"},
			{"u16"},
			{"u32"},
			{"u64"},
			{"ti"},
			{"da"},
			{"te"},
			{"bo"},
			{"js"},
			{"bl"},
		},
	},
	{
		`
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
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
			{"f32"},
			{"f64"},
			{"b"},
			{"f"},
			{"id"},
			{"i8"},
			{"i16"},
			{"i32"},
			{"i64"},
			{"u8"},
			{"u16"},
			{"u32"},
			{"u64"},
			{"ti"},
			{"da"},
			{"te"},
			{"bo"},
			{"js"},
			{"bl"},
		},
	},
	{
		`
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1 HAVING SUBSTRING(COLUMN_NAME, 1, 1) = "s"
		`,
		[]sql.Row{{"s"}, {"s2"}},
	},
}

// Set to a query to run only tests for that query
var debugQuery = ""

func TestQueries(t *testing.T) {

	type indexDriverInitalizer func(map[string]sql.Table) sql.IndexDriver
	type indexDriverTestCase struct {
		name string
		initializer indexDriverInitalizer
	}

	var numPartitionsVals []int
	var indexDrivers []*indexDriverTestCase
	var parallelVals []int

	// Test all queries with these combinations, for a total of 12 runs:
	// 1) Partitioned tables / non partitioned tables
	// 2) Mergeable / unmergeable / no indexes
	// 3) Parallelism on / off
	if debugQuery == "" {
		numPartitionsVals = []int{
			1,
			testNumPartitions,
		}
		indexDrivers = []*indexDriverTestCase{
			nil,
			{"unmergableIndexes", unmergableIndexDriver},
			{"mergableIndexes", mergableIndexDriver},
		}
		parallelVals = []int{
			1,
			2,
		}
	} else {
		numPartitionsVals = []int{ 1 }
		indexDrivers = []*indexDriverTestCase{{"unmergableIndexes", unmergableIndexDriver}}
		parallelVals = []int{ 1 }
	}

	for _, numPartitions := range numPartitionsVals {
		for _, indexDriverInit := range indexDrivers {
			for _, parallelism := range parallelVals {
				tables := allTestTables(t, numPartitions)

				var indexDriver sql.IndexDriver
				if indexDriverInit != nil {
					indexDriver = indexDriverInit.initializer(tables)
				}
				engine := newEngineWithParallelism(t, parallelism, tables, indexDriver)

				indexDriverName := "none"
				if indexDriverInit != nil {
					indexDriverName = indexDriverInit.name
				}
				testName := fmt.Sprintf("partitions=%d,indexes=%v,parallelism=%v", numPartitions, indexDriverName, parallelism)

				t.Run(testName, func(t *testing.T) {
					for _, tt := range queries {
						if debugQuery != "" && debugQuery != tt.query {
							t.Log("Skipping query in debug mode:", tt.query)
							continue
						}
						testQuery(t, engine, tt.query, tt.expected)
					}
				})
			}
		}
	}
}

var infoSchemaTables = []string {
	"mytable",
	"othertable",
	"tabletest",
	"bigtable",
	"floattable",
	"niltable",
	"newlinetable",
	"typestable",
	"other_table",
}

// Test the info schema queries separately to avoid having to alter test query results when more test tables are added.
// To get this effect, we only install a fixed subset of the tables defined by allTestTables().
func TestInfoSchema(t *testing.T) {
	tables := allTestTables(t, 1)
	reducedTables := make(map[string]sql.Table)

	for _, table := range infoSchemaTables {
		reducedTables[table] = tables[table]
	}

	engine := newEngineWithParallelism(t, 1, reducedTables, nil)
	for _, tt := range infoSchemaQueries {
		testQuery(t, engine, tt.query, tt.expected)
	}
}

func unmergableIndexDriver(tables map[string]sql.Table) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.Index{
		"mytable": {
			newUnmergableIndex(tables, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false)),
			newUnmergableIndex(tables, "mytable",
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
			newUnmergableIndex(tables, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
		},
		"othertable": {
			newUnmergableIndex(tables, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false)),
			newUnmergableIndex(tables, "othertable",
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
			newUnmergableIndex(tables, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
		},
		"bigtable": {
			newUnmergableIndex(tables, "bigtable",
				expression.NewGetFieldWithTable(0, sql.Text, "bigtable", "t", false)),
		},
		"floattable": {
			newUnmergableIndex(tables, "floattable",
				expression.NewGetFieldWithTable(2, sql.Text, "floattable", "f64", false)),
		},
		"niltable": {
			newUnmergableIndex(tables, "niltable",
				expression.NewGetFieldWithTable(0, sql.Int64, "niltable", "i", false)),
		},
		"one_pk": {
				newUnmergableIndex(tables, "one_pk",
					expression.NewGetFieldWithTable(0, sql.Int8, "one_pk", "pk", false)),
		},
		"two_pk": {
			newUnmergableIndex(tables, "two_pk",
				expression.NewGetFieldWithTable(0, sql.Int8, "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, sql.Int8, "two_pk", "pk2", false),
			),
		},
	})
}

func mergableIndexDriver(tables map[string]sql.Table) sql.IndexDriver {
	return memory.NewIndexDriver("mydb", map[string][]sql.Index{
		"mytable": {
			newMergableIndex(tables, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false)),
			newMergableIndex(tables, "mytable",
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
			newMergableIndex(tables, "mytable",
				expression.NewGetFieldWithTable(0, sql.Int64, "mytable", "i", false),
				expression.NewGetFieldWithTable(1, sql.Text, "mytable", "s", false)),
		},
		"othertable": {
			newMergableIndex(tables, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false)),
			newMergableIndex(tables, "othertable",
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
			newMergableIndex(tables, "othertable",
				expression.NewGetFieldWithTable(0, sql.Text, "othertable", "s2", false),
				expression.NewGetFieldWithTable(1, sql.Text, "othertable", "i2", false)),
		},
		"bigtable": {
			newMergableIndex(tables, "bigtable",
				expression.NewGetFieldWithTable(0, sql.Text, "bigtable", "t", false)),
		},
		"floattable": {
			newMergableIndex(tables, "floattable",
				expression.NewGetFieldWithTable(2, sql.Text, "floattable", "f64", false)),
		},
		"niltable": {
			newMergableIndex(tables, "niltable",
				expression.NewGetFieldWithTable(0, sql.Int64, "niltable", "i", false)),
		},
		"one_pk": {
			newMergableIndex(tables, "one_pk",
				expression.NewGetFieldWithTable(0, sql.Int8, "one_pk", "pk", false)),
		},
		"two_pk": {
			newMergableIndex(tables, "two_pk",
				expression.NewGetFieldWithTable(0, sql.Int8, "two_pk", "pk1", false),
				expression.NewGetFieldWithTable(1, sql.Int8, "two_pk", "pk2", false),
			),
		},
	})
}

func newUnmergableIndex(tables map[string]sql.Table, tableName string, exprs ...sql.Expression) *memory.UnmergeableIndex {
	table := tables[tableName]
	switch table := table.(type) {
	case *memory.Table:
		return &memory.UnmergeableIndex{
			DB:         "mydb",
			DriverName: memory.IndexDriverId,
			TableName:  tableName,
			Tbl:        table,
			Exprs:      exprs,
		}
	default:
		panic("unknown table type")
	}
}

func newMergableIndex(tables map[string]sql.Table, tableName string, exprs ...sql.Expression) *memory.MergeableIndex {
	table := tables[tableName]
	switch table := table.(type) {
	case *memory.Table:
		return &memory.MergeableIndex{
			DB:         "mydb",
			DriverName: memory.IndexDriverId,
			TableName:  tableName,
			Tbl:        table,
			Exprs:      exprs,
		}
	default:
		panic("unknown table type")
	}
}

type planTest struct {
	query string
	expected string
}

var planTests = []planTest{
	{
		query:    "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2",
		expected: "IndexedJoin(mytable.i = othertable.i2)\n" +
			" ├─ Table(mytable): Projected \n" +
			" └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i = i2",
		expected: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(mytable): Projected \n" +
			"     └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM othertable JOIN mytable ON i = i2",
		expected: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(mytable.i = othertable.i2)\n" +
			"     ├─ Table(othertable): Projected \n" +
			"     └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM othertable JOIN mytable ON i = i2",
		expected: "IndexedJoin(mytable.i = othertable.i2)\n" +
			" ├─ Table(othertable): Projected \n" +
			" └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i2 = i",
		expected: "IndexedJoin(othertable.i2 = mytable.i)\n" +
			" ├─ Table(mytable): Projected \n" +
			" └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM mytable INNER JOIN othertable ON i2 = i",
		expected: "Project(othertable.s2, othertable.i2, mytable.i)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(mytable): Projected \n" +
			"     └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM othertable JOIN mytable ON i2 = i",
		expected: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ IndexedJoin(othertable.i2 = mytable.i)\n" +
			"     ├─ Table(othertable): Projected \n" +
			"     └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT s2, i2, i FROM othertable JOIN mytable ON i2 = i",
		expected: "IndexedJoin(othertable.i2 = mytable.i)\n" +
			" ├─ Table(othertable): Projected \n" +
			" └─ Table(mytable): Projected \n" +
			"",
	},
	{
		query:    "SELECT * FROM mytable mt INNER JOIN othertable ot ON mt.i = ot.i2 AND mt.i > 2",
		expected: "IndexedJoin(mytable.i = othertable.i2)\n" +
			" ├─ TableAlias(mt)\n" +
			" │   └─ Table(mytable): Projected Filtered \n" +
			" └─ TableAlias(ot)\n" +
			"     └─ Table(othertable): Projected \n" +
			"",
	},
	{
		query:    "SELECT i, i2, s2 FROM mytable RIGHT JOIN othertable ON i = i2 - 1",
		expected: "Project(mytable.i, othertable.i2, othertable.s2)\n" +
			" └─ RightIndexedJoin(mytable.i = othertable.i2 - 1)\n" +
			"     ├─ Table(othertable)\n" +
			"     └─ Table(mytable)\n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		expected: "IndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			" ├─ Table(one_pk): Projected \n" +
			" └─ Table(two_pk): Projected \n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk RIGHT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ RightIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(two_pk)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT i,pk1,pk2 FROM mytable JOIN two_pk ON i-1=pk1 AND i-2=pk2",
		expected: "IndexedJoin(mytable.i - 1 = two_pk.pk1 AND mytable.i - 2 = two_pk.pk2)\n" +
			" ├─ Table(mytable): Projected \n" +
			" └─ Table(two_pk): Projected \n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON pk=pk1",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(niltable)\n" +
			"     └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i AND f IS NOT NULL",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftJoin(one_pk.pk = niltable.i AND NOT(niltable.f IS NULL))\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i and pk > 0",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ RightJoin(one_pk.pk = niltable.i AND one_pk.pk > 0)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE f IS NOT NULL",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE f IS NOT NULL",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(NOT(niltable.f IS NULL))\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i WHERE pk > 1",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 1)\n" +
			"     └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(one_pk)\n" +
			"         └─ Table(niltable)\n" +
			"",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i WHERE pk > 0",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ Filter(one_pk.pk > 0)\n" +
			"     └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
			"         ├─ Table(niltable)\n" +
			"         └─ Table(one_pk)\n" +
			"",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk LEFT JOIN two_pk ON one_pk.pk=two_pk.pk1 AND one_pk.pk=two_pk.pk2",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = two_pk.pk1 AND one_pk.pk = two_pk.pk2)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(two_pk)\n",
	},
	{
		query:    "SELECT pk,pk1,pk2 FROM one_pk JOIN two_pk ON pk=pk1",
		expected: "Project(one_pk.pk, two_pk.pk1, two_pk.pk2)\n" +
			" └─ IndexedJoin(one_pk.pk = two_pk.pk1)\n" +
			"     ├─ Table(two_pk): Projected \n" +
			"     └─ Table(one_pk): Projected \n",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk LEFT JOIN niltable ON pk=i",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
			" └─ LeftIndexedJoin(one_pk.pk = niltable.i)\n" +
			"     ├─ Table(one_pk)\n" +
			"     └─ Table(niltable)\n",
	},
	{
		query:    "SELECT pk,i,f FROM one_pk RIGHT JOIN niltable ON pk=i",
		expected: "Project(one_pk.pk, niltable.i, niltable.f)\n" +
				" └─ RightIndexedJoin(one_pk.pk = niltable.i)\n" +
				"     ├─ Table(niltable)\n" +
				"     └─ Table(one_pk)\n",
	},
}

// Tests of choosing the correct execution plan independent of result correctness. Mostly useful for confirming that
// the right indexes are being used for joining tables.
func TestQueryPlans(t *testing.T) {
	tables := allTestTables(t, 2)

	indexDriver := unmergableIndexDriver(tables)
	engine := newEngineWithParallelism(t, 1, tables, indexDriver)

	for _, tt := range planTests {
		t.Run(tt.query, func(t *testing.T) {
			ctx := sql.NewEmptyContext()

			parsed, err := parse.Parse(ctx, tt.query)
			require.NoError(t, err)

			node, err := engine.Analyzer.Analyze(ctx, parsed)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, extractQueryNode(node).String())
		})
	}
}

func extractQueryNode(node sql.Node) sql.Node {
	switch node := node.(type) {
	case *plan.QueryProcess:
		return extractQueryNode(node.Child)
	case *analyzer.Releaser:
		return extractQueryNode(node.Child)
	default:
		return node
	}
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
			nil,
		},
	}

	e := newEngine(t)
	ep := newEngineWithParallelism(t, 2, allTestTables(t, testNumPartitions), nil)

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

	ep := newEngineWithParallelism(t, 2, allTestTables(t, testNumPartitions), nil)

	query := `DESCRIBE FORMAT=TREE SELECT * FROM mytable`
	expectedSeq := []sql.Row{
		sql.NewRow("Table(mytable): Projected "),
	}

	expectedParallel := []sql.Row{
		{"Exchange(parallelism=2)"},
		{" └─ Table(mytable): Projected "},
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
	var insertions = []struct {
		insertQuery    string
		expectedInsert []sql.Row
		selectQuery    string
		expectedSelect []sql.Row
	}{
		{
			"INSERT INTO mytable (s, i) VALUES ('x', 999);",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"INSERT INTO niltable (f) VALUES (10.0), (12.0);",
			[]sql.Row{{int64(2)}},
			"SELECT f FROM niltable WHERE f IN (10.0, 12.0) ORDER BY f;",
			[]sql.Row{{10.0}, {12.0}},
		},
		{
			"INSERT INTO mytable SET s = 'x', i = 999;",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"INSERT INTO mytable VALUES (999, 'x');",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"INSERT INTO mytable SET i = 999, s = 'x';",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			`INSERT INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata'
			);`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`INSERT INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata'
			;`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`INSERT INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '', ''
			);`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`INSERT INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '', bl = ''
			;`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`INSERT INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null);`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			`INSERT INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null;`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			"INSERT INTO mytable SELECT * FROM mytable",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable(i,s) SELECT * FROM mytable",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable (i,s) SELECT i+10, 'new' FROM mytable",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable ORDER BY i",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(11), "new"},
				{int64(12), "new"},
				{int64(13), "new"},
			},
		},
		{
			"INSERT INTO mytable SELECT i2, s2 FROM othertable",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable ORDER BY i,s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "third"},
				{int64(2), "second"},
				{int64(2), "second row"},
				{int64(3), "first"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable (s,i) SELECT * FROM othertable",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable ORDER BY i,s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "third"},
				{int64(2), "second"},
				{int64(2), "second row"},
				{int64(3), "first"},
				{int64(3), "third row"},
			},
		},
		{
			"INSERT INTO mytable (s,i) SELECT concat(m.s, o.s2), m.i FROM othertable o JOIN mytable m ON m.i=o.i2",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable ORDER BY i,s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(1), "first rowthird"},
				{int64(2), "second row"},
				{int64(2), "second rowsecond"},
				{int64(3), "third row"},
				{int64(3), "third rowfirst"},
			},
		},
		{
			"INSERT INTO mytable (i,s) SELECT (i + 10.0) / 10.0 + 10, concat(s, ' new') FROM mytable",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable ORDER BY i, s",
			[]sql.Row{
				{int64(1), "first row"},
				{int64(2), "second row"},
				{int64(3), "third row"},
				{int64(11), "first row new"},
				{int64(11), "second row new"},
				{int64(11), "third row new"},
			},
		},
	}

	for _, insertion := range insertions {
		e := newEngine(t)
		ctx := newCtx()
		testQueryWithContext(ctx, t, e, insertion.insertQuery, insertion.expectedInsert)
		testQueryWithContext(ctx, t, e, insertion.selectQuery, insertion.expectedSelect)
	}
}

func TestInsertIntoErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"too few values",
			"INSERT INTO mytable (s, i) VALUES ('x');",
		},
		{
			"too many values one column",
			"INSERT INTO mytable (s) VALUES ('x', 999);",
		},
		{
			"too many values two columns",
			"INSERT INTO mytable (i, s) VALUES (999, 'x', 'y');",
		},
		{
			"too few values no columns specified",
			"INSERT INTO mytable VALUES (999);",
		},
		{
			"too many values no columns specified",
			"INSERT INTO mytable VALUES (999, 'x', 'y');",
		},
		{
			"non-existent column values",
			"INSERT INTO mytable (i, s, z) VALUES (999, 'x', 999);",
		},
		{
			"non-existent column set",
			"INSERT INTO mytable SET i = 999, s = 'x', z = 999;",
		},
		{
			"duplicate column",
			"INSERT INTO mytable (i, s, s) VALUES (999, 'x', 'x');",
		},
		{
			"duplicate column set",
			"INSERT INTO mytable SET i = 999, s = 'y', s = 'y';",
		},
		{
			"null given to non-nullable",
			"INSERT INTO mytable (i, s) VALUES (null, 'y');",
		},
		{
			"incompatible types",
			"INSERT INTO mytable (i, s) select * FROM othertable",
		},
		{
			"column count mismatch in select",
			"INSERT INTO mytable (i) select * FROM othertable",
		},
		{
			"column count mismatch in select",
			"INSERT INTO mytable select s FROM othertable",
		},
		{
			"column count mismatch in join select",
			"INSERT INTO mytable (s,i) SELECT * FROM othertable o JOIN mytable m ON m.i=o.i2",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			_, _, err := newEngine(t).Query(newCtx(), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

func TestReplaceInto(t *testing.T) {
	var insertions = []struct {
		replaceQuery    string
		expectedReplace []sql.Row
		selectQuery     string
		expectedSelect  []sql.Row
	}{
		{
			"REPLACE INTO mytable VALUES (1, 'first row');",
			[]sql.Row{{int64(2)}},
			"SELECT s FROM mytable WHERE i = 1;",
			[]sql.Row{{"first row"}},
		},
		{
			"REPLACE INTO mytable SET i = 1, s = 'first row';",
			[]sql.Row{{int64(2)}},
			"SELECT s FROM mytable WHERE i = 1;",
			[]sql.Row{{"first row"}},
		},
		{
			"REPLACE INTO mytable VALUES (1, 'new row same i');",
			[]sql.Row{{int64(1)}},
			"SELECT s FROM mytable WHERE i = 1;",
			[]sql.Row{{"first row"}, {"new row same i"}},
		},
		{
			"REPLACE INTO mytable (s, i) VALUES ('x', 999);",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"REPLACE INTO mytable SET s = 'x', i = 999;",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"REPLACE INTO mytable VALUES (999, 'x');",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			"REPLACE INTO mytable SET i = 999, s = 'x';",
			[]sql.Row{{int64(1)}},
			"SELECT i FROM mytable WHERE s = 'x';",
			[]sql.Row{{int64(999)}},
		},
		{
			`REPLACE INTO typestable VALUES (
			999, 127, 32767, 2147483647, 9223372036854775807,
			255, 65535, 4294967295, 18446744073709551615,
			3.40282346638528859811704183484516925440e+38, 1.797693134862315708145274237317043567981e+308,
			'2037-04-05 12:51:36', '2231-11-07',
			'random text', true, '{"key":"value"}', 'blobdata'
			);`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`REPLACE INTO typestable SET
			id = 999, i8 = 127, i16 = 32767, i32 = 2147483647, i64 = 9223372036854775807,
			u8 = 255, u16 = 65535, u32 = 4294967295, u64 = 18446744073709551615,
			f32 = 3.40282346638528859811704183484516925440e+38, f64 = 1.797693134862315708145274237317043567981e+308,
			ti = '2037-04-05 12:51:36', da = '2231-11-07',
			te = 'random text', bo = true, js = '{"key":"value"}', bl = 'blobdata'
			;`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(math.MaxInt8), int16(math.MaxInt16), int32(math.MaxInt32), int64(math.MaxInt64),
				uint8(math.MaxUint8), uint16(math.MaxUint16), uint32(math.MaxUint32), uint64(math.MaxUint64),
				float32(math.MaxFloat32), float64(math.MaxFloat64),
				sql.Timestamp.MustConvert("2037-04-05 12:51:36"), sql.Date.MustConvert("2231-11-07"),
				"random text", sql.True, ([]byte)(`{"key":"value"}`), "blobdata",
			}},
		},
		{
			`REPLACE INTO typestable VALUES (
			999, -128, -32768, -2147483648, -9223372036854775808,
			0, 0, 0, 0,
			1.401298464324817070923729583289916131280e-45, 4.940656458412465441765687928682213723651e-324,
			'0000-00-00 00:00:00', '0000-00-00',
			'', false, '', ''
			);`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`REPLACE INTO typestable SET
			id = 999, i8 = -128, i16 = -32768, i32 = -2147483648, i64 = -9223372036854775808,
			u8 = 0, u16 = 0, u32 = 0, u64 = 0,
			f32 = 1.401298464324817070923729583289916131280e-45, f64 = 4.940656458412465441765687928682213723651e-324,
			ti = '0000-00-00 00:00:00', da = '0000-00-00',
			te = '', bo = false, js = '', bl = ''
			;`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{
				int64(999), int8(-math.MaxInt8 - 1), int16(-math.MaxInt16 - 1), int32(-math.MaxInt32 - 1), int64(-math.MaxInt64 - 1),
				uint8(0), uint16(0), uint32(0), uint64(0),
				float32(math.SmallestNonzeroFloat32), float64(math.SmallestNonzeroFloat64),
				sql.Timestamp.Zero(), sql.Date.Zero(),
				"", sql.False, ([]byte)(`""`), "",
			}},
		},
		{
			`REPLACE INTO typestable VALUES (999, null, null, null, null, null, null, null, null,
			null, null, null, null, null, null, null, null);`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
		{
			`REPLACE INTO typestable SET id=999, i8=null, i16=null, i32=null, i64=null, u8=null, u16=null, u32=null, u64=null,
			f32=null, f64=null, ti=null, da=null, te=null, bo=null, js=null, bl=null;`,
			[]sql.Row{{int64(1)}},
			"SELECT * FROM typestable WHERE id = 999;",
			[]sql.Row{{int64(999), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}},
		},
	}

	for _, insertion := range insertions {
		e := newEngine(t)
		ctx := newCtx()
		testQueryWithContext(ctx, t, e, insertion.replaceQuery, insertion.expectedReplace)
		testQueryWithContext(ctx, t, e, insertion.selectQuery, insertion.expectedSelect)
	}
}

func TestReplaceIntoErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"too few values",
			"REPLACE INTO mytable (s, i) VALUES ('x');",
		},
		{
			"too many values one column",
			"REPLACE INTO mytable (s) VALUES ('x', 999);",
		},
		{
			"too many values two columns",
			"REPLACE INTO mytable (i, s) VALUES (999, 'x', 'y');",
		},
		{
			"too few values no columns specified",
			"REPLACE INTO mytable VALUES (999);",
		},
		{
			"too many values no columns specified",
			"REPLACE INTO mytable VALUES (999, 'x', 'y');",
		},
		{
			"non-existent column values",
			"REPLACE INTO mytable (i, s, z) VALUES (999, 'x', 999);",
		},
		{
			"non-existent column set",
			"REPLACE INTO mytable SET i = 999, s = 'x', z = 999;",
		},
		{
			"duplicate column values",
			"REPLACE INTO mytable (i, s, s) VALUES (999, 'x', 'x');",
		},
		{
			"duplicate column set",
			"REPLACE INTO mytable SET i = 999, s = 'y', s = 'y';",
		},
		{
			"null given to non-nullable values",
			"INSERT INTO mytable (i, s) VALUES (null, 'y');",
		},
		{
			"null given to non-nullable set",
			"INSERT INTO mytable SET i = null, s = 'y';",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			_, _, err := newEngine(t).Query(newCtx(), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

func TestUpdate(t *testing.T) {
	var updates = []struct {
		updateQuery    string
		expectedUpdate []sql.Row
		selectQuery    string
		expectedSelect []sql.Row
	}{
		{
			"UPDATE mytable SET s = 'updated';",
			[]sql.Row{{int64(3), int64(3)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE mytable SET s = 'updated' WHERE i > 9999;",
			[]sql.Row{{int64(0), int64(0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated' WHERE i = 1;",
			[]sql.Row{{int64(1), int64(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated' WHERE i <> 9999;",
			[]sql.Row{{int64(3), int64(3)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE floattable SET f32 = f32 + f32, f64 = f32 * f64 WHERE i = 2;",
			[]sql.Row{{int64(1), int64(1)}},
			"SELECT * FROM floattable WHERE i = 2;",
			[]sql.Row{{int64(2), float32(3.0), float64(4.5)}},
		},
		{
			"UPDATE floattable SET f32 = 5, f32 = 4 WHERE i = 1;",
			[]sql.Row{{int64(1), int64(1)}},
			"SELECT f32 FROM floattable WHERE i = 1;",
			[]sql.Row{{float32(4.0)}},
		},
		{
			"UPDATE mytable SET s = 'first row' WHERE i = 1;",
			[]sql.Row{{int64(1), int64(0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"UPDATE niltable SET b = NULL WHERE f IS NULL;",
			[]sql.Row{{int64(2), int64(1)}},
			"SELECT * FROM niltable WHERE f IS NULL;",
			[]sql.Row{{int64(4), nil, nil}, {nil, nil, nil}},
		},
		{
			"UPDATE mytable SET s = 'updated' ORDER BY i ASC LIMIT 2;",
			[]sql.Row{{int64(2), int64(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated' ORDER BY i DESC LIMIT 2;",
			[]sql.Row{{int64(2), int64(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE mytable SET s = 'updated' ORDER BY i LIMIT 1 OFFSET 1;",
			[]sql.Row{{int64(1), int64(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "updated"}, {int64(3), "third row"}},
		},
		{
			"UPDATE mytable SET s = 'updated';",
			[]sql.Row{{int64(3), int64(3)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "updated"}, {int64(2), "updated"}, {int64(3), "updated"}},
		},
		{
			"UPDATE typestable SET ti = '2020-03-06 00:00:00';",
			[]sql.Row{{int64(1), int64(1)}},
			"SELECT * FROM typestable;",
			[]sql.Row{{
				int64(1),
				int8(2),
				int16(3),
				int32(4),
				int64(5),
				uint8(6),
				uint16(7),
				uint32(8),
				uint64(9),
				float32(10),
				float64(11),
				sql.Timestamp.MustConvert("2020-03-06 00:00:00"),
				sql.Date.MustConvert("2019-12-31"),
				"fourteen",
				false,
				nil,
				nil}},
		},
		{
			"UPDATE typestable SET ti = '2020-03-06 00:00:00', da = '2020-03-06';",
			[]sql.Row{{int64(1), int64(1)}},
			"SELECT * FROM typestable;",
			[]sql.Row{{
				int64(1),
				int8(2),
				int16(3),
				int32(4),
				int64(5),
				uint8(6),
				uint16(7),
				uint32(8),
				uint64(9),
				float32(10),
				float64(11),
				sql.Timestamp.MustConvert("2020-03-06 00:00:00"),
				sql.Date.MustConvert("2020-03-06"),
				"fourteen",
				false,
				nil,
				nil}},
		},
		{
			"UPDATE typestable SET da = '0000-00-00', ti = '0000-00-00 00:00:00';",
			[]sql.Row{{int64(1), int64(1)}},
			"SELECT * FROM typestable;",
			[]sql.Row{{
				int64(1),
				int8(2),
				int16(3),
				int32(4),
				int64(5),
				uint8(6),
				uint16(7),
				uint32(8),
				uint64(9),
				float32(10),
				float64(11),
				sql.Timestamp.Zero(),
				sql.Date.Zero(),
				"fourteen",
				false,
				nil,
				nil}},
		},
	}

	for _, update := range updates {
		e := newEngine(t)
		ctx := newCtx()
		testQueryWithContext(ctx, t, e, update.updateQuery, update.expectedUpdate)
		testQueryWithContext(ctx, t, e, update.selectQuery, update.expectedSelect)
	}
}

func TestUpdateErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"invalid table",
			"UPDATE doesnotexist SET i = 0;",
		},
		{
			"invalid column set",
			"UPDATE mytable SET z = 0;",
		},
		{
			"invalid column set value",
			"UPDATE mytable SET i = z;",
		},
		{
			"invalid column where",
			"UPDATE mytable SET s = 'hi' WHERE z = 1;",
		},
		{
			"invalid column order by",
			"UPDATE mytable SET s = 'hi' ORDER BY z;",
		},
		{
			"negative limit",
			"UPDATE mytable SET s = 'hi' LIMIT -1;",
		},
		{
			"negative offset",
			"UPDATE mytable SET s = 'hi' LIMIT 1 OFFSET -1;",
		},
		{
			"set null on non-nullable",
			"UPDATE mytable SET s = NULL;",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			_, _, err := newEngine(t).Query(newCtx(), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

const testNumPartitions = 5

func TestAmbiguousColumnResolution(t *testing.T) {
	require := require.New(t)

	table := memory.NewPartitionedTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Text, Source: "foo"},
	}, testNumPartitions)

	insertRows(
		t, table,
		sql.NewRow(int64(1), "foo"),
		sql.NewRow(int64(2), "bar"),
		sql.NewRow(int64(3), "baz"),
	)

	table2 := memory.NewPartitionedTable("bar", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "bar"},
		{Name: "c", Type: sql.Int64, Source: "bar"},
	}, testNumPartitions)
	insertRows(
		t, table2,
		sql.NewRow("qux", int64(3)),
		sql.NewRow("mux", int64(2)),
		sql.NewRow("pux", int64(1)),
	)

	db := memory.NewDatabase("mydb")
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

func TestCreateTable(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require := require.New(t)

	e := newEngine(t)
	testQuery(t, e,
		"CREATE TABLE t1(a INTEGER, b TEXT, c DATE, "+
			"d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, "+
			"b1 BOOL, b2 BOOLEAN NOT NULL, g DATETIME, h CHAR(40))",
		[]sql.Row(nil),
	)

	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err := db.GetTableInsensitive(ctx, "t1")
	require.NoError(err)
	require.True(ok)

	s := sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: true, Source: "t1"},
		{Name: "b", Type: sql.Text, Nullable: true, Source: "t1"},
		{Name: "c", Type: sql.Date, Nullable: true, Source: "t1"},
		{Name: "d", Type: sql.Timestamp, Nullable: true, Source: "t1"},
		{Name: "e", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20), Nullable: true, Source: "t1"},
		{Name: "f", Type: sql.Blob, Source: "t1"},
		{Name: "b1", Type: sql.Boolean, Nullable: true, Source: "t1"},
		{Name: "b2", Type: sql.Boolean, Source: "t1"},
		{Name: "g", Type: sql.Datetime, Nullable: true, Source: "t1"},
		{Name: "h", Type: sql.MustCreateStringWithDefaults(sqltypes.Char, 40), Nullable: true, Source: "t1"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e,
		"CREATE TABLE t2 (a INTEGER NOT NULL PRIMARY KEY, "+
			"b VARCHAR(10) NOT NULL)",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t2")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t2"},
		{Name: "b", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10), Nullable: false, Source: "t2"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e,
		"CREATE TABLE t3(a INTEGER NOT NULL,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a,b))",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t3")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t3"},
		{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: true, Source: "t3"},
		{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t3"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e,
		"CREATE TABLE t4(a INTEGER,"+
			"b TEXT NOT NULL COMMENT 'comment',"+
			"c bool, primary key (a))",
		[]sql.Row(nil),
	)

	db, err = e.Catalog.Database("mydb")
	require.NoError(err)

	testTable, ok, err = db.GetTableInsensitive(ctx, "t4")
	require.NoError(err)
	require.True(ok)

	s = sql.Schema{
		{Name: "a", Type: sql.Int32, Nullable: false, PrimaryKey: true, Source: "t4"},
		{Name: "b", Type: sql.Text, Nullable: false, PrimaryKey: false, Source: "t4", Comment: "comment"},
		{Name: "c", Type: sql.Boolean, Nullable: true, Source: "t4"},
	}

	require.Equal(s, testTable.Schema())

	testQuery(t, e,
		"CREATE TABLE IF NOT EXISTS t4(a INTEGER,"+
				"b TEXT NOT NULL,"+
				"c bool, primary key (a))",
		[]sql.Row(nil),
	)

	_, _, err = e.Query(newCtx(), "CREATE TABLE t4(a INTEGER,"+
			"b TEXT NOT NULL,"+
			"c bool, primary key (a))")
	require.Error(err)
	require.True(sql.ErrTableAlreadyExists.Is(err))
}

func TestDropTable(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require := require.New(t)

	e := newEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	_, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.True(ok)

	testQuery(t, e,
		"DROP TABLE IF EXISTS mytable, not_exist",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e,
		"DROP TABLE IF EXISTS othertable, tabletest",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "tabletest")
	require.NoError(err)
	require.False(ok)

	_, _, err = e.Query(newCtx(), "DROP TABLE not_exist")
	require.Error(err)
}

func TestRenameTable(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require := require.New(t)

	e := newEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	_, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e,
		"RENAME TABLE mytable TO newTableName",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e,
		"RENAME TABLE othertable to othertable2, newTableName to mytable",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "othertable2")
	require.NoError(err)
	require.True(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)

	testQuery(t, e,
		"ALTER TABLE mytable RENAME newTableName",
		[]sql.Row(nil),
	)

	_, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.False(ok)

	_, ok, err = db.GetTableInsensitive(ctx, "newTableName")
	require.NoError(err)
	require.True(ok)


	_, _, err = e.Query(newCtx(), "ALTER TABLE not_exist RENAME foo")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE typestable RENAME niltable")
	require.Error(err)
	require.True(sql.ErrTableAlreadyExists.Is(err))
}

func TestRenameColumn(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require := require.New(t)

	e := newEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e,
		"ALTER TABLE mytable RENAME COLUMN i TO i2",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i2", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	_, _, err = e.Query(newCtx(), "ALTER TABLE not_exist RENAME COLUMN foo TO bar")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE mytable RENAME COLUMN foo TO bar")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))
}

func TestAddColumn(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require := require.New(t)

	e := newEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e,
		"ALTER TABLE mytable ADD COLUMN i2 INT COMMENT 'hello' default 42",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	testQuery(t, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), "first row", int32(42)),
			sql.NewRow(int64(2), "second row", int32(42)),
			sql.NewRow(int64(3), "third row", int32(42)),
		},
	)

	testQuery(t, e,
		"ALTER TABLE mytable ADD COLUMN s2 TEXT COMMENT 'hello' AFTER i",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	testQuery(t, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow(int64(1), nil, "first row", int32(42)),
			sql.NewRow(int64(2), nil, "second row", int32(42)),
			sql.NewRow(int64(3), nil, "third row", int32(42)),
		},
	)

	testQuery(t, e,
		"ALTER TABLE mytable ADD COLUMN s3 TEXT COMMENT 'hello' default 'yay' FIRST",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s3", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true, Default: "yay"},
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s2", Type: sql.Text, Source: "mytable", Comment: "hello", Nullable: true},
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i2", Type: sql.Int32, Source: "mytable", Comment: "hello", Nullable: true, Default: int32(42)},
	}, tbl.Schema())

	testQuery(t, e,
		"SELECT * FROM mytable ORDER BY i",
		[]sql.Row{
			sql.NewRow("yay", int64(1), nil, "first row", int32(42)),
			sql.NewRow("yay", int64(2), nil, "second row", int32(42)),
			sql.NewRow("yay", int64(3), nil, "third row", int32(42)),
		},
	)

	_, _, err = e.Query(newCtx(), "ALTER TABLE not_exist ADD COLUMN i2 INT COMMENT 'hello'")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE mytable ADD COLUMN b BIGINT COMMENT 'ok' AFTER not_exist")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE mytable ADD COLUMN b INT NOT NULL")
	require.Error(err)
	require.True(plan.ErrNullDefault.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE mytable ADD COLUMN b INT NOT NULL DEFAULT 'yes'")
	require.Error(err)
	require.True(plan.ErrIncompatibleDefaultType.Is(err))
}

func TestModifyColumn(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require := require.New(t)

	e := newEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e,
		"ALTER TABLE mytable MODIFY COLUMN i TEXT NOT NULL COMMENT 'modified'",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Text, Source: "mytable", Comment:"modified"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	testQuery(t, e,
		"ALTER TABLE mytable MODIFY COLUMN i TINYINT NULL COMMENT 'yes' AFTER s",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s", Type: sql.Text, Source: "mytable"},
		{Name: "i", Type: sql.Int8, Source: "mytable", Comment:"yes", Nullable: true},
	}, tbl.Schema())

	testQuery(t, e,
		"ALTER TABLE mytable MODIFY COLUMN i BIGINT NOT NULL COMMENT 'ok' FIRST",
		[]sql.Row(nil),
	)

	tbl, ok, err = db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable", Comment:"ok"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	_, _, err = e.Query(newCtx(), "ALTER TABLE mytable MODIFY not_exist BIGINT NOT NULL COMMENT 'ok' FIRST")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE mytable MODIFY i BIGINT NOT NULL COMMENT 'ok' AFTER not_exist")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE not_exist MODIFY COLUMN i INT NOT NULL COMMENT 'hello'")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))
}

func TestDropColumn(t *testing.T) {
	ctx := sql.NewEmptyContext()
	require := require.New(t)

	e := newEngine(t)
	db, err := e.Catalog.Database("mydb")
	require.NoError(err)

	testQuery(t, e,
		"ALTER TABLE mytable DROP COLUMN i",
		[]sql.Row(nil),
	)

	tbl, ok, err := db.GetTableInsensitive(ctx, "mytable")
	require.NoError(err)
	require.True(ok)
	require.Equal(sql.Schema{
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, tbl.Schema())

	_, _, err = e.Query(newCtx(), "ALTER TABLE not_exist DROP COLUMN s")
	require.Error(err)
	require.True(sql.ErrTableNotFound.Is(err))

	_, _, err = e.Query(newCtx(), "ALTER TABLE mytable DROP COLUMN i")
	require.Error(err)
	require.True(plan.ErrColumnNotFound.Is(err))
}

func TestNaturalJoin(t *testing.T) {
	require := require.New(t)

	t1 := memory.NewPartitionedTable("t1", sql.Schema{
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

	t2 := memory.NewPartitionedTable("t2", sql.Schema{
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

	db := memory.NewDatabase("mydb")
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

	t1 := memory.NewPartitionedTable("t1", sql.Schema{
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

	t2 := memory.NewPartitionedTable("t2", sql.Schema{
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

	db := memory.NewDatabase("mydb")
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

	t1 := memory.NewPartitionedTable("t1", sql.Schema{
		{Name: "a", Type: sql.Text, Source: "t1"},
	}, testNumPartitions)

	insertRows(
		t, t1,
		sql.NewRow("a1"),
		sql.NewRow("a2"),
		sql.NewRow("a3"),
	)

	t2 := memory.NewPartitionedTable("t2", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "t2"},
	}, testNumPartitions)
	insertRows(
		t, t2,
		sql.NewRow("b1"),
		sql.NewRow("b2"),
		sql.NewRow("b3"),
	)

	db := memory.NewDatabase("mydb")
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

	table1 := memory.NewPartitionedTable("table1", sql.Schema{
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

	table2 := memory.NewPartitionedTable("table2", sql.Schema{
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

	table3 := memory.NewPartitionedTable("table3", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "table3"},
		{Name: "f2", Type: sql.Float64, Source: "table3"},
		{Name: "t3", Type: sql.Text, Source: "table3"},
	}, testNumPartitions)

	insertRows(
		t, table3,
		sql.NewRow(int32(1), float64(2.2), "table3"),
		sql.NewRow(int32(2), float64(2.2), "table3"),
		sql.NewRow(int32(30), float64(2.2), "table3"),
	)

	db := memory.NewDatabase("mydb")
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

		// .Equal gives better error messages than .ElementsMatch, so use it when possible
		if orderBy || len(expected) <= 1 {
			require.Equal(expected, rows)
		} else {
			require.ElementsMatch(expected, rows)
		}
	})
}

func allTestTables(t *testing.T, numPartitions int) map[string]sql.Table {
	tables := make(map[string]sql.Table)

	tables["mytable"] = memory.NewPartitionedTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, numPartitions)

	insertRows(
		t, tables["mytable"].(*memory.Table),
		sql.NewRow(int64(1), "first row"),
		sql.NewRow(int64(2), "second row"),
		sql.NewRow(int64(3), "third row"),
	)

	tables["one_pk"] = memory.NewPartitionedTable("one_pk", sql.Schema{
		{Name: "pk", Type: sql.Int8, Source: "one_pk", PrimaryKey: true},
		{Name: "c1", Type: sql.Int8, Source: "one_pk"},
		{Name: "c2", Type: sql.Int8, Source: "one_pk"},
		{Name: "c3", Type: sql.Int8, Source: "one_pk"},
		{Name: "c4", Type: sql.Int8, Source: "one_pk"},
		{Name: "c5", Type: sql.Int8, Source: "one_pk", Comment: "column 5"},
	}, numPartitions)

	insertRows(t,
		tables["one_pk"].(*memory.Table),
		sql.NewRow(0, 0, 0, 0, 0, 0),
		sql.NewRow(1, 10, 10, 10, 10, 10),
		sql.NewRow(2, 20, 20, 20, 20, 20),
		sql.NewRow(3, 30, 30, 30, 30, 30),
	)

	tables["two_pk"] = memory.NewPartitionedTable("two_pk", sql.Schema{
		{Name: "pk1", Type: sql.Int8, Source: "two_pk", PrimaryKey: true},
		{Name: "pk2", Type: sql.Int8, Source: "two_pk", PrimaryKey: true},
		{Name: "c1", Type: sql.Int8, Source: "two_pk"},
		{Name: "c2", Type: sql.Int8, Source: "two_pk"},
		{Name: "c3", Type: sql.Int8, Source: "two_pk"},
		{Name: "c4", Type: sql.Int8, Source: "two_pk"},
		{Name: "c5", Type: sql.Int8, Source: "two_pk"},
	}, numPartitions)

	insertRows(t,
		tables["two_pk"].(*memory.Table),
		sql.NewRow(0, 0, 0, 0, 0, 0 ,0),
		sql.NewRow(0, 1, 10, 10, 10, 10, 10),
		sql.NewRow(1, 0, 20, 20, 20, 20, 20),
		sql.NewRow(1, 1, 30, 30, 30, 30, 30),
	)

	tables["two_pk2"] = memory.NewPartitionedTable("two_pk2", sql.Schema{
		{Name: "pk1", Type: sql.Int8, Source: "two_pk2", PrimaryKey: true},
		{Name: "pk2", Type: sql.Int8, Source: "two_pk2", PrimaryKey: true},
		{Name: "c1", Type: sql.Int8, Source: "two_pk2"},
	}, numPartitions)

	insertRows(t,
		tables["two_pk2"].(*memory.Table),
		sql.NewRow(0, 0, 0),
		sql.NewRow(0, 1, 10),
		sql.NewRow(1, 0, 20),
		sql.NewRow(1, 1, 30),
	)

	tables["othertable"] = memory.NewPartitionedTable("othertable", sql.Schema{
		{Name: "s2", Type: sql.Text, Source: "othertable"},
		{Name: "i2", Type: sql.Int64, Source: "othertable"},
	}, numPartitions)

	insertRows(
		t, tables["othertable"].(*memory.Table),
		sql.NewRow("first", int64(3)),
		sql.NewRow("second", int64(2)),
		sql.NewRow("third", int64(1)),
	)

	tables["tabletest"] = memory.NewPartitionedTable("tabletest", sql.Schema{
		{Name: "i", Type: sql.Int32, Source: "tabletest"},
		{Name: "s", Type: sql.Text, Source: "tabletest"},
	}, numPartitions)

	insertRows(
		t, tables["tabletest"].(*memory.Table),
		sql.NewRow(int64(1), "first row"),
		sql.NewRow(int64(2), "second row"),
		sql.NewRow(int64(3), "third row"),
	)

	tables["other_table"] = memory.NewPartitionedTable("other_table", sql.Schema{
		{Name: "text", Type: sql.Text, Source: "tabletest"},
		{Name: "number", Type: sql.Int32, Source: "tabletest"},
	}, numPartitions)

	insertRows(
		t, tables["other_table"].(*memory.Table),
		sql.NewRow("a", int32(4)),
		sql.NewRow("b", int32(2)),
		sql.NewRow("c", int32(0)),
	)

	tables["bigtable"] = memory.NewPartitionedTable("bigtable", sql.Schema{
		{Name: "t", Type: sql.Text, Source: "bigtable"},
		{Name: "n", Type: sql.Int64, Source: "bigtable"},
	}, numPartitions)

	insertRows(
		t, tables["bigtable"].(*memory.Table),
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

	tables["floattable"] = memory.NewPartitionedTable("floattable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "floattable"},
		{Name: "f32", Type: sql.Float32, Source: "floattable"},
		{Name: "f64", Type: sql.Float64, Source: "floattable"},
	}, numPartitions)

	insertRows(
		t, tables["floattable"].(*memory.Table),
		sql.NewRow(int64(1), float32(1.0), float64(1.0)),
		sql.NewRow(int64(2), float32(1.5), float64(1.5)),
		sql.NewRow(int64(3), float32(2.0), float64(2.0)),
		sql.NewRow(int64(4), float32(2.5), float64(2.5)),
		sql.NewRow(int64(-1), float32(-1.0), float64(-1.0)),
		sql.NewRow(int64(-2), float32(-1.5), float64(-1.5)),
	)

	tables["niltable"] = memory.NewPartitionedTable("niltable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "niltable", Nullable: true},
		{Name: "b", Type: sql.Boolean, Source: "niltable", Nullable: true},
		{Name: "f", Type: sql.Float64, Source: "niltable", Nullable: true},
	}, numPartitions)

	insertRows(
		t, tables["niltable"].(*memory.Table),
		sql.NewRow(int64(1), true, float64(1.0)),
		sql.NewRow(int64(2), nil, float64(2.0)),
		sql.NewRow(nil, false, float64(3.0)),
		sql.NewRow(int64(4), true, nil),
		sql.NewRow(nil, nil, nil),
	)

	tables["newlinetable"] = memory.NewPartitionedTable("newlinetable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "newlinetable"},
		{Name: "s", Type: sql.Text, Source: "newlinetable"},
	}, numPartitions)

	insertRows(
		t, tables["newlinetable"].(*memory.Table),
		sql.NewRow(int64(1), "\nthere is some text in here"),
		sql.NewRow(int64(2), "there is some\ntext in here"),
		sql.NewRow(int64(3), "there is some text\nin here"),
		sql.NewRow(int64(4), "there is some text in here\n"),
		sql.NewRow(int64(5), "there is some text in here"),
	)

	tables["typestable"] = memory.NewPartitionedTable("typestable", sql.Schema{
		{Name: "id", Type: sql.Int64, Source: "typestable"},
		{Name: "i8", Type: sql.Int8, Source: "typestable", Nullable: true},
		{Name: "i16", Type: sql.Int16, Source: "typestable", Nullable: true},
		{Name: "i32", Type: sql.Int32, Source: "typestable", Nullable: true},
		{Name: "i64", Type: sql.Int64, Source: "typestable", Nullable: true},
		{Name: "u8", Type: sql.Uint8, Source: "typestable", Nullable: true},
		{Name: "u16", Type: sql.Uint16, Source: "typestable", Nullable: true},
		{Name: "u32", Type: sql.Uint32, Source: "typestable", Nullable: true},
		{Name: "u64", Type: sql.Uint64, Source: "typestable", Nullable: true},
		{Name: "f32", Type: sql.Float32, Source: "typestable", Nullable: true},
		{Name: "f64", Type: sql.Float64, Source: "typestable", Nullable: true},
		{Name: "ti", Type: sql.Timestamp, Source: "typestable", Nullable: true},
		{Name: "da", Type: sql.Date, Source: "typestable", Nullable: true},
		{Name: "te", Type: sql.Text, Source: "typestable", Nullable: true},
		{Name: "bo", Type: sql.Boolean, Source: "typestable", Nullable: true},
		{Name: "js", Type: sql.JSON, Source: "typestable", Nullable: true},
		{Name: "bl", Type: sql.Blob, Source: "typestable", Nullable: true},
	}, numPartitions)

	t1, err := time.Parse(time.RFC3339, "2019-12-31T12:00:00Z")
	require.NoError(t, err)
	t2, err := time.Parse(time.RFC3339, "2019-12-31T00:00:00Z")
	require.NoError(t, err)

	insertRows(
		t, tables["typestable"].(*memory.Table),
		sql.NewRow(
			int64(1),
			int8(2),
			int16(3),
			int32(4),
			int64(5),
			uint8(6),
			uint16(7),
			uint32(8),
			uint64(9),
			float32(10),
			float64(11),
			t1,
			t2,
			"fourteen",
			false,
			nil,
			nil,
			),
	)

	tables["stringandtable"] = memory.NewPartitionedTable("stringandtable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "stringandtable", Nullable: true},
		{Name: "v", Type: sql.Text, Source: "stringandtable", Nullable: true},
	}, numPartitions)

	insertRows(
		t, tables["stringandtable"].(*memory.Table),
		sql.NewRow(int64(0), "0"),
		sql.NewRow(int64(1), "1"),
		sql.NewRow(int64(2), ""),
		sql.NewRow(int64(3), "true"),
		sql.NewRow(int64(4), "false"),
		sql.NewRow(int64(5), nil),
		sql.NewRow(nil, "2"),

	)

	tables["myhistorytable-2019-01-01"] = memory.NewPartitionedTable("myhistorytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "myhistorytable"},
		{Name: "s", Type: sql.Text, Source: "myhistorytable"},
	}, numPartitions)

	insertRows(
		t, tables["myhistorytable-2019-01-01"].(*memory.Table),
		sql.NewRow(int64(1), "first row, 1"),
		sql.NewRow(int64(2), "second row, 1"),
		sql.NewRow(int64(3), "third row, 1"),
	)

	tables["myhistorytable-2019-01-02"] = memory.NewPartitionedTable("myhistorytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "myhistorytable"},
		{Name: "s", Type: sql.Text, Source: "myhistorytable"},
	}, numPartitions)

	insertRows(
		t, tables["myhistorytable-2019-01-02"].(*memory.Table),
		sql.NewRow(int64(1), "first row, 2"),
		sql.NewRow(int64(2), "second row, 2"),
		sql.NewRow(int64(3), "third row, 2"),
	)

	return tables
}

func newEngine(t *testing.T) *sqle.Engine {
	return newEngineWithParallelism(t, 1, allTestTables(t, testNumPartitions), nil)
}

func newEngineWithParallelism(t *testing.T, parallelism int, tables map[string]sql.Table, driver sql.IndexDriver) *sqle.Engine {
	revisions := make(map[interface{}]*memory.Database)
	for name, table := range tables {
		if strings.HasPrefix(name, "myhistorytable") {
			revisionStr := name[len("myhistorytable-"):]
			db := newDatabaseWithoutHistoryTables(tables)
			db.AddTable("myhistorytable", table)
			revisions[revisionStr] = db
		}
	}

	db := memory.NewHistoryDatabase(revisions, revisions["2019-01-02"])

	db2 := memory.NewDatabase("foo")
	db2.AddTable("other_table", tables["other_table"])

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

	if driver != nil {
		catalog.RegisterIndexDriver(driver)
	}

	engine := sqle.New(catalog, a, new(sqle.Config))
	require.NoError(t, engine.Init())

	return engine
}

func newDatabaseWithoutHistoryTables(tables map[string]sql.Table) *memory.Database {
	db := memory.NewDatabase("mydb")
	for name, table := range tables {
		if name != "other_table" && !strings.HasPrefix(name, "myhistorytable") {
			db.AddTable(name, table)
		}
	}
	return db
}

const expectedTree = `Limit(5)
 └─ Offset(2)
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

func TestOrderByGroupBy(t *testing.T) {
	require := require.New(t)

	table := memory.NewPartitionedTable("members", sql.Schema{
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

	db := memory.NewDatabase("db")
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

	_, _, err = e.Query(
		newCtx(),
		"SELECT team, COUNT(*) FROM members GROUP BY team ORDER BY columndoesnotexist",
	)
	require.Error(err)
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

	table := memory.NewPartitionedTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	}, testNumPartitions)

	db := memory.NewDatabase("mydb")
	db.AddTable("mytable", table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	au := auth.NewNativeSingle("user", "pass", auth.ReadPerm)
	cfg := &sqle.Config{Auth: au}
	a := analyzer.NewBuilder(catalog).Build()
	e := sqle.New(catalog, a, cfg)

	_, _, err := e.Query(newCtx(), `SELECT i FROM mytable`)
	require.NoError(err)

	writingQueries := []string{
		`CREATE INDEX foo ON mytable USING pilosa (i, s)`,
		`DROP INDEX foo ON mytable`,
		`INSERT INTO mytable (i, s) VALUES(42, 'yolo')`,
		`CREATE VIEW myview AS SELECT i FROM mytable`,
		`DROP VIEW myview`,
	}

	for _, query := range writingQueries {
		_, _, err = e.Query(newCtx(), query)
		require.Error(err)
		require.True(auth.ErrNotAuthorized.Is(err))
	}
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

	require.Equal([]sql.Row{{int8(1), ",STRICT_TRANS_TABLES"}}, rows)
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
	SELECT SUBSTRING(s, 1, 10) AS sub_s, SUBSTRING(sub_s, 2, 3) AS sub_sub_s
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

	t1 := newLockableTable(memory.NewTable("t1", nil))
	t2 := newLockableTable(memory.NewTable("t2", nil))
	t3 := memory.NewTable("t3", nil)
	catalog := sql.NewCatalog()
	db := memory.NewDatabase("db")
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
	query := `DESCRIBE FORMAT=TREE SELECT SUBSTRING(s, 1, 1) AS foo, s, i FROM mytable WHERE foo = 'f'`
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

func TestDeleteFrom(t *testing.T) {
	var deletions = []struct {
		deleteQuery    string
		expectedDelete []sql.Row
		selectQuery    string
		expectedSelect []sql.Row
	}{
		{
			"DELETE FROM mytable;",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable;",
			nil,
		},
		{
			"DELETE FROM mytable WHERE i = 2;",
			[]sql.Row{{int64(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i < 3;",
			[]sql.Row{{int64(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i > 1;",
			[]sql.Row{{int64(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}},
		},
		{
			"DELETE FROM mytable WHERE i <= 2;",
			[]sql.Row{{int64(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i >= 2;",
			[]sql.Row{{int64(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}},
		},
		{
			"DELETE FROM mytable WHERE s = 'first row';",
			[]sql.Row{{int64(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE s <> 'dne';",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable;",
			nil,
		},
		{
			"DELETE FROM mytable WHERE s LIKE '%row';",
			[]sql.Row{{int64(3)}},
			"SELECT * FROM mytable;",
			nil,
		},
		{
			"DELETE FROM mytable WHERE s = 'dne';",
			[]sql.Row{{int64(0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable WHERE i = 'invalid';",
			[]sql.Row{{int64(0)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}, {int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable ORDER BY i ASC LIMIT 2;",
			[]sql.Row{{int64(2)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(3), "third row"}},
		},
		{
			"DELETE FROM mytable ORDER BY i DESC LIMIT 1;",
			[]sql.Row{{int64(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(2), "second row"}},
		},
		{
			"DELETE FROM mytable ORDER BY i DESC LIMIT 1 OFFSET 1;",
			[]sql.Row{{int64(1)}},
			"SELECT * FROM mytable;",
			[]sql.Row{{int64(1), "first row"}, {int64(3), "third row"}},
		},
	}

	for _, deletion := range deletions {
		e := newEngine(t)
		ctx := newCtx()
		testQueryWithContext(ctx, t, e, deletion.deleteQuery, deletion.expectedDelete)
		testQueryWithContext(ctx, t, e, deletion.selectQuery, deletion.expectedSelect)
	}
}

func TestDeleteFromErrors(t *testing.T) {
	var expectedFailures = []struct {
		name  string
		query string
	}{
		{
			"invalid table",
			"DELETE FROM invalidtable WHERE x < 1;",
		},
		{
			"invalid column",
			"DELETE FROM mytable WHERE z = 'dne';",
		},
		{
			"negative limit",
			"DELETE FROM mytable LIMIT -1;",
		},
		{
			"negative offset",
			"DELETE FROM mytable LIMIT 1 OFFSET -1;",
		},
		{
			"missing keyword from",
			"DELETE mytable WHERE id = 1;",
		},
	}

	for _, expectedFailure := range expectedFailures {
		t.Run(expectedFailure.name, func(t *testing.T) {
			_, _, err := newEngine(t).Query(newCtx(), expectedFailure.query)
			require.Error(t, err)
		})
	}
}

type mockSpan struct {
	opentracing.Span
	finished bool
}

func (m *mockSpan) Finish() {
	m.finished = true
}

func TestRootSpanFinish(t *testing.T) {
	e := newEngine(t)
	fakeSpan := &mockSpan{Span: opentracing.NoopTracer{}.StartSpan("")}
	ctx := sql.NewContext(
		context.Background(),
		sql.WithRootSpan(fakeSpan),
	)

	_, iter, err := e.Query(ctx, "SELECT 1")
	require.NoError(t, err)

	_, err = sql.RowIterToRows(iter)
	require.NoError(t, err)

	require.True(t, fakeSpan.finished)
}

var generatorQueries = []struct {
	query    string
	expected []sql.Row
}{
	{
		`SELECT a, EXPLODE(b), c FROM t`,
		[]sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		`SELECT a, EXPLODE(b) AS x, c FROM t`,
		[]sql.Row{
			{int64(1), "a", "first"},
			{int64(1), "b", "first"},
			{int64(2), "c", "second"},
			{int64(2), "d", "second"},
			{int64(3), "e", "third"},
			{int64(3), "f", "third"},
		},
	},
	{
		`SELECT EXPLODE(SPLIT(c, "")) FROM t LIMIT 5`,
		[]sql.Row{
			{"f"},
			{"i"},
			{"r"},
			{"s"},
			{"t"},
		},
	},
	{
		`SELECT a, EXPLODE(b) AS x, c FROM t WHERE x = 'e'`,
		[]sql.Row{
			{int64(3), "e", "third"},
		},
	},
}

func TestGenerators(t *testing.T) {
	table := memory.NewPartitionedTable("t", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "t"},
		{Name: "b", Type: sql.CreateArray(sql.Text), Source: "t"},
		{Name: "c", Type: sql.Text, Source: "t"},
	}, testNumPartitions)

	insertRows(
		t, table,
		sql.NewRow(int64(1), []interface{}{"a", "b"}, "first"),
		sql.NewRow(int64(2), []interface{}{"c", "d"}, "second"),
		sql.NewRow(int64(3), []interface{}{"e", "f"}, "third"),
	)

	db := memory.NewDatabase("db")
	db.AddTable("t", table)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)
	e := sqle.New(catalog, analyzer.NewDefault(catalog), new(sqle.Config))

	for _, q := range generatorQueries {
		testQuery(t, e, q.query, q.expected)
	}
}

func insertRows(t *testing.T, table sql.InsertableTable, rows ...sql.Row) {
	t.Helper()

	ctx := newCtx()
	inserter := table.Inserter(ctx)
	for _, r := range rows {
		require.NoError(t, inserter.Insert(ctx, r))
	}
	require.NoError(t, inserter.Close(ctx))
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

func timeParse(layout string, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
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
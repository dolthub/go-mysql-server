package sqle_test

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/analyzer"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index/pilosa"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"
	"gopkg.in/src-d/go-mysql-server.v0/test"

	"github.com/stretchr/testify/require"
)

const driverName = "engine_tests"

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
		[]sql.Row{{int32(3)}},
	},
	{
		"SELECT COUNT(*) FROM mytable LIMIT 1;",
		[]sql.Row{{int32(3)}},
	},
	{
		"SELECT COUNT(*) AS c FROM mytable;",
		[]sql.Row{{int32(3)}},
	},
	{
		"SELECT substring(s, 2, 3) FROM mytable",
		[]sql.Row{{"irs"}, {"eco"}, {"hir"}},
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
		"SELECT i FROM mytable WHERE i BETWEEN 1 AND 2",
		[]sql.Row{{int64(1)}, {int64(2)}},
	},
	{
		"SELECT i FROM mytable WHERE i NOT BETWEEN 1 AND 2",
		[]sql.Row{{int64(3)}},
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
			{int32(1), "first row"},
			{int32(1), "second row"},
			{int32(1), "third row"},
		},
	},
	{
		`SELECT COUNT(*) as cnt, s as fi FROM mytable GROUP BY fi`,
		[]sql.Row{
			{int32(1), "first row"},
			{int32(1), "second row"},
			{int32(1), "third row"},
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
		"SELECT text > 2 FROM tabletest",
		[]sql.Row{
			{false},
			{false},
			{false},
		},
	},
	{
		"SELECT * FROM tabletest WHERE text > 0",
		nil,
	},
	{
		"SELECT * FROM tabletest WHERE text = 0",
		[]sql.Row{
			{"a", int32(1)},
			{"b", int32(2)},
			{"c", int32(3)},
		},
	},
	{
		"SELECT * FROM tabletest WHERE text = 'a'",
		[]sql.Row{
			{"a", int32(1)},
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
			{"a", int32(1), int64(1), "first row", "third", int64(1)},
			{"a", int32(1), int64(2), "second row", "second", int64(2)},
			{"a", int32(1), int64(3), "third row", "first", int64(3)},
			{"b", int32(2), int64(1), "first row", "third", int64(1)},
			{"b", int32(2), int64(2), "second row", "second", int64(2)},
			{"b", int32(2), int64(3), "third row", "first", int64(3)},
			{"c", int32(3), int64(1), "first row", "third", int64(1)},
			{"c", int32(3), int64(2), "second row", "second", int64(2)},
			{"c", int32(3), int64(3), "third row", "first", int64(3)},
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
			{int32(1), int64(3)},
			{int32(1), int64(2)},
			{int32(1), int64(1)},
		},
	},
	{
		`SELECT COUNT(*) c, i as foo FROM mytable GROUP BY i ORDER BY foo, i DESC`,
		[]sql.Row{
			{int32(1), int64(3)},
			{int32(1), int64(2)},
			{int32(1), int64(1)},
		},
	},
	{
		`SELECT CONCAT("a", "b", "c")`,
		[]sql.Row{
			{string("abc")},
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
}

func TestQueries(t *testing.T) {
	e := newEngine(t)

	ep := newEngineWithParallelism(t, 2)

	t.Run("sequential", func(t *testing.T) {
		for _, tt := range queries {
			testQuery(t, e, tt.query, tt.expected)
		}
	})

	t.Run("parallel", func(t *testing.T) {
		for _, tt := range queries {
			testQuery(t, ep, tt.query, tt.expected)
		}
	})
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
		"CREATE TABLE t1(a INTEGER, b TEXT, c DATE,"+
			"d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL)",
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
	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		session := newCtx()

		_, iter, err := e.Query(session, q)
		require.NoError(err)

		rows, err := sql.RowIterToRows(iter)
		require.NoError(err)

		require.ElementsMatch(expected, rows)
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
		{Name: "text", Type: sql.Text, Source: "tabletest"},
		{Name: "number", Type: sql.Int32, Source: "tabletest"},
	}, testNumPartitions)

	insertRows(
		t, table3,
		sql.NewRow("a", int32(1)),
		sql.NewRow("b", int32(2)),
		sql.NewRow("c", int32(3)),
	)

	db := mem.NewDatabase("mydb")
	db.AddTable("mytable", table)
	db.AddTable("othertable", table2)
	db.AddTable("tabletest", table3)

	catalog := sql.NewCatalog()
	catalog.AddDatabase(db)

	var a *analyzer.Analyzer
	if parallelism > 1 {
		a = analyzer.NewBuilder(catalog).WithParallelism(parallelism).Build()
	} else {
		a = analyzer.NewDefault(catalog)
	}

	a.CurrentDatabase = "mydb"

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

func TestIndexes(t *testing.T) {
	e := newEngine(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-test")
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(tmpDir, 0644))
	e.Catalog.RegisterIndexDriver(pilosa.NewDriver(tmpDir))

	_, _, err = e.Query(
		newCtx(),
		"CREATE INDEX myidx ON mytable USING pilosa (i) WITH (async = false)",
	)
	require.NoError(t, err)

	_, _, err = e.Query(
		newCtx(),
		"CREATE INDEX myidx_multi ON mytable USING pilosa (i, s) WITH (async = false)",
	)
	require.NoError(t, err)

	defer func() {
		done, err := e.Catalog.DeleteIndex("mydb", "myidx", true)
		require.NoError(t, err)
		<-done

		done, err = e.Catalog.DeleteIndex("foo", "myidx_multi", true)
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
		{"purple", int32(1)},
		{"red", int32(2)},
		{"orange", int32(3)},
	}

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

	a := analyzer.NewBuilder(catalog).ReadOnly().Build()
	a.CurrentDatabase = "mydb"
	e := sqle.New(catalog, a, nil)

	_, _, err := e.Query(newCtx(), `SELECT i FROM mytable`)
	require.NoError(err)

	_, _, err = e.Query(newCtx(), `CREATE INDEX foo ON mytable USING pilosa (i, s)`)
	require.Error(err)
	require.True(analyzer.ErrQueryNotAllowed.Is(err))

	_, _, err = e.Query(newCtx(), `DROP INDEX foo ON mytable`)
	require.Error(err)
	require.True(analyzer.ErrQueryNotAllowed.Is(err))

	_, _, err = e.Query(newCtx(), `INSERT INTO foo (i, s) VALUES(42, 'yolo')`)
	require.Error(err)
	require.True(analyzer.ErrQueryNotAllowed.Is(err))
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

func insertRows(t *testing.T, table sql.Inserter, rows ...sql.Row) {
	t.Helper()

	for _, r := range rows {
		require.NoError(t, table.Insert(newCtx(), r))
	}
}

var pid uint64

func newCtx() *sql.Context {
	pid++
	return sql.NewContext(context.Background(), sql.WithPid(pid))
}

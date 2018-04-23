package sqle_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"

	"github.com/stretchr/testify/require"
	jaeger "github.com/uber/jaeger-client-go"
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
}

func TestQueries(t *testing.T) {
	e := newEngine(t)

	for _, tt := range queries {
		testQuery(t, e, tt.query, tt.expected)
	}
}

func TestOrderByColumns(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	_, iter, err := e.Query(sql.NewEmptyContext(), "SELECT s, i FROM mytable ORDER BY 2 DESC")
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

func TestAmbiguousColumnResolution(t *testing.T) {
	require := require.New(t)

	table := mem.NewTable("foo", sql.Schema{
		{Name: "a", Type: sql.Int64, Source: "foo"},
		{Name: "b", Type: sql.Text, Source: "foo"},
	})
	require.Nil(table.Insert(sql.NewRow(int64(1), "foo")))
	require.Nil(table.Insert(sql.NewRow(int64(2), "bar")))
	require.Nil(table.Insert(sql.NewRow(int64(3), "baz")))

	table2 := mem.NewTable("bar", sql.Schema{
		{Name: "b", Type: sql.Text, Source: "bar"},
		{Name: "c", Type: sql.Int64, Source: "bar"},
	})
	require.Nil(table2.Insert(sql.NewRow("qux", int64(3))))
	require.Nil(table2.Insert(sql.NewRow("mux", int64(2))))
	require.Nil(table2.Insert(sql.NewRow("pux", int64(1))))

	db := mem.NewDatabase("mydb")

	memDb, ok := db.(*mem.Database)
	require.True(ok)

	memDb.AddTable(table.Name(), table)
	memDb.AddTable(table2.Name(), table2)

	e := sqle.New()
	e.AddDatabase(db)

	q := `SELECT f.a, bar.b, f.b FROM foo f INNER JOIN bar ON f.a = bar.c`
	ctx := sql.NewEmptyContext()

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

func testQuery(t *testing.T, e *sqle.Engine, q string, r []sql.Row) {
	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		session := sql.NewEmptyContext()

		_, rows, err := e.Query(session, q)
		require.NoError(err)

		var rs []sql.Row
		for {
			row, err := rows.Next()
			if err == io.EOF {
				break
			}
			require.NoError(err)

			rs = append(rs, row)
		}

		require.ElementsMatch(r, rs)
	})
}

func newEngine(t *testing.T) *sqle.Engine {
	require := require.New(t)

	table := mem.NewTable("mytable", sql.Schema{
		{Name: "i", Type: sql.Int64, Source: "mytable"},
		{Name: "s", Type: sql.Text, Source: "mytable"},
	})
	require.Nil(table.Insert(sql.NewRow(int64(1), "first row")))
	require.Nil(table.Insert(sql.NewRow(int64(2), "second row")))
	require.Nil(table.Insert(sql.NewRow(int64(3), "third row")))

	table2 := mem.NewTable("othertable", sql.Schema{
		{Name: "s2", Type: sql.Text, Source: "othertable"},
		{Name: "i2", Type: sql.Int64, Source: "othertable"},
	})
	require.Nil(table2.Insert(sql.NewRow("first", int64(3))))
	require.Nil(table2.Insert(sql.NewRow("second", int64(2))))
	require.Nil(table2.Insert(sql.NewRow("third", int64(1))))

	table3 := mem.NewTable("tabletest", sql.Schema{
		{Name: "text", Type: sql.Text, Source: "tabletest"},
		{Name: "number", Type: sql.Int32, Source: "tabletest"},
	})
	require.Nil(table3.Insert(sql.NewRow("a", int32(1))))
	require.Nil(table3.Insert(sql.NewRow("b", int32(2))))
	require.Nil(table3.Insert(sql.NewRow("c", int32(3))))

	db := mem.NewDatabase("mydb")
	memDb, ok := db.(*mem.Database)
	require.True(ok)

	memDb.AddTable(table.Name(), table)
	memDb.AddTable(table2.Name(), table2)
	memDb.AddTable(table3.Name(), table3)

	e := sqle.New()
	e.AddDatabase(db)

	return e
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
	node, err := parse.Parse(sql.NewEmptyContext(), `
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

func TestTracing(t *testing.T) {
	require := require.New(t)
	e := newEngine(t)

	reporter := jaeger.NewInMemoryReporter()
	tracer, closer := jaeger.NewTracer("go-mysql-server", jaeger.NewConstSampler(true), reporter)
	defer func() {
		require.NoError(closer.Close())
	}()

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

	spans := reporter.GetSpans()

	var expectedSpans = []string{
		"expression.Equals",
		"expression.Equals",
		"expression.Equals",
		"plan.Filter",
		"plan.Limit",
		"plan.Distinct",
		"plan.Project",
		"plan.Sort",
	}

	var spanOperations []string
	for _, s := range spans {
		name := s.(*jaeger.Span).OperationName()
		// only check the ones inside the execution tree
		if strings.HasPrefix(name, "plan.") ||
			strings.HasPrefix(name, "expression.") ||
			strings.HasPrefix(name, "function.") ||
			strings.HasPrefix(name, "aggregation.") {
			spanOperations = append(spanOperations, name)
		}
	}

	require.Equal(expectedSpans, spanOperations)
}

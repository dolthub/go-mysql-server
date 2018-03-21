package sqle_test

import (
	"context"
	"io"
	"testing"

	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/parse"

	"github.com/stretchr/testify/require"
)

const driverName = "engine_tests"

func TestQueries(t *testing.T) {
	e := newEngine(t)

	testQuery(t, e,
		"SELECT i FROM mytable;",
		[][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE i = 2;",
		[][]interface{}{{int64(2)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable ORDER BY i DESC;",
		[][]interface{}{{int64(3)}, {int64(2)}, {int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'first row' ORDER BY i DESC LIMIT 1;",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) FROM mytable;",
		[][]interface{}{{int32(3)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) FROM mytable LIMIT 1;",
		[][]interface{}{{int32(3)}},
	)

	testQuery(t, e,
		"SELECT COUNT(*) AS c FROM mytable;",
		[][]interface{}{{int32(3)}},
	)

	testQuery(t, e,
		"SELECT substring(s, 2, 3) FROM mytable",
		[][]interface{}{{"irs"}, {"eco"}, {"hir"}},
	)

	testQuery(t, e,
		"SELECT YEAR('2007-12-11') FROM mytable",
		[][]interface{}{{int32(2007)}, {int32(2007)}, {int32(2007)}},
	)

	testQuery(t, e,
		"SELECT MONTH('2007-12-11') FROM mytable",
		[][]interface{}{{int32(12)}, {int32(12)}, {int32(12)}},
	)

	testQuery(t, e,
		"SELECT DAY('2007-12-11') FROM mytable",
		[][]interface{}{{int32(11)}, {int32(11)}, {int32(11)}},
	)

	testQuery(t, e,
		"SELECT HOUR('2007-12-11 20:21:22') FROM mytable",
		[][]interface{}{{int32(20)}, {int32(20)}, {int32(20)}},
	)

	testQuery(t, e,
		"SELECT MINUTE('2007-12-11 20:21:22') FROM mytable",
		[][]interface{}{{int32(21)}, {int32(21)}, {int32(21)}},
	)

	testQuery(t, e,
		"SELECT SECOND('2007-12-11 20:21:22') FROM mytable",
		[][]interface{}{{int32(22)}, {int32(22)}, {int32(22)}},
	)

	testQuery(t, e,
		"SELECT DAYOFYEAR('2007-12-11 20:21:22') FROM mytable",
		[][]interface{}{{int32(345)}, {int32(345)}, {int32(345)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE i BETWEEN 1 AND 2",
		[][]interface{}{{int64(1)}, {int64(2)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE i NOT BETWEEN 1 AND 2",
		[][]interface{}{{int64(3)}},
	)

	testQuery(t, e,
		"SELECT i, i2, s2 FROM mytable INNER JOIN othertable ON i = i2",
		[][]interface{}{
			{int64(1), int64(1), "third"},
			{int64(2), int64(2), "second"},
			{int64(3), int64(3), "first"},
		},
	)

	testQuery(t, e,
		"SELECT s FROM mytable INNER JOIN othertable "+
			"ON substring(s2, 1, 2) != '' AND i = i2",
		[][]interface{}{
			{"first row"},
			{"second row"},
			{"third row"},
		},
	)

	testQuery(t, e,
		`SELECT COUNT(*) as cnt, fi FROM (
			SELECT tbl.s AS fi
			FROM mytable tbl
		) t
		GROUP BY fi`,
		[][]interface{}{
			{int32(1), "first row"},
			{int32(1), "second row"},
			{int32(1), "third row"},
		},
	)

	testQuery(t, e,
		"SELECT CAST(-3 AS UNSIGNED) FROM mytable",
		[][]interface{}{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	)

	testQuery(t, e,
		"SELECT CONVERT(-3, UNSIGNED) FROM mytable",
		[][]interface{}{
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
			{uint64(18446744073709551613)},
		},
	)

	testQuery(t, e,
		"SELECT '3' > 2 FROM tabletest",
		[][]interface{}{
			{true},
			{true},
			{true},
		},
	)

	testQuery(t, e,
		"SELECT text > 2 FROM tabletest",
		[][]interface{}{
			{false},
			{false},
			{false},
		},
	)

	testQuery(t, e,
		"SELECT * FROM tabletest WHERE text > 0",
		nil,
	)

	testQuery(t, e,
		"SELECT * FROM tabletest WHERE text = 0",
		[][]interface{}{
			{"a", int32(1)},
			{"b", int32(2)},
			{"c", int32(3)},
		},
	)

	testQuery(t, e,
		"SELECT * FROM tabletest WHERE text = 'a'",
		[][]interface{}{
			{"a", int32(1)},
		},
	)

	testQuery(t, e,
		"SELECT s FROM mytable WHERE i IN (1, 2, 5)",
		[][]interface{}{
			{"first row"},
			{"second row"},
		},
	)

	testQuery(t, e,
		"SELECT s FROM mytable WHERE i NOT IN (1, 2, 5)",
		[][]interface{}{
			{"third row"},
		},
	)
}

func TestInsertInto(t *testing.T) {
	e := newEngine(t)
	testQuery(t, e,
		"INSERT INTO mytable (s, i) VALUES ('x', 999);",
		[][]interface{}{{int64(1)}},
	)

	testQuery(t, e,
		"SELECT i FROM mytable WHERE s = 'x';",
		[][]interface{}{{int64(999)}},
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
	ctx := sql.NewContext(context.TODO(), sql.NewBaseSession())

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
		[][]interface{}(nil),
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

func testQuery(t *testing.T, e *sqle.Engine, q string, r [][]interface{}) {
	t.Run(q, func(t *testing.T) {
		require := require.New(t)
		session := sql.NewContext(context.TODO(), sql.NewBaseSession())

		_, rows, err := e.Query(session, q)
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
	node, err := parse.Parse(nil, `
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

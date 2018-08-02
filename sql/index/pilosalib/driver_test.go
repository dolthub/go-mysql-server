package pilosalib

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index"
	"gopkg.in/src-d/go-mysql-server.v0/test"
)

var tmpDir string

func setup(t *testing.T) {
	var err error

	tmpDir, err = ioutil.TempDir("", "pilosalib")
	if err != nil {
		t.Fatal(err)
	}
}

func cleanup(t *testing.T) {
	err := os.RemoveAll(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
}

func TestID(t *testing.T) {
	d := &Driver{}

	require := require.New(t)
	require.Equal(DriverID, d.ID())
}

func TestLoadAll(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	d := NewDriver(tmpDir)
	idx1, err := d.Create("db", "table", "id1", makeExpressions("table", "hash1"), nil)
	require.NoError(err)

	idx2, err := d.Create("db", "table", "id2", makeExpressions("table", "hash1"), nil)
	require.NoError(err)

	indexes, err := d.LoadAll("db", "table")
	require.NoError(err)

	require.Equal(2, len(indexes))
	i1, ok := idx1.(*pilosaIndex)
	require.True(ok)
	i2, ok := idx2.(*pilosaIndex)
	require.True(ok)

	require.Equal(i1.index.Name(), i2.index.Name())
}

type logLoc struct {
	loc []byte
	err error
}

func TestSaveAndLoad(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions(table, "lang", "hash")

	d := NewDriver(tmpDir)
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.NoError(err)

	it := &testIndexKeyValueIter{
		offset:      0,
		total:       64,
		expressions: sqlIdx.Expressions(),
		location:    randLocation,
	}

	tracer := new(test.MemTracer)
	ctx := sql.NewContext(context.Background(), sql.WithTracer(tracer))
	err = d.Save(ctx, sqlIdx, it)
	require.NoError(err)

	indexes, err := d.LoadAll(db, table)
	require.NoError(err)
	require.Equal(1, len(indexes))

	for _, r := range it.records {
		lookup, err := sqlIdx.Get(r.values...)
		require.NoError(err)

		found, foundLoc := false, []string{}
		lit, err := lookup.Values()
		require.NoError(err)

		var logs []logLoc
		for i := 0; ; i++ {
			loc, err := lit.Next()

			// make a copy of location to save in the log
			loc2 := make([]byte, len(loc))
			copy(loc2, loc)
			logs = append(logs, logLoc{loc2, err})

			if err == io.EOF {
				if i == 0 {
					for j, l := range logs {
						t.Logf("[%d] values: %v location: %x loc: %x err: %v\n",
							j, r.values, r.location, l.loc, l.err)
					}

					t.Errorf("No data for r.values: %v\tr.location: %x",
						r.values, r.location)
					t.FailNow()
				}

				break
			}

			require.NoError(err)
			found = found || reflect.DeepEqual(r.location, loc)
			foundLoc = append(foundLoc, hex.EncodeToString(loc))
		}
		require.Truef(found, "Expected: %s\nGot: %v\n", hex.EncodeToString(r.location), foundLoc)

		err = lit.Close()
		require.NoError(err)
	}

	// test that not found values do not cause error
	lookup, err := sqlIdx.Get("do not exist", "none")
	require.NoError(err)
	lit, err := lookup.Values()
	require.NoError(err)
	_, err = lit.Next()
	require.Equal(io.EOF, err)

	found := false
	for _, span := range tracer.Spans {
		if span == "pilosa.Save.bitBatch" {
			found = true
			break
		}
	}

	require.True(found)
}

func TestSaveAndGetAll(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions(table, "lang", "hash")

	d := NewDriver(tmpDir)
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.NoError(err)

	it := &testIndexKeyValueIter{
		offset:      0,
		total:       64,
		expressions: sqlIdx.Expressions(),
		location:    randLocation,
	}

	err = d.Save(sql.NewEmptyContext(), sqlIdx, it)
	require.NoError(err)

	indexes, err := d.LoadAll(db, table)
	require.NoError(err)
	require.Equal(1, len(indexes))

	_, err = sqlIdx.Get()
	require.Error(err)
	require.True(errInvalidKeys.Is(err))
}

func TestLoadCorruptedIndex(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	d := NewDriver(tmpDir)
	_, err := d.Create("db", "table", "id", nil, nil)
	require.NoError(err)

	require.NoError(index.CreateProcessingFile(d.processingFilePath("db", "table", "id")))

	_, err = d.loadIndex("db", "table", "id")
	require.Error(err)
	require.True(errCorruptedIndex.Is(err))

	_, err = os.Stat(d.processingFilePath("db", "table", "id"))
	require.Error(err)
	require.True(os.IsNotExist(err))
}

func TestDelete(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table, id := "db_name", "table_name", "index_id"

	expressions := []sql.Expression{
		expression.NewGetFieldWithTable(0, sql.Int64, table, "lang", true),
		expression.NewGetFieldWithTable(1, sql.Int64, table, "field", true),
	}

	d := NewDriver(tmpDir)
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.NoError(err)

	err = d.Delete(sqlIdx)
	require.NoError(err)
}

func TestLoadAllDirectoryDoesNotExist(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	driver := NewDriver(tmpDir)
	indexes, err := driver.LoadAll("foo", "bar")
	require.NoError(err)
	require.Len(indexes, 0)
}

func TestAscendDescendIndex(t *testing.T) {
	idx, cleanup := setupAscendDescend(t)
	defer cleanup()

	must := func(lookup sql.IndexLookup, err error) sql.IndexLookup {
		require.NoError(t, err)
		return lookup
	}

	testCases := []struct {
		name     string
		lookup   sql.IndexLookup
		expected []string
	}{
		{
			"ascend range",
			must(idx.AscendRange(
				[]interface{}{int64(1), int64(1)},
				[]interface{}{int64(7), int64(10)},
			)),
			[]string{"1", "5", "6", "7", "8", "9"},
		},
		{
			"ascend greater or equal",
			must(idx.AscendGreaterOrEqual(int64(7), int64(6))),
			[]string{"2", "4"},
		},
		{
			"ascend less than",
			must(idx.AscendLessThan(int64(5), int64(3))),
			[]string{"1", "10"},
		},
		{
			"descend range",
			must(idx.DescendRange(
				[]interface{}{int64(6), int64(9)},
				[]interface{}{int64(0), int64(0)},
			)),
			[]string{"9", "8", "7", "6", "5", "1"},
		},
		{
			"descend less or equal",
			must(idx.DescendLessOrEqual(int64(4), int64(2))),
			[]string{"10", "1"},
		},
		{
			"descend greater",
			must(idx.DescendGreater(int64(6), int64(5))),
			[]string{"4", "2"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			iter, err := tt.lookup.Values()
			require.NoError(err)

			var result []string
			for {
				k, err := iter.Next()
				if err == io.EOF {
					break
				}
				require.NoError(err)

				result = append(result, string(k))
			}

			require.Equal(tt.expected, result)
		})
	}
}

func TestIntersection(t *testing.T) {
	ctx := sql.NewContext(context.Background())
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table := "db_name", "table_name"
	idxLang, expLang := "idx_lang", makeExpressions(table, "lang")
	idxPath, expPath := "idx_path", makeExpressions(table, "path")

	d := NewDriver(tmpDir)
	sqlIdxLang, err := d.Create(db, table, idxLang, expLang, nil)
	require.NoError(err)

	sqlIdxPath, err := d.Create(db, table, idxPath, expPath, nil)
	require.NoError(err)

	itLang := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdxLang.Expressions(),
		location:    offsetLocation,
	}

	itPath := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdxPath.Expressions(),
		location:    offsetLocation,
	}

	err = d.Save(ctx, sqlIdxLang, itLang)
	require.NoError(err)

	err = d.Save(ctx, sqlIdxPath, itPath)
	require.NoError(err)

	lookupLang, err := sqlIdxLang.Get(itLang.records[0].values...)
	require.NoError(err)
	lookupPath, err := sqlIdxPath.Get(itPath.records[itPath.total-1].values...)
	require.NoError(err)

	m, ok := lookupLang.(sql.Mergeable)
	require.True(ok)
	require.True(m.IsMergeable(lookupPath))

	interLookup, ok := lookupLang.(sql.SetOperations)
	require.True(ok)
	interIt, err := interLookup.Intersection(lookupPath).Values()
	require.NoError(err)
	loc, err := interIt.Next()
	fmt.Println(loc, err, err == io.EOF)

	require.True(err == io.EOF)
	require.NoError(interIt.Close())

	lookupLang, err = sqlIdxLang.Get(itLang.records[0].values...)
	require.NoError(err)
	lookupPath, err = sqlIdxPath.Get(itPath.records[0].values...)
	require.NoError(err)

	interLookup, ok = lookupPath.(sql.SetOperations)
	require.True(ok)
	interIt, err = interLookup.Intersection(lookupLang).Values()
	require.NoError(err)
	loc, err = interIt.Next()
	require.NoError(err)
	require.Equal(loc, itPath.records[0].location)
	_, err = interIt.Next()
	require.True(err == io.EOF)

	require.NoError(interIt.Close())
}

func TestUnion(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table := "db_name", "table_name"
	idxLang, expLang := "idx_lang", makeExpressions(table, "lang")
	idxPath, expPath := "idx_path", makeExpressions(table, "path")

	d := NewDriver(tmpDir)
	sqlIdxLang, err := d.Create(db, table, idxLang, expLang, nil)
	require.NoError(err)

	sqlIdxPath, err := d.Create(db, table, idxPath, expPath, nil)
	require.NoError(err)

	itLang := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdxLang.Expressions(),
		location:    offsetLocation,
	}

	itPath := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdxPath.Expressions(),
		location:    offsetLocation,
	}

	ctx := sql.NewContext(context.Background())

	err = d.Save(ctx, sqlIdxLang, itLang)
	require.NoError(err)

	err = d.Save(ctx, sqlIdxPath, itPath)
	require.NoError(err)

	lookupLang, err := sqlIdxLang.Get(itLang.records[0].values...)
	require.NoError(err)
	litLang, err := lookupLang.Values()
	require.NoError(err)

	loc, err := litLang.Next()
	require.NoError(err)
	require.Equal(itLang.records[0].location, loc)
	_, err = litLang.Next()
	require.True(err == io.EOF)
	err = litLang.Close()
	require.NoError(err)

	lookupPath, err := sqlIdxPath.Get(itPath.records[itPath.total-1].values...)
	require.NoError(err)
	litPath, err := lookupPath.Values()
	require.NoError(err)

	loc, err = litPath.Next()
	require.NoError(err)
	require.Equal(itPath.records[itPath.total-1].location, loc)
	_, err = litPath.Next()
	require.True(err == io.EOF)
	err = litLang.Close()
	require.NoError(err)

	m, ok := lookupLang.(sql.Mergeable)
	require.True(ok)
	require.True(m.IsMergeable(lookupPath))

	unionLookup, ok := lookupLang.(sql.SetOperations)
	unionIt, err := unionLookup.Union(lookupPath).Values()
	require.NoError(err)
	// 0
	loc, err = unionIt.Next()
	require.Equal(itLang.records[0].location, loc)

	// total-1
	loc, err = unionIt.Next()
	require.Equal(itPath.records[itPath.total-1].location, loc)

	_, err = unionIt.Next()
	require.True(err == io.EOF)

	require.NoError(unionIt.Close())
}

func TestDifference(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table := "db_name", "table_name"
	idxLang, expLang := "idx_lang", makeExpressions(table, "lang")
	idxPath, expPath := "idx_path", makeExpressions(table, "path")

	d := NewDriver(tmpDir)
	sqlIdxLang, err := d.Create(db, table, idxLang, expLang, nil)
	require.NoError(err)

	sqlIdxPath, err := d.Create(db, table, idxPath, expPath, nil)
	require.NoError(err)

	itLang := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdxLang.Expressions(),
		location:    offsetLocation,
	}

	itPath := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdxPath.Expressions(),
		location:    offsetLocation,
	}

	ctx := sql.NewContext(context.Background())

	err = d.Save(ctx, sqlIdxLang, itLang)
	require.NoError(err)

	err = d.Save(ctx, sqlIdxPath, itPath)
	require.NoError(err)

	lookupLang, err := sqlIdxLang.Get(itLang.records[0].values...)
	require.NoError(err)

	lookupPath, err := sqlIdxPath.Get(itPath.records[itPath.total-1].values...)
	require.NoError(err)

	m, ok := lookupLang.(sql.Mergeable)
	require.True(ok)
	require.True(m.IsMergeable(lookupPath))

	unionOp, ok := lookupLang.(sql.SetOperations)
	require.True(ok)
	unionLookup, ok := unionOp.Union(lookupPath).(sql.SetOperations)
	require.True(ok)

	diffLookup := unionLookup.Difference(lookupLang)
	diffIt, err := diffLookup.Values()
	require.NoError(err)

	// total-1
	loc, err := diffIt.Next()
	require.NoError(err)
	require.Equal(itPath.records[itPath.total-1].location, loc)

	_, err = diffIt.Next()
	require.True(err == io.EOF)

	require.NoError(diffIt.Close())
}

func TestUnionDiffAsc(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table := "db_name", "table_name"
	idx, exp := "idx_lang", makeExpressions(table, "lang")

	d := NewDriver(tmpDir)
	sqlIdx, err := d.Create(db, table, idx, exp, nil)
	require.NoError(err)
	pilosaIdx, ok := sqlIdx.(*pilosaIndex)
	require.True(ok)
	it := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdx.Expressions(),
		location:    offsetLocation,
	}

	ctx := sql.NewContext(context.Background())

	err = d.Save(ctx, pilosaIdx, it)
	require.NoError(err)

	sqlLookup, err := pilosaIdx.AscendLessThan(it.records[it.total-1].values...)
	require.NoError(err)
	ascLookup, ok := sqlLookup.(*ascendLookup)
	require.True(ok)

	ls := make([]*indexLookup, it.total)
	for i, r := range it.records {
		l, err := pilosaIdx.Get(r.values...)
		require.NoError(err)
		ls[i], _ = l.(*indexLookup)
	}

	unionLookup := ls[0].Union(ls[2], ls[4], ls[6], ls[8])

	diffLookup := ascLookup.Difference(unionLookup)
	diffIt, err := diffLookup.Values()
	require.NoError(err)

	for i := 1; i < it.total-1; i += 2 {
		loc, err := diffIt.Next()
		require.NoError(err)

		require.Equal(it.records[i].location, loc)
	}

	_, err = diffIt.Next()
	require.True(err == io.EOF)
	require.NoError(diffIt.Close())
}

func TestInterRanges(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table := "db_name", "table_name"
	idx, exp := "idx_lang", makeExpressions(table, "lang")

	d := NewDriver(tmpDir)
	sqlIdx, err := d.Create(db, table, idx, exp, nil)
	require.NoError(err)
	pilosaIdx, ok := sqlIdx.(*pilosaIndex)
	require.True(ok)
	it := &testIndexKeyValueIter{
		offset:      0,
		total:       10,
		expressions: sqlIdx.Expressions(),
		location:    offsetLocation,
	}

	ctx := sql.NewContext(context.Background())

	err = d.Save(ctx, pilosaIdx, it)
	require.NoError(err)

	ranges := [2]int{3, 9}
	sqlLookup, err := pilosaIdx.AscendLessThan(it.records[ranges[1]].values...)
	require.NoError(err)
	lessLookup, ok := sqlLookup.(*ascendLookup)
	require.True(ok)

	sqlLookup, err = pilosaIdx.AscendGreaterOrEqual(it.records[ranges[0]].values...)
	require.NoError(err)
	greaterLookup, ok := sqlLookup.(*ascendLookup)
	require.True(ok)

	interLookup := lessLookup.Intersection(greaterLookup)
	require.NotNil(interLookup)
	interIt, err := interLookup.Values()
	require.NoError(err)

	for i := ranges[0]; i < ranges[1]; i++ {
		loc, err := interIt.Next()
		require.NoError(err)
		require.Equal(it.records[i].location, loc)
	}

	_, err = interIt.Next()
	require.True(err == io.EOF)
	require.NoError(interIt.Close())
}

func TestNegateIndex(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	db, table := "db_name", "table_name"

	d := NewDriver(tmpDir)
	idx, err := d.Create(db, table, "index_id", makeExpressions(table, "a"), nil)
	require.NoError(err)

	multiIdx, err := d.Create(
		db, table, "multi_index_id",
		makeExpressions(table, "a", "b"),
		nil,
	)
	require.NoError(err)

	it := &fixtureKeyValueIter{
		fixtures: []kvfixture{
			{"1", []interface{}{int64(2)}},
			{"2", []interface{}{int64(7)}},
			{"3", []interface{}{int64(1)}},
			{"4", []interface{}{int64(1)}},
			{"5", []interface{}{int64(7)}},
		},
	}

	err = d.Save(sql.NewEmptyContext(), idx, it)
	require.NoError(err)

	multiIt := &fixtureKeyValueIter{
		fixtures: []kvfixture{
			{"1", []interface{}{int64(2), int64(6)}},
			{"2", []interface{}{int64(7), int64(5)}},
			{"3", []interface{}{int64(1), int64(2)}},
			{"4", []interface{}{int64(1), int64(3)}},
			{"5", []interface{}{int64(7), int64(6)}},
			{"6", []interface{}{int64(10), int64(6)}},
			{"7", []interface{}{int64(5), int64(1)}},
			{"8", []interface{}{int64(6), int64(2)}},
			{"9", []interface{}{int64(4), int64(0)}},
			{"10", []interface{}{int64(3), int64(5)}},
		},
	}

	err = d.Save(sql.NewEmptyContext(), multiIdx, multiIt)
	require.NoError(err)

	lookup, err := idx.(sql.NegateIndex).Not(int64(1))
	require.NoError(err)

	values, err := lookupValues(lookup)
	require.NoError(err)

	expected := []string{"1", "2", "5"}
	require.Equal(expected, values)

	lookup, err = multiIdx.(sql.NegateIndex).Not(int64(1), int64(6))
	require.NoError(err)

	values, err = lookupValues(lookup)
	require.NoError(err)

	expected = []string{"2", "7", "8", "9", "10"}
	require.Equal(expected, values)
}

func TestPilosaHolder(t *testing.T) {
	require := require.New(t)
	setup(t)
	defer cleanup(t)

	h := pilosa.NewHolder()
	h.Path = tmpDir
	err := h.Open()
	require.NoError(err)

	idx1, err := h.CreateIndexIfNotExists("idx", pilosa.IndexOptions{})
	require.NoError(err)
	err = idx1.Open()
	require.NoError(err)

	f1, err := idx1.CreateFieldIfNotExists("f1", pilosa.OptFieldTypeDefault())
	require.NoError(err)

	_, err = f1.SetBit(0, 0, nil)
	require.NoError(err)
	_, err = f1.SetBit(0, 2, nil)
	require.NoError(err)
	r0, err := f1.Row(0)
	require.NoError(err)

	_, err = f1.SetBit(1, 0, nil)
	require.NoError(err)
	_, err = f1.SetBit(1, 1, nil)
	require.NoError(err)
	r1, err := f1.Row(1)
	require.NoError(err)

	_, err = f1.SetBit(2, 2, nil)
	require.NoError(err)
	_, err = f1.SetBit(2, 3, nil)
	require.NoError(err)
	r2, err := f1.Row(2)
	require.NoError(err)

	row := r0.Intersect(r1).Union(r2)
	cols := row.Columns()
	require.Equal(3, len(cols))
	require.Equal(uint64(0), cols[0])
	require.Equal(uint64(2), cols[1])
	require.Equal(uint64(3), cols[2])

	f2, err := idx1.CreateFieldIfNotExists("f2", pilosa.OptFieldTypeDefault())
	require.NoError(err)

	rowIDs := []uint64{0, 0, 1, 1}
	colIDs := []uint64{1, 2, 0, 3}
	err = f2.Import(rowIDs, colIDs, nil)
	require.NoError(err)

	r0, err = f2.Row(0)
	require.NoError(err)

	r1, err = f2.Row(1)
	require.NoError(err)

	row = r0.Union(r1)
	cols = row.Columns()
	require.Equal(4, len(cols))
	require.Equal(uint64(0), cols[0])
	require.Equal(uint64(1), cols[1])
	require.Equal(uint64(2), cols[2])
	require.Equal(uint64(3), cols[3])

	r1, err = f1.Row(1)
	require.NoError(err)
	r0, err = f2.Row(0)
	require.NoError(err)

	row = r1.Intersect(r0)
	cols = row.Columns()
	require.Equal(1, len(cols))
	require.Equal(uint64(1), cols[0])

	err = idx1.Close()
	require.NoError(err)
	// -------------------------------------------------------------------------

	idx2, err := h.CreateIndexIfNotExists("idx", pilosa.IndexOptions{})
	require.NoError(err)
	err = idx2.Open()
	require.NoError(err)

	f1 = idx2.Field("f1")

	r2, err = f1.Row(2)
	require.NoError(err)

	f2 = idx2.Field("f2")

	r0, err = f2.Row(0)
	require.NoError(err)

	r1, err = f2.Row(1)
	require.NoError(err)

	row = r0.Union(r1)
	cols = row.Columns()
	require.Equal(4, len(cols))
	require.Equal(uint64(0), cols[0])
	require.Equal(uint64(1), cols[1])
	require.Equal(uint64(2), cols[2])
	require.Equal(uint64(3), cols[3])

	err = idx2.Close()
	require.NoError(err)

	err = h.Close()
	require.NoError(err)
}

func makeExpressions(table string, names ...string) []sql.Expression {
	var expressions []sql.Expression

	for i, n := range names {
		expressions = append(expressions,
			expression.NewGetFieldWithTable(i, sql.Int64, table, n, true))
	}

	return expressions
}

func randLocation(offset int) []byte {
	b := make([]byte, 1)
	rand.Read(b)
	return b
}

func offsetLocation(offset int) []byte {
	b := make([]byte, 1)
	b[0] = byte(offset % 10)
	return b
}

// test implementation of sql.IndexKeyValueIter interface
type testIndexKeyValueIter struct {
	offset      int
	total       int
	expressions []string
	location    func(int) []byte

	records []struct {
		values   []interface{}
		location []byte
	}
}

func (it *testIndexKeyValueIter) Next() ([]interface{}, []byte, error) {
	if it.offset >= it.total {
		return nil, nil, io.EOF
	}

	b := it.location(it.offset)

	values := make([]interface{}, len(it.expressions))
	for i, e := range it.expressions {
		values[i] = e + "-" + hex.EncodeToString(b)
	}

	it.records = append(it.records, struct {
		values   []interface{}
		location []byte
	}{
		values,
		b,
	})
	it.offset++

	return values, b, nil
}

func (it *testIndexKeyValueIter) Close() error {
	it.offset = 0
	it.records = nil
	return nil
}

func setupAscendDescend(t *testing.T) (*pilosaIndex, func()) {
	t.Helper()
	require := require.New(t)
	setup(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions(table, "a", "b")

	d := NewDriver(tmpDir)
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.NoError(err)

	it := &fixtureKeyValueIter{
		fixtures: []kvfixture{
			{"9", []interface{}{int64(2), int64(6)}},
			{"3", []interface{}{int64(7), int64(5)}},
			{"1", []interface{}{int64(1), int64(2)}},
			{"7", []interface{}{int64(1), int64(3)}},
			{"4", []interface{}{int64(7), int64(6)}},
			{"2", []interface{}{int64(10), int64(6)}},
			{"5", []interface{}{int64(5), int64(1)}},
			{"6", []interface{}{int64(6), int64(2)}},
			{"10", []interface{}{int64(4), int64(0)}},
			{"8", []interface{}{int64(3), int64(5)}},
		},
	}

	err = d.Save(sql.NewEmptyContext(), sqlIdx, it)
	require.NoError(err)

	return sqlIdx.(*pilosaIndex), func() {
		cleanup(t)
	}
}

func lookupValues(lookup sql.IndexLookup) ([]string, error) {
	iter, err := lookup.Values()
	if err != nil {
		return nil, err
	}

	var result []string
	for {
		k, err := iter.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		result = append(result, string(k))
	}

	return result, nil
}

type kvfixture struct {
	key    string
	values []interface{}
}

type fixtureKeyValueIter struct {
	fixtures []kvfixture
	pos      int
}

func (i *fixtureKeyValueIter) Next() ([]interface{}, []byte, error) {
	if i.pos >= len(i.fixtures) {
		return nil, nil, io.EOF
	}

	f := i.fixtures[i.pos]
	i.pos++
	return f.values, []byte(f.key), nil
}

func (i *fixtureKeyValueIter) Close() error { return nil }

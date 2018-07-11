package pilosa

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	pilosa "github.com/pilosa/go-pilosa"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/index"
	"gopkg.in/src-d/go-mysql-server.v0/test"
)

// Pilosa tests require running docker. If `docker ps` command returned an error
// we skip some of the tests
var (
	dockerIsRunning bool
	dockerCmdOutput string
)

func init() {
	cmd := exec.Command("docker", "ps")
	b, err := cmd.CombinedOutput()

	dockerCmdOutput, dockerIsRunning = string(b), (err == nil)
}

func TestID(t *testing.T) {
	d := &Driver{}

	require := require.New(t)
	require.Equal(DriverID, d.ID())
}

func TestLoadAll(t *testing.T) {
	require := require.New(t)

	path, err := ioutil.TempDir(os.TempDir(), "indexes")
	require.NoError(err)
	defer os.RemoveAll(path)

	d := NewIndexDriver(path)
	idx1, err := d.Create("db", "table", "id1", makeExpressions("hash1"), nil)
	require.NoError(err)

	idx2, err := d.Create("db", "table", "id2", makeExpressions("hash1"), nil)
	require.NoError(err)

	indexes, err := d.LoadAll("db", "table")
	require.NoError(err)

	for _, idx := range indexes {
		if idx.ID() == "id1" {
			assertEqualIndexes(t, idx1, idx)
		} else {
			assertEqualIndexes(t, idx2, idx)
		}
	}
}

func assertEqualIndexes(t *testing.T, a, b sql.Index) {
	t.Helper()
	require.Equal(t, withoutMapping(a), withoutMapping(b))
}

func withoutMapping(a sql.Index) sql.Index {
	if i, ok := a.(*pilosaIndex); ok {
		b := *i
		b.mapping = nil
		return &b
	}
	return a
}

type logLoc struct {
	loc []byte
	err error
}

func TestSaveAndLoad(t *testing.T) {
	if !dockerIsRunning {
		t.Skipf("Skip TestSaveAndLoad: %s", dockerCmdOutput)
	}
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions("lang", "hash")
	path, err := ioutil.TempDir(os.TempDir(), "indexes")
	require.NoError(err)
	defer os.RemoveAll(path)

	d := NewDriver(path, newClientWithTimeout(200*time.Millisecond))
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.NoError(err)

	it := &testIndexKeyValueIter{
		offset:      0,
		total:       64,
		expressions: expressions,
		location:    randLocation,
	}

	tracer := new(test.MemTracer)
	ctx := sql.NewContext(context.Background(), sql.WithTracer(tracer))
	err = d.Save(ctx, sqlIdx, it)
	require.NoError(err)

	indexes, err := d.LoadAll(db, table)
	require.NoError(err)
	require.Equal(1, len(indexes))
	assertEqualIndexes(t, sqlIdx, indexes[0])

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
	if !dockerIsRunning {
		t.Skipf("Skip TestSaveAndGetAll: %s", dockerCmdOutput)
	}
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions("lang", "hash")
	path, err := ioutil.TempDir(os.TempDir(), "indexes")
	require.NoError(err)
	defer os.RemoveAll(path)

	d := NewDriver(path, newClientWithTimeout(200*time.Millisecond))
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.NoError(err)

	it := &testIndexKeyValueIter{
		offset:      0,
		total:       64,
		expressions: expressions,
		location:    randLocation,
	}

	err = d.Save(sql.NewEmptyContext(), sqlIdx, it)
	require.NoError(err)

	indexes, err := d.LoadAll(db, table)
	require.NoError(err)
	require.Equal(1, len(indexes))
	assertEqualIndexes(t, sqlIdx, indexes[0])

	_, err = sqlIdx.Get()
	require.Error(err)
	require.True(errInvalidKeys.Is(err))
}

func TestLoadCorruptedIndex(t *testing.T) {
	require := require.New(t)
	path, err := ioutil.TempDir(os.TempDir(), "indexes")
	require.NoError(err)
	defer os.RemoveAll(path)

	require.NoError(index.CreateProcessingFile(path))

	_, err = new(Driver).loadIndex(path)
	require.Error(err)
	require.True(errCorruptedIndex.Is(err))

	_, err = os.Stat(path)
	require.Error(err)
	require.True(os.IsNotExist(err))
}

func TestDelete(t *testing.T) {
	if !dockerIsRunning {
		t.Skipf("Skip TestDelete: %s", dockerCmdOutput)
	}
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	path, err := ioutil.TempDir(os.TempDir(), "indexes")
	require.NoError(err)
	defer os.RemoveAll(path)

	h1 := sha1.Sum([]byte("lang"))
	exh1 := sql.ExpressionHash(h1[:])

	h2 := sha1.Sum([]byte("hash"))
	exh2 := sql.ExpressionHash(h2[:])

	expressions := []sql.ExpressionHash{exh1, exh2}

	d := NewIndexDriver(path)
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.NoError(err)

	err = d.Delete(sqlIdx)
	require.NoError(err)
}

func TestLoadAllDirectoryDoesNotExist(t *testing.T) {
	require := require.New(t)
	tmpDir, err := ioutil.TempDir(os.TempDir(), "pilosa-")
	require.NoError(err)

	defer func() {
		require.NoError(os.RemoveAll(tmpDir))
	}()

	driver := &Driver{root: tmpDir}
	drivers, err := driver.LoadAll("foo", "bar")
	require.NoError(err)
	require.Len(drivers, 0)
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

func setupAscendDescend(t *testing.T) (*pilosaIndex, func()) {
	t.Helper()
	if !dockerIsRunning {
		t.Skipf("Skip test: %s", dockerCmdOutput)
	}
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions("a", "b")
	path, err := mkdir(os.TempDir(), "indexes")
	require.NoError(err)

	d := NewDriver(path, newClientWithTimeout(200*time.Millisecond))
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
		require.NoError(os.RemoveAll(path))
	}
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

// test implementation of sql.IndexKeyValueIter interface
type testIndexKeyValueIter struct {
	offset      int
	total       int
	expressions []sql.ExpressionHash
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
		values[i] = hex.EncodeToString(e) + "-" + hex.EncodeToString(b)
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

func makeExpressions(names ...string) []sql.ExpressionHash {
	var expressions []sql.ExpressionHash

	for _, n := range names {
		h := sha1.Sum([]byte(n))
		exh := sql.ExpressionHash(h[:])
		expressions = append(expressions, sql.ExpressionHash(exh))
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

func newClientWithTimeout(timeout time.Duration) *pilosa.Client {
	cli, err := pilosa.NewClient(pilosa.DefaultURI(),
		pilosa.OptClientConnectTimeout(timeout),
		pilosa.OptClientSocketTimeout(timeout))
	if err != nil {
		panic(err)
	}

	return cli
}

func retry(ctx context.Context, fn func() error) error {
	var (
		backoffDuration = 200 * time.Millisecond
		maxRetries      = 10

		err error
	)

	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(backoffDuration):

		}
	}

	return err
}

// pilosaHiccup restarts pilosa every interval
// it requires the Pilosa running in docker container:
// docker run --name pilosa -d -p 127.0.0.1:10101:10101 pilosa/pilosa:v0.9.0
func pilosaHiccup(ctx context.Context, interval time.Duration) error {
	cmd := exec.Command("docker", "restart", "pilosa")
	err := cmd.Start()
	for err == nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()

		case <-time.After(interval):
			err = cmd.Start()
		}
	}

	return err
}

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

	path, err := mkdir(os.TempDir(), "indexes")
	require.Nil(err)
	defer os.RemoveAll(path)

	d := NewIndexDriver(path)
	idx1, err := d.Create("db", "table", "id1", makeExpressions("hash1"), nil)
	require.Nil(err)

	idx2, err := d.Create("db", "table", "id2", makeExpressions("hash1"), nil)
	require.Nil(err)

	indexes, err := d.LoadAll("db", "table")
	require.Nil(err)

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

func TestSaveAndLoad(t *testing.T) {
	if !dockerIsRunning {
		t.Skipf("Skip TestSaveAndLoad: %s", dockerCmdOutput)
	}
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions("lang", "hash")
	path, err := mkdir(os.TempDir(), "indexes")
	require.Nil(err)
	defer os.RemoveAll(path)

	d := NewDriver(path, newClientWithTimeout(200*time.Millisecond))
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.Nil(err)

	it := &testIndexKeyValueIter{
		offset:      0,
		total:       64,
		expressions: expressions,
		location:    randLocation,
	}

	err = d.Save(context.Background(), sqlIdx, it)
	require.Nil(err)

	indexes, err := d.LoadAll(db, table)
	require.Nil(err)
	require.Equal(1, len(indexes))
	assertEqualIndexes(t, sqlIdx, indexes[0])

	for _, r := range it.records {
		lookup, err := sqlIdx.Get(r.values...)
		require.NoError(err)

		found, foundLoc := false, []string{}
		lit, err := lookup.Values()
		require.NoError(err)

		for i := 0; ; i++ {
			loc, err := lit.Next()
			t.Logf("[%d] values: %v location: %x loc: %x err: %v\n", i, r.values, r.location, loc, err)

			if err == io.EOF {
				require.Truef(i > 0, "No data for r.values: %v\tr.location: %x", r.values, r.location)
				break
			}

			require.Nil(err)
			found = found || reflect.DeepEqual(r.location, loc)
			foundLoc = append(foundLoc, hex.EncodeToString(loc))
		}
		require.Truef(found, "Expected: %s\nGot: %v\n", hex.EncodeToString(r.location), foundLoc)

		err = lit.Close()
		require.Nil(err)
	}
}

func TestSaveAndGetAll(t *testing.T) {
	if !dockerIsRunning {
		t.Skipf("Skip TestSaveAndGetAll: %s", dockerCmdOutput)
	}
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions("lang", "hash")
	path, err := mkdir(os.TempDir(), "indexes")
	require.Nil(err)
	defer os.RemoveAll(path)

	d := NewDriver(path, newClientWithTimeout(200*time.Millisecond))
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.Nil(err)

	it := &testIndexKeyValueIter{
		offset:      0,
		total:       64,
		expressions: expressions,
		location:    randLocation,
	}

	err = d.Save(context.Background(), sqlIdx, it)
	require.Nil(err)

	indexes, err := d.LoadAll(db, table)
	require.Nil(err)
	require.Equal(1, len(indexes))
	assertEqualIndexes(t, sqlIdx, indexes[0])

	_, err = sqlIdx.Get()
	require.Error(err)
	require.True(errInvalidKeys.Is(err))
}

func TestPilosaHiccup(t *testing.T) {
	if !dockerIsRunning {
		t.Skipf("Skip TestPilosaHiccup: %s", dockerCmdOutput)
	}
	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, table, id := "db_name", "table_name", "index_id"
	expressions := makeExpressions("lang", "hash")
	path, err := mkdir(os.TempDir(), "indexes")
	require.Nil(err)
	defer os.RemoveAll(path)

	d := NewDriver(path, newClientWithTimeout(time.Second))
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.Nil(err)

	it := &testIndexKeyValueIter{
		offset:      0,
		total:       64,
		expressions: expressions,
		location:    offsetLocation,
	}

	// restart pilosa container every second
	go pilosaHiccup(ctx, time.Second)

	// retry save index - if pilosa failed, reset iterator and start over
	err = retry(ctx, func() error {
		if e := d.Save(ctx, sqlIdx, it); e != nil {
			t.Logf("Save err: %s", e)
			// reset iterator!
			it.Close()
			return e
		}
		return nil
	})
	require.Nil(err)

	// load indexes - it doesn't require pilosa, yet.
	indexes, err := d.LoadAll(db, table)
	require.Nil(err)
	require.Equal(1, len(indexes))
	assertEqualIndexes(t, sqlIdx, indexes[0])

	for i, r := range it.records {
		var lookup sql.IndexLookup
		// retry to get the next location - pilosa should recover
		err = retry(ctx, func() error {
			lookup, err = sqlIdx.Get(r.values...)
			if err != nil {
				t.Logf("Get err: %s", err)
			}
			return err
		})
		require.NoError(err)

		lit, err := lookup.Values()
		require.NoError(err)

		loc, err := lit.Next()
		t.Logf("[%d] values: %v location: %x loc: %x err: %v\n", i, r.values, r.location, loc, err)
		if err == io.EOF {
			break
		}

		require.Nil(err)
		require.True(reflect.DeepEqual(r.location, loc), "Expected: %s\nGot: %v\n", hex.EncodeToString(r.location), hex.EncodeToString(loc))

		err = lit.Close()
		require.Nil(err)
	}
}

func TestDelete(t *testing.T) {
	if !dockerIsRunning {
		t.Skipf("Skip TestDelete: %s", dockerCmdOutput)
	}
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	path, err := mkdir(os.TempDir(), "indexes")
	require.Nil(err)
	defer os.RemoveAll(path)

	h1 := sha1.Sum([]byte("lang"))
	exh1 := sql.ExpressionHash(h1[:])

	h2 := sha1.Sum([]byte("hash"))
	exh2 := sql.ExpressionHash(h2[:])

	expressions := []sql.ExpressionHash{exh1, exh2}

	d := NewIndexDriver(path)
	sqlIdx, err := d.Create(db, table, id, expressions, nil)
	require.Nil(err)

	err = d.Delete(sqlIdx)
	require.Nil(err)
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
		maxRetries      = 5

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

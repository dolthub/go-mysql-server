package index

import (
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

func TestConfig(t *testing.T) {
	require := require.New(t)

	driver := "driver"
	db, table, id := "db_name", "table_name", "index_id"
	dir := filepath.Join(os.TempDir(), driver)
	subdir := filepath.Join(dir, db, table)
	err := os.MkdirAll(subdir, 0750)
	require.NoError(err)
	file := filepath.Join(subdir, id+".cfg")

	defer os.RemoveAll(dir)

	h1 := sha1.Sum([]byte("h1"))
	h2 := sha1.Sum([]byte("h2"))
	exh1 := sql.ExpressionHash(h1[:])
	exh2 := sql.ExpressionHash(h2[:])

	cfg1 := NewConfig(
		db,
		table,
		id,
		[]sql.ExpressionHash{exh1, exh2},
		"pilosa",
		map[string]string{
			"port": "10101",
			"host": "localhost",
		},
	)

	err = WriteConfigFile(file, cfg1)
	require.NoError(err)

	cfg2, err := ReadConfigFile(file)
	require.NoError(err)
	require.Equal(cfg1, cfg2)
}

func TestLockFile(t *testing.T) {
	require := require.New(t)

	dir := os.TempDir()
	file := filepath.Join(dir, ".processing")
	defer require.NoError(os.RemoveAll(file))

	ok, err := ExistsProcessingFile(file)
	require.NoError(err)
	require.False(ok)

	require.NoError(CreateProcessingFile(file))

	ok, err = ExistsProcessingFile(file)
	require.NoError(err)
	require.True(ok)

	require.NoError(RemoveProcessingFile(file))

	ok, err = ExistsProcessingFile(file)
	require.NoError(err)
	require.False(ok)
}

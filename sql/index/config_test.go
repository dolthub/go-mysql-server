package index

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	require := require.New(t)

	db, table, id := "db_name", "table_name", "index_id"
	path := filepath.Join(os.TempDir(), db, table, id)
	err := os.MkdirAll(path, 0750)

	require.NoError(err)
	defer os.RemoveAll(path)

	cfg1 := NewConfig(
		db,
		table,
		id,
		[]string{"h1", "h2"},
		"DriverID",
		map[string]string{
			"port": "10101",
			"host": "localhost",
		},
	)

	err = WriteConfigFile(path, cfg1)
	require.NoError(err)

	cfg2, err := ReadConfigFile(path)
	require.NoError(err)
	require.Equal(cfg1, cfg2)
}

func TestProcessingFile(t *testing.T) {
	require := require.New(t)

	dir, err := ioutil.TempDir(os.TempDir(), "processing-file")
	require.NoError(err)
	defer func() {
		require.NoError(os.RemoveAll(dir))
	}()

	ok, err := ExistsProcessingFile(dir)
	require.NoError(err)
	require.False(ok)

	require.NoError(CreateProcessingFile(dir))

	ok, err = ExistsProcessingFile(dir)
	require.NoError(err)
	require.True(ok)

	require.NoError(RemoveProcessingFile(dir))

	ok, err = ExistsProcessingFile(dir)
	require.NoError(err)
	require.False(ok)
}

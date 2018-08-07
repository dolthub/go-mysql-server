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
	tmpDir, err := ioutil.TempDir("", "index")
	require.NoError(err)
	defer func() { require.NoError(os.RemoveAll(tmpDir)) }()

	driver := "driver"
	db, table, id := "db_name", "table_name", "index_id"
	dir := filepath.Join(tmpDir, driver)
	subdir := filepath.Join(dir, db, table)
	err = os.MkdirAll(subdir, 0750)
	require.NoError(err)
	file := filepath.Join(subdir, id+".cfg")

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

	err = WriteConfigFile(file, cfg1)
	require.NoError(err)

	cfg2, err := ReadConfigFile(file)
	require.NoError(err)
	require.Equal(cfg1, cfg2)
}

func TestProcessingFile(t *testing.T) {
	require := require.New(t)
	tmpDir, err := ioutil.TempDir("", "index")
	require.NoError(err)
	defer func() { require.NoError(os.RemoveAll(tmpDir)) }()

	file := filepath.Join(tmpDir, ".processing")

	ok, err := ExistsProcessingFile(file)
	require.NoError(err)
	require.False(ok)

	require.NoError(CreateProcessingFile(file))

	ok, err = ExistsProcessingFile(file)
	require.NoError(err)
	require.True(ok)

	require.NoError(WriteProcessingFile(file, []byte("test")))

	ok, err = ExistsProcessingFile(file)
	require.NoError(err)
	require.True(ok)

	require.NoError(RemoveProcessingFile(file))

	ok, err = ExistsProcessingFile(file)
	require.NoError(err)
	require.False(ok)
}
